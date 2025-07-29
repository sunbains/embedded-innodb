/****************************************************************************
Copyright (c) 1995, 2010, Innobase Oy. All Rights Reserved.
Copyright (c) 2008, 2009 Google Inc.
Copyright (c) 2009, Percona Inc.
Copyright (c) 2024 Sunny Bains. All rights reserved.

Portions of this file contain modifications contributed and copyrighted by
Google, Inc. Those modifications are gratefully acknowledged and are described
briefly in the InnoDB documentation. The contributions by Google are
incorporated with their permission, and subject to the conditions contained in
the file COPYING.Google.

Portions of this file contain modifications contributed and copyrighted
by Percona Inc.. Those modifications are
gratefully acknowledged and are described briefly in the InnoDB
documentation. The contributions by Percona Inc. are incorporated with
their permission, and subject to the conditions contained in the file
COPYING.Percona.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc., 59 Temple
Place, Suite 330, Boston, MA 02111-1307 USA

*****************************************************************************/

/** @file srv/srv0srv.c
The database server main program

Created 10/8/1995 Heikki Tuuri
*******************************************************/

#include "innodb0types.h"

#include "srv0srv.h"

#include "api0ucode.h"
#include "btr0cur.h"

#include "buf0flu.h"
#include "buf0lru.h"
#include "ddl0ddl.h"
#include "dict0load.h"
#include "dict0store.h"
#include "lock0lock.h"
#include "log0recv.h"
#include "mem0mem.h"
#include "os0proc.h"
#include "os0sync.h"
#include "pars0pars.h"
#include "que0que.h"
#include "srv0srv.h"
#include "sync0sync.h"
#include "trx0purge.h"
#include "usr0sess.h"
#include "ut0mem.h"
#include "ut0ut.h"

/* FIXME: When we setup the session variables infrastructure. */
#define sess_lock_wait_timeout(t) (ses_lock_wait_timeout)

ulint ses_lock_wait_timeout = 1024 * 1024 * 1024;

bool srv_lower_case_table_names = false;

/** The following counter is incremented whenever there is some user activity
in the server */
ulint srv_activity_count = 0;

/** The following is the maximum allowed duration of a lock wait. */
ulint srv_fatal_semaphore_wait_threshold = 600;

/** How much data manipulation language (DML) statements need to be delayed,
in microseconds, in order to reduce the lagging of the purge thread. */
ulint srv_dml_needed_delay = 0;

bool srv_lock_timeout_active = false;
bool srv_monitor_active = false;
bool srv_error_monitor_active = false;

const char *srv_main_thread_op_info = "";

/* Server parameters which are read from the initfile */

char *srv_log_group_home_dir = nullptr;

#if DICT_TF_FORMAT_51
#error "DICT_TF_FORMAT_51 must be 0!"
#endif

/** Size in database pages */
ulint srv_log_buffer_size = ULINT_MAX;

/** Maximum number of times allowed to conditionally acquire
mutex before switching to blocking wait on the mutex */
constexpr ulint MAX_MUTEX_NOWAIT = 20;

/** Check whether the number of failed nonblocking mutex
acquisition attempts exceeds maximum allowed value. If so,
srv_printf_innodb_monitor() will request mutex acquisition
with mutex_enter(), which will wait until it gets the mutex. */
#define MUTEX_NOWAIT(mutex_skipped) ((mutex_skipped) < MAX_MUTEX_NOWAIT)

/** Variable counts amount of data read in total (in bytes) */
ulint srv_data_read = 0;

/** Here we count the amount of data written in total (in bytes) */
ulint srv_data_written = 0;

/** The number of the log write requests done */
ulint srv_log_write_requests = 0;

/** The number of physical writes to the log performed */
ulint srv_log_writes = 0;

/** Amount of data written to the log files in bytes */
ulint srv_os_log_written = 0;

/** Amount of writes being done to the log files */
ulint srv_os_log_pending_writes = 0;

/** We increase this counter, when there we don't have enough space in the
log buffer and have to flush it */
ulint srv_log_waits = 0;

/** This variable counts the amount of times, when the doublewrite buffer
was flushed */
ulint srv_dblwr_writes = 0;

/** Here we store the number of pages that have been flushed to the
doublewrite buffer */
ulint srv_dblwr_pages_written = 0;

/** Here we store the number of times when we had to wait for a free page
in the buffer pool. It happens when the buffer pool is full and we need
to make a flush, in order to be able to read or create a page. */
ulint srv_buf_pool_wait_free = 0;

/** Variable to count the number of pages that were written from buffer
pool to the disk */
ulint srv_buf_pool_flushed = 0;

/** Number of buffer pool reads that led to the
reading of a disk page */
ulint srv_buf_pool_reads = 0;

/** Structure to pass status variables to the client */
export_struc export_vars;

/** This mutex protects srv_conc data structures */
static std::mutex srv_conc_mutex;

struct srv_conc_slot_t {
  /** event to wait */
  Cond_var *m_event{};

  /** true if slot reserved */
  bool m_reserved{};

  /** true when another thread has already set the event
  and the thread in this slot is free to proceed; but
  reserved may still be true at that point */
  bool m_wait_ended{};

  /** queue node */
  UT_LIST_NODE_T(srv_conc_slot_t) m_srv_conc_queue;
};

/** Queue of threads waiting to get in */
static UT_LIST_BASE_NODE_T(srv_conc_slot_t, m_srv_conc_queue) srv_conc_queue;

/** Array of wait slots */
static srv_conc_slot_t *srv_conc_slots;

/** The system log file names */
static char **srv_log_group_home_dirs = nullptr;

/*-------------------------------------------*/
ulong srv_n_spin_wait_rounds = 30;
ulong srv_spin_wait_delay = 6;

#ifdef UNIV_DEBUG
bool srv_print_thread_releases = false;
bool srv_print_lock_waits = false;
bool srv_print_buf_io = false;
bool srv_print_log_io = false;
bool srv_print_latch_waits = false;
#endif /* UNIV_DEBUG */

ulint srv_n_rows_inserted = 0;
ulint srv_n_rows_updated = 0;
ulint srv_n_rows_deleted = 0;
ulint srv_n_rows_read = 0;

static ulint srv_n_rows_inserted_old = 0;
static ulint srv_n_rows_updated_old = 0;
static ulint srv_n_rows_deleted_old = 0;
static ulint srv_n_rows_read_old = 0;

ulint srv_n_lock_wait_count = 0;
ulint srv_n_lock_wait_current_count = 0;
int64_t srv_n_lock_wait_time = 0;
ulint srv_n_lock_max_wait_time = 0;

/** Set the following to 0 if you want InnoDB to write messages on
ib_stream on startup/shutdown */
bool srv_print_verbose_log = true;
bool srv_print_innodb_monitor = false;
bool srv_print_innodb_tablespace_monitor = false;
bool srv_print_innodb_table_monitor = false;

static time_t srv_last_monitor_time;

static mutex_t srv_innodb_monitor_mutex;

/** Mutex for locking srv_monitor_file */
mutex_t srv_monitor_file_mutex;

#ifdef UNIV_LINUX
static ulint srv_main_thread_process_no = 0;
#endif /* UNIV_LINUX */

static ulint srv_main_thread_id = 0;

/* The following count work done by srv_master_thread. */

/** Iterations by the 'once per second' loop. */
static ulint srv_main_1_second_loops = 0;

/** Calls to sleep by the 'once per second' loop. */
static ulint srv_main_sleeps = 0;

/** Iterations by the 'once per 10 seconds' loop. */
static ulint srv_main_10_second_loops = 0;

/** Iterations of the loop bounded by the 'background_loop' label. */
static ulint srv_main_background_loops = 0;

/** Iterations of the loop bounded by the 'flush_loop' label. */
static ulint srv_main_flush_loops = 0;

/** Log writes involving flush. */
static ulint srv_log_writes_and_flush = 0;

/** This is only ever touched by the master thread. It records the
time when the last flush of log file has happened. The master
thread ensures that we flush the log files at least once per
second. */
static time_t srv_last_log_flush_time;

/** The master thread performs various tasks based on the current
state of IO activity and the level of IO utilization is past
intervals. Following macros define thresholds for these conditions. */
#define SRV_PEND_IO_THRESHOLD (PCT_IO(3))
#define SRV_RECENT_IO_ACTIVITY (PCT_IO(5))
#define SRV_PAST_IO_ACTIVITY (PCT_IO(200))

Config srv_config{};

/*
        IMPLEMENTATION OF THE SERVER MAIN PROGRAM
        =========================================

There is the following analogue between this database
server and an operating system kernel:

DB concept			equivalent OS concept
----------			---------------------
transaction		--	process;

query thread		--	thread;

lock			--	semaphore;

transaction set to
the rollback state	--	kill signal delivered to a process;

kernel			--	kernel;

query thread execution:
(a) without kernel mutex
reserved		--	process executing in user mode;
(b) with kernel mutex reserved
                        --	process executing in kernel mode;

The server is controlled by a master thread which runs at
a priority higher than normal, that is, higher than user threads.
It sleeps most of the time, and wakes up, say, every 300 milliseconds,
to check whether there is anything happening in the server which
requires intervention of the master thread. Such situations may be,
for example, when flushing of dirty blocks is needed in the buffer
pool or old version of database rows have to be cleaned away.

The threads which we call user threads serve the queries of
the clients and input from the console of the server.
They run at normal priority. The server may have several
communications endpoints. A dedicated set of user threads waits
at each of these endpoints ready to receive a client request.
Each request is taken by a single user thread, which then starts
processing and, when the result is ready, sends it to the client
and returns to wait at the same endpoint the thread started from.

So, we do not have dedicated communication threads listening at
the endpoints and dealing the jobs to dedicated worker threads.
Our architecture saves one thread swithch per request, compared
to the solution with dedicated communication threads
which amounts to 15 microseconds on 100 MHz Pentium
running NT. If the client
is communicating over a network, this saving is negligible, but
if the client resides in the same machine, maybe in an SMP machine
on a different processor from the server thread, the saving
can be important as the threads can communicate over shared
memory with an overhead of a few microseconds.

We may later implement a dedicated communication thread solution
for those endpoints which communicate over a network.

Our solution with user threads has two problems: for each endpoint
there has to be a number of listening threads. If there are many
communication endpoints, it may be difficult to set the right number
of concurrent threads in the system, as many of the threads
may always be waiting at less busy endpoints. Another problem
is queuing of the messages, as the server internally does not
offer any queue for jobs.

Another group of user threads is intended for splitting the
queries and processing them in parallel. Let us call these
parallel communication threads. These threads are waiting for
parallelized tasks, suspended on event semaphores.

A single user thread waits for input from the console,
like a command to shut the database.

Utility threads are a different group of threads which takes
care of the buffer pool flushing and other, mainly background
operations, in the server.
Some of these utility threads always run at a lower than normal
priority, so that they are always in background. Some of them
may dynamically boost their priority by the pri_adjust function,
even to higher than normal priority, if their task becomes urgent.
The running of utilities is controlled by high- and low-water marks
of urgency. The urgency may be measured by the number of dirty blocks
in the buffer pool, in the case of the flush thread, for example.
When the high-water mark is exceeded, an utility starts running, until
the urgency drops under the low-water mark. Then the utility thread
suspend itself to wait for an event. The master thread is
responsible of signaling this event when the utility thread is
again needed.

For each individual type of utility, some threads always remain
at lower than normal priority. This is because pri_adjust is implemented
so that the threads at normal or higher priority control their
share of running time by calling sleep. Thus, if the load of the
system sudenly drops, these threads cannot necessarily utilize
the system fully. The background priority threads make up for this,
starting to run when the load drops.

When there is no activity in the system, also the master thread
suspends itself to wait for an event making
the server totally silent. The responsibility to signal this
event is on the user thread which again receives a message
from a client.

There is still one complication in our server design. If a
background utility thread obtains a resource (e.g., mutex) needed by a user
thread, and there is also some other user activity in the system,
the user thread may have to wait indefinitely long for the
resource, as the OS does not schedule a background thread if
there is some other runnable user thread. This problem is called
priority inversion in real-time programming.

One solution to the priority inversion problem would be to
keep record of which thread owns which resource and
in the above case boost the priority of the background thread
so that it will be scheduled and it can release the resource.
This solution is called priority inheritance in real-time programming.
A drawback of this solution is that the overhead of acquiring a mutex
increases slightly, maybe 0.2 microseconds on a 100 MHz Pentium, because
the thread has to call os_thread_get_curr_id.
This may be compared to 0.5 microsecond overhead for a mutex lock-unlock
pair. Note that the thread
cannot store the information in the resource, say mutex, itself,
because competing threads could wipe out the information if it is
stored before acquiring the mutex, and if it stored afterwards,
the information is outdated for the time of one machine instruction,
at least. (To be precise, the information could be stored to
lock_word in mutex if the machine supports atomic swap.)

The above solution with priority inheritance may become actual in the
future, but at the moment we plan to implement a more coarse solution,
which could be called a global priority inheritance. If a thread
has to wait for a long time, say 300 milliseconds, for a resource,
we just guess that it may be waiting for a resource owned by a background
thread, and boost the priority of all runnable background threads
to the normal level. The background threads then themselves adjust
their fixed priority back to background after releasing all resources
they had (or, at some fixed points in their program code).

What is the performance of the global priority inheritance solution?
We may weigh the length of the wait time 300 milliseconds, during
which the system processes some other thread
to the cost of boosting the priority of each runnable background
thread, rescheduling it, and lowering the priority again.
On 100 MHz Pentium + NT this overhead may be of the order 100
microseconds per thread. So, if the number of runnable background
threads is not very big, say < 100, the cost is tolerable.
Utility threads probably will access resources used by
user threads not very often, so collisions of user threads
to preempted utility threads should not happen very often.

The thread table contains
information of the current status of each thread existing in the system,
and also the event semaphores used in suspending the master thread
and utility and parallel communication threads when they have nothing to do.
The thread table can be seen as an analogue to the process table
in a traditional Unix implementation.

The thread table is also used in the global priority inheritance
scheme. This brings in one additional complication: threads accessing
the thread table must have at least normal fixed priority,
because the priority inheritance solution does not work if a background
thread is preempted while possessing the mutex protecting the thread table.
So, if a thread accesses the thread table, its priority has to be
boosted at least to normal. This priority requirement can be seen similar to
the privileged mode used when processing the kernel calls in traditional
Unix.*/

/** Thread slot in the thread table */
struct srv_slot_t {
  /** thread id */
  os_thread_id_t m_id;

  /** thread handle */
  os_thread_t m_handle;

  /** thread type: user, utility etc. */
  srv_thread_type m_type;

  /** true if this slot is in use */
  bool m_in_use;

  /** true if the thread is waiting for the event of this slot */
  bool m_suspended;

  /** time when the thread was suspended */
  ib_time_t m_suspend_time;

  /** event used in suspending the thread when it has nothing to do */
  Cond_var *m_event;

  /*!< suspended query thread (only used for client threads) */
  que_thr_t *m_thr;
};

/** The server system struct */
struct srv_sys_t {
  /** Server thread table */
  srv_slot_t *m_threads{};

  /** Task queue */
  UT_LIST_BASE_NODE_T(que_thr_t, queue) m_tasks{};
};

/** Table for client threads where they will be suspended to wait for locks */
static srv_slot_t *srv_client_table = nullptr;

Cond_var *srv_lock_timeout_thread_event;

static srv_sys_t *srv_sys = nullptr;

/* padding to prevent other memory update hotspots from residing on
the same memory cache line */
byte srv_pad1[64];

/** Mutex protecting the server, trx structs, query threads, and lock table */
mutex_t *kernel_mutex_temp;

/* padding to prevent other memory update hotspots from residing on the same
 * memory cache line */
byte srv_pad2[64];

/* The following values give info about the activity going on in
the database. They are protected by the server mutex. The arrays
are indexed by the type of the thread. */

ulint srv_n_threads_active[SRV_MASTER + 1];
static ulint srv_n_threads[SRV_MASTER + 1];

/** The value passed to srv_parse_log_group_home_dirs() is copied to
this variable. Since the function does a destructive read. */
static char *log_path_buf;

/** The value passed to parse_data_file_paths_and_sizes() is copied to
this variable. Since the function does a destructive read. */
static char *data_path_buf;

/* global variable for indicating if we've paniced or not. If we have,
   we try not to do anything at all. */
int srv_panic_status = 0;

void *ib_panic_data = nullptr;

ib_panic_handler_t ib_panic = nullptr;

/**
 * Adds a slash or a backslash to the end of a string if it is missing
 * and the string is not empty.
 *
 * @param[in] str                  Nul terminated char array.
 *
 * @return	string which has the separator if the string is not empty.
 *      The string is alloc'ed using malloc() and the caller is
 *      responsible for freeing the string.
 */
static char *srv_add_path_separator_if_needed(const char *str) noexcept {
  auto len = strlen(str);
  auto out_str = static_cast<char *>(malloc(len + 2));

  std::strcpy(out_str, str);

  if (len > 0 && out_str[len - 1] != SRV_PATH_SEPARATOR) {
    out_str[len] = SRV_PATH_SEPARATOR;
    out_str[len + 1] = 0;
  }

  return out_str;
}

/**
 * Prints counters for work done by srv_master_thread.
 */
static void srv_print_master_thread_info() noexcept {
  log_warn(std::format(
    "srv_master_thread loops: {} 1_second, {} sleeps, "
    "{} 10_second, {} background, {} flush",
    srv_main_1_second_loops,
    srv_main_sleeps,
    srv_main_10_second_loops,
    srv_main_background_loops,
    srv_main_flush_loops
  ));

  log_warn("srv_master_thread log flush and writes: ", srv_log_writes_and_flush);
}

bool InnoDB::parse_log_group_home_dirs(const char *usr_str) noexcept {
  ulint i;
  char *str;
  char *path;
  int n_bytes;
  char *input_str;

  if (log_path_buf != nullptr) {
    ::free(log_path_buf);
    log_path_buf = nullptr;
  }

  log_path_buf = static_cast<char *>(malloc(strlen(usr_str) + 1));

  strcpy(log_path_buf, usr_str);
  str = log_path_buf;

  if (srv_log_group_home_dirs != nullptr) {

    for (i = 0; srv_log_group_home_dirs[i] != nullptr; ++i) {
      ::free(srv_log_group_home_dirs[i]);
      srv_log_group_home_dirs[i] = nullptr;
    }

    ::free(srv_log_group_home_dirs);
    srv_log_group_home_dirs = nullptr;
  }

  i = 0;
  input_str = str;

  /* First calculate the number of directories and check syntax:
  path;path;... */

  while (*str != '\0') {
    path = str;

    while (*str != ';' && *str != '\0') {
      str++;
    }

    i++;

    if (*str == ';') {
      str++;
    } else if (*str != '\0') {

      return false;
    }
  }

  if (i != 1) {
    /* If log_group_home_dir was defined it must
    contain exactly one path definition. */

    return false;
  }

  /* Add sentinel element to the array. */
  n_bytes = (i + 1) * sizeof(*srv_log_group_home_dirs);
  srv_log_group_home_dirs = static_cast<char **>(malloc(n_bytes));
  memset(srv_log_group_home_dirs, 0x0, n_bytes);

  /* Then store the actual values to our array */

  str = input_str;
  i = 0;

  while (*str != '\0') {
    path = str;

    while (*str != ';' && *str != '\0') {
      str++;
    }

    if (*str == ';') {
      *str = '\0';
      str++;
    }

    /* Note that this memory is malloc() and so must be freed. */
    srv_log_group_home_dirs[i] = srv_add_path_separator_if_needed(path);

    i++;
  }

  /* We rely on this sentinel value during free. */
  ut_a(i > 0);
  ut_a(srv_log_group_home_dirs[i] == nullptr);

  return true;
}

void InnoDB::free_paths_and_sizes() noexcept {
  if (srv_log_group_home_dirs != nullptr) {
    for (ulint i = 0; srv_log_group_home_dirs[i] != nullptr; ++i) {
      ::free(srv_log_group_home_dirs[i]);
      srv_log_group_home_dirs[i] = nullptr;
    }

    ::free(srv_log_group_home_dirs);
    srv_log_group_home_dirs = nullptr;
  }

  if (data_path_buf != nullptr) {
    ::free(data_path_buf);
    data_path_buf = nullptr;
  }

  if (log_path_buf != nullptr) {
    ::free(log_path_buf);
    log_path_buf = nullptr;
  }
}

void InnoDB::var_init() noexcept {
  free_paths_and_sizes();

  ses_lock_wait_timeout = 1024 * 1024 * 1024;
  srv_lower_case_table_names = false;
  srv_activity_count = 0;
  srv_fatal_semaphore_wait_threshold = 600;
  srv_dml_needed_delay = 0;

  srv_monitor_active = false;
  srv_lock_timeout_active = false;

  srv_error_monitor_active = false;
  srv_main_thread_op_info = "";

  srv_log_buffer_size = ULINT_MAX;

  srv_data_read = 0;

  srv_data_written = 0;

  srv_log_write_requests = 0;

  srv_log_writes = 0;

  srv_os_log_written = 0;

  srv_os_log_pending_writes = 0;

  srv_log_waits = 0;

  srv_dblwr_writes = 0;

  srv_dblwr_pages_written = 0;

  srv_buf_pool_wait_free = 0;

  srv_buf_pool_flushed = 0;

  srv_buf_pool_reads = 0;

  srv_conc_slots = nullptr;
  srv_last_monitor_time = 0;

#ifdef UNIV_LINUX
  srv_main_thread_process_no = 0;
#endif /* UNIV_LINUX */

  srv_print_verbose_log = true;
  ses_rollback_on_timeout = false;

  srv_start_lsn = 0;
  srv_shutdown_lsn = 0;
  srv_client_table = nullptr;
  srv_lock_timeout_thread_event = nullptr;
  kernel_mutex_temp = nullptr;

  memset(srv_n_threads_active, 0x0, sizeof(srv_n_threads_active));
  memset(srv_n_threads, 0x0, sizeof(srv_n_threads));

  memset(&export_vars, 0x0, sizeof(export_vars));

  srv_shutdown_state = SRV_SHUTDOWN_NONE;
}

/**
 * Accessor function to get pointer to n'th slot in the server thread table.
 *
 * @param index in: index of the slot
 * @return pointer to the slot
 */
static srv_slot_t *srv_table_get_nth_slot(ulint index) {
  ut_a(index < OS_THREAD_MAX_N);

  return srv_sys->m_threads + index;
}

ulint srv_get_n_threads() {
  ulint n_threads = 0;

  mutex_enter(&kernel_mutex);

  for (ulint i = SRV_COM; i < SRV_MASTER + 1; i++) {

    n_threads += srv_n_threads[i];
  }

  mutex_exit(&kernel_mutex);

  return n_threads;
}

/**
 * Reserves a slot in the thread table for the current thread. Also creates the
 * thread local storage struct for the current thread. NOTE! The server mutex
 * has to be reserved by the caller!
 *
 * @param type in: type of the thread
 * @return reserved slot index
 */
static ulint srv_table_reserve_slot(srv_thread_type type) {
  ut_a(type > 0);
  ut_a(type <= SRV_MASTER);

  ulint i = 0;
  auto slot = srv_table_get_nth_slot(i);

  while (slot->m_in_use) {
    i++;
    slot = srv_table_get_nth_slot(i);
  }

  ut_a(slot->m_in_use == false);

  slot->m_type = type;
  slot->m_in_use = true;
  slot->m_suspended = false;
  slot->m_id = os_thread_get_curr_id();
  slot->m_handle = os_thread_get_curr();

  return i;
}

/**
 * Suspends the calling thread to wait for the event in its thread slot.
 * NOTE! The server mutex has to be reserved by the caller!
 *
 * @return	event for the calling thread to wait
 */
static Cond_var *srv_suspend_thread(srv_slot_t *slot) {
  ut_ad(mutex_own(&kernel_mutex));

  auto type = slot->m_type;

  ut_ad(type >= SRV_WORKER);
  ut_ad(type <= SRV_MASTER);

  auto event = slot->m_event;

  slot->m_suspended = true;

  ut_ad(srv_n_threads_active[type] > 0);

  --srv_n_threads_active[type];

  os_event_reset(event);

  return event;
}

ulint InnoDB::release_threads(srv_thread_type type, ulint n) noexcept {
  ut_ad(type >= SRV_WORKER);
  ut_ad(type <= SRV_MASTER);
  ut_ad(n > 0);
  ut_ad(mutex_own(&kernel_mutex));

  ulint count = 0;

  for (ulint i = 0; i < OS_THREAD_MAX_N; i++) {

    auto slot = srv_table_get_nth_slot(i);

    if (slot->m_in_use && slot->m_type == type && slot->m_suspended) {

      slot->m_suspended = false;

      ++srv_n_threads_active[type];

      os_event_set(slot->m_event);

      ++count;

      if (count == n) {
        break;
      }
    }
  }

  return count;
}

void InnoDB::init() noexcept {
  srv_sys = static_cast<srv_sys_t *>(mem_alloc(sizeof(srv_sys_t)));

  kernel_mutex_temp = static_cast<mutex_t *>(mem_alloc(sizeof(mutex_t)));

  mutex_create(&kernel_mutex, IF_DEBUG("kernel_mutex", ) IF_SYNC_DEBUG(SYNC_KERNEL, ) Current_location());

  mutex_create(&srv_innodb_monitor_mutex, IF_DEBUG("monitor_mutex", ) IF_SYNC_DEBUG(SYNC_NO_ORDER_CHECK, ) Current_location());

  srv_sys->m_threads = static_cast<srv_slot_t *>(mem_alloc(OS_THREAD_MAX_N * sizeof(srv_slot_t)));

  srv_slot_t *slot;

  for (ulint i = 0; i < OS_THREAD_MAX_N; i++) {
    slot = srv_table_get_nth_slot(i);
    slot->m_in_use = false;
    slot->m_type = SRV_NONE;
    slot->m_event = os_event_create(nullptr);
    ut_a(slot->m_event);
  }

  srv_client_table = static_cast<srv_slot_t *>(mem_alloc(OS_THREAD_MAX_N * sizeof(srv_slot_t)));

  slot = srv_client_table;

  for (ulint i = 0; i < OS_THREAD_MAX_N; ++i, ++slot) {
    slot->m_in_use = false;
    slot->m_type = SRV_NONE;
    slot->m_event = os_event_create(nullptr);
    ut_a(slot->m_event);
  }

  srv_lock_timeout_thread_event = os_event_create(nullptr);

  for (ulint i = 0; i < SRV_MASTER + 1; i++) {
    srv_n_threads_active[i] = 0;
    srv_n_threads[i] = 0;
  }

  UT_LIST_INIT(srv_sys->m_tasks);

  /* Init the server concurrency restriction data structures */

  UT_LIST_INIT(srv_conc_queue);

  srv_conc_slots = static_cast<srv_conc_slot_t *>(mem_alloc(OS_THREAD_MAX_N * sizeof(srv_conc_slot_t)));

  auto conc_slot = srv_conc_slots;

  for (ulint i = 0; i < OS_THREAD_MAX_N; ++i, ++conc_slot) {
    conc_slot->m_reserved = false;
    conc_slot->m_event = os_event_create(nullptr);
    ut_a(conc_slot->m_event);
  }
}

void InnoDB::free() noexcept {
  for (ulint i{}; i < OS_THREAD_MAX_N; ++i) {
    auto slot = srv_table_get_nth_slot(i);
    auto conc_slot = srv_conc_slots + i;

    os_event_free(slot->m_event);
    os_event_free(conc_slot->m_event);
  }

  os_event_free(srv_lock_timeout_thread_event);
  srv_lock_timeout_thread_event = nullptr;

  mem_free(srv_sys->m_threads);
  srv_sys->m_threads = nullptr;

  mem_free(srv_client_table);
  srv_client_table = nullptr;

  mem_free(srv_conc_slots);
  srv_conc_slots = nullptr;

  mutex_free(&srv_innodb_monitor_mutex);
  mutex_free(&kernel_mutex);

  mem_free(kernel_mutex_temp);
  kernel_mutex_temp = nullptr;

  mem_free(srv_sys);
  srv_sys = nullptr;
}

void InnoDB::general_init() noexcept {
  /* The order here is siginificant. */
  /* Reset the system variables in the recovery module. */
  recv_sys_var_init();
  os_sync_init();
  sync_init();
}

/** Normalizes init parameter values to use units we use inside InnoDB.
@return	DB_SUCCESS or error code */
static db_err srv_normalize_init_values() {
  srv_config.m_log_file_size = srv_config.m_log_file_curr_size / UNIV_PAGE_SIZE;
  srv_config.m_log_file_curr_size = srv_config.m_log_file_size * UNIV_PAGE_SIZE;

  srv_config.m_log_buffer_size = srv_config.m_log_buffer_curr_size / UNIV_PAGE_SIZE;
  srv_config.m_log_buffer_curr_size = srv_config.m_log_buffer_size * UNIV_PAGE_SIZE;

  srv_config.m_lock_table_size = 5 * (srv_config.m_buf_pool_size / UNIV_PAGE_SIZE);

  return DB_SUCCESS;
}

void InnoDB::modules_var_init() noexcept {
  /* The order here shouldn't matter. None of the functions
  below should have any dependencies. */
  rw_lock_var_init();
  que_var_init();
  pars_var_init();
  os_proc_var_init();
  os_file_var_init();
  sync_var_init();
  Log::var_init();
  dfield_var_init();
  dtype_var_init();
  ut_mem_var_init();
  os_sync_var_init();
}

db_err InnoDB::boot() noexcept {
  /* Transform the init parameter values given by the user to
  use units we use inside */

  auto err = srv_normalize_init_values();

  if (err != DB_SUCCESS) {
    return err;
  }

  /* Initialize synchronization primitives, memory management, and thread
  local storage */

  InnoDB::general_init();

  /* Initialize this module */

  InnoDB::init();

  return DB_SUCCESS;
}

/**
 * Reserves a slot in the thread table for the current user OS thread.
 * NOTE! The kernel mutex has to be reserved by the caller!
 *
 * @return	reserved slot
 */
static srv_slot_t *srv_table_reserve_slot_for_user_thread() {
  ut_ad(mutex_own(&kernel_mutex));

  ulint i{};
  auto slot = srv_client_table + i;

  while (slot->m_in_use) {
    i++;

    if (i >= OS_THREAD_MAX_N) {

      log_err(std::format(
        "There appear to be {} user threads currently waiting"
        " inside InnoDB, which is the upper limit. Cannot continue"
        " operation. We intentionally generate a seg fault to print"
        " a stack trace on Linux. But first we print a list of waiting"
        " threads.",
        (ulong)i
      ));

      for (i = 0; i < OS_THREAD_MAX_N; i++) {

        slot = srv_client_table + i;

        log_err(std::format(
          "Slot {}: thread id {}, type {}, in use {}, susp {}, time {}",
          i,
          os_thread_pf(slot->m_id),
          (ulong)slot->m_type,
          slot->m_in_use,
          slot->m_suspended,
          difftime(ut_time(), slot->m_suspend_time)
        ));
      }

      ut_error;
    }

    slot = srv_client_table + i;
  }

  ut_a(slot->m_in_use == false);

  slot->m_in_use = true;
  slot->m_id = os_thread_get_curr_id();
  slot->m_handle = os_thread_get_curr();

  return slot;
}

void InnoDB::suspend_user_thread(que_thr_t *thr) noexcept {
  double wait_time;
  ulint had_dict_lock;
  int64_t start_time = 0;
  int64_t finish_time;
  ulint diff_time;
  ulint sec;
  ulint ms;
  ulong lock_wait_timeout;

  ut_ad(!mutex_own(&kernel_mutex));

  auto trx = thr_get_trx(thr);

  os_event_set(srv_lock_timeout_thread_event);

  mutex_enter(&kernel_mutex);

  trx->m_error_state = DB_SUCCESS;

  if (thr->state == QUE_THR_RUNNING) {

    ut_ad(thr->is_active == true);

    /* The lock has already been released or this transaction
    was chosen as a deadlock victim: no need to suspend */

    if (trx->m_was_chosen_as_deadlock_victim) {

      trx->m_error_state = DB_DEADLOCK;
      trx->m_was_chosen_as_deadlock_victim = false;
    }

    mutex_exit(&kernel_mutex);

    return;
  }

  ut_ad(thr->is_active == false);

  auto slot = srv_table_reserve_slot_for_user_thread();
  auto event = slot->m_event;

  slot->m_thr = thr;

  os_event_reset(event);

  slot->m_suspend_time = ut_time();

  if (thr->lock_state == QUE_THR_LOCK_ROW) {
    srv_n_lock_wait_count++;
    srv_n_lock_wait_current_count++;

    if (ut_usectime(&sec, &ms) == -1) {
      start_time = -1;
    } else {
      start_time = (int64_t)sec * 1000000 + ms;
    }
  }
  /* Wake the lock timeout monitor thread, if it is suspended */

  os_event_set(srv_lock_timeout_thread_event);

  mutex_exit(&kernel_mutex);

  had_dict_lock = trx->m_dict_operation_lock_mode;

  switch (had_dict_lock) {
    case RW_S_LATCH:
      /* Release foreign key check latch */
      srv_dict_sys->unfreeze_data_dictionary(trx);
      break;
    case RW_X_LATCH:
      /* Release fast index creation latch */
      srv_dict_sys->unlock_data_dictionary(trx);
      break;
  }

  ut_a(trx->m_dict_operation_lock_mode == 0);

  /* Suspend this thread and wait for the event. */

  os_event_wait(event);

  /* After resuming, reacquire the data dictionary latch if
  necessary. */

  switch (had_dict_lock) {
    case RW_S_LATCH:
      srv_dict_sys->freeze_data_dictionary(trx);
      break;
    case RW_X_LATCH:
      srv_dict_sys->lock_data_dictionary(trx);
      break;
  }

  mutex_enter(&kernel_mutex);

  /* Release the slot for others to use */

  slot->m_in_use = false;

  wait_time = ut_difftime(ut_time(), slot->m_suspend_time);

  if (thr->lock_state == QUE_THR_LOCK_ROW) {
    if (ut_usectime(&sec, &ms) == -1) {
      finish_time = -1;
    } else {
      finish_time = (int64_t)sec * 1000000 + ms;
    }

    diff_time = (ulint)(finish_time - start_time);

    srv_n_lock_wait_current_count--;
    srv_n_lock_wait_time = srv_n_lock_wait_time + diff_time;

    if (diff_time > srv_n_lock_max_wait_time &&
        /* only update the variable if we successfully
        retrieved the start and finish times. See Bug#36819. */
        start_time != -1 && finish_time != -1) {
      srv_n_lock_max_wait_time = diff_time;
    }
  }

  if (trx->m_was_chosen_as_deadlock_victim) {

    trx->m_error_state = DB_DEADLOCK;
    trx->m_was_chosen_as_deadlock_victim = false;
  }

  mutex_exit(&kernel_mutex);

  /* InnoDB system transactions (such as the purge, and
  incomplete transactions that are being rolled back after crash
  recovery) will use the global value of
  innodb_lock_wait_timeout, because trx->m_client_ctx == nullptr. */
  lock_wait_timeout = sess_lock_wait_timeout(trx);

  if (trx->is_interrupted() || (lock_wait_timeout < 100000000 && wait_time > (double)lock_wait_timeout)) {

    trx->m_error_state = DB_LOCK_WAIT_TIMEOUT;
  }
}

void InnoDB::release_user_thread_if_suspended(que_thr_t *thr) noexcept {
  ut_ad(mutex_own(&kernel_mutex));

  for (ulint i{}; i < OS_THREAD_MAX_N; ++i) {

    auto slot = srv_client_table + i;

    if (slot->m_in_use && slot->m_thr == thr) {

      os_event_set(slot->m_event);

      return;
    }
  }
}

/**
 * Refreshes the values used to calculate per-second averages.
 */
static void srv_refresh_innodb_monitor_stats() noexcept {
  mutex_enter(&srv_innodb_monitor_mutex);

  srv_last_monitor_time = time(nullptr);

  os_file_refresh_stats();

  log_sys->refresh_stats();

  srv_buf_pool->refresh_io_stats();

  srv_n_rows_inserted_old = srv_n_rows_inserted;
  srv_n_rows_updated_old = srv_n_rows_updated;
  srv_n_rows_deleted_old = srv_n_rows_deleted;
  srv_n_rows_read_old = srv_n_rows_read;

  mutex_exit(&srv_innodb_monitor_mutex);
}

bool InnoDB::printf_innodb_monitor(ib_stream_t ib_stream, bool nowait, ulint *trx_start, ulint *trx_end) noexcept {
  mutex_enter(&srv_innodb_monitor_mutex);

  auto current_time = time(nullptr);

  /* We add 0.001 seconds to time_elapsed to prevent division
  by zero if two users happen to call SHOW INNODB STATUS at the same
  time */

  auto time_elapsed = difftime(current_time, srv_last_monitor_time) + 0.001;

  srv_last_monitor_time = time(nullptr);

  log_warn("InnoDB: _monitor thread output");

  log_warn("\n=====================================\n");

  log_warn(std::format(
    " INNODB MONITOR OUTPUT\n"
    "=====================================\n"
    "Per second averages calculated from the last {} seconds",
    time_elapsed
  ));

  log_warn(
    "----------\n"
    "BACKGROUND THREAD\n"
    "----------\n"
  );

  srv_print_master_thread_info();

  log_warn(
    "----------\n"
    "SEMAPHORES\n"
    "----------\n"
  );

  sync_print(ib_stream);

#ifdef WITH_FOREIGN_KEY
  /* Conceptually, srv_innodb_monitor_mutex has a very high latching
  order level in sync0sync.h, while dict_foreign_err_mutex has a very
  low level 135. Therefore we can reserve the latter mutex here without
  a danger of a deadlock of threads. */

  mutex_enter(&dict_foreign_err_mutex);

  if (ftell(dict_foreign_err_file) != 0L) {
    log_warn(
      "------------------------\n"
      "LATEST FOREIGN KEY ERROR\n"
      "------------------------\n"
    );
    ut_copy_file(ib_stream, dict_foreign_err_file);
  }

  mutex_exit(&dict_foreign_err_mutex);
#endif /* WITH_FOREIGN_KEY */

  srv_lock_sys->print_info_all_transactions();

  log_warn(
    "--------\n"
    "I/O\n"
    "--------\n"
  );

  log_warn("{}", srv_aio->to_string().c_str());

  /* Only if lock_print_info_summary proceeds correctly,
  before we call the lock_print_info_all_transactions
  to print all the lock information. */
  auto ret = srv_lock_sys->print_info_summary(nowait);

  if (ret) {
    if (trx_start) {
      auto t = ftell(ib_stream);

      if (t < 0) {
        *trx_start = ULINT_UNDEFINED;
      } else {
        *trx_start = (ulint)t;
      }
    }

    srv_lock_sys->print_info_all_transactions();

    if (trx_end) {
      auto t = ftell(ib_stream);
      if (t < 0) {
        *trx_end = ULINT_UNDEFINED;
      } else {
        *trx_end = (ulint)t;
      }
    }
  }

  log_warn(
    "----------------------\n"
    "BUFFER POOL AND MEMORY\n"
    "----------------------\n"
  );
  log_warn("Total memory allocated ", ut_total_allocated_memory());
  log_warn("Dictionary memory allocated ", srv_dict_sys->m_size);

  srv_buf_pool->print_io(ib_stream);

  log_warn(
    "--------------\n"
    "ROW OPERATIONS\n"
    "--------------\n"
  );

  log_warn(std::format("{} read views open inside InnoDB", UT_LIST_GET_LEN(srv_trx_sys->m_view_list)));

  auto n_reserved = srv_fil->space_get_n_reserved_extents(0);

  if (n_reserved > 0) {
    log_warn(std::format("{} tablespace extents now reserved for B-tree split operationsn", n_reserved));
  }

#ifdef UNIV_LINUX
  log_warn(std::format(
    "Main thread process no. {}, id {}, state: {}", srv_main_thread_process_no, srv_main_thread_id, srv_main_thread_op_info
  ));
#else
  log_warn(std::format("Main thread id {}, state: {}", srv_main_thread_id, srv_main_thread_op_info));
#endif /* UNIV_LINUX */

  log_warn(std::format(
    "Number of rows inserted {}, updated {}, deleted {}, read {}",
    srv_n_rows_inserted,
    srv_n_rows_updated,
    srv_n_rows_deleted,
    srv_n_rows_read
  ));

  log_warn(std::format(
    "{:.2f} inserts/s, {:.2f} updates/s,"
    " {:.2f} deletes/s, {:.2f} reads/s",
    (srv_n_rows_inserted - srv_n_rows_inserted_old) / time_elapsed,
    (srv_n_rows_updated - srv_n_rows_updated_old) / time_elapsed,
    (srv_n_rows_deleted - srv_n_rows_deleted_old) / time_elapsed,
    (srv_n_rows_read - srv_n_rows_read_old) / time_elapsed
  ));

  srv_n_rows_inserted_old = srv_n_rows_inserted;

  srv_n_rows_updated_old = srv_n_rows_updated;

  srv_n_rows_deleted_old = srv_n_rows_deleted;

  srv_n_rows_read_old = srv_n_rows_read;

  log_warn(
    "----------------------------\n"
    "END OF INNODB MONITOR OUTPUT\n"
    "============================\n"
  );

  mutex_exit(&srv_innodb_monitor_mutex);

  return ret;
}

void InnoDB::export_innodb_status() noexcept {
  mutex_enter(&srv_innodb_monitor_mutex);

  export_vars.innodb_data_pending_reads = os_n_pending_reads;
  export_vars.innodb_data_pending_writes = os_n_pending_writes;
  export_vars.innodb_data_pending_fsyncs = srv_fil->get_pending_log_flushes() + srv_fil->get_pending_tablespace_flushes();
  export_vars.innodb_data_fsyncs = os_n_fsyncs;
  export_vars.innodb_data_read = srv_data_read;
  export_vars.innodb_data_reads = os_n_file_reads;
  export_vars.innodb_data_writes = os_n_file_writes;
  export_vars.innodb_data_written = srv_data_written;
  export_vars.innodb_buffer_pool_read_requests = srv_buf_pool->m_stat.n_page_gets;
  export_vars.innodb_buffer_pool_write_requests = srv_buf_pool->m_write_requests;
  export_vars.innodb_buffer_pool_wait_free = srv_buf_pool_wait_free;
  export_vars.innodb_buffer_pool_pages_flushed = srv_buf_pool_flushed;
  export_vars.innodb_buffer_pool_reads = srv_buf_pool_reads;
  export_vars.innodb_buffer_pool_read_ahead = srv_buf_pool->m_stat.n_ra_pages_read;
  export_vars.innodb_buffer_pool_read_ahead_evicted = srv_buf_pool->m_stat.n_ra_pages_evicted;
  export_vars.innodb_buffer_pool_pages_data = UT_LIST_GET_LEN(srv_buf_pool->m_LRU_list);
  export_vars.innodb_buffer_pool_pages_dirty = UT_LIST_GET_LEN(srv_buf_pool->m_flush_list);
  export_vars.innodb_buffer_pool_pages_free = UT_LIST_GET_LEN(srv_buf_pool->m_free_list);

  ut_d(export_vars.innodb_buffer_pool_pages_latched = srv_buf_pool->get_latched_pages_number());

  export_vars.innodb_buffer_pool_pages_total = srv_buf_pool->m_curr_size;

  export_vars.innodb_buffer_pool_pages_misc =
    srv_buf_pool->m_curr_size - UT_LIST_GET_LEN(srv_buf_pool->m_LRU_list) - UT_LIST_GET_LEN(srv_buf_pool->m_free_list);

  export_vars.innodb_have_atomic_builtins = 1;
  export_vars.innodb_page_size = UNIV_PAGE_SIZE;
  export_vars.innodb_log_waits = srv_log_waits;
  export_vars.innodb_os_log_written = srv_os_log_written;
  export_vars.innodb_os_log_fsyncs = srv_fil->get_log_flushes();
  export_vars.innodb_os_log_pending_fsyncs = srv_fil->get_pending_log_flushes();
  export_vars.innodb_os_log_pending_writes = srv_os_log_pending_writes;
  export_vars.innodb_log_write_requests = srv_log_write_requests;
  export_vars.innodb_log_writes = srv_log_writes;
  export_vars.innodb_dblwr_pages_written = srv_dblwr_pages_written;
  export_vars.innodb_dblwr_writes = srv_dblwr_writes;
  export_vars.innodb_pages_created = srv_buf_pool->m_stat.n_pages_created;
  export_vars.innodb_pages_read = srv_buf_pool->m_stat.n_pages_read;
  export_vars.innodb_pages_written = srv_buf_pool->m_stat.n_pages_written;
  export_vars.innodb_row_lock_waits = srv_n_lock_wait_count;
  export_vars.innodb_row_lock_current_waits = srv_n_lock_wait_current_count;
  export_vars.innodb_row_lock_time = srv_n_lock_wait_time / 1000;

  if (srv_n_lock_wait_count > 0) {
    export_vars.innodb_row_lock_time_avg = (ulint)(srv_n_lock_wait_time / 1000 / srv_n_lock_wait_count);
  } else {
    export_vars.innodb_row_lock_time_avg = 0;
  }
  export_vars.innodb_row_lock_time_max = srv_n_lock_max_wait_time / 1000;
  export_vars.innodb_rows_read = srv_n_rows_read;
  export_vars.innodb_rows_inserted = srv_n_rows_inserted;
  export_vars.innodb_rows_updated = srv_n_rows_updated;
  export_vars.innodb_rows_deleted = srv_n_rows_deleted;

  mutex_exit(&srv_innodb_monitor_mutex);
}

void *InnoDB::monitor_thread(void *) noexcept {
  double time_elapsed;
  time_t current_time;

  srv_last_monitor_time = time(nullptr);

  ulint mutex_skipped{};
  auto last_table_monitor_time = time(nullptr);
  auto last_tablespace_monitor_time = time(nullptr);
  auto last_monitor_time = time(nullptr);
  auto last_srv_print_monitor = srv_print_innodb_monitor;

loop:
  srv_monitor_active = true;

  /* Wake up every 5 seconds to see if we need to print
  monitor information. */

  os_thread_sleep(5000000);

  current_time = time(nullptr);

  time_elapsed = difftime(current_time, last_monitor_time);

  if (time_elapsed > 15) {
    last_monitor_time = time(nullptr);

    if (srv_print_innodb_monitor) {
      /* Reset mutex_skipped counter everytime
      srv_print_innodb_monitor changes. This is to
      ensure we will not be blocked by kernel_mutex
      for short duration information printing,
      such as requested by sync_array_print_long_waits() */
      if (!last_srv_print_monitor) {
        mutex_skipped = 0;
        last_srv_print_monitor = true;
      }

      if (!printf_innodb_monitor(ib_stream, MUTEX_NOWAIT(mutex_skipped), nullptr, nullptr)) {
        ++mutex_skipped;
      } else {
        /* Reset the counter */
        mutex_skipped = 0;
      }
    } else {
      last_srv_print_monitor = false;
    }

    if (srv_config.m_status) {
      mutex_enter(&srv_monitor_file_mutex);

      if (!printf_innodb_monitor(ib_stream, MUTEX_NOWAIT(mutex_skipped), nullptr, nullptr)) {
        ++mutex_skipped;
      } else {
        mutex_skipped = 0;
      }

      mutex_exit(&srv_monitor_file_mutex);
    }

    if (srv_print_innodb_tablespace_monitor && difftime(current_time, last_tablespace_monitor_time) > 60) {
      last_tablespace_monitor_time = time(nullptr);

      log_warn("================================================");

      log_warn(
        " INNODB TABLESPACE MONITOR OUTPUT\n"
        "================================================\n"
      );

      srv_fsp->print(0);
      log_warn("Validating tablespace");

      {
        auto success = srv_fsp->validate(0);
        ut_a(success);
      }

      log_warn(
        "Validation ok\n"
        "---------------------------------------\n"
        "END OF INNODB TABLESPACE MONITOR OUTPUT\n"
        "=======================================\n"
      );
    }

    if (srv_print_innodb_table_monitor && difftime(current_time, last_table_monitor_time) > 60) {

      last_table_monitor_time = time(nullptr);

      log_warn(
        "===========================================\n"
        " INNODB TABLE MONITOR OUTPUT\n"
        "==========================================="
      );

      srv_dict_sys->to_string();

      log_warn(
        "-----------------------------------\n"
        "END OF INNODB TABLE MONITOR OUTPUT\n"
        "==================================\n"
      );
    }
  }

  if (srv_shutdown_state >= SRV_SHUTDOWN_CLEANUP) {
    goto exit_func;
  }

  if (srv_print_innodb_monitor || srv_lock_sys->is_print_lock_monitor_set() || srv_print_innodb_tablespace_monitor ||
      srv_print_innodb_table_monitor) {

    goto loop;
  }

  srv_monitor_active = false;

  goto loop;

exit_func:
  srv_monitor_active = false;

  /* We count the number of threads in os_thread_exit(). A created
  thread should always use that to exit and not use return() to exit. */

  os_thread_exit();

  return nullptr;
}

void *InnoDB::lock_timeout_thread(void *) noexcept {
  for (;;) {
    /* When someone is waiting for a lock, we wake up every second
    and check if a timeout has passed for a lock wait */

    os_thread_sleep(1000000);

    srv_lock_timeout_active = true;

    mutex_enter(&kernel_mutex);

    auto some_waits = false;

    /* Check of all slots if a thread is waiting there, and if it
    has exceeded the time limit */

    for (ulint i = 0; i < OS_THREAD_MAX_N; i++) {

      auto slot = srv_client_table + i;

      if (slot->m_in_use) {
        some_waits = true;
        auto wait_time = ut_difftime(ut_time(), slot->m_suspend_time);
        auto trx = thr_get_trx(slot->m_thr);
        auto lock_wait_timeout = sess_lock_wait_timeout(trx);

        if (trx->is_interrupted() || (lock_wait_timeout < 100000000 && (wait_time > (double)lock_wait_timeout || wait_time < 0))) {

          /* Timeout exceeded or a wrap-around in system time counter:
          cancel the lock request queued by the transaction and release possible
          other transactions waiting behind; it is possible that the lock has
          already been granted: in that case do nothing */

          if (trx->m_wait_lock) {
            srv_lock_sys->cancel_waiting_and_release(trx->m_wait_lock);
          }
        }
      }
    }

    os_event_reset(srv_lock_timeout_thread_event);

    mutex_exit(&kernel_mutex);

    if (srv_shutdown_state >= SRV_SHUTDOWN_CLEANUP) {
      srv_lock_timeout_active = false;

      /* We count the number of threads in os_thread_exit(). A created
      thread should always use that to exit and not use return() to exit. */

      os_thread_exit();

      return nullptr;
    }

    if (!some_waits) {
      srv_lock_timeout_active = false;
    }
  }
}

void *InnoDB::error_monitor_thread(void *) noexcept {
  /* Number of successive fatal timeouts observed */
  ulint fatal_cnt = 0;
  auto old_lsn = srv_start_lsn;

loop:
  srv_error_monitor_active = true;

  /* Try to track a strange bug reported by Harald Fuchs and others,
  where the lsn seems to decrease at times */

  auto new_lsn = log_sys->get_lsn();

  if (new_lsn < old_lsn) {
    log_err(std::format(
      "old log sequence number {} was greater than the new log sequence number {}!"
      " Please submit a bug report, check the InnoDB website for details",
      old_lsn,
      new_lsn
    ));
  }

  old_lsn = new_lsn;

  if (difftime(time(nullptr), srv_last_monitor_time) > 60) {
    /* We referesh InnoDB Monitor values so that averages are
    printed from at most 60 last seconds */

    srv_refresh_innodb_monitor_stats();
  }

  /* Update the statistics collected for deciding LRU eviction policy. */
  srv_buf_pool->m_LRU->stat_update();

  /* Update the statistics collected for flush rate policy. */
  srv_buf_pool->m_flusher->stat_update();

  /* In case mutex_exit is not a memory barrier, it is
  theoretically possible some threads are left waiting though
  the semaphore is already released. Wake up those threads: */

  sync_arr_wake_threads_if_sema_free();

  if (sync_array_print_long_waits()) {
    fatal_cnt++;
    if (fatal_cnt > 10) {

      log_warn(std::format(
        "Semaphore wait has lasted > {} seconds. We intentionally crash the"
        " server, because it appears to be hung.",
        (ulong)srv_fatal_semaphore_wait_threshold
      ));

      ut_error;
    }
  } else {
    fatal_cnt = 0;
  }

  os_thread_sleep(1000000);

  if (srv_shutdown_state < SRV_SHUTDOWN_CLEANUP) {

    goto loop;
  }

  srv_error_monitor_active = false;

  /* We count the number of threads in os_thread_exit(). A created
  thread should always use that to exit and not use return() to exit. */

  os_thread_exit();

  return nullptr;
}

void InnoDB::active_wake_master_thread() noexcept {
  ++srv_activity_count;

  if (srv_n_threads_active[SRV_MASTER] == 0) {

    mutex_enter(&kernel_mutex);

    release_threads(SRV_MASTER, 1);

    mutex_exit(&kernel_mutex);
  }
}

void InnoDB::wake_master_thread() noexcept {
  ++srv_activity_count;

  mutex_enter(&kernel_mutex);

  release_threads(SRV_MASTER, 1);

  mutex_exit(&kernel_mutex);
}

/**
 * The master thread is tasked to ensure that flush of log file happens
 * once every second in the background. This is to ensure that not more
 * than one second of trxs are lost in case of crash when
 * innodb_flush_logs_at_trx_commit != 1
 */
static void srv_sync_log_buffer_in_background() {
  auto current_time = time(nullptr);

  srv_main_thread_op_info = "flushing log";

  if (difftime(current_time, srv_last_log_flush_time) >= 1) {
    log_sys->buffer_sync_in_background(true);
    srv_last_log_flush_time = current_time;
    srv_log_writes_and_flush++;
  }
}

void *InnoDB::master_thread(void *) noexcept {
  Cond_var *event;
  ulint old_activity_count;
  ulint n_pages_purged = 0;
  ulint n_pages_flushed;
  ulint n_tables_to_drop;
  ulint n_ios;
  ulint n_ios_very_old;
  ulint n_pend_ios;
  bool skip_sleep = false;
  ulint i;

#ifdef UNIV_LINUX
  srv_main_thread_process_no = os_proc_get_number();
#endif /* UNIV_LINUX */

  srv_main_thread_id = os_thread_pf(os_thread_get_curr_id());

  auto slot_no = srv_table_reserve_slot(SRV_MASTER);

  mutex_enter(&kernel_mutex);

  srv_n_threads_active[SRV_MASTER]++;

  mutex_exit(&kernel_mutex);

loop:
  /* When there is database activity by users, we cycle in this loop */

  srv_main_thread_op_info = "reserving kernel mutex";

  n_ios_very_old = log_sys->m_n_log_ios + srv_buf_pool->m_stat.n_pages_read + srv_buf_pool->m_stat.n_pages_written;
  mutex_enter(&kernel_mutex);

  /* Store the user activity counter at the start of this loop */
  old_activity_count = srv_activity_count;

  mutex_exit(&kernel_mutex);

  if (srv_config.m_force_recovery >= IB_RECOVERY_NO_BACKGROUND) {

    goto suspend_thread;
  }

  /* We run the following loop approximately once per second
  when there is database activity */

  srv_last_log_flush_time = time(nullptr);
  /* No need to sleep if user has signalled shutdown. */
  skip_sleep = (srv_shutdown_state != SRV_SHUTDOWN_NONE);

  for (i = 0; i < 10; i++) {
    srv_main_thread_op_info = "sleeping";
    srv_main_1_second_loops++;

    if (!skip_sleep) {

      os_thread_sleep(1000000);
      srv_main_sleeps++;
    }

    /* No need to sleep if user has signalled shutdown. */
    skip_sleep = (srv_shutdown_state != SRV_SHUTDOWN_NONE);

    /* No need to sleep if user has signalled shutdown. */

    /* ALTER TABLE on Unix requires that the table handler
    can drop tables lazily after there no longer are SELECT
    queries to them. */

    srv_main_thread_op_info = "doing background drop tables";

    (void)srv_dict_sys->m_ddl.drop_tables_in_background();

    srv_main_thread_op_info = "";

    if (srv_config.m_fast_shutdown != IB_SHUTDOWN_NORMAL && srv_shutdown_state > SRV_SHUTDOWN_NONE) {

      goto background_loop;
    }

    /* Flush logs if needed */
    srv_sync_log_buffer_in_background();

    srv_main_thread_op_info = "making checkpoint";
    log_sys->free_check();

    n_pend_ios = srv_buf_pool->get_n_pending_ios() + log_sys->m_n_pending_writes;

    n_ios = log_sys->m_n_log_ios + srv_buf_pool->m_stat.n_pages_read + srv_buf_pool->m_stat.n_pages_written;

    if (unlikely(srv_buf_pool->get_modified_ratio_pct() > srv_config.m_max_buf_pool_modified_pct)) {

      /* Try to keep the number of modified pages in the
      buffer pool under the limit wished by the user */

      srv_main_thread_op_info = "flushing buffer pool pages";
      n_pages_flushed = srv_buf_pool->m_flusher->batch(srv_dblwr, BUF_FLUSH_LIST, PCT_IO(100), IB_UINT64_T_MAX);

      /* If we had to do the flush, it may have taken
      even more than 1 second, and also, there may be more
      to flush. Do not sleep 1 second during the next
      iteration of this loop. */

      skip_sleep = true;

    } else if (srv_config.m_adaptive_flushing) {

      /* Try to keep the rate of flushing of dirty
      pages such that redo log generation does not
      produce bursts of IO at checkpoint time. */
      ulint n_flush = srv_buf_pool->m_flusher->get_desired_flush_rate();

      if (n_flush) {
        srv_main_thread_op_info = "flushing buffer pool pages";
        n_flush = std::min<ulint>(PCT_IO(100), n_flush);
        n_pages_flushed = srv_buf_pool->m_flusher->batch(srv_dblwr, BUF_FLUSH_LIST, n_flush, IB_ULONGLONG_MAX);

        if (n_flush == PCT_IO(100)) {
          skip_sleep = true;
        }
      }
    }

    if (srv_activity_count == old_activity_count) {

      /* There is no user activity at the moment, go to
      the background loop */

      goto background_loop;
    }
  }

  /* If i/os during the 10 second period were less than 200% of
  capacity, we assume that there is free disk i/o capacity
  available, and it makes sense to flush srv_io_capacity pages.

  Note that this is done regardless of the fraction of dirty
  pages relative to the max requested by the user. The one second
  loop above requests writes for that case. The writes done here
  are not required, and may be disabled. */

  n_pend_ios = srv_buf_pool->get_n_pending_ios() + log_sys->m_n_pending_writes;
  n_ios = log_sys->m_n_log_ios + srv_buf_pool->m_stat.n_pages_read + srv_buf_pool->m_stat.n_pages_written;

  ++srv_main_10_second_loops;

  if (n_pend_ios < SRV_PEND_IO_THRESHOLD && (n_ios - n_ios_very_old < SRV_PAST_IO_ACTIVITY)) {

    srv_main_thread_op_info = "flushing buffer pool pages";
    srv_buf_pool->m_flusher->batch(srv_dblwr, BUF_FLUSH_LIST, PCT_IO(100), IB_ULONGLONG_MAX);

    /* Flush logs if needed */
    srv_sync_log_buffer_in_background();
  }

  /* Flush logs if needed */
  srv_sync_log_buffer_in_background();

  /* We run a full purge every 10 seconds, even if the server
  were active */
  do {

    if (srv_config.m_fast_shutdown != IB_SHUTDOWN_NORMAL && srv_shutdown_state > 0) {

      goto background_loop;
    }

    srv_main_thread_op_info = "purging";
    n_pages_purged = srv_trx_sys->m_purge->run();

    /* Flush logs if needed */
    srv_sync_log_buffer_in_background();

  } while (n_pages_purged);

  srv_main_thread_op_info = "flushing buffer pool pages";

  /* Flush a few oldest pages to make a new checkpoint younger */

  if (srv_buf_pool->get_modified_ratio_pct() > srv_config.m_max_buf_pool_modified_pct) {

    /* If there are lots of modified pages in the buffer pool
    (> 70 %), we assume we can afford reserving the disk(s) for
    the time it requires to flush 100 pages */

    n_pages_flushed = srv_buf_pool->m_flusher->batch(srv_dblwr, BUF_FLUSH_LIST, PCT_IO(100), IB_UINT64_T_MAX);
  } else {
    /* Otherwise, we only flush a small number of pages so that
    we do not unnecessarily use much disk i/o capacity from
    other work */

    n_pages_flushed = srv_buf_pool->m_flusher->batch(srv_dblwr, BUF_FLUSH_LIST, PCT_IO(10), IB_UINT64_T_MAX);
  }

  srv_main_thread_op_info = "making checkpoint";

  /* Make a new checkpoint about once in 10 seconds */

  {
    auto success = log_sys->checkpoint(true, false);

    if (!success) {
      log_info("Checkpoint already running");
    }
  }

  srv_main_thread_op_info = "reserving kernel mutex";

  mutex_enter(&kernel_mutex);

  /* When there is database activity, we jump from here back to
  the start of loop */

  if (srv_activity_count != old_activity_count) {
    mutex_exit(&kernel_mutex);
    goto loop;
  }

  mutex_exit(&kernel_mutex);

  /* If the database is quiet, we enter the background loop */

background_loop:

  /* In this loop we run background operations when the server
  is quiet from user activity. Also in the case of a shutdown, we
  loop here, flushing the buffer pool to the data files. */

  /* The server has been quiet for a while: start running background
  operations */
  srv_main_background_loops++;
  srv_main_thread_op_info = "doing background drop tables";

  n_tables_to_drop = srv_dict_sys->m_ddl.drop_tables_in_background();

  if (n_tables_to_drop > 0) {
    /* Do not monopolize the CPU even if there are tables waiting
    in the background drop queue. (It is essentially a bug if
    user tries to drop a table while there are still open handles
    to it and we had to put it to the background drop queue.) */

    os_thread_sleep(100000);
  }

  srv_main_thread_op_info = "purging";

  /* Run a full purge */
  do {
    if (srv_config.m_fast_shutdown != IB_SHUTDOWN_NORMAL && srv_shutdown_state > 0) {

      break;
    }

    srv_main_thread_op_info = "purging";
    n_pages_purged = srv_trx_sys->m_purge->run();

    /* Flush logs if needed */
    srv_sync_log_buffer_in_background();

  } while (n_pages_purged);

  srv_main_thread_op_info = "reserving kernel mutex";

  mutex_enter(&kernel_mutex);
  if (srv_activity_count != old_activity_count) {
    mutex_exit(&kernel_mutex);
    goto loop;
  }
  mutex_exit(&kernel_mutex);

  srv_main_thread_op_info = "reserving kernel mutex";

  mutex_enter(&kernel_mutex);

  if (srv_activity_count != old_activity_count) {
    mutex_exit(&kernel_mutex);
    goto loop;
  }

  mutex_exit(&kernel_mutex);

flush_loop:
  srv_main_thread_op_info = "flushing buffer pool pages";
  srv_main_flush_loops++;
  if (srv_config.m_fast_shutdown != IB_SHUTDOWN_NO_BUFPOOL_FLUSH) {
    n_pages_flushed = srv_buf_pool->m_flusher->batch(srv_dblwr, BUF_FLUSH_LIST, PCT_IO(100), IB_UINT64_T_MAX);
  } else {
    /* In the fastest shutdown we do not flush the buffer pool
    to data files: we set n_pages_flushed to 0 artificially. */

    n_pages_flushed = 0;
  }

  srv_main_thread_op_info = "reserving kernel mutex";

  mutex_enter(&kernel_mutex);
  if (srv_activity_count != old_activity_count) {
    mutex_exit(&kernel_mutex);
    goto loop;
  }
  mutex_exit(&kernel_mutex);

  srv_main_thread_op_info = "waiting for buffer pool flush to end";
  srv_buf_pool->m_flusher->wait_batch_end(BUF_FLUSH_LIST);

  /* Flush logs if needed */
  srv_sync_log_buffer_in_background();

  srv_main_thread_op_info = "making checkpoint";

  {
    auto success = log_sys->checkpoint(true, false);

    if (!success) {
      log_info("Checkpoint already running");
    }
  }

  if (srv_buf_pool->get_modified_ratio_pct() > srv_config.m_max_buf_pool_modified_pct) {

    /* Try to keep the number of modified pages in the
    buffer pool under the limit wished by the user */

    goto flush_loop;
  }

  srv_main_thread_op_info = "reserving kernel mutex";

  mutex_enter(&kernel_mutex);
  if (srv_activity_count != old_activity_count) {
    mutex_exit(&kernel_mutex);
    goto loop;
  }
  mutex_exit(&kernel_mutex);

  /* Keep looping in the background loop if still work to do */

  if (srv_config.m_fast_shutdown != IB_SHUTDOWN_NORMAL && srv_shutdown_state > 0) {
    if (n_tables_to_drop + n_pages_flushed != 0) {

      /* If we are doing a fast shutdown (= the default) we do not do purge.
      But we flush the buffer pool completely to disk. In a 'very fast'
      shutdown we do not flush the buffer pool to data files: we have
      set n_pages_flushed to 0 artificially. */

      goto background_loop;
    }
  } else if (n_tables_to_drop + n_pages_purged + n_pages_flushed != 0) {
    /* In a 'slow' shutdown we run purge to completion */

    goto background_loop;
  }

  /* There is no work for background operations either: suspend
  master thread to wait for more server activity */

suspend_thread:
  srv_main_thread_op_info = "suspending";

  mutex_enter(&kernel_mutex);

  if (srv_dict_sys->m_ddl.get_background_drop_list_len() > 0) {
    mutex_exit(&kernel_mutex);

    goto loop;
  }

  auto slot = srv_table_get_nth_slot(slot_no);
  event = srv_suspend_thread(slot);

  mutex_exit(&kernel_mutex);

  /* DO NOT CHANGE THIS STRING. innobase_start_or_create()
  waits for database activity to die down when converting < 4.1.x
  databases, and relies on this string being exactly as it is. InnoDB
  manual also mentions this string in several places. */
  srv_main_thread_op_info = "waiting for server activity";

  os_event_wait(event);

  if (srv_shutdown_state == SRV_SHUTDOWN_EXIT_THREADS) {
    /* This is only extra safety, the thread should exit
    already when the event wait ends */

    os_thread_exit();
    return nullptr;
  } else {
    /* When there is user activity, InnoDB will set the event and the
    main thread goes back to loop. */

    goto loop;
  }
}

void InnoDB::que_task_enqueue_low(que_thr_t *thr) noexcept {
  ut_ad(mutex_own(&kernel_mutex));

  UT_LIST_ADD_LAST(srv_sys->m_tasks, thr);

  release_threads(SRV_WORKER, 1);
}

void InnoDB::panic(int panic_ib_error, char *fmt, ...) noexcept {
  va_list ap;
  srv_panic_status = panic_ib_error;

  va_start(ap, fmt);

  if (ib_panic) {
    ib_panic(ib_panic_data, panic_ib_error, fmt, ap);
    return;
  } else {
    log_warn("Database forced shutdown! err: ", panic_ib_error);
    log_warn(fmt, ap);
    exit(EXIT_FAILURE);
  }

  va_end(ap);
}

std::string InnoDB::get_log_dir() noexcept {
  /** FIXME: We currently only support a single log group. */
  return srv_log_group_home_dirs[0];
}
