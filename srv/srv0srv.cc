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

#include <cstdarg>

#include "ut0ucode.h"
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

/** Maximum number of times allowed to conditionally acquire
mutex before switching to blocking wait on the mutex */
constexpr ulint MAX_MUTEX_NOWAIT = 20;

/** Check whether the number of failed nonblocking mutex
acquisition attempts exceeds maximum allowed value. If so,
printf_innodb_monitor() will request mutex acquisition
with mutex_enter(), which will wait until it gets the mutex. */
#define MUTEX_NOWAIT(mutex_skipped) ((mutex_skipped) < MAX_MUTEX_NOWAIT)

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

/**
 * Prints counters for work done by srv_master_thread.
 */
void Srv::print_master_thread_info() noexcept {
  log_warn(std::format(
    "srv_master_thread loops: {} 1_second, {} sleeps, "
    "{} 10_second, {} background, {} flush",
    m_main_1_second_loops,
    m_main_sleeps,
    m_main_10_second_loops,
    m_main_background_loops,
    m_main_flush_loops
  ));

  log_warn("srv_master_thread log flush and writes: ", m_log_writes_and_flush);
}

/**
 * Accessor function to get pointer to n'th slot in the server thread table.
 *
 * @param index in: index of the slot
 * @return pointer to the slot
 */
SrvSlot *Srv::table_get_nth_slot(ulint index) noexcept {
  return &m_threads.at(index);
}

/**
 * Reserves a slot in the thread table for the current thread. Also creates the
 * thread local storage struct for the current thread. NOTE! The server mutex
 * has to be reserved by the caller!
 *
 * @param type in: type of the thread
 * @return reserved slot index
 */
ulint Srv::table_reserve_slot(SrvThreadType type) noexcept {
  ut_a(type > 0);
  ut_a(type <= SRV_MASTER);

  ulint i = 0;
  auto slot = table_get_nth_slot(i);

  while (slot->m_in_use) {
    i++;
    slot = table_get_nth_slot(i);
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
CondVar *Srv::suspend_thread(SrvSlot *slot) noexcept {
  ut_ad(mutex_own(&m_sys.m_trx_sys->m_mutex));

  auto type = slot->m_type;

  ut_ad(type >= SRV_WORKER);
  ut_ad(type <= SRV_MASTER);

  auto event = slot->m_event;

  slot->m_suspended = true;

  ut_ad(m_n_threads_active[type] > 0);

  --m_n_threads_active[type];

  os_event_reset(event);

  return event;
}

ulint Srv::release_threads(SrvThreadType type, ulint n) noexcept {
  ut_ad(type >= SRV_WORKER);
  ut_ad(type <= SRV_MASTER);
  ut_ad(n > 0);
  ut_ad(mutex_own(&m_sys.m_trx_sys->m_mutex));

  ulint count = 0;

  for (ulint i = 0; i < m_sys.m_config.m_srv.m_max_n_threads; i++) {

    auto slot = table_get_nth_slot(i);

    if (slot->m_in_use && slot->m_type == type && slot->m_suspended) {

      slot->m_suspended = false;

      ++m_n_threads_active[type];

      os_event_set(slot->m_event);

      ++count;

      if (count == n) {
        break;
      }
    }
  }

  return count;
}

Srv::Srv(System &sys) noexcept
    : m_sys(sys),
      m_threads(sys.m_config.m_srv.m_max_n_threads),
      m_client_table(sys.m_config.m_srv.m_max_n_threads) {

  mutex_create(&m_innodb_monitor_mutex, IF_DEBUG("monitor_mutex", )  Current_location());
  mutex_create(&m_monitor_file_mutex, IF_DEBUG("monitor_file_mutex", )  Current_location());

  for (auto &slot : m_threads) {
    slot.m_event = os_event_create(nullptr);
  }

  for (auto &slot : m_client_table) {
    slot.m_event = os_event_create(nullptr);
  }

  m_lock_timeout_thread_event = os_event_create(nullptr);

  UT_LIST_INIT(m_tasks);
}

Srv::~Srv() noexcept {
  for (auto &slot : m_threads) {
    os_event_free(slot.m_event);
  }

  for (auto &slot : m_client_table) {
    os_event_free(slot.m_event);
  }

  os_event_free(m_lock_timeout_thread_event);

  mutex_free(&m_monitor_file_mutex);
  mutex_free(&m_innodb_monitor_mutex);
}

/**
 * Reserves a slot in the thread table for the current user OS thread.
 * NOTE! The kernel mutex has to be reserved by the caller!
 *
 * @return	reserved slot
 */
SrvSlot *Srv::table_reserve_slot_for_user_thread() noexcept {
  ut_ad(mutex_own(&m_sys.m_trx_sys->m_mutex));

  ulint i{};
  auto slot = &m_client_table[i];

  while (slot->m_in_use) {
    i++;

    if (i >= m_sys.m_config.m_srv.m_max_n_threads) {

      log_err(std::format(
        "There appear to be {} user threads currently waiting"
        " inside InnoDB, which is the upper limit. Cannot continue"
        " operation. We intentionally generate a seg fault to print"
        " a stack trace on Linux. But first we print a list of waiting"
        " threads.",
        (ulong)i
      ));

      for (i = 0; i < m_sys.m_config.m_srv.m_max_n_threads; i++) {

        slot = &m_client_table[i];

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

    slot = &m_client_table[i];
  }

  ut_a(slot->m_in_use == false);

  slot->m_in_use = true;
  slot->m_id = os_thread_get_curr_id();
  slot->m_handle = os_thread_get_curr();

  return slot;
}

void Srv::suspend_user_thread(QueThr *thr) noexcept {
  double wait_time;
  ulint had_dict_lock;
  int64_t start_time = 0;
  int64_t finish_time;
  ulint diff_time;
  ulint sec;
  ulint ms;
  ulong lock_wait_timeout;

  ut_ad(!mutex_own(&m_sys.m_trx_sys->m_mutex));

  auto trx = thr_get_trx(thr);

  os_event_set(m_lock_timeout_thread_event);

  mutex_enter(&m_sys.m_trx_sys->m_mutex);

  trx->m_error_state = DB_SUCCESS;

  if (thr->state == QUE_THR_RUNNING) {

    ut_ad(thr->is_active == true);

    /* The lock has already been released or this transaction
    was chosen as a deadlock victim: no need to suspend */

    if (trx->m_was_chosen_as_deadlock_victim) {

      trx->m_error_state = DB_DEADLOCK;
      trx->m_was_chosen_as_deadlock_victim = false;
    }

    mutex_exit(&m_sys.m_trx_sys->m_mutex);

    return;
  }

  ut_ad(thr->is_active == false);

  auto slot = table_reserve_slot_for_user_thread();
  auto event = slot->m_event;

  slot->m_thr = thr;

  os_event_reset(event);

  slot->m_suspend_time = ut_time();

  if (thr->lock_state == QUE_THR_LOCK_ROW) {
    m_stats.m_n_lock_wait_count++;
    m_stats.m_n_lock_wait_current_count++;

    if (ut_usectime(&sec, &ms) == -1) {
      start_time = -1;
    } else {
      start_time = (int64_t)sec * 1000000 + ms;
    }
  }
  /* Wake the lock timeout monitor thread, if it is suspended */

  os_event_set(m_lock_timeout_thread_event);

  mutex_exit(&m_sys.m_trx_sys->m_mutex);

  had_dict_lock = trx->m_dict_operation_lock_mode;

  switch (had_dict_lock) {
    case RW_S_LATCH:
      /* Release foreign key check latch */
      m_sys.m_dict->unfreeze_data_dictionary(trx);
      break;
    case RW_X_LATCH:
      /* Release fast index creation latch */
      m_sys.m_dict->unlock_data_dictionary(trx);
      break;
  }

  ut_a(trx->m_dict_operation_lock_mode == 0);

  /* Suspend this thread and wait for the event. */

  os_event_wait(event);

  /* After resuming, reacquire the data dictionary latch if
  necessary. */

  switch (had_dict_lock) {
    case RW_S_LATCH:
      m_sys.m_dict->freeze_data_dictionary(trx);
      break;
    case RW_X_LATCH:
      m_sys.m_dict->lock_data_dictionary(trx);
      break;
  }

  mutex_enter(&m_sys.m_trx_sys->m_mutex);

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

    m_stats.m_n_lock_wait_current_count--;
    m_stats.m_n_lock_wait_time = m_stats.m_n_lock_wait_time + diff_time;

    if (diff_time > m_stats.m_n_lock_max_wait_time &&
        /* only update the variable if we successfully
        retrieved the start and finish times. See Bug#36819. */
        start_time != -1 && finish_time != -1) {
      m_stats.m_n_lock_max_wait_time = diff_time;
    }
  }

  if (trx->m_was_chosen_as_deadlock_victim) {

    trx->m_error_state = DB_DEADLOCK;
    trx->m_was_chosen_as_deadlock_victim = false;
  }

  mutex_exit(&m_sys.m_trx_sys->m_mutex);

  /* InnoDB system transactions (such as the purge, and
  incomplete transactions that are being rolled back after crash
  recovery) will use the global value of
  innodb_lock_wait_timeout, because trx->m_client_ctx == nullptr. */
  lock_wait_timeout = ulong(m_sys.m_config.m_lock.m_wait_timeout.count());

  if (trx->is_interrupted() || (lock_wait_timeout < 100000000 && wait_time > (double)lock_wait_timeout)) {

    trx->m_error_state = DB_LOCK_WAIT_TIMEOUT;
  }
}

void Srv::release_user_thread_if_suspended(QueThr *thr) noexcept {
  ut_ad(mutex_own(&m_sys.m_trx_sys->m_mutex));

  for (ulint i{}; i < m_sys.m_config.m_srv.m_max_n_threads; ++i) {

    auto slot = &m_client_table[i];

    if (slot->m_in_use && slot->m_thr == thr) {

      os_event_set(slot->m_event);

      return;
    }
  }
}

/**
 * Refreshes the values used to calculate per-second averages.
 */
void Srv::refresh_innodb_monitor_stats() noexcept {
  mutex_enter(&m_innodb_monitor_mutex);

  m_last_monitor_time = time(nullptr);


  m_sys.m_log->refresh_stats();

  m_sys.m_buf_pool->refresh_io_stats();

  m_n_rows_inserted_old = m_stats.m_n_rows_inserted;
  m_n_rows_updated_old = m_stats.m_n_rows_updated;
  m_n_rows_deleted_old = m_stats.m_n_rows_deleted;
  m_n_rows_read_old = m_stats.m_n_rows_read;

  mutex_exit(&m_innodb_monitor_mutex);
}

bool Srv::printf_innodb_monitor(FILE *ib_stream, bool nowait, ulint *trx_start, ulint *trx_end) noexcept {
  mutex_enter(&m_innodb_monitor_mutex);

  auto current_time = time(nullptr);

  /* We add 0.001 seconds to time_elapsed to prevent division
  by zero if two users happen to call SHOW INNODB STATUS at the same
  time */

  auto time_elapsed = difftime(current_time, m_last_monitor_time) + 0.001;

  m_last_monitor_time = time(nullptr);

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

  print_master_thread_info();


#ifdef WITH_FOREIGN_KEY
  /* Conceptually, m_innodb_monitor_mutex has a very high latching
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

  m_sys.m_lock_sys->print_info_all_transactions();

  log_warn(
    "--------\n"
    "I/O\n"
    "--------\n"
  );

  log_warn("{}", m_sys.m_aio->to_string().c_str());

  /* Only if lock_print_info_summary proceeds correctly,
  before we call the lock_print_info_all_transactions
  to print all the lock information. */
  auto ret = m_sys.m_lock_sys->print_info_summary(nowait);

  if (ret) {
    if (trx_start) {
      auto t = ftell(ib_stream);

      if (t < 0) {
        *trx_start = ULINT_UNDEFINED;
      } else {
        *trx_start = (ulint)t;
      }
    }

    m_sys.m_lock_sys->print_info_all_transactions();

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
  log_warn("Dictionary memory allocated ", m_sys.m_dict->m_size);

  m_sys.m_buf_pool->print_io(ib_stream);

  log_warn(
    "--------------\n"
    "ROW OPERATIONS\n"
    "--------------\n"
  );

  log_warn(std::format("{} read views open inside InnoDB", UT_LIST_GET_LEN(m_sys.m_trx_sys->m_view_list)));

  auto n_reserved = m_sys.m_fil->space_get_n_reserved_extents(0);

  if (n_reserved > 0) {
    log_warn(std::format("{} tablespace extents now reserved for B-tree split operationsn", n_reserved));
  }

#ifdef UNIV_LINUX
  log_warn(std::format(
    "Main thread process no. {}, id {}, state: {}", m_main_thread_process_no, m_main_thread_id, m_main_thread_op_info
  ));
#else
  log_warn(std::format("Main thread id {}, state: {}", m_main_thread_id, m_main_thread_op_info));
#endif /* UNIV_LINUX */

  log_warn(std::format(
    "Number of rows inserted {}, updated {}, deleted {}, read {}",
    m_stats.m_n_rows_inserted,
    m_stats.m_n_rows_updated,
    m_stats.m_n_rows_deleted,
    m_stats.m_n_rows_read
  ));

  log_warn(std::format(
    "{:.2f} inserts/s, {:.2f} updates/s,"
    " {:.2f} deletes/s, {:.2f} reads/s",
    (m_stats.m_n_rows_inserted - m_n_rows_inserted_old) / time_elapsed,
    (m_stats.m_n_rows_updated - m_n_rows_updated_old) / time_elapsed,
    (m_stats.m_n_rows_deleted - m_n_rows_deleted_old) / time_elapsed,
    (m_stats.m_n_rows_read - m_n_rows_read_old) / time_elapsed
  ));

  m_n_rows_inserted_old = m_stats.m_n_rows_inserted;

  m_n_rows_updated_old = m_stats.m_n_rows_updated;

  m_n_rows_deleted_old = m_stats.m_n_rows_deleted;

  m_n_rows_read_old = m_stats.m_n_rows_read;

  log_warn(
    "----------------------------\n"
    "END OF INNODB MONITOR OUTPUT\n"
    "============================\n"
  );

  mutex_exit(&m_innodb_monitor_mutex);

  return ret;
}

void Srv::export_innodb_status() noexcept {
  mutex_enter(&m_innodb_monitor_mutex);

  m_export_vars.innodb_data_pending_reads = m_sys.m_buf_pool->m_n_pend_reads;
  m_export_vars.innodb_data_pending_writes = m_sys.m_buf_pool->get_n_pending_ios() - m_sys.m_buf_pool->m_n_pend_reads;
  m_export_vars.innodb_data_pending_fsyncs = m_sys.m_fil->get_pending_log_flushes() + m_sys.m_fil->get_pending_tablespace_flushes();
  m_export_vars.innodb_data_fsyncs = m_sys.m_fil->get_log_flushes();
  m_export_vars.innodb_data_read = m_stats.m_data_read;
  m_export_vars.innodb_data_reads = m_sys.m_buf_pool->m_stat.n_pages_read;
  m_export_vars.innodb_data_writes = m_sys.m_buf_pool->m_stat.n_pages_written;
  m_export_vars.innodb_data_written = m_stats.m_data_written;
  m_export_vars.innodb_buffer_pool_read_requests = m_sys.m_buf_pool->m_stat.n_page_gets;
  m_export_vars.innodb_buffer_pool_write_requests = m_sys.m_buf_pool->m_write_requests;
  m_export_vars.innodb_buffer_pool_wait_free = m_stats.m_buf_pool_wait_free;
  m_export_vars.innodb_buffer_pool_pages_flushed = m_stats.m_buf_pool_flushed;
  m_export_vars.innodb_buffer_pool_reads = m_stats.m_buf_pool_reads;
  m_export_vars.innodb_buffer_pool_read_ahead = m_sys.m_buf_pool->m_stat.n_ra_pages_read;
  m_export_vars.innodb_buffer_pool_read_ahead_evicted = m_sys.m_buf_pool->m_stat.n_ra_pages_evicted;
  m_export_vars.innodb_buffer_pool_pages_data = UT_LIST_GET_LEN(m_sys.m_buf_pool->m_LRU_list);
  m_export_vars.innodb_buffer_pool_pages_dirty = UT_LIST_GET_LEN(m_sys.m_buf_pool->m_flush_list);
  m_export_vars.innodb_buffer_pool_pages_free = UT_LIST_GET_LEN(m_sys.m_buf_pool->m_free_list);

  ut_d(m_export_vars.innodb_buffer_pool_pages_latched = m_sys.m_buf_pool->get_latched_pages_number());

  m_export_vars.innodb_buffer_pool_pages_total = m_sys.m_buf_pool->m_curr_size;

  m_export_vars.innodb_buffer_pool_pages_misc =
    m_sys.m_buf_pool->m_curr_size - UT_LIST_GET_LEN(m_sys.m_buf_pool->m_LRU_list) - UT_LIST_GET_LEN(m_sys.m_buf_pool->m_free_list);

  m_export_vars.innodb_have_atomic_builtins = 1;
  m_export_vars.innodb_page_size = UNIV_PAGE_SIZE;
  m_export_vars.innodb_log_waits = m_stats.m_log_waits;
  m_export_vars.innodb_os_log_written = m_stats.m_os_log_written;
  m_export_vars.innodb_os_log_fsyncs = m_sys.m_fil->get_log_flushes();
  m_export_vars.innodb_os_log_pending_fsyncs = m_sys.m_fil->get_pending_log_flushes();
  m_export_vars.innodb_os_log_pending_writes = m_stats.m_os_log_pending_writes;
  m_export_vars.innodb_log_write_requests = m_stats.m_log_write_requests;
  m_export_vars.innodb_log_writes = m_stats.m_log_writes;
  m_export_vars.innodb_dblwr_pages_written = m_stats.m_dblwr_pages_written;
  m_export_vars.innodb_dblwr_writes = m_stats.m_dblwr_writes;
  m_export_vars.innodb_pages_created = m_sys.m_buf_pool->m_stat.n_pages_created;
  m_export_vars.innodb_pages_read = m_sys.m_buf_pool->m_stat.n_pages_read;
  m_export_vars.innodb_pages_written = m_sys.m_buf_pool->m_stat.n_pages_written;
  m_export_vars.innodb_row_lock_waits = m_stats.m_n_lock_wait_count;
  m_export_vars.innodb_row_lock_current_waits = m_stats.m_n_lock_wait_current_count;
  m_export_vars.innodb_row_lock_time = m_stats.m_n_lock_wait_time / 1000;

  if (m_stats.m_n_lock_wait_count > 0) {
    m_export_vars.innodb_row_lock_time_avg = (ulint)(m_stats.m_n_lock_wait_time / 1000 / m_stats.m_n_lock_wait_count);
  } else {
    m_export_vars.innodb_row_lock_time_avg = 0;
  }
  m_export_vars.innodb_row_lock_time_max = m_stats.m_n_lock_max_wait_time / 1000;
  m_export_vars.innodb_rows_read = m_stats.m_n_rows_read;
  m_export_vars.innodb_rows_inserted = m_stats.m_n_rows_inserted;
  m_export_vars.innodb_rows_updated = m_stats.m_n_rows_updated;
  m_export_vars.innodb_rows_deleted = m_stats.m_n_rows_deleted;

  mutex_exit(&m_innodb_monitor_mutex);
}

void Srv::monitor_thread() noexcept {
  double time_elapsed;
  time_t current_time;

  m_last_monitor_time = time(nullptr);

  ulint mutex_skipped{};
  auto last_table_monitor_time = time(nullptr);
  auto last_tablespace_monitor_time = time(nullptr);
  auto last_monitor_time = time(nullptr);
  auto last_srv_print_monitor = m_print_innodb_monitor;

loop:
  m_monitor_active = true;

  /* Wake up every 5 seconds to see if we need to print
  monitor information. */

  os_thread_sleep(5000000);

  current_time = time(nullptr);

  time_elapsed = difftime(current_time, last_monitor_time);

  if (time_elapsed > 15) {
    last_monitor_time = time(nullptr);

    if (m_print_innodb_monitor) {
      /* Reset mutex_skipped counter everytime
      m_print_innodb_monitor changes. This is to
      ensure we will not be blocked by m_sys.m_trx_sys->m_mutex
      for short duration information printing,
      such as requested by sync_array_print_long_waits() */
      if (!last_srv_print_monitor) {
        mutex_skipped = 0;
        last_srv_print_monitor = true;
      }

      if (!printf_innodb_monitor(stderr, MUTEX_NOWAIT(mutex_skipped), nullptr, nullptr)) {
        ++mutex_skipped;
      } else {
        /* Reset the counter */
        mutex_skipped = 0;
      }
    } else {
      last_srv_print_monitor = false;
    }

    if (m_sys.m_config.m_srv.m_status) {
      mutex_enter(&m_monitor_file_mutex);

      if (!printf_innodb_monitor(stderr, MUTEX_NOWAIT(mutex_skipped), nullptr, nullptr)) {
        ++mutex_skipped;
      } else {
        mutex_skipped = 0;
      }

      mutex_exit(&m_monitor_file_mutex);
    }

    if (m_print_innodb_tablespace_monitor && difftime(current_time, last_tablespace_monitor_time) > 60) {
      last_tablespace_monitor_time = time(nullptr);

      log_warn("================================================");

      log_warn(
        " INNODB TABLESPACE MONITOR OUTPUT\n"
        "================================================\n"
      );

      m_sys.m_fsp->print(0);
      log_warn("Validating tablespace");

      {
        auto success = m_sys.m_fsp->validate(0);
        ut_a(success);
      }

      log_warn(
        "Validation ok\n"
        "---------------------------------------\n"
        "END OF INNODB TABLESPACE MONITOR OUTPUT\n"
        "=======================================\n"
      );
    }

    if (m_print_innodb_table_monitor && difftime(current_time, last_table_monitor_time) > 60) {

      last_table_monitor_time = time(nullptr);

      log_warn(
        "===========================================\n"
        " INNODB TABLE MONITOR OUTPUT\n"
        "==========================================="
      );

      m_sys.m_dict->to_string();

      log_warn(
        "-----------------------------------\n"
        "END OF INNODB TABLE MONITOR OUTPUT\n"
        "==================================\n"
      );
    }
  }

  if (m_shutdown_state >= SRV_SHUTDOWN_CLEANUP) {
    goto exit_func;
  }

  if (m_print_innodb_monitor || m_sys.m_lock_sys->is_print_lock_monitor_set() || m_print_innodb_tablespace_monitor ||
      m_print_innodb_table_monitor) {

    goto loop;
  }

  m_monitor_active = false;

  goto loop;

exit_func:
  m_monitor_active = false;



  return;
}

void Srv::lock_timeout_thread() noexcept {
  for (;;) {
    /* When someone is waiting for a lock, we wake up every second
    and check if a timeout has passed for a lock wait */

    os_thread_sleep(1000000);

    m_lock_timeout_active = true;

    mutex_enter(&m_sys.m_trx_sys->m_mutex);

    auto some_waits = false;

    /* Check of all slots if a thread is waiting there, and if it
    has exceeded the time limit */

    for (ulint i = 0; i < m_sys.m_config.m_srv.m_max_n_threads; i++) {

      auto slot = &m_client_table[i];

      if (slot->m_in_use) {
        some_waits = true;
        auto wait_time = ut_difftime(ut_time(), slot->m_suspend_time);
        auto trx = thr_get_trx(slot->m_thr);
        auto lock_wait_timeout = ulong(m_sys.m_config.m_lock.m_wait_timeout.count());

        if (trx->is_interrupted() || (lock_wait_timeout < 100000000 && (wait_time > (double)lock_wait_timeout || wait_time < 0))) {

          /* Timeout exceeded or a wrap-around in system time counter:
          cancel the lock request queued by the transaction and release possible
          other transactions waiting behind; it is possible that the lock has
          already been granted: in that case do nothing */

          if (trx->m_wait_lock) {
            m_sys.m_lock_sys->cancel_waiting_and_release(trx->m_wait_lock);
          }
        }
      }
    }

    os_event_reset(m_lock_timeout_thread_event);

    mutex_exit(&m_sys.m_trx_sys->m_mutex);

    if (m_shutdown_state >= SRV_SHUTDOWN_CLEANUP) {
      m_lock_timeout_active = false;



      return;
    }

    if (!some_waits) {
      m_lock_timeout_active = false;
    }
  }
}

void Srv::error_monitor_thread() noexcept {
  auto old_lsn = m_start_lsn;

loop:
  m_error_monitor_active = true;

  /* Try to track a strange bug reported by Harald Fuchs and others,
  where the lsn seems to decrease at times */

  auto new_lsn = m_sys.m_log->get_lsn();

  if (new_lsn < old_lsn) {
    log_err(std::format(
      "old log sequence number {} was greater than the new log sequence number {}!"
      " Please submit a bug report, check the InnoDB website for details",
      old_lsn,
      new_lsn
    ));
  }

  old_lsn = new_lsn;

  if (difftime(time(nullptr), m_last_monitor_time) > 60) {
    /* We referesh InnoDB Monitor values so that averages are
    printed from at most 60 last seconds */

    refresh_innodb_monitor_stats();
  }

  /* Update the statistics collected for deciding LRU eviction policy. */
  m_sys.m_buf_pool->m_LRU->stat_update();

  /* Update the statistics collected for flush rate policy. */
  m_sys.m_buf_pool->m_flusher->stat_update();

  os_thread_sleep(1000000);

  if (m_shutdown_state < SRV_SHUTDOWN_CLEANUP) {

    goto loop;
  }

  m_error_monitor_active = false;



  return;
}

void Srv::active_wake_master_thread() noexcept {
  ++m_activity_count;

  if (m_n_threads_active[SRV_MASTER] == 0) {

    mutex_enter(&m_sys.m_trx_sys->m_mutex);

    release_threads(SRV_MASTER, 1);

    mutex_exit(&m_sys.m_trx_sys->m_mutex);
  }
}

void Srv::wake_master_thread() noexcept {
  ++m_activity_count;

  mutex_enter(&m_sys.m_trx_sys->m_mutex);

  release_threads(SRV_MASTER, 1);

  mutex_exit(&m_sys.m_trx_sys->m_mutex);
}

/**
 * The master thread is tasked to ensure that flush of log file happens
 * once every second in the background. This is to ensure that not more
 * than one second of trxs are lost in case of crash when
 * innodb_flush_logs_at_trx_commit != 1
 */
void Srv::sync_log_buffer_in_background() noexcept {
  auto current_time = time(nullptr);

  m_main_thread_op_info = "flushing log";

  if (difftime(current_time, m_last_log_flush_time) >= 1) {
    m_sys.m_log->buffer_sync_in_background(true);
    m_last_log_flush_time = current_time;
    m_log_writes_and_flush++;
  }
}

void Srv::master_thread() noexcept {
  CondVar *event;
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
  m_main_thread_process_no = os_proc_get_number();
#endif /* UNIV_LINUX */

  m_main_thread_id = os_thread_pf(os_thread_get_curr_id());

  auto slot_no = table_reserve_slot(SRV_MASTER);

  mutex_enter(&m_sys.m_trx_sys->m_mutex);

  m_n_threads_active[SRV_MASTER]++;

  mutex_exit(&m_sys.m_trx_sys->m_mutex);

loop:
  /* When there is database activity by users, we cycle in this loop */

  m_main_thread_op_info = "reserving kernel mutex";

  n_ios_very_old = m_sys.m_log->m_n_log_ios + m_sys.m_buf_pool->m_stat.n_pages_read + m_sys.m_buf_pool->m_stat.n_pages_written;
  mutex_enter(&m_sys.m_trx_sys->m_mutex);

  /* Store the user activity counter at the start of this loop */
  old_activity_count = m_activity_count;

  mutex_exit(&m_sys.m_trx_sys->m_mutex);

  if (m_sys.m_config.m_recv.m_force_recovery >= IB_RECOVERY_NO_BACKGROUND) {

    goto suspend_thread;
  }

  /* We run the following loop approximately once per second
  when there is database activity */

  m_last_log_flush_time = time(nullptr);
  /* No need to sleep if user has signalled shutdown. */
  skip_sleep = (m_shutdown_state != SRV_SHUTDOWN_NONE);

  for (i = 0; i < 10; i++) {
    m_main_thread_op_info = "sleeping";
    m_main_1_second_loops++;

    if (!skip_sleep) {

      os_thread_sleep(1000000);
      m_main_sleeps++;
    }

    /* No need to sleep if user has signalled shutdown. */
    skip_sleep = (m_shutdown_state != SRV_SHUTDOWN_NONE);

    /* No need to sleep if user has signalled shutdown. */

    /* ALTER TABLE on Unix requires that the table handler
    can drop tables lazily after there no longer are SELECT
    queries to them. */

    m_main_thread_op_info = "doing background drop tables";

    (void)m_sys.m_dict->m_ddl.drop_tables_in_background();

    m_main_thread_op_info = "";

    if (m_fast_shutdown != IB_SHUTDOWN_NORMAL && m_shutdown_state > SRV_SHUTDOWN_NONE) {

      goto background_loop;
    }

    /* Flush logs if needed */
    sync_log_buffer_in_background();

    m_main_thread_op_info = "making checkpoint";
    m_sys.m_log->free_check();

    n_pend_ios = m_sys.m_buf_pool->get_n_pending_ios() + m_sys.m_log->m_n_pending_writes;

    n_ios = m_sys.m_log->m_n_log_ios + m_sys.m_buf_pool->m_stat.n_pages_read + m_sys.m_buf_pool->m_stat.n_pages_written;

    if (unlikely(m_sys.m_buf_pool->get_modified_ratio_pct() > m_sys.m_config.m_buf_pool.m_max_modified_pct)) {

      /* Try to keep the number of modified pages in the
      buffer pool under the limit wished by the user */

      m_main_thread_op_info = "flushing buffer pool pages";
      n_pages_flushed = m_sys.m_buf_pool->m_flusher->batch(BUF_FLUSH_LIST, pct_io(100), IB_UINT64_T_MAX);

      /* If we had to do the flush, it may have taken
      even more than 1 second, and also, there may be more
      to flush. Do not sleep 1 second during the next
      iteration of this loop. */

      skip_sleep = true;

    } else if (m_sys.m_config.m_buf_pool.m_adaptive_flushing) {

      /* Try to keep the rate of flushing of dirty
      pages such that redo log generation does not
      produce bursts of IO at checkpoint time. */
      ulint n_flush = m_sys.m_buf_pool->m_flusher->get_desired_flush_rate();

      if (n_flush) {
        m_main_thread_op_info = "flushing buffer pool pages";
        n_flush = std::min<ulint>(pct_io(100), n_flush);
        n_pages_flushed = m_sys.m_buf_pool->m_flusher->batch(BUF_FLUSH_LIST, n_flush, IB_ULONGLONG_MAX);

        if (n_flush == pct_io(100)) {
          skip_sleep = true;
        }
      }
    }

    if (m_activity_count == old_activity_count) {

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

  n_pend_ios = m_sys.m_buf_pool->get_n_pending_ios() + m_sys.m_log->m_n_pending_writes;
  n_ios = m_sys.m_log->m_n_log_ios + m_sys.m_buf_pool->m_stat.n_pages_read + m_sys.m_buf_pool->m_stat.n_pages_written;

  ++m_main_10_second_loops;

  if (n_pend_ios < pct_io(3) && (n_ios - n_ios_very_old < pct_io(200))) {

    m_main_thread_op_info = "flushing buffer pool pages";
    m_sys.m_buf_pool->m_flusher->batch(BUF_FLUSH_LIST, pct_io(100), IB_ULONGLONG_MAX);

    /* Flush logs if needed */
    sync_log_buffer_in_background();
  }

  /* Flush logs if needed */
  sync_log_buffer_in_background();

  /* We run a full purge every 10 seconds, even if the server
  were active */
  do {

    if (m_fast_shutdown != IB_SHUTDOWN_NORMAL && m_shutdown_state > 0) {

      goto background_loop;
    }

    m_main_thread_op_info = "purging";
    n_pages_purged = m_sys.m_trx_sys->m_purge->run();

    /* Flush logs if needed */
    sync_log_buffer_in_background();

  } while (n_pages_purged);

  m_main_thread_op_info = "flushing buffer pool pages";

  /* Flush a few oldest pages to make a new checkpoint younger */

  if (m_sys.m_buf_pool->get_modified_ratio_pct() > m_sys.m_config.m_buf_pool.m_max_modified_pct) {

    /* If there are lots of modified pages in the buffer pool
    (> 70 %), we assume we can afford reserving the disk(s) for
    the time it requires to flush 100 pages */

    n_pages_flushed = m_sys.m_buf_pool->m_flusher->batch(BUF_FLUSH_LIST, pct_io(100), IB_UINT64_T_MAX);
  } else {
    /* Otherwise, we only flush a small number of pages so that
    we do not unnecessarily use much disk i/o capacity from
    other work */

    n_pages_flushed = m_sys.m_buf_pool->m_flusher->batch(BUF_FLUSH_LIST, pct_io(10), IB_UINT64_T_MAX);
  }

  m_main_thread_op_info = "making checkpoint";

  /* Make a new checkpoint about once in 10 seconds */

  {
    auto success = m_sys.m_log->checkpoint(true, false);

    if (!success) {
      log_info("Checkpoint already running");
    }
  }

  m_main_thread_op_info = "reserving kernel mutex";

  mutex_enter(&m_sys.m_trx_sys->m_mutex);

  /* When there is database activity, we jump from here back to
  the start of loop */

  if (m_activity_count != old_activity_count) {
    mutex_exit(&m_sys.m_trx_sys->m_mutex);
    goto loop;
  }

  mutex_exit(&m_sys.m_trx_sys->m_mutex);

  /* If the database is quiet, we enter the background loop */

background_loop:

  /* In this loop we run background operations when the server
  is quiet from user activity. Also in the case of a shutdown, we
  loop here, flushing the buffer pool to the data files. */

  /* The server has been quiet for a while: start running background
  operations */
  m_main_background_loops++;
  m_main_thread_op_info = "doing background drop tables";

  n_tables_to_drop = m_sys.m_dict->m_ddl.drop_tables_in_background();

  if (n_tables_to_drop > 0) {
    /* Do not monopolize the CPU even if there are tables waiting
    in the background drop queue. (It is essentially a bug if
    user tries to drop a table while there are still open handles
    to it and we had to put it to the background drop queue.) */

    os_thread_sleep(100000);
  }

  m_main_thread_op_info = "purging";

  /* Run a full purge */
  do {
    if (m_fast_shutdown != IB_SHUTDOWN_NORMAL && m_shutdown_state > 0) {

      break;
    }

    m_main_thread_op_info = "purging";
    n_pages_purged = m_sys.m_trx_sys->m_purge->run();

    /* Flush logs if needed */
    sync_log_buffer_in_background();

  } while (n_pages_purged);

  m_main_thread_op_info = "reserving kernel mutex";

  mutex_enter(&m_sys.m_trx_sys->m_mutex);
  if (m_activity_count != old_activity_count) {
    mutex_exit(&m_sys.m_trx_sys->m_mutex);
    goto loop;
  }
  mutex_exit(&m_sys.m_trx_sys->m_mutex);

  m_main_thread_op_info = "reserving kernel mutex";

  mutex_enter(&m_sys.m_trx_sys->m_mutex);

  if (m_activity_count != old_activity_count) {
    mutex_exit(&m_sys.m_trx_sys->m_mutex);
    goto loop;
  }

  mutex_exit(&m_sys.m_trx_sys->m_mutex);

flush_loop:
  m_main_thread_op_info = "flushing buffer pool pages";
  m_main_flush_loops++;
  if (m_fast_shutdown != IB_SHUTDOWN_NO_BUFPOOL_FLUSH) {
    n_pages_flushed = m_sys.m_buf_pool->m_flusher->batch(BUF_FLUSH_LIST, pct_io(100), IB_UINT64_T_MAX);
  } else {
    /* In the fastest shutdown we do not flush the buffer pool
    to data files: we set n_pages_flushed to 0 artificially. */

    n_pages_flushed = 0;
  }

  m_main_thread_op_info = "reserving kernel mutex";

  mutex_enter(&m_sys.m_trx_sys->m_mutex);
  if (m_activity_count != old_activity_count) {
    mutex_exit(&m_sys.m_trx_sys->m_mutex);
    goto loop;
  }
  mutex_exit(&m_sys.m_trx_sys->m_mutex);

  m_main_thread_op_info = "waiting for buffer pool flush to end";
  m_sys.m_buf_pool->m_flusher->wait_batch_end(BUF_FLUSH_LIST);

  /* Flush logs if needed */
  sync_log_buffer_in_background();

  m_main_thread_op_info = "making checkpoint";

  {
    auto success = m_sys.m_log->checkpoint(true, false);

    if (!success) {
      log_info("Checkpoint already running");
    }
  }

  if (m_sys.m_buf_pool->get_modified_ratio_pct() > m_sys.m_config.m_buf_pool.m_max_modified_pct) {

    /* Try to keep the number of modified pages in the
    buffer pool under the limit wished by the user */

    goto flush_loop;
  }

  m_main_thread_op_info = "reserving kernel mutex";

  mutex_enter(&m_sys.m_trx_sys->m_mutex);
  if (m_activity_count != old_activity_count) {
    mutex_exit(&m_sys.m_trx_sys->m_mutex);
    goto loop;
  }
  mutex_exit(&m_sys.m_trx_sys->m_mutex);

  /* Keep looping in the background loop if still work to do */

  if (m_fast_shutdown != IB_SHUTDOWN_NORMAL && m_shutdown_state > 0) {
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
  m_main_thread_op_info = "suspending";

  mutex_enter(&m_sys.m_trx_sys->m_mutex);

  if (m_sys.m_dict->m_ddl.get_background_drop_list_len() > 0) {
    mutex_exit(&m_sys.m_trx_sys->m_mutex);

    goto loop;
  }

  auto slot = table_get_nth_slot(slot_no);
  event = suspend_thread(slot);

  mutex_exit(&m_sys.m_trx_sys->m_mutex);

  /* DO NOT CHANGE THIS STRING. innobase_start_or_create()
  waits for database activity to die down when converting < 4.1.x
  databases, and relies on this string being exactly as it is. InnoDB
  manual also mentions this string in several places. */
  m_main_thread_op_info = "waiting for server activity";

  os_event_wait(event);

  if (m_shutdown_state == SRV_SHUTDOWN_EXIT_THREADS) {
    /* This is only extra safety, the thread should exit
    already when the event wait ends */

    return;
  } else {
    /* When there is user activity, InnoDB will set the event and the
    main thread goes back to loop. */

    goto loop;
  }
}

void Srv::que_task_enqueue_low(QueThr *thr) noexcept {
  ut_ad(mutex_own(&m_sys.m_trx_sys->m_mutex));

  UT_LIST_ADD_LAST(m_tasks, thr);

  release_threads(SRV_WORKER, 1);
}

void Srv::request_monitor_print() noexcept {
  m_print_innodb_monitor = true;
  os_event_set(m_lock_timeout_thread_event);
}

void Srv::create_thread(void *(*f)(void *), void *arg) noexcept {
  /* Count it before it runs, so that shutdown can't miss it. */
  m_n_background_threads.fetch_add(1, std::memory_order_release);

  os_thread_id_t id;

  os_thread_create(f, arg, &id);
}

void Srv::start_threads() noexcept {
  /* Watches the timeouts for lock waits */
  create_thread(&thread_main<&Srv::lock_timeout_thread>, this);

  /* Warns of long semaphore waits */
  create_thread(&thread_main<&Srv::error_monitor_thread>, this);

  /* Prints the InnoDB monitor info */
  create_thread(&thread_main<&Srv::monitor_thread>, this);

  /* Does purge and other utility operations */
  create_thread(&thread_main<&Srv::master_thread>, this);
}
