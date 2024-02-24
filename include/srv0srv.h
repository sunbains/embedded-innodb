/** 
Copyright (c) 1995, 2010, Innobase Oy. All Rights Reserved.
Copyright (c) 2008, 2009, Google Inc.
Copyright (c) 2009, Percona Inc.

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

/** @file include/srv0srv.h
The server main program

Created 10/10/1995 Heikki Tuuri
*******************************************************/

#ifndef srv0srv_h
#define srv0srv_h

#include "univ.i"
#ifndef UNIV_HOTBACKUP
#include "sync0sync.h"
#include "os0sync.h"
#include "que0types.h"
#include "trx0types.h"
#include "api0api.h"

extern const char*	srv_main_thread_op_info;

/* When this event is set the lock timeout and InnoDB monitor
thread starts running */
extern os_event_t	srv_lock_timeout_thread_event;

/* If the last data file is auto-extended, we add this many pages to it
at a time */
#define SRV_AUTO_EXTEND_INCREMENT	\
	(srv_auto_extend_increment * ((1024 * 1024) / UNIV_PAGE_SIZE))

/* FIXME: This is set to TRUE if the user has requested it. */
extern ibool	srv_lower_case_table_names;

/* FIXME: This is a session variable. */
extern ulint	ses_lock_wait_timeout;
/* FIXME: This is a session variable. */
extern ibool	ses_rollback_on_timeout;

/* Server parameters which are read from the initfile */

extern char*	srv_data_home;
extern char*	srv_log_group_home_dir;

#ifdef UNIV_LOG_ARCHIVE
extern char*	srv_arch_dir;
#endif /* UNIV_LOG_ARCHIVE */

/** store to its own file each table created by an user; data
dictionary tables are in the system tablespace 0 */
extern ibool	srv_file_per_table;
/** The file format to use on new *.ibd files. */
extern ulint	srv_file_format;
/** Whether to check file format during startup.  A value of
DICT_TF_FORMAT_MAX + 1 means no checking ie. FALSE.  The default is to
set it to the highest format we support. */
extern ulint	srv_check_file_format_at_startup;
#endif /* !UNIV_HOTBACKUP */

extern ulint	srv_n_data_files;
extern ulint*	srv_data_file_sizes;
extern ulint*	srv_data_file_is_raw_partition;

extern ibool	srv_auto_extend_last_data_file;
extern ulint	srv_last_file_size_max;
#ifndef UNIV_HOTBACKUP
extern ulong	srv_auto_extend_increment;

extern ibool	srv_created_new_raw;

extern ulint	srv_n_log_files;
extern ulint	srv_log_file_size;
extern ulint	srv_log_file_curr_size;
extern ulint	srv_log_buffer_size;
extern ulint	srv_log_buffer_curr_size;
extern ulong	srv_flush_log_at_trx_commit;
extern ibool	srv_adaptive_flushing;

extern ibool	srv_use_sys_malloc;

extern ulint	srv_buf_pool_size;	/*!< requested size in bytes */
extern ulint	srv_buf_pool_old_size;	/*!< previously requested size */
extern ulint	srv_buf_pool_curr_size;	/*!< current size in bytes */
extern ulint	srv_mem_pool_size;
extern ulint	srv_lock_table_size;

extern ulint	srv_n_file_io_threads;
extern ulong	srv_read_ahead_threshold;
extern ulint	srv_n_read_io_threads;
extern ulint	srv_n_write_io_threads;

/* Number of IO operations per second the server can do */
extern ulong    srv_io_capacity;
/* Returns the number of IO operations that is X percent of the
capacity. PCT_IO(5) -> returns the number of IO operations that
is 5% of the max where max is srv_io_capacity.  */
#define PCT_IO(p) ((ulong) (srv_io_capacity * ((double) p / 100.0)))

#ifdef UNIV_LOG_ARCHIVE
extern ibool	srv_log_archive_on;
extern ibool	srv_archive_recovery;
extern ib_uint64_t	srv_archive_recovery_limit_lsn;
#endif /* UNIV_LOG_ARCHIVE */

extern ulint	srv_unix_file_flush_method;
extern ulint	srv_win_file_flush_method;

extern ulint	srv_max_n_open_files;

extern ulint	srv_max_dirty_pages_pct;

extern ulint	srv_force_recovery;
extern ulong	srv_thread_concurrency;

extern ulint	srv_max_n_threads;

extern ulint	srv_conc_n_waiting_threads;

extern ib_shutdown_t	srv_fast_shutdown;	

extern ibool	srv_innodb_status;

extern unsigned long long	srv_stats_sample_pages;

extern ibool	srv_use_doublewrite_buf;
extern ibool	srv_use_checksums;

extern ibool	srv_set_thread_priorities;
extern int	srv_query_thread_priority;

extern ulong	srv_max_buf_pool_modified_pct;
extern ulong	srv_max_purge_lag;

/*-------------------------------------------*/

extern ulint	srv_n_rows_inserted;
extern ulint	srv_n_rows_updated;
extern ulint	srv_n_rows_deleted;
extern ulint	srv_n_rows_read;

extern ibool	srv_print_innodb_monitor;
extern ibool	srv_print_innodb_lock_monitor;
extern ibool	srv_print_innodb_tablespace_monitor;
extern ibool	srv_print_verbose_log;
extern ibool	srv_print_innodb_table_monitor;

extern ibool	srv_lock_timeout_active;
extern ibool	srv_monitor_active;
extern ibool	srv_error_monitor_active;

extern ulong	srv_n_spin_wait_rounds;
extern ulong	srv_spin_wait_delay;

#ifdef UNIV_DEBUG
extern	ibool	srv_print_thread_releases;
extern	ibool	srv_print_lock_waits;
extern	ibool	srv_print_buf_io;
extern	ibool	srv_print_log_io;
extern	ibool	srv_print_latch_waits;
#else /* UNIV_DEBUG */
# define srv_print_thread_releases	FALSE
# define srv_print_lock_waits		FALSE
# define srv_print_buf_io		FALSE
# define srv_print_log_io		FALSE
# define srv_print_latch_waits		FALSE
#endif /* UNIV_DEBUG */

extern ulint	srv_activity_count;
extern ulint	srv_fatal_semaphore_wait_threshold;
extern ulint	srv_dml_needed_delay;

extern mutex_t*	kernel_mutex_temp;/* mutex protecting the server, trx structs,
				query threads, and lock table: we allocate
				it from dynamic memory to get it to the
				same DRAM page as other hotspot semaphores */
#define kernel_mutex (*kernel_mutex_temp)

#define SRV_MAX_N_IO_THREADS	130

/* the number of the log write requests done */
extern ulint srv_log_write_requests;

/* the number of physical writes to the log performed */
extern ulint srv_log_writes;

/* amount of data written to the log files in bytes */
extern ulint srv_os_log_written;

/* amount of writes being done to the log files */
extern ulint srv_os_log_pending_writes;

/* we increase this counter, when there we don't have enough space in the
log buffer and have to flush it */
extern ulint srv_log_waits;

/* variable that counts amount of data read in total (in bytes) */
extern ulint srv_data_read;

/* here we count the amount of data written in total (in bytes) */
extern ulint srv_data_written;

/* this variable counts the amount of times, when the doublewrite buffer
was flushed */
extern ulint srv_dblwr_writes;

/* here we store the number of pages that have been flushed to the
doublewrite buffer */
extern ulint srv_dblwr_pages_written;

/* in this variable we store the number of write requests issued */
extern ulint srv_buf_pool_write_requests;

/* here we store the number of times when we had to wait for a free page
in the buffer pool. It happens when the buffer pool is full and we need
to make a flush, in order to be able to read or create a page. */
extern ulint srv_buf_pool_wait_free;

/* variable to count the number of pages that were written from the
buffer pool to disk */
extern ulint srv_buf_pool_flushed;

/** Number of buffer pool reads that led to the
reading of a disk page */
extern ulint srv_buf_pool_reads;

/* In this structure we store status variables to be passed to the client. */
typedef struct export_var_struct export_struc;

/** Status variables to be passed to MySQL */
extern export_struc export_vars;

#endif /* !UNIV_HOTBACKUP */

/** Types of raw partitions in innodb_data_file_path */
enum {
	SRV_NOT_RAW = 0,	/*!< Not a raw partition */
	SRV_NEW_RAW,		/*!< A 'newraw' partition, only to be
				initialized */
	SRV_OLD_RAW		/*!< An initialized raw partition */
};

/** Alternatives for the file flush option in Unix; see the InnoDB manual
about what these mean */
enum {
	SRV_UNIX_FSYNC = 1,	/*!< fsync, the default */
	SRV_UNIX_O_DSYNC,	/*!< open log files in O_SYNC mode */
	SRV_UNIX_LITTLESYNC,	/*!< do not call os_file_flush()
				when writing data files, but do flush
				after writing to log files */
	SRV_UNIX_NOSYNC,	/*!< do not flush after writing */
	SRV_UNIX_O_DIRECT	/*!< invoke os_file_set_nocache() on
				data files */
};

/** Alternatives for file i/o in Windows */
enum {
	SRV_WIN_IO_NORMAL = 1,	/*!< buffered I/O */
	SRV_WIN_IO_UNBUFFERED	/*!< unbuffered I/O; this is the default */
};

/** Alternatives for srv_force_recovery. Non-zero values are intended
to help the user get a damaged database up so that he can dump intact
tables and rows with SELECT INTO OUTFILE. The database must not otherwise
be used with these options! A bigger number below means that all precautions
of lower numbers are included.

NOTE: The order below is important, the code uses <= and >= to check
for recovery levels. */
typedef enum ib_recovery_enum {
	IB_RECOVERY_DEFAULT,		/*!< stop on all the errors that are
					listed in the other options below */

	IB_RECOVERY_IGNORE_CORRUPT,	/*!< let the server run even if it
					detects a corrupt page */

	IB_RECOVERY_NO_BACKGROUND,	/*!< prevent the main thread from
					running: if a crash would occur
					in purge, this prevents it */

	IB_RECOVERY_NO_TRX_UNDO,	/*!< do not run trx rollback after
					recovery */

	IB_RECOVERY_NO_IBUF_MERGE,	/*!< prevent also ibuf operations:
					if they would cause a crash, better
					not do them */

	IB_RECOVERY_NO_UNDO_LOG_SCAN,	/*!< do not look at undo logs when
					starting the database: InnoDB will
					treat even incomplete transactions
					as committed */

	IB_RECOVERY_NO_LOG_REDO		/*!< do not do the log roll-forward
					in connection with recovery */
} ib_recovery_t;

#ifndef UNIV_HOTBACKUP
/** Types of threads existing in the system. */
enum srv_thread_type {
	SRV_COM = 1,	/**< threads serving communication and queries */
	SRV_CONSOLE,	/**< thread serving console */
	SRV_WORKER,	/**< threads serving parallelized queries and
			queries released from lock wait */
#if 0
	/* Utility threads */
	SRV_BUFFER,	/**< thread flushing dirty buffer blocks */
	SRV_RECOVERY,	/**< threads finishing a recovery */
	SRV_INSERT,	/**< thread flushing the insert buffer to disk */
#endif
	SRV_MASTER	/**< the master thread, (whose type number must
			be biggest) */
};

/** Boots Innobase server.
@return	DB_SUCCESS or error code */
UNIV_INTERN
ulint
srv_boot(void);
/** Frees the data structures created in srv_init(). */
UNIV_INTERN
void
srv_free(void);
/** Initializes the synchronization primitives, memory system, and the thread
local storage. */
UNIV_INTERN
void
srv_general_init(void);
/** Gets the number of threads in the system.
@return	sum of srv_n_threads[] */
UNIV_INTERN
ulint
srv_get_n_threads(void);
/** Returns the calling thread type.
@return	SRV_COM, ... */

enum srv_thread_type
srv_get_thread_type(void);
/** Releases threads of the type given from suspension in the thread table.
NOTE! The server mutex has to be reserved by the caller!
@return number of threads released: this may be less than n if not
enough threads were suspended at the moment */
UNIV_INTERN
ulint
srv_release_threads(
	enum srv_thread_type	type,	/*!< in: thread type */
	ulint			n);	/*!< in: number of threads to release */
/** The master thread controlling the server.
@return	a dummy parameter */
UNIV_INTERN
os_thread_ret_t
srv_master_thread(
	void*	arg);	/*!< in: a dummy parameter required by
			os_thread_create */
/** Tells the Innobase server that there has been activity in the database
and wakes up the master thread if it is suspended (not sleeping). Used
in the client interface. Note that there is a small chance that the master
thread stays suspended (we do not protect our operation with the kernel
mutex, for performace reasons). */
UNIV_INTERN
void
srv_active_wake_master_thread(void);
/** Wakes up the master thread if it is suspended or being suspended. */
UNIV_INTERN
void
srv_wake_master_thread(void);
/** Puts an OS thread to wait if there are too many concurrent threads
(>= srv_thread_concurrency) inside InnoDB. The threads wait in a FIFO queue. */
UNIV_INTERN
void
srv_conc_enter_innodb(
	trx_t*	trx);	/*!< in: transaction object associated with the
			thread */
/** This must be called when a thread exits InnoDB in a lock wait or at the
end of an SQL statement. */
UNIV_INTERN
void
srv_conc_force_exit_innodb(
	trx_t*	trx);	/*!< in: transaction object associated with the
			thread */
/** Puts a user OS thread to wait for a lock to be released. If an error
occurs during the wait trx->error_state associated with thr is
!= DB_SUCCESS when we return. DB_LOCK_WAIT_TIMEOUT and DB_DEADLOCK
are possible errors. DB_DEADLOCK is returned if selective deadlock
resolution chose this transaction as a victim. */
UNIV_INTERN
void
srv_suspend_user_thread(
	que_thr_t*	thr);	/*!< in: query thread associated with the 
				client OS thread */
/** Releases a user OS thread waiting for a lock to be released, if the
thread is already suspended. */
UNIV_INTERN
void
srv_release_user_thread_if_suspended(
	que_thr_t*	thr);	/*!< in: query thread associated with the
				client OS thread	 */
/** A thread which wakes up threads whose lock wait may have lasted too long.
@return	a dummy parameter */
UNIV_INTERN
os_thread_ret_t
srv_lock_timeout_thread(
	void*	arg);	/*!< in: a dummy parameter required by
			os_thread_create */
/** A thread which prints the info output by various InnoDB monitors.
@return	a dummy parameter */
UNIV_INTERN
os_thread_ret_t
srv_monitor_thread(
	void*	arg);	/*!< in: a dummy parameter required by
			os_thread_create */
/** A thread which prints warnings about semaphore waits which have lasted
too long. These can be used to track bugs which cause hangs.
@return	a dummy parameter */
UNIV_INTERN
os_thread_ret_t
srv_error_monitor_thread(
	void*	arg);	/*!< in: a dummy parameter required by
			os_thread_create */
/** Outputs to a file the output of the InnoDB Monitor.
@return FALSE if not all information printed
due to failure to obtain necessary mutex */
UNIV_INTERN
ibool
srv_printf_innodb_monitor(
	ib_stream_t	ib_stream,	/*!< in: output stream */
	ibool		nowait,		/*!< in: whether to wait for
					kernel mutex */
	ulint*		trx_start,	/*!< out: file position of the start of
					the list of active transactions */
	ulint*		trx_end);	/*!< out: file position of the end of
					the list of active transactions */

/** Function to pass InnoDB status variables to client */
UNIV_INTERN
void
srv_export_innodb_status(void);

/** Reset variables. */
UNIV_INTERN
void
srv_var_init(void);

/** Resets the variables of all the InnoDB modules. */
UNIV_INTERN
void
srv_modules_var_init(void);

/** In this structure we store status variables to be passed to the client. */
struct export_var_struct{
	ulint innodb_data_pending_reads;	/*!< Pending reads */
	ulint innodb_data_pending_writes;	/*!< Pending writes */
	ulint innodb_data_pending_fsyncs;	/*!< Pending fsyncs */
	ulint innodb_data_fsyncs;		/*!< Number of fsyncs so far */
	ulint innodb_data_read;			/*!< Data bytes read */
	ulint innodb_data_writes;		/*!< I/O write requests */
	ulint innodb_data_written;		/*!< Data bytes written */
	ulint innodb_data_reads;		/*!< I/O read requests */
	ulint innodb_buffer_pool_pages_total;	/*!< Buffer pool size */
	ulint innodb_buffer_pool_pages_data;	/*!< Data pages */
	ulint innodb_buffer_pool_pages_dirty;	/*!< Dirty data pages */
	ulint innodb_buffer_pool_pages_misc;	/*!< Miscellanous pages */
	ulint innodb_buffer_pool_pages_free;	/*!< Free pages */
#ifdef UNIV_DEBUG
	ulint innodb_buffer_pool_pages_latched;	/*!< Latched pages */
#endif /* UNIV_DEBUG */
	ulint innodb_buffer_pool_read_requests;	/*!< buf_pool->stat.n_page_gets */
	ulint innodb_buffer_pool_reads;		/*!< srv_buf_pool_reads */
	ulint innodb_buffer_pool_wait_free;	/*!< srv_buf_pool_wait_free */
	ulint innodb_buffer_pool_pages_flushed;	/*!< srv_buf_pool_flushed */
	ulint innodb_buffer_pool_write_requests;/*!< srv_buf_pool_write_requests */
	ulint innodb_buffer_pool_read_ahead;	/*!< srv_read_ahead */
	ulint innodb_buffer_pool_read_ahead_evicted;/*!< srv_read_ahead evicted*/
	ulint innodb_dblwr_pages_written;	/*!< srv_dblwr_pages_written */
	ulint innodb_dblwr_writes;		/*!< srv_dblwr_writes */
	ibool innodb_have_atomic_builtins;	/*!< HAVE_ATOMIC_BUILTINS */
	ulint innodb_log_waits;			/*!< srv_log_waits */
	ulint innodb_log_write_requests;	/*!< srv_log_write_requests */
	ulint innodb_log_writes;		/*!< srv_log_writes */
	ulint innodb_os_log_written;		/*!< srv_os_log_written */
	ulint innodb_os_log_fsyncs;		/*!< fil_n_log_flushes */
	ulint innodb_os_log_pending_writes;	/*!< srv_os_log_pending_writes */
	ulint innodb_os_log_pending_fsyncs;	/*!< fil_n_pending_log_flushes */
	ulint innodb_page_size;			/*!< UNIV_PAGE_SIZE */
	ulint innodb_pages_created;		/*!< buf_pool->stat.n_pages_created */
	ulint innodb_pages_read;		/*!< buf_pool->stat.n_pages_read */
	ulint innodb_pages_written;		/*!< buf_pool->stat.n_pages_written */
	ulint innodb_row_lock_waits;		/*!< srv_n_lock_wait_count */
	ulint innodb_row_lock_current_waits;	/*!< srv_n_lock_wait_current_count */
	ib_int64_t innodb_row_lock_time;	/*!< srv_n_lock_wait_time
						/ 1000 */
	ulint innodb_row_lock_time_avg;		/*!< srv_n_lock_wait_time
						/ 1000
						/ srv_n_lock_wait_count */
	ulint innodb_row_lock_time_max;		/*!< srv_n_lock_max_wait_time
						/ 1000 */
	ulint innodb_rows_read;			/*!< srv_n_rows_read */
	ulint innodb_rows_inserted;		/*!< srv_n_rows_inserted */
	ulint innodb_rows_updated;		/*!< srv_n_rows_updated */
	ulint innodb_rows_deleted;		/*!< srv_n_rows_deleted */
};

extern ulint	srv_n_threads_active[];
#else /* !UNIV_HOTBACKUP */
# define srv_use_checksums			TRUE
# define srv_use_adaptive_hash_indexes		FALSE
# define srv_force_recovery			0UL
# define srv_is_being_started			0
# define srv_win_file_flush_method		SRV_WIN_IO_UNBUFFERED
# define srv_unix_file_flush_method		SRV_UNIX_O_DSYNC
# define srv_start_raw_disk_in_use		0
# define srv_file_per_table			1
#endif /* !UNIV_HOTBACKUP */

extern void* ib_panic_data;
typedef void (*ib_panic_function_t)(void*, int, char*, ...);
UNIV_INTERN void srv_panic(int panic_ib_error, char* fmt, ...);

#endif
