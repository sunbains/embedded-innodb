/*****************************************************************************
Copyright (c) 1995, 2009, Innobase Oy. All Rights Reserved.

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

/** @file os/os0sync.c
The interface to the operating system
synchronization primitives.

Created 9/6/1995 Heikki Tuuri
*******************************************************/

#include "os0sync.h"
#ifdef UNIV_NONINL
#include "os0sync.ic"
#endif

#include "srv0start.h"
#include "ut0mem.h"

/* Type definition for an operating system mutex struct */
struct os_mutex_struct {
  /** Used by sync0arr.c for queing threads */
  os_event_t event;

  /** OS handle to mutex */
  void *handle;

  /** we use this counter to check that the same thread
  does not recursively lock the mutex: we do not assume
  that the OS mutex supports recursive locking, though NT seems to do that */
  ulint count;

  /** list of all 'slow' OS mutexes created */
  UT_LIST_NODE_T(os_mutex_str_t) os_mutex_list;
};

/** Mutex protecting counts and the lists of OS mutexes and events */
os_mutex_t os_sync_mutex;

/** TRUE if os_sync_mutex has been initialized */
static ibool os_sync_mutex_inited = FALSE;

/** TRUE when os_sync_free() is being executed */
static ibool os_sync_free_called = FALSE;

/** This is incremented by 1 in os_thread_create and decremented by 1 in
os_thread_exit */
ulint os_thread_count = 0;

/** The list of all events created */
static UT_LIST_BASE_NODE_T(os_event_struct_t) os_event_list;

/** The list of all OS 'slow' mutexes */
static UT_LIST_BASE_NODE_T(os_mutex_str_t) os_mutex_list;

ulint os_event_count = 0;
ulint os_mutex_count = 0;
ulint os_fast_mutex_count = 0;

/* Because a mutex is embedded inside an event and there is an
event embedded inside a mutex, on free, this generates a recursive call.
This version of the free event function doesn't acquire the global lock */
static void os_event_free_internal(os_event_t event);

void os_sync_var_init() {
  os_sync_mutex = nullptr;
  os_sync_mutex_inited = FALSE;
  os_sync_free_called = FALSE;
  os_thread_count = 0;

  memset(&os_event_list, 0x0, sizeof(os_event_list));
  memset(&os_mutex_list, 0x0, sizeof(os_mutex_list));

  os_event_count = 0;
  os_mutex_count = 0;
  os_fast_mutex_count = 0;
}

void os_sync_init(void) {
  UT_LIST_INIT(os_event_list);
  UT_LIST_INIT(os_mutex_list);

  os_sync_mutex = nullptr;
  os_sync_mutex_inited = FALSE;

  os_sync_mutex = os_mutex_create(nullptr);

  os_sync_mutex_inited = TRUE;
}

void os_sync_free(void) {
  os_mutex_t mutex;

  os_sync_free_called = TRUE;
  auto event = UT_LIST_GET_FIRST(os_event_list);

  while (event) {

    os_event_free(event);

    event = UT_LIST_GET_FIRST(os_event_list);
  }

  mutex = UT_LIST_GET_FIRST(os_mutex_list);

  while (mutex) {
    if (mutex == os_sync_mutex) {
      /* Set the flag to FALSE so that we do not try to
      reserve os_sync_mutex any more in remaining freeing
      operations in shutdown */
      os_sync_mutex_inited = FALSE;
    }

    os_mutex_free(mutex);

    mutex = UT_LIST_GET_FIRST(os_mutex_list);
  }
  os_sync_free_called = FALSE;
}

os_event_t os_event_create(const char *name) {
  UT_NOT_USED(name);

  auto event = static_cast<os_event_struct*>(ut_malloc(sizeof(os_event_struct)));

  os_fast_mutex_init(&event->os_mutex);

  ut_a(0 == pthread_cond_init(&(event->cond_var), nullptr));

  event->is_set = FALSE;

  /* We return this value in os_event_reset(), which can then be
  be used to pass to the os_event_wait_low(). The value of zero
  is reserved in os_event_wait_low() for the case when the
  caller does not want to pass any signal_count value. To
  distinguish between the two cases we initialize signal_count
  to 1 here. */
  event->signal_count = 1;

  /* The os_sync_mutex can be nullptr because during startup an event
  can be created [ because it's embedded in the mutex/rwlock ] before
  this module has been initialized */
  if (os_sync_mutex != nullptr) {
    os_mutex_enter(os_sync_mutex);
  }

  /* Put to the list of events */
  UT_LIST_ADD_FIRST(os_event_list, os_event_list, event);

  os_event_count++;

  if (os_sync_mutex != nullptr) {
    os_mutex_exit(os_sync_mutex);
  }

  return event;
}

void os_event_set(os_event_t event) {
  ut_a(event);

  os_fast_mutex_lock(&(event->os_mutex));

  if (!event->is_set) {
    event->is_set = TRUE;
    event->signal_count += 1;
    ut_a(0 == pthread_cond_broadcast(&(event->cond_var)));
  }

  os_fast_mutex_unlock(&(event->os_mutex));
}

int64_t os_event_reset(os_event_t event) {
  int64_t ret = 0;

  ut_a(event);

  os_fast_mutex_lock(&(event->os_mutex));

  if (event->is_set) {
    event->is_set = FALSE;
  }
  ret = event->signal_count;

  os_fast_mutex_unlock(&(event->os_mutex));

  return ret;
}

/** Frees an event object, without acquiring the global lock.
@param[in,oun] event            Event to free. */
static void os_event_free_internal(os_event_t event) {
  ut_a(event);

  /* This is to avoid freeing the mutex twice */
  os_fast_mutex_free(&(event->os_mutex));

  ut_a(0 == pthread_cond_destroy(&(event->cond_var)));

  /* Remove from the list of events */
  UT_LIST_REMOVE(os_event_list, os_event_list, event);

  --os_event_count;

  ut_free(event);
}

void os_event_free(os_event_t event) {
  ut_a(event);

  os_fast_mutex_free(&(event->os_mutex));
  ut_a(0 == pthread_cond_destroy(&(event->cond_var)));

  /* Remove from the list of events */
  os_mutex_enter(os_sync_mutex);

  UT_LIST_REMOVE(os_event_list, os_event_list, event);

  --os_event_count;

  os_mutex_exit(os_sync_mutex);

  ut_free(event);
}

void os_event_wait_low(os_event_t event, int64_t reset_sig_count) {
  int64_t old_signal_count;

  os_fast_mutex_lock(&(event->os_mutex));

  if (reset_sig_count) {
    old_signal_count = reset_sig_count;
  } else {
    old_signal_count = event->signal_count;
  }

  for (;;) {
    if (event->is_set == TRUE || event->signal_count != old_signal_count) {

      os_fast_mutex_unlock(&(event->os_mutex));

      if (srv_shutdown_state == SRV_SHUTDOWN_EXIT_THREADS) {

        os_thread_exit(nullptr);
      }

      return;
    }

    pthread_cond_wait(&(event->cond_var), &(event->os_mutex));

    /* Solaris manual said that spurious wakeups may occur: we
    have to check if the event really has been signaled after
    we came here to wait */
  }
}

os_mutex_t os_mutex_create(const char *name) {
  UT_NOT_USED(name);

  auto mutex = static_cast<os_fast_mutex_t*>(ut_malloc(sizeof(os_fast_mutex_t)));

  os_fast_mutex_init(mutex);

  auto mutex_str = static_cast<os_mutex_str_t*>(ut_malloc(sizeof(os_mutex_str_t)));

  mutex_str->handle = mutex;
  mutex_str->count = 0;
  mutex_str->event = os_event_create(nullptr);

  if (UNIV_LIKELY(os_sync_mutex_inited)) {
    /* When creating os_sync_mutex itself we cannot reserve it */
    os_mutex_enter(os_sync_mutex);
  }

  UT_LIST_ADD_FIRST(os_mutex_list, os_mutex_list, mutex_str);

  os_mutex_count++;

  if (UNIV_LIKELY(os_sync_mutex_inited)) {
    os_mutex_exit(os_sync_mutex);
  }

  return mutex_str;
}

void os_mutex_enter(os_mutex_t mutex) {
  os_fast_mutex_lock(static_cast<os_fast_mutex_t*>(mutex->handle));

  ++mutex->count;

  ut_a(mutex->count == 1);
}

void os_mutex_exit(os_mutex_t mutex) {
  ut_a(mutex);

  ut_a(mutex->count == 1);

  --mutex->count;

  os_fast_mutex_unlock(static_cast<os_fast_mutex_t*>(mutex->handle));
}

void os_mutex_free(os_mutex_t mutex) {
  ut_a(mutex);

  if (UNIV_LIKELY(!os_sync_free_called)) {
    os_event_free_internal(mutex->event);
  }

  if (UNIV_LIKELY(os_sync_mutex_inited)) {
    os_mutex_enter(os_sync_mutex);
  }

  UT_LIST_REMOVE(os_mutex_list, os_mutex_list, mutex);

  --os_mutex_count;

  if (UNIV_LIKELY(os_sync_mutex_inited)) {
    os_mutex_exit(os_sync_mutex);
  }

  os_fast_mutex_free(static_cast<os_fast_mutex_t*>(mutex->handle));
  ut_free(mutex->handle);
  ut_free(mutex);
}

void os_fast_mutex_init(os_fast_mutex_t *fast_mutex) {
  ut_a(0 == pthread_mutex_init(fast_mutex, nullptr));

  if (UNIV_LIKELY(os_sync_mutex_inited)) {
    /* When creating os_sync_mutex itself (in Unix) we cannot
    reserve it */

    os_mutex_enter(os_sync_mutex);
  }

  os_fast_mutex_count++;

  if (UNIV_LIKELY(os_sync_mutex_inited)) {
    os_mutex_exit(os_sync_mutex);
  }
}

void os_fast_mutex_lock(os_fast_mutex_t *fast_mutex) {
  pthread_mutex_lock(fast_mutex);
}

void os_fast_mutex_unlock(os_fast_mutex_t *fast_mutex) {
  pthread_mutex_unlock(fast_mutex);
}

void os_fast_mutex_free(os_fast_mutex_t *fast_mutex) {
  auto ret = pthread_mutex_destroy(fast_mutex);

  if (UNIV_UNLIKELY(ret != 0)) {
    ut_print_timestamp(ib_stream);
    ib_logger(ib_stream,
              "  InnoDB: error: return value %lu when calling\n"
              "InnoDB: pthread_mutex_destroy().\n",
              (ulint)ret);
    ib_logger(ib_stream, "InnoDB: Byte contents of the pthread mutex at %p:\n",
              (void *)fast_mutex);
    ut_print_buf(ib_stream, fast_mutex, sizeof(os_fast_mutex_t));
    ib_logger(ib_stream, "\n");
  }

  if (UNIV_LIKELY(os_sync_mutex_inited)) {
    /* When freeing the last mutexes, we have
    already freed os_sync_mutex */

    os_mutex_enter(os_sync_mutex);
  }

  ut_ad(os_fast_mutex_count > 0);
  --os_fast_mutex_count;

  if (UNIV_LIKELY(os_sync_mutex_inited)) {
    os_mutex_exit(os_sync_mutex);
  }
}
