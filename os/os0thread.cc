/**
Copyright (c) 1995, 2010, Innobase Oy. All Rights Reserved.

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

/** @file os/os0thread.c
The interface to the operating system thread control primitives

Created 9/8/1995 Heikki Tuuri
*******************************************************/

#include "os0thread.h"

#include "os0sync.h"
#include "srv0srv.h"

ibool os_thread_eq(os_thread_id_t a, os_thread_id_t b) {
  if (pthread_equal(a, b)) {
    return TRUE;
  }
  return FALSE;
}

ulint os_thread_pf(os_thread_id_t a) {
  return (ulint)a;
}

os_thread_id_t os_thread_get_curr_id() {
  return pthread_self();
}

os_thread_t os_thread_create(void *(*f)(void *), void *arg, os_thread_id_t *thread_id) {
  os_thread_t pthread;
  pthread_attr_t attr;

  memset(&attr, 0x0, sizeof(attr));

  os_mutex_enter(os_sync_mutex);

  ++os_thread_count;

  os_mutex_exit(os_sync_mutex);

  auto ret = pthread_create(&pthread, &attr, f, arg);

  if (ret != 0) {
    log_fatal("InnoDB: Error: pthread_create returned %d\n", ret);
  }

  if (srv_set_thread_priorities) {
#ifdef HAVE_PTHREAD_SETPRIO
    pthread_setprio(pthread, srv_query_thread_priority);
#endif /* HAVE_PTHREAD_SETPRIO */
  }

  if (thread_id != nullptr) {
    *thread_id = pthread;
  }

  return pthread;
}

void os_thread_exit(void *exit_value) {
  os_mutex_enter(os_sync_mutex);
  --os_thread_count;
  os_mutex_exit(os_sync_mutex);

  auto ret = pthread_detach(pthread_self());
  ut_a(ret == 0);

  pthread_exit(exit_value);
}

os_thread_t os_thread_get_curr() {
  return pthread_self();
}

void os_thread_yield(void) {
#if defined(HAVE_PTHREAD_YIELD_ZERO_ARG)
  pthread_yield();
#elif defined(HAVE_PTHREAD_YIELD_ONE_ARG)
  pthread_yield(0);
#else
  os_thread_sleep(0);
#endif
}

void os_thread_sleep(ulint tm) {
  struct timeval t;

  t.tv_sec = tm / 1000000;
  t.tv_usec = tm % 1000000;

  select(0, NULL, NULL, NULL, &t);
}

void os_thread_set_priority(os_thread_t handle, ulint pri) {
  UT_NOT_USED(handle);
  UT_NOT_USED(pri);
}

ulint os_thread_get_priority(os_thread_t handle __attribute__((unused))) {
  return 0;
}

ulint os_thread_get_last_error(void) {
  return 0;
}
