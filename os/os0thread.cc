/****************************************************************************
Copyright (c) 1995, 2010, Innobase Oy. All Rights Reserved.
Copyright (c) 2024 Sunny Bains. All rights reserved.

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

#include <chrono>
#include <thread>

#include "os0thread.h"

#include "os0sync.h"
#include "srv0srv.h"

bool os_thread_eq(os_thread_id_t a, os_thread_id_t b) {
  return pthread_equal(a, b) > 0;
}

ulint os_thread_pf(os_thread_id_t a) {
  return ulint(a);
}

os_thread_id_t os_thread_get_curr_id() {
  return pthread_self();
}

os_thread_t os_thread_create(void *(*f)(void *), void *arg, os_thread_id_t *thread_id) {
  os_thread_t pthread;
  pthread_attr_t attr;

  memset(&attr, 0x0, sizeof(attr));

  os_thread_count.fetch_add(1, std::memory_order_relaxed);

  auto ret = pthread_create(&pthread, &attr, f, arg);

  if (ret != 0) {
    log_fatal("pthread_create returned %d", ret, " : ", strerror(ret));
  }

  if (thread_id != nullptr) {
    *thread_id = pthread;
  }

  return pthread;
}

void os_thread_exit() {
  os_thread_count.fetch_sub(1, std::memory_order_relaxed);
}

os_thread_t os_thread_get_curr() {
  return pthread_self();
}

void os_thread_yield() noexcept {
  std::this_thread::yield();
}

void os_thread_sleep(ulint sleep_time) noexcept {
  std::this_thread::sleep_for(std::chrono::microseconds(sleep_time));
}
