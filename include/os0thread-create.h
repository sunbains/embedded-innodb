/*****************************************************************************
Copyright (c) 1995, 2021, Oracle and/or its affiliates.
Copyright (c) 2024 Sunny Bains. All rights reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation.

This program is also distributed with certain software (including but not
limited to OpenSSL) that is licensed under separate terms, as designated in a
particular file or component or in included license documentation. The authors
of MySQL hereby grant you an additional permission to link the program and
your derivative works with the separately licensed software that they have
included with MySQL.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/

/** @file include/os0thread-create.h
 The interface to the threading wrapper

 Created 2016-May-17 Sunny Bains
 *******************************************************/

#pragma once

#include "os0thread.h"

#include <atomic>
#include <functional>
#include <future>

/** Number of threads active. */
extern std::atomic_int os_thread_count;

struct Runnable {
  Runnable() = default;

  /** Method to execute the callable
  @param[in] f                  Callable object
  @param[in] args               Variable number of args to F
  @retval f return value. */
  template <typename F, typename... Args>
  void operator()(F &&f, Args &&...args) {
    auto task = std::bind(std::forward<F>(f), std::forward<Args>(args)...);

    std::atomic_thread_fence(std::memory_order_release);

    {
      const auto old = os_thread_count.fetch_add(1, std::memory_order_relaxed);
      ut_a(old <= static_cast<int>(srv_config.m_max_n_threads) - 1);
    }

    task();

    std::atomic_thread_fence(std::memory_order_release);

    {
      const auto old = os_thread_count.fetch_sub(1, std::memory_order_relaxed);
      ut_a(old > 0);
    }
  }
};

/** Create a detached non-started thread. After thread is created, you should
assign the received object to any of variables/fields which you later could
access to check thread's state. You are allowed to either move or copy that
object (any number of copies is allowed). After assigning you are allowed to
start the thread by calling start() on any of those objects.
@param[in]	f         Callable instance
@param[in]	args      Zero or more args
@return thread instance. */
template <typename F, typename... Args>
std::thread create_detached_thread(F &&f, Args &&...args) {
  std::thread thread(std::move(Runnable()), f, args...);

  thread.detach();

  return thread;
}

/** Create a detached non-started thread. After thread is created, you should
assign the received object to any of variables/fields which you later could
access to check thread's state. You are allowed to either move or copy that
object (any number of copies is allowed). After assigning you are allowed to
start the thread by calling start() on any of those objects.
@param[in]	f         Callable instance
@param[in]	args      Zero or more args
@return thread instance. */
template <typename F, typename... Args>
std::thread create_joinable_thread(F &&f, Args &&...args) {
  std::thread thread(std::move(Runnable()), f, args...);

  return thread;
}

/** Parallel for loop over a container.
@param[in]	n        Number of threads to create
@param[in]	f        Callable instance
@param[in]	args     Zero or more args */
template <typename Container, typename F, typename... Args>
void par_for(const Container &c, size_t n, F &&f, Args &&...args) {
  if (c.empty()) {
    return;
  }

  size_t slice = (n > 0) ? c.size() / n : 0;

  using Workers = std::vector<std::thread>;

  Workers workers;

  workers.reserve(n);

  for (size_t i = 0; i < n; ++i) {
    auto b = c.begin() + (i * slice);
    auto e = b + slice;

    auto worker = os_thread_create(f, b, e, i, args...);
    worker.start();

    workers.push_back(std::move(worker));
  }

  f(c.begin() + (n * slice), c.end(), n, args...);

  for (auto &worker : workers) {
    worker.join();
  }
}

#define par_for(...) par_for(__VA_ARGS__)
