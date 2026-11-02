/** Copyright (c) 2024 Sunny Bains. All rights reserved. */

#pragma once

#include "innodb0types.h"

#include <atomic>

#include "os0sync.h"
#include "os0thread.h"
#include "ut0lst.h"

using lock_word_t = std::atomic<bool>;

/** Value of mutex_struct::magic_n */
constexpr ulint MUTEX_MAGIC_N = 979585;

/** InnoDB mutex */
struct Mutex {

  Mutex() = default;

  void init() {
    ut_a(event == nullptr);
    event = os_event_create(nullptr);
  }

  /** Used by sync0arr.c for the wait queue */
  CondVar *event{};

  /** lock_word is the target of the atomic test-and-set
  instruction when atomic operations are enabled. */
  lock_word_t lock_word{};

  /** This ulint is set to 1 if there are (or may be) threads waiting in
  the global wait array for this mutex to be released. Otherwise, this is 0. */
  std::atomic<bool> m_waiters{};

  /** All allocated mutexes are put into a list. Pointers to the
  next and prev. */
  UT_LIST_NODE_T(Mutex) list{};

  /** File name where mutex created */
  const char *cfile_name{};

  /** Line where created */
  uint32_t cline{};

  /** count of os_wait */
  ulong count_os_wait{};

#ifdef UNIV_DEBUG
  /** The thread id of the thread which locked the mutex. */
  os_thread_id_t thread_id{};

  /** count of times mutex used */
  ulong count_using{};

  /** count of spin loops */
  ulong count_spin_loop{};

  /** count of spin rounds */
  ulong count_spin_rounds{};

  /** count of os_wait */
  ulong count_os_yield{};

  /** mutex os_wait timer msec */
  uint64_t lspent_time{};

  /** mutex os_wait timer msec */
  uint64_t lmax_spent_time{};

  /** mutex name */
  const char *cmutex_name{};

  /** 0=usual mutex, 1=rw_lock mutex */
  ulint mutex_type{};

  /** MUTEX_MAGIC_N */
  ulint magic_n{MUTEX_MAGIC_N};
#endif /* UNIV_DEBUG */

};

static_assert(std::is_standard_layout<Mutex>::value, "Mutex must have standard layout");
