/****************************************************************************
Copyright (c) 1994, 2010, Innobase Oy. All Rights Reserved.

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

/** @file ut/ut0mem.c
Memory primitives

Created 5/11/1994 Heikki Tuuri
*************************************************************************/

#include "ut0mem.h"

#include <errno.h>

#include "os0thread.h"
#include "srv0srv.h"
#include "srv0start.h"
#include "ut0logger.h"

#include <stdlib.h>

/** The value of ut_mem_block_struct::magic_n.  Used in detecting
memory corruption. */
constexpr uint32_t UT_MEM_MAGIC_N = 1601650166;

/** Dynamically allocated memory block */
struct Mem_block {
  Mem_block(uint32_t size) : m_size(size), m_magic_n(UT_MEM_MAGIC_N) {}

  /** mem block list node */
  UT_LIST_NODE_T(Mem_block) m_mem_block_list;

  /** Size of allocated memory */
  uint32_t m_size;

  /** magic number (UT_MEM_MAGIC_N) */
  uint32_t m_magic_n;

  /** List of all memory blocks allocated from the operating system
  with malloc.  Protected by s_mutex. */
  using Blocks = UT_LIST_BASE_NODE_T(Mem_block, m_mem_block_list);

  /** Mutex protecting ut_total_allocated_memory and ut_mem_block_list */
  static std::mutex s_mutex;

  /** Total blocks allocated. */
  static Blocks s_blocks;

  /** Total number of bytes allocated. Disabled if srv_use_sys_malloc is set. */
  static std::atomic<ulint> s_total_memory;
};

/** We need this to alias a buf_block_t from a buf_page_t. */
static_assert(std::is_standard_layout<Mem_block>::value, "Mem_block must have a standard layout");

std::mutex Mem_block::s_mutex{};
std::atomic<ulint> Mem_block::s_total_memory{};
Mem_block::Blocks Mem_block::s_blocks{};

/** Flag: has ut_mem_block_list been initialized? */
static bool ut_mem_block_list_inited = false;

void ut_mem_var_init() {
  Mem_block::s_total_memory.store(0, std::memory_order_release);

  ut_mem_block_list_inited = false;
}

void ut_mem_init() {
  ut_a(!srv_was_started);

  if (!ut_mem_block_list_inited) {
    UT_LIST_INIT(Mem_block::s_blocks);
    ut_mem_block_list_inited = true;
  }
}

static void *allocate(ulint n, bool set_to_zero, bool assert_on_error) {
  if (srv_use_sys_malloc) {
    auto ptr = new char [n];
    ut_a(ptr != nullptr || !assert_on_error);
    return ptr;
  }

   /* check alignment ok */
  ut_ad((sizeof(Mem_block) % 8) == 0);
  ut_a(ut_mem_block_list_inited);

  ulint retry_count{};
  const auto size = sizeof(Mem_block) + n;

  for (;;) {
    Mem_block::s_mutex.lock();

    auto ptr = new (std::nothrow) char[size];

    if (ptr == nullptr && retry_count < 60) {
      if (retry_count == 0) {
        ut_print_timestamp(ib_stream);

        log_err(
          "Cannot allocate", n, " bytes of memory with malloc!"
          " Total allocated memory by InnoDB ",
          Mem_block::s_total_memory.load(std::memory_order_acquire),
          " bytes. Operating system errno:", errno, " '", strerror(errno), "'.");
      }

      Mem_block::s_mutex.unlock();

      /* Sleep for a second and retry the allocation; maybe this is
      just a temporary shortage of memory */
  
      os_thread_sleep(1000000);
  
      ++retry_count;

      continue;

    } else if (ptr == nullptr) {

      Mem_block::s_mutex.unlock();

      /* Make an intentional seg fault so that we get a stack trace */
      /* Intentional segfault on NetWare causes an abend. Avoid this
      by graceful exit handling in ut_a(). */

      if (assert_on_error) {
        ut_error;
      } else {
        return nullptr;
      }
    }

    auto mem_block = new (ptr) Mem_block(size);

    UNIV_MEM_ALLOC(ret, n + sizeof(Mem_block));

    Mem_block::s_total_memory += n + sizeof(Mem_block);

    UT_LIST_ADD_FIRST(Mem_block::s_blocks, mem_block);

    Mem_block::s_mutex.unlock();

    ptr = reinterpret_cast<char*>(mem_block + 1);

    if (set_to_zero) {
      memset(ptr, 0x0, n);
    }

    return ptr;
  }
}

void *ut_new(ulint n) {
  return allocate(n, true, true);
}

void ut_delete(void *p) {
  auto ptr = reinterpret_cast<char*>(p);

  if (ptr == nullptr) {
    return;
  } else if (srv_use_sys_malloc) {
    delete [] ptr;
    return;
  }

  auto block = reinterpret_cast<Mem_block *>(ptr - sizeof(Mem_block));

  ut_a(Mem_block::s_total_memory.load(std::memory_order_acquire) >= block->m_size);

  Mem_block::s_total_memory.fetch_sub(block->m_size, std::memory_order_relaxed);

  std::lock_guard<std::mutex> lock(Mem_block::s_mutex);

  UT_LIST_REMOVE(Mem_block::s_blocks, block);

  call_destructor(block);
  delete [] reinterpret_cast<char*>(block);
}

void *ut_realloc(void *p, ulint n) {
  auto ptr = reinterpret_cast<char*>(p);

  if (ptr == nullptr) {
    return ut_new(n);
  }

  if (n == 0) {
    ut_delete(p);
    return nullptr;
  }

  auto block = reinterpret_cast<Mem_block *>(ptr - sizeof(Mem_block));

  ut_a(block->m_magic_n == UT_MEM_MAGIC_N);

  auto old_size = block->m_size - sizeof(Mem_block);
  auto min_size  = n < old_size ? n : old_size;

  auto new_ptr = ut_new(n);

  if (new_ptr == nullptr) {

    return nullptr;
  }

  memcpy(new_ptr, ptr, min_size);

  ut_delete(ptr);

  return new_ptr;
}

void ut_delete_all_mem() {
  /* If the sub-system hasn't been initialized, then ignore request. */
  if (!ut_mem_block_list_inited) {
    return;
  }

  while (auto block = UT_LIST_GET_FIRST(Mem_block::s_blocks)) {
    ut_a(block->m_magic_n == UT_MEM_MAGIC_N);
    ut_a(Mem_block::s_total_memory.load(std::memory_order_acquire) >= block->m_size);

    Mem_block::s_total_memory.fetch_sub(block->m_size, std::memory_order_relaxed);

    UT_LIST_REMOVE(Mem_block::s_blocks, block);

    call_destructor(block);

    delete [] reinterpret_cast<char*>(block);
  }

  if (Mem_block::s_total_memory != 0) {
    log_warn("After shutdown total allocated memory is ",
              Mem_block::s_total_memory.load(std::memory_order_acquire),
              " bytes");
  }

  ut_mem_block_list_inited = false;
}

void ut_allocated_memory(ulint size) {
  Mem_block::s_total_memory.fetch_add(size, std::memory_order_relaxed);
}

void ut_deallocated_memory(ulint size) {
  Mem_block::s_total_memory.fetch_sub(size, std::memory_order_relaxed);
}

ulint ut_strlcpy(char *dst, const char *src, ulint size) {
  ulint src_size = strlen(src);

  if (size != 0) {
    ulint n = ut_min(src_size, size - 1);

    memcpy(dst, src, n);
    dst[n] = '\0';
  }

  return src_size;
}

ulint ut_strlcpy_rev(char *dst, const char *src, ulint size) {
  ulint src_size = strlen(src);

  if (size != 0) {
    ulint n = ut_min(src_size, size - 1);

    memcpy(dst, src + src_size - n, n + 1);
  }

  return src_size;
}

ulint ut_total_allocated_memory() {
  return Mem_block::s_total_memory.load(std::memory_order_acquire);
}