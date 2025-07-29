/****************************************************************************
Copyright (c) 2025 Sunny Bains. All rights reserved.

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

/** @file log/log0core.cc
Database log - core functionality with minimal dependencies

Created for testing and minimal coupling
*******************************************************/

#include "log0core.h"

#include <cstring>
#include <ctime>

#include "mach0data.h"
#include "mem0mem.h"
#include "ut0ut.h"

// For testing - include srv0srv.h for srv_config
#include "srv0srv.h"

// Basic constants
constexpr ulint LOG_BUF_FLUSH_RATIO = 2;
constexpr ulint LOG_BUF_WRITE_MARGIN = 4 * IB_FILE_BLOCK_SIZE;
constexpr ulint LOG_BUF_FLUSH_MARGIN = LOG_BUF_WRITE_MARGIN + 4 * UNIV_PAGE_SIZE;

Log_core::Log_core() noexcept {
  m_lsn = LOG_START_LSN;
  
  // Allocate log buffer
  ut_a(LOG_BUFFER_SIZE >= 16 * IB_FILE_BLOCK_SIZE);
  ut_a(LOG_BUFFER_SIZE >= 4 * UNIV_PAGE_SIZE);
  
  m_buf_ptr = static_cast<byte*>(mem_alloc(LOG_BUFFER_SIZE + IB_FILE_BLOCK_SIZE));
  m_buf = static_cast<byte*>(ut_align(m_buf_ptr, IB_FILE_BLOCK_SIZE));
  m_buf_size = LOG_BUFFER_SIZE;
  
  memset(m_buf, '\0', LOG_BUFFER_SIZE);
  
  m_max_buf_free = m_buf_size / LOG_BUF_FLUSH_RATIO - LOG_BUF_FLUSH_MARGIN;
  
  // Initialize first block
  block_init(m_buf, m_lsn);
  block_set_first_rec_group(m_buf, LOG_BLOCK_HDR_SIZE);
  m_buf_free = LOG_BLOCK_HDR_SIZE;
  m_lsn = LOG_START_LSN + LOG_BLOCK_HDR_SIZE;
}

Log_core::~Log_core() noexcept {
  if (m_buf_ptr) {
    mem_free(m_buf_ptr);
    m_buf_ptr = nullptr;
  }
}

lsn_t Log_core::reserve_and_write_fast(const void* str, ulint len, lsn_t* start_lsn) noexcept {
  auto data_len = len + m_buf_free % IB_FILE_BLOCK_SIZE;
  
  if (data_len >= IB_FILE_BLOCK_SIZE - LOG_BLOCK_TRL_SIZE) {
    return 0;
  }
  
  *start_lsn = m_lsn;
  memcpy(m_buf + m_buf_free, str, len);
  
  auto log_block = static_cast<byte*>(ut_align_down(m_buf + m_buf_free, IB_FILE_BLOCK_SIZE));
  block_set_data_len(log_block, data_len);
  
  m_buf_free += len;
  ut_ad(m_buf_free <= m_buf_size);
  m_lsn += len;
  
  return m_lsn;
}

void Log_core::write_low(const byte* str, ulint str_len) noexcept {
  while (str_len > 0) {
    auto data_len = (m_buf_free % IB_FILE_BLOCK_SIZE) + str_len;
    ulint len;
    
    if (data_len <= IB_FILE_BLOCK_SIZE - LOG_BLOCK_TRL_SIZE) {
      len = str_len;
    } else {
      data_len = IB_FILE_BLOCK_SIZE - LOG_BLOCK_TRL_SIZE;
      len = IB_FILE_BLOCK_SIZE - (m_buf_free % IB_FILE_BLOCK_SIZE) - LOG_BLOCK_TRL_SIZE;
    }
    
    memcpy(m_buf + m_buf_free, str, len);
    
    str_len -= len;
    str += len;
    
    auto log_block = static_cast<byte*>(ut_align_down(m_buf + m_buf_free, IB_FILE_BLOCK_SIZE));
    block_set_data_len(log_block, data_len);
    
    if (data_len == IB_FILE_BLOCK_SIZE - LOG_BLOCK_TRL_SIZE) {
      block_set_data_len(log_block, IB_FILE_BLOCK_SIZE);
      len += LOG_BLOCK_HDR_SIZE + LOG_BLOCK_TRL_SIZE;
      m_lsn += len;
      
      // Initialize the next block header
      block_init(log_block + IB_FILE_BLOCK_SIZE, m_lsn);
    } else {
      m_lsn += len;
    }
    
    m_buf_free += len;
    ut_ad(m_buf_free <= m_buf_size);
  }
}

void Log_core::block_init(byte* log_block, lsn_t lsn) noexcept {
  const auto no = block_convert_lsn_to_no(lsn);
  block_set_hdr_no(log_block, no);
  block_set_data_len(log_block, LOG_BLOCK_HDR_SIZE);
  block_set_first_rec_group(log_block, 0);
}

ulint Log_core::block_convert_lsn_to_no(lsn_t lsn) noexcept {
  return ((ulint)(lsn / IB_FILE_BLOCK_SIZE) & 0x3FFFFFFFUL) + 1;
}

void Log_core::block_set_hdr_no(byte* log_block, ulint n) noexcept {
  ut_ad(n > 0);
  ut_ad(n < LOG_BLOCK_FLUSH_BIT_MASK);
  mach_write_to_4(log_block + LOG_BLOCK_HDR_NO, n);
}

ulint Log_core::block_get_hdr_no(const byte* log_block) noexcept {
  return ~LOG_BLOCK_FLUSH_BIT_MASK & mach_read_from_4(log_block + LOG_BLOCK_HDR_NO);
}

void Log_core::block_set_data_len(byte* log_block, ulint len) noexcept {
  mach_write_to_2(log_block + LOG_BLOCK_HDR_DATA_LEN, len);
}

ulint Log_core::block_get_data_len(const byte* log_block) noexcept {
  return mach_read_from_2(log_block + LOG_BLOCK_HDR_DATA_LEN);
}

void Log_core::block_set_first_rec_group(byte* log_block, ulint offset) noexcept {
  mach_write_to_2(log_block + LOG_BLOCK_FIRST_REC_GROUP, offset);
}

ulint Log_core::block_get_first_rec_group(const byte* log_block) noexcept {
  return mach_read_from_2(log_block + LOG_BLOCK_FIRST_REC_GROUP);
}

uint32_t Log_core::block_calc_checksum(const byte* block) noexcept {
  uint32_t sh = 0;
  uint32_t sum = 1;
  
  for (ulint i = 0; i < IB_FILE_BLOCK_SIZE - LOG_BLOCK_TRL_SIZE; i++) {
    auto b = uint32_t(block[i]);
    sum &= 0x7FFFFFFFUL;
    sum += b;
    sum += b << sh;
    sh++;
    if (sh > 24) {
      sh = 0;
    }
  }
  
  return sum;
}

void Log_core::block_set_checksum(byte* log_block, ulint checksum) noexcept {
  mach_write_to_4(log_block + IB_FILE_BLOCK_SIZE - LOG_BLOCK_CHECKSUM, checksum);
}

uint32_t Log_core::block_get_checksum(const byte* log_block) noexcept {
  return mach_read_from_4(log_block + IB_FILE_BLOCK_SIZE - LOG_BLOCK_CHECKSUM);
}

void Log_core::block_store_checksum(byte* block) noexcept {
  block_set_checksum(block, block_calc_checksum(block));
}

lsn_t Log_core::get_lsn() const noexcept {
  return m_lsn;
}

ulint Log_core::get_buf_free() const noexcept {
  return m_buf_free;
}

const byte* Log_core::get_buf() const noexcept {
  return m_buf;
}

ulint Log_core::get_buf_size() const noexcept {
  return m_buf_size;
}

bool Log_core::is_buf_available(ulint len) const noexcept {
  return m_buf_free + len <= m_buf_size;
}

void Log_core::reset() noexcept {
  m_lsn = LOG_START_LSN;
  memset(m_buf, '\0', LOG_BUFFER_SIZE);
  
  block_init(m_buf, m_lsn);
  block_set_first_rec_group(m_buf, LOG_BLOCK_HDR_SIZE);
  m_buf_free = LOG_BLOCK_HDR_SIZE;
  m_lsn = LOG_START_LSN + LOG_BLOCK_HDR_SIZE;
}
