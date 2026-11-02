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

#pragma once

#include "innodb0types.h"
#include "log0config.h"
#include "sync0sync.h"
#include "ut0ut.h"

#include <optional>
#include <ranges>
#include <span>
#include <vector>

using Log_phy_blocks = std::vector<LogPhyBlock, IO_allocator<LogPhyBlock>>;

inline auto make_log_write_view(Log_phy_blocks &blocks) noexcept {
  auto spans = blocks | std::views::transform([](LogPhyBlock &block) { return std::span<byte>(block.m_data); });
  return spans | std::views::join;
}

inline auto make_log_read_view(const Log_phy_blocks &blocks) noexcept {
  auto spans = blocks | std::views::transform([](const LogPhyBlock &block) {
                 return std::span<const byte>(block.m_data, block.get_data_len());
               });
  return spans | std::views::join;
}

/**
 * Log buffer for managing physical log blocks
 */
struct LogBuffer {
  using Phy_blocks = Log_phy_blocks;

  using write_view_t = decltype(make_log_write_view(std::declval<Phy_blocks &>()));
  using read_view_t = decltype(make_log_read_view(std::declval<const Phy_blocks &>()));

  /** The first size_t is the current offset in the view, the second is the total size of the view. */
  using read_state_t = std::tuple<read_view_t, size_t, size_t>;
  using write_state_t = std::tuple<write_view_t, size_t, size_t>;

  /**
   * Constructor
   *
   * @param[in] size The size of the log buffer in bytes.
   */
  explicit LogBuffer(size_t size) noexcept {
    m_phy_blocks.reserve(size / sizeof(LogPhyBlock));

    const auto total_size = m_phy_blocks.size() * sizeof(LogPhyBlock);

    m_read_state = std::make_tuple(make_log_read_view(m_phy_blocks), size_t{0}, total_size);
    m_write_state = std::make_tuple(make_log_write_view(m_phy_blocks), size_t{0}, total_size);
  }

  ~LogBuffer() noexcept;

  /**
   * Acquire the log buffer mutex.
   */
  void acquire() noexcept {
    ut_ad(!mutex_own(&m_mutex));
    mutex_enter(&m_mutex);
  }

  [[nodiscard]] size_t size() const noexcept { return m_phy_blocks.size() * sizeof(LogPhyBlock); }

  /**
   * Appends data to the log buffer.
   *
   * @param[in] str The data to append.
   * @param[in] len The length of the data to append.
   *
   * @return the number of bytes appended, -1 on error.
   */
  ssize_t append(const byte *str, ulint len) noexcept {
    auto &[view, offset, total_size] = m_write_state.value();

    if (offset + len > total_size) {
      len = total_size - offset;
    }

    auto it = std::ranges::begin(view);
    auto last = std::ranges::end(view);

    if (std::ranges::advance(it, offset, last) != 0) {
      return -1;
    }

    std::copy(str, str + len, it);
    offset += len;

    return len;
  }

  void reset() noexcept {
    // FIXME: We need a circular buffer, otherwise we have to move the unwritten data to the start of the buffer.
  }

  [[nodiscard]] bool empty() const noexcept {
    // FIXME: Implement this.
    ut_error;
    return false;
  }

  LogPhyBlock &back() noexcept {
    // FIXME: We need to return the current log block being written to.
    ut_error;
    return m_phy_blocks.back();
  }

  const LogPhyBlock &back() const noexcept {
    // FIXME: We need to return the current log block being written to.
    ut_error;
    return m_phy_blocks.back();
  }

  /**
   * Releases the log buffer mutex.
   */
  void release() noexcept {
    ut_ad(mutex_own(&m_mutex));
    mutex_exit(&m_mutex);
  }

  mutable Mutex m_mutex{};

  /** Log buffer physical blocks */
  Phy_blocks m_phy_blocks{};

  /** Threshold for writing the log buffer to the log vfile. Counted in bytes. */
  size_t m_write_threshold{};

  /** Range for writing to the log buffer physical blocks data. */
  std::optional<write_state_t> m_write_state;

  /** Range for reading the log buffer physical blocks data for parsing.*/
  std::optional<read_state_t> m_read_state;
};
