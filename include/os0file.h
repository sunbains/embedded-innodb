/**********************************************************************
Copyright (c) 1995, 2010, Innobase Oy. All Rights Reserved.
Copyright (c) 2009, Percona Inc.
Copyright (c) 2024 Sunny Bains. All rights reserved.

Portions of this file contain modifications contributed and copyrighted
by Percona Inc.. Those modifications are
gratefully acknowledged and are described briefly in the InnoDB
documentation. The contributions by Percona Inc. are incorporated with
their permission, and subject to the conditions contained in the file
COPYING.Percona.

This program is free software; you can redistribute it and/or modify it
under the terms of the GNU General Public License as published by the
Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
Public License for more details.

You should have received a copy of the GNU General Public License along
with this program; if not, write to the Free Software Foundation, Inc.,
59 Temple Place, Suite 330, Boston, MA 02111-1307 USA

***********************************************************************/

/** @file include/os0file.h
Files for I/O: a descriptor that closes itself, the reads and writes of a whole buffer, durability,
size and allocation. Everything else about files and directories is std::filesystem, called with its
std::error_code overloads: nothing here or there throws.

Errors are returned, std::error_code or std::expected, never logged or made fatal here: what a
failure means is the caller's to say. The one exception is a cache that can't be bypassed
(O_DIRECT refused, as on tmpfs): the file is used cached, and that is logged.

Created 10/21/1995 Heikki Tuuri
*******************************************************/

#pragma once

#include <sys/types.h>

#include <cstddef>
#include <cstdint>
#include <expected>
#include <filesystem>
#include <system_error>
#include <utility>

namespace os {

/** How a file is opened. */
enum class Open : uint8_t {
  /** An existing file, read only. */
  READ,

  /** An existing file, read and write. */
  READ_WRITE,

  /** A new file, read and write: an error if it exists. */
  CREATE,

  /** A new file, or an existing one emptied, read and write. */
  OVERWRITE
};

/** What errno doesn't say: the errors of this module's own category. */
enum class FileErr : int {
  /** A read of a whole buffer found the end of the file before it was full. */
  END_OF_FILE = 1
};

/** @return the category of FileErr. */
[[nodiscard]] const std::error_category &file_category() noexcept;

[[nodiscard]] inline std::error_code make_error_code(FileErr err) noexcept { return {int(err), file_category()}; }

/**
 * Read n bytes at off of a descriptor: all of them, through short reads and interrupts.
 *
 * @return an errno code, or FileErr::END_OF_FILE if the file ends before n bytes.
 */
[[nodiscard]] std::error_code read_at(int fd, void *buf, size_t n, off_t off) noexcept;

/**
 * Write n bytes at off of a descriptor: all of them, through short writes and interrupts.
 *
 * @return an errno code: ENOSPC for a full disk.
 */
[[nodiscard]] std::error_code write_at(int fd, const void *buf, size_t n, off_t off) noexcept;

/** An open file: owns its descriptor, closes it when it goes. Moves, doesn't copy. */
class File {
 public:
  File() noexcept = default;

  /** Own an open descriptor. */
  explicit File(int fd) noexcept : m_fd(fd) {}

  File(File &&other) noexcept : m_fd(std::exchange(other.m_fd, -1)) {}

  File &operator=(File &&other) noexcept {
    if (this != &other) {
      (void)close();
      m_fd = std::exchange(other.m_fd, -1);
    }

    return *this;
  }

  File(const File &) = delete;
  File &operator=(const File &) = delete;

  ~File() noexcept { (void)close(); }

  /**
   * Open a file.
   *
   * @param[in] path            The file.
   * @param[in] how             What for, see Open.
   * @param[in] direct          Bypass the OS cache (O_DIRECT): the file is used cached, and that is
   *                            logged, where the file system refuses it.
   *
   * @return the file, or an errno code: ENOENT, EEXIST for CREATE, EACCES...
   */
  [[nodiscard]] static std::expected<File, std::error_code> open(const std::filesystem::path &path, Open how, bool direct = false) noexcept;

  [[nodiscard]] bool is_open() const noexcept { return m_fd >= 0; }

  [[nodiscard]] int fd() const noexcept { return m_fd; }

  /** Read n bytes at off, see os::read_at(). */
  [[nodiscard]] std::error_code read(void *buf, size_t n, off_t off) const noexcept { return read_at(m_fd, buf, n, off); }

  /** Write n bytes at off, see os::write_at(). */
  [[nodiscard]] std::error_code write(const void *buf, size_t n, off_t off) const noexcept { return write_at(m_fd, buf, n, off); }

  /** Make what's written durable: fsync(), through interrupts and ENOLCK. */
  [[nodiscard]] std::error_code sync() const noexcept;

  /** @return the size of the file in bytes. */
  [[nodiscard]] std::expected<off_t, std::error_code> size() const noexcept;

  /**
   * Allocate len bytes of the file at off, which read as zeros after: the blocks are reserved
   * (fallocate), or written with zeros where the file system can't reserve them.
   *
   * @return an errno code: ENOSPC for a full disk, with what fits allocated.
   */
  [[nodiscard]] std::error_code allocate(off_t off, off_t len) const noexcept;

  /** Close the file. @return the error of close(), the descriptor is released anyway. */
  std::error_code close() noexcept;

  /** @return the descriptor, which this no longer owns. */
  [[nodiscard]] int release() noexcept { return std::exchange(m_fd, -1); }

 private:
  int m_fd{-1};
};

}  // namespace os

template <>
struct std::is_error_code_enum<os::FileErr> : std::true_type {};
