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

/** @file os/os0file.cc
Files for I/O, see os0file.h.

Created 10/21/1995 Heikki Tuuri
*******************************************************/

#include "os0file.h"

#include "innodb0types.h"
#include "ut0logger.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <format>
#include <memory>
#include <new>
#include <string>
#include <thread>

namespace os {

namespace {

/** rw-rw----, less the umask. */
constexpr mode_t CREATE_MODE = S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP;

/** The alignment of a buffer for a file opened with O_DIRECT. */
constexpr size_t ALIGN = 4096;

/** The zeros written at a time where blocks can't be reserved. */
constexpr size_t ZERO_CHUNK = 1024 * 1024;

[[nodiscard]] inline std::error_code last_error() noexcept { return {errno, std::generic_category()}; }

struct FileCategory final : std::error_category {
  [[nodiscard]] const char *name() const noexcept override { return "os::File"; }

  [[nodiscard]] std::string message(int ev) const override {
    switch (FileErr(ev)) {
      case FileErr::END_OF_FILE:
        return "end of file before the buffer is full";
    }

    return "unknown os::File error";
  }
};

[[nodiscard]] int flags_of(Open how) noexcept {
  switch (how) {
    case Open::READ:
      return O_RDONLY;
    case Open::READ_WRITE:
      return O_RDWR;
    case Open::CREATE:
      return O_RDWR | O_CREAT | O_EXCL;
    case Open::OVERWRITE:
      return O_RDWR | O_CREAT | O_TRUNC;
  }

  return O_RDONLY;
}

}  // namespace

const std::error_category &file_category() noexcept {
  static const FileCategory category;

  return category;
}

std::error_code read_at(int fd, void *buf, size_t n, off_t off) noexcept {
  auto ptr = static_cast<char *>(buf);

  while (n > 0) {
    const auto got = ::pread(fd, ptr, n, off);

    if (got < 0) {
      if (errno == EINTR || errno == EAGAIN) {
        continue;
      }

      return last_error();
    }

    if (got == 0) {
      return FileErr::END_OF_FILE;
    }

    ptr += got;
    n -= size_t(got);
    off += got;
  }

  return {};
}

std::error_code write_at(int fd, const void *buf, size_t n, off_t off) noexcept {
  auto ptr = static_cast<const char *>(buf);

  while (n > 0) {
    const auto put = ::pwrite(fd, ptr, n, off);

    if (put < 0) {
      if (errno == EINTR || errno == EAGAIN) {
        continue;
      }

      return last_error();
    }

    /* Nothing written and no error: the device takes no more, as a full disk. */
    if (put == 0) {
      return std::make_error_code(std::errc::no_space_on_device);
    }

    ptr += put;
    n -= size_t(put);
    off += put;
  }

  return {};
}

std::expected<File, std::error_code> File::open(const std::filesystem::path &path, Open how, bool direct) noexcept {
  int fd;

  do {
    fd = ::open(path.c_str(), flags_of(how) | O_CLOEXEC, CREATE_MODE);
  } while (fd < 0 && errno == EINTR);

  if (fd < 0) {
    return std::unexpected(last_error());
  }

  if (direct) {
    const auto flags = ::fcntl(fd, F_GETFL);

    if (flags < 0 || ::fcntl(fd, F_SETFL, flags | O_DIRECT) < 0) {
      /* Not an error: the file works, through the cache. */
      log_warn(std::format("O_DIRECT refused for {}: {}, the file is used cached", path.string(), last_error().message()));
    }
  }

  return File{fd};
}

std::error_code File::sync() const noexcept {
  for (;;) {
    if (::fsync(m_fd) == 0) {
      return {};
    }

    if (errno == EINTR) {
      continue;
    }

    if (errno == ENOLCK) {
      /* No locks available, on NFS: they come back. */
      std::this_thread::sleep_for(std::chrono::milliseconds(200));
      continue;
    }

    return last_error();
  }
}

std::expected<off_t, std::error_code> File::size() const noexcept {
  struct stat st;

  if (::fstat(m_fd, &st) != 0) {
    return std::unexpected(last_error());
  }

  return st.st_size;
}

std::error_code File::allocate(off_t off, off_t len) const noexcept {
  if (len <= 0) {
    return {};
  }

  if (::fallocate(m_fd, 0, off, len) == 0) {
    return {};
  }

  if (errno != EOPNOTSUPP && errno != ENOSYS) {
    return last_error();
  }

  /* The file system can't reserve blocks: write zeros, aligned for O_DIRECT. */
  struct Free {
    void operator()(std::byte *p) const noexcept { ::operator delete[](p, std::align_val_t{ALIGN}); }
  };

  const auto chunk = size_t(std::min<off_t>(len, ZERO_CHUNK));
  std::unique_ptr<std::byte[], Free> zeros{static_cast<std::byte *>(::operator new[](chunk, std::align_val_t{ALIGN}, std::nothrow))};

  if (zeros == nullptr) {
    return std::make_error_code(std::errc::not_enough_memory);
  }

  std::fill_n(zeros.get(), chunk, std::byte{});

  for (const auto end = off + len; off < end;) {
    const auto n = size_t(std::min<off_t>(end - off, off_t(chunk)));

    if (const auto err = write(zeros.get(), n, off); err) {
      return err;
    }

    off += off_t(n);
  }

  return {};
}

std::error_code File::close() noexcept {
  if (m_fd < 0) {
    return {};
  }

  /* Not again on EINTR: on Linux the descriptor is gone whatever close() says. */
  const auto fd = std::exchange(m_fd, -1);

  return ::close(fd) == 0 ? std::error_code{} : last_error();
}

}  // namespace os
