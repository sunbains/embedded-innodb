/****************************************************************************
Copyright (c) 2026 Sunny Bains. All rights reserved.

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

/** @file unit-tests/test_file.cc
Files for I/O, os0file.h: os::File, os::read_at() and os::write_at(), their errors.

The error paths are driven by fault injection: this program defines pwrite(), pread(), fsync() and
fallocate(), which the engine's calls bind to, and which fail as told for one file descriptor (an
"Arm"), otherwise they are the system calls. ENOSPC, EINTR, EAGAIN, EIO, ENOLCK, EOPNOTSUPP and short
reads and writes are tested that way, without root or a full disk. */

#include "ut0env.h"
#include "ut0perf.h"

#include "os0file.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <algorithm>
#include <cstdarg>
#include <atomic>
#include <cstring>
#include <format>
#include <iostream>
#include <fstream>
#include <random>
#include <string>
#include <thread>
#include <vector>

namespace {

/** What a wrapped system call does on the armed file descriptor. */
struct Fault {
  /** The descriptor the fault applies to, -1: none. */
  std::atomic<int> m_fd{-1};

  /** Calls that pass before the failures start. */
  std::atomic<int> m_skip{};

  /** The number of calls that fail with m_errno. */
  std::atomic<int> m_fail{};

  /** The errno of the failing calls. */
  std::atomic<int> m_errno{};

  /** If > 0, at most this many bytes per call: short reads and writes. */
  std::atomic<size_t> m_max{};

  /** Calls on m_fd. */
  std::atomic<int> m_calls{};

  /** @return true if this call on fd must fail (errno is set); n is capped. */
  bool hit(int fd, size_t &n) {
    if (fd != m_fd.load() || fd < 0) {
      return false;
    }

    ++m_calls;

    if (m_skip.load() > 0) {
      --m_skip;
    } else if (m_fail.load() > 0) {
      --m_fail;
      errno = m_errno.load();
      return true;
    }

    if (m_max.load() > 0 && n > m_max.load()) {
      n = m_max.load();
    }

    return false;
  }
};

Fault g_pwrite;
Fault g_pread;
Fault g_fsync;
Fault g_fallocate;
Fault g_setfl;

/** Arms a fault for the life of the object. */
struct Arm {
  Arm(Fault &f, int fd, int err, int fail, size_t max = 0, int skip = 0) : m_f(f) {
    m_f.m_skip = skip;
    m_f.m_fail = fail;
    m_f.m_errno = err;
    m_f.m_max = max;
    m_f.m_calls = 0;
    m_f.m_fd = fd;
  }

  ~Arm() {
    m_f.m_fd = -1;
    m_f.m_fail = 0;
    m_f.m_max = 0;
  }

  Fault &m_f;
};

}  // namespace

/* The engine's calls bind to these. */
extern "C" ssize_t pwrite(int fd, const void *buf, size_t n, off_t off) {
  if (g_pwrite.hit(fd, n)) {
    return -1;
  }

  return syscall(SYS_pwrite64, fd, buf, n, off);
}

extern "C" ssize_t pread(int fd, void *buf, size_t n, off_t off) {
  if (g_pread.hit(fd, n)) {
    return -1;
  }

  return syscall(SYS_pread64, fd, buf, n, off);
}

extern "C" int fsync(int fd) {
  size_t n{};

  if (g_fsync.hit(fd, n)) {
    return -1;
  }

  return int(syscall(SYS_fsync, fd));
}

/* F_SETFL only, the rest are the system call: the third argument as glibc reads it, a long. */
extern "C" int fcntl(int fd, int cmd, ...) {
  va_list ap;

  va_start(ap, cmd);

  const auto arg = va_arg(ap, long);

  va_end(ap);

  size_t n{};

  if (cmd == F_SETFL && g_setfl.hit(fd, n)) {
    return -1;
  }

  return int(syscall(SYS_fcntl, fd, cmd, arg));
}

extern "C" int fallocate(int fd, int mode, off_t off, off_t len) {
  size_t n{};

  if (g_fallocate.hit(fd, n)) {
    return -1;
  }

  return int(syscall(SYS_fallocate, fd, mode, off, len));
}

namespace {

namespace fs = std::filesystem;

/** @return n bytes from seed. */
std::vector<std::byte> pattern(size_t n, uint32_t seed) {
  std::mt19937 rng(seed);
  std::vector<std::byte> v(n);

  for (auto &b : v) {
    b = std::byte(rng());
  }

  return v;
}

/** @return the contents of the file at path. */
std::string slurp(const fs::path &path) {
  std::ifstream in(path, std::ios::binary);

  return std::string(std::istreambuf_iterator<char>(in), {});
}

/** @return true if fd is an open descriptor. */
bool is_open_fd(int fd) { return ::fcntl(fd, F_GETFD) != -1; }

/** A scratch directory and a few helpers. */
struct FileTest : public ::testing::Test {
  /** @return the path of name in the scratch directory. */
  fs::path p(const std::string &name) const { return m_dir.m_path / name; }

  /** @return a new file, read and write. */
  os::File create(const std::string &name) {
    auto file = os::File::open(p(name), os::Open::CREATE);

    EXPECT_TRUE(file) << name << ": " << file.error().message();

    return file ? std::move(*file) : os::File{};
  }

  /** Write a file with plain C++. */
  void put(const std::string &name, const std::string &contents) {
    std::ofstream out(p(name), std::ios::binary);

    out << contents;
  }

  ut::TestDir m_dir;
};

/* ------------------------------------------------------------- open */

/** CREATE makes a new file, rw-rw---- less the umask, and refuses one that exists. */
TEST_F(FileTest, Create) {
  auto file = create("f");

  ASSERT_TRUE(file.is_open());

  struct stat st;

  ASSERT_EQ(::fstat(file.fd(), &st), 0);
  EXPECT_TRUE(S_ISREG(st.st_mode));
  EXPECT_EQ(st.st_mode & (S_IRWXO | S_IXUSR | S_IXGRP), 0U) << "no execute, nothing for others";
  EXPECT_EQ(st.st_size, 0);

  const auto again = os::File::open(p("f"), os::Open::CREATE);

  ASSERT_FALSE(again);
  EXPECT_EQ(again.error(), std::errc::file_exists);
}

/** READ and READ_WRITE need the file; READ can't write. */
TEST_F(FileTest, OpenExisting) {
  const auto missing = os::File::open(p("missing"), os::Open::READ);

  ASSERT_FALSE(missing);
  EXPECT_EQ(missing.error(), std::errc::no_such_file_or_directory);
  EXPECT_FALSE(os::File::open(p("missing"), os::Open::READ_WRITE));

  put("f", "abc");

  auto ro = os::File::open(p("f"), os::Open::READ);

  ASSERT_TRUE(ro);

  char buf[3];

  EXPECT_FALSE(ro->read(buf, 3, 0));
  EXPECT_EQ(std::string(buf, 3), "abc");
  EXPECT_EQ(ro->write("x", 1, 0), std::errc::bad_file_descriptor) << "read only";

  auto rw = os::File::open(p("f"), os::Open::READ_WRITE);

  ASSERT_TRUE(rw);
  EXPECT_FALSE(rw->write("x", 1, 0));
  EXPECT_EQ(slurp(p("f")), "xbc");
}

/** OVERWRITE empties a file that exists, makes one that doesn't. */
TEST_F(FileTest, Overwrite) {
  put("f", "contents");

  auto file = os::File::open(p("f"), os::Open::OVERWRITE);

  ASSERT_TRUE(file);
  EXPECT_EQ(*file->size(), 0);

  EXPECT_TRUE(os::File::open(p("new"), os::Open::OVERWRITE));
  EXPECT_TRUE(fs::exists(p("new")));
}

/** A path that isn't a file: a directory can't be opened for writing, nor a path through a file. */
TEST_F(FileTest, OpenNotAFile) {
  fs::create_directory(p("d"));

  const auto dir = os::File::open(p("d"), os::Open::READ_WRITE);

  ASSERT_FALSE(dir);
  EXPECT_EQ(dir.error(), std::errc::is_a_directory);

  put("f", "x");

  const auto through = os::File::open(p("f") / "g", os::Open::CREATE);

  ASSERT_FALSE(through);
  EXPECT_EQ(through.error(), std::errc::not_a_directory);
}

/** direct: O_DIRECT where the file system takes it; the file works either way. */
TEST_F(FileTest, Direct) {
  auto file = os::File::open(p("f"), os::Open::CREATE, true);

  ASSERT_TRUE(file);

  /* tmpfs refuses O_DIRECT: then it's cached, and still a file. */
  const bool direct = (::fcntl(file->fd(), F_GETFL) & O_DIRECT) != 0;
  const auto data = pattern(4096, 1);
  auto buf = static_cast<std::byte *>(::operator new[](4096, std::align_val_t{4096}));

  std::memcpy(buf, data.data(), 4096);
  EXPECT_FALSE(file->write(buf, 4096, 0)) << "direct " << direct;
  std::memset(buf, 0, 4096);
  EXPECT_FALSE(file->read(buf, 4096, 0));
  EXPECT_EQ(std::memcmp(buf, data.data(), 4096), 0);

  ::operator delete[](buf, std::align_val_t{4096});

  EXPECT_EQ((::fcntl(create("cached").fd(), F_GETFL) & O_DIRECT), 0) << "not direct unless asked";
}

/** A file system that refuses O_DIRECT (tmpfs): the direct open works, cached. The refusal is
injected for every descriptor the open makes. */
TEST_F(FileTest, DirectRefused) {
  /* The next descriptor: the lowest free one, which a dup() of stdin and its close show. */
  const auto next = ::dup(0);

  ASSERT_GE(next, 0);
  ::close(next);

  Arm arm(g_setfl, next, EINVAL, 1);
  auto file = os::File::open(p("f"), os::Open::CREATE, true);

  ASSERT_TRUE(file) << file.error().message();
  ASSERT_EQ(file->fd(), next);
  EXPECT_EQ(g_setfl.m_calls, 1) << "O_DIRECT was asked for";
  EXPECT_EQ(::fcntl(file->fd(), F_GETFL) & O_DIRECT, 0) << "cached";
  EXPECT_FALSE(file->write("abc", 3, 0));
  EXPECT_EQ(slurp(p("f")), "abc");
}

/* ------------------------------------------------------- read, write */

/** What's written is read back, at any offset, past a hole of zeros. */
TEST_F(FileTest, RoundTrip) {
  auto file = create("f");
  const auto data = pattern(10000, 2);

  ASSERT_FALSE(file.write(data.data(), data.size(), 5000));

  std::vector<std::byte> back(data.size());

  ASSERT_FALSE(file.read(back.data(), back.size(), 5000));
  EXPECT_EQ(back, data);

  std::vector<std::byte> hole(5000, std::byte{1});

  ASSERT_FALSE(file.read(hole.data(), hole.size(), 0));
  EXPECT_EQ(hole, std::vector<std::byte>(5000)) << "a hole reads as zeros";
  EXPECT_EQ(*file.size(), 15000);
}

/** Zero bytes: nothing to do, at any offset, even past the end. */
TEST_F(FileTest, ZeroBytes) {
  auto file = create("f");

  EXPECT_FALSE(file.write(nullptr, 0, 1 << 20));
  EXPECT_FALSE(file.read(nullptr, 0, 1 << 20));
  EXPECT_EQ(*file.size(), 0);
}

/** A read past the end is END_OF_FILE, in its own category; up to the end is fine. */
TEST_F(FileTest, ReadPastEnd) {
  auto file = create("f");

  ASSERT_FALSE(file.write("abcd", 4, 0));

  char buf[8];

  EXPECT_FALSE(file.read(buf, 4, 0)) << "to the end";

  const auto err = file.read(buf, 5, 0);

  EXPECT_EQ(err, os::FileErr::END_OF_FILE);
  EXPECT_EQ(&err.category(), &os::file_category());
  EXPECT_STREQ(err.category().name(), "os::File");
  EXPECT_FALSE(err.message().empty());
  EXPECT_EQ(file.read(buf, 1, 4), os::FileErr::END_OF_FILE) << "at the end";
  EXPECT_EQ(file.read(buf, 1, 100), os::FileErr::END_OF_FILE) << "past the end";
}

/** Short reads and writes, a byte at a time at worst: still the whole buffer. */
TEST_F(FileTest, Short) {
  auto file = create("f");
  const auto data = pattern(1000, 3);

  {
    Arm arm(g_pwrite, file.fd(), 0, 0, 7);

    ASSERT_FALSE(file.write(data.data(), data.size(), 0));
    EXPECT_EQ(g_pwrite.m_calls, int((1000 + 6) / 7));
  }

  std::vector<std::byte> back(data.size());

  {
    Arm arm(g_pread, file.fd(), 0, 0, 1);

    ASSERT_FALSE(file.read(back.data(), back.size(), 0));
    EXPECT_EQ(g_pread.m_calls, 1000);
  }

  EXPECT_EQ(back, data);
}

/** EINTR and EAGAIN are retried, for reads and writes. */
TEST_F(FileTest, Retries) {
  auto file = create("f");
  const auto data = pattern(100, 4);

  for (const int err : {EINTR, EAGAIN}) {
    {
      Arm arm(g_pwrite, file.fd(), err, 3);

      EXPECT_FALSE(file.write(data.data(), data.size(), 0)) << err;
      EXPECT_EQ(g_pwrite.m_calls, 4) << err;
    }

    std::vector<std::byte> back(data.size());
    Arm arm(g_pread, file.fd(), err, 3);

    EXPECT_FALSE(file.read(back.data(), back.size(), 0)) << err;
    EXPECT_EQ(g_pread.m_calls, 4) << err;
    EXPECT_EQ(back, data);
  }
}

/** The other errors are returned as they are, after what was done before them. */
TEST_F(FileTest, Errors) {
  auto file = create("f");
  const auto data = pattern(100, 5);

  {
    Arm arm(g_pwrite, file.fd(), ENOSPC, 1);

    EXPECT_EQ(file.write(data.data(), data.size(), 0), std::errc::no_space_on_device);
  }

  {
    /* The first 10 bytes go, then the disk is full. */
    Arm arm(g_pwrite, file.fd(), ENOSPC, 1, 10, 1);

    EXPECT_EQ(file.write(data.data(), data.size(), 0), std::errc::no_space_on_device);
    EXPECT_EQ(*file.size(), 10);
  }

  ASSERT_FALSE(file.write(data.data(), data.size(), 0));

  std::vector<std::byte> back(data.size());
  Arm arm(g_pread, file.fd(), EIO, 1);

  EXPECT_EQ(file.read(back.data(), back.size(), 0), std::errc::io_error);
}

/** read_at() and write_at() on a descriptor the caller owns. */
TEST_F(FileTest, AtDescriptor) {
  const auto fd = ::open(p("f").c_str(), O_RDWR | O_CREAT, 0600);

  ASSERT_GE(fd, 0);
  EXPECT_FALSE(os::write_at(fd, "hello", 5, 3));

  char buf[5];

  EXPECT_FALSE(os::read_at(fd, buf, 5, 3));
  EXPECT_EQ(std::string(buf, 5), "hello");
  EXPECT_EQ(os::read_at(fd, buf, 5, 4), os::FileErr::END_OF_FILE);
  EXPECT_TRUE(is_open_fd(fd)) << "not closed";
  ::close(fd);

  EXPECT_EQ(os::write_at(fd, "x", 1, 0), std::errc::bad_file_descriptor) << "closed";
  EXPECT_EQ(os::read_at(-1, buf, 1, 0), std::errc::bad_file_descriptor);
}

/** Disjoint blocks written by threads at once are all there. */
TEST_F(FileTest, Concurrent) {
  auto file = create("f");
  constexpr size_t BLOCK = 4096;
  constexpr int THREADS = 8;
  constexpr int PER = 64;
  std::vector<std::thread> threads;

  for (int t = 0; t < THREADS; ++t) {
    threads.emplace_back([&, t] {
      for (int i = 0; i < PER; ++i) {
        const auto data = pattern(BLOCK, uint32_t(t * PER + i));

        EXPECT_FALSE(file.write(data.data(), BLOCK, off_t(t * PER + i) * off_t(BLOCK)));
      }
    });
  }

  for (auto &t : threads) {
    t.join();
  }

  std::vector<std::byte> back(BLOCK);

  for (int b = 0; b < THREADS * PER; ++b) {
    ASSERT_FALSE(file.read(back.data(), BLOCK, off_t(b) * off_t(BLOCK)));
    ASSERT_EQ(back, pattern(BLOCK, uint32_t(b))) << b;
  }
}

/* ------------------------------------------------------- sync, size */

/** sync() makes it durable; ENOLCK and EINTR are retried, an error is returned. */
TEST_F(FileTest, Sync) {
  auto file = create("f");

  ASSERT_FALSE(file.write("x", 1, 0));
  EXPECT_FALSE(file.sync());

  {
    Arm arm(g_fsync, file.fd(), ENOLCK, 2);

    EXPECT_FALSE(file.sync());
    EXPECT_EQ(g_fsync.m_calls, 3);
  }

  {
    Arm arm(g_fsync, file.fd(), EINTR, 2);

    EXPECT_FALSE(file.sync());
    EXPECT_EQ(g_fsync.m_calls, 3);
  }

  Arm arm(g_fsync, file.fd(), EIO, 1);

  EXPECT_EQ(file.sync(), std::errc::io_error);
}

/** The size follows the writes; a closed file has none. */
TEST_F(FileTest, Size) {
  auto file = create("f");

  EXPECT_EQ(*file.size(), 0);
  ASSERT_FALSE(file.write("abc", 3, 1 << 20));
  EXPECT_EQ(*file.size(), (1 << 20) + 3);

  (void)file.close();

  const auto size = file.size();

  ASSERT_FALSE(size);
  EXPECT_EQ(size.error(), std::errc::bad_file_descriptor);
}

/* ---------------------------------------------------------- allocate */

/** allocate() grows the file to off + len, the new part reads as zeros, what was there stays. */
TEST_F(FileTest, Allocate) {
  auto file = create("f");

  ASSERT_FALSE(file.write("abc", 3, 0));
  ASSERT_FALSE(file.allocate(0, 1 << 20));
  EXPECT_EQ(*file.size(), 1 << 20);

  std::vector<char> back(1 << 20);

  ASSERT_FALSE(file.read(back.data(), back.size(), 0));
  EXPECT_EQ(std::string(back.data(), 3), "abc");
  EXPECT_TRUE(std::all_of(back.begin() + 3, back.end(), [](char c) { return c == 0; }));

  /* Inside the file: nothing changes. */
  ASSERT_FALSE(file.allocate(100, 100));
  EXPECT_EQ(*file.size(), 1 << 20);

  /* Nothing to allocate. */
  EXPECT_FALSE(file.allocate(1 << 21, 0));
  EXPECT_EQ(*file.size(), 1 << 20);
}

/** Where fallocate() isn't supported, zeros are written: the same file. */
TEST_F(FileTest, AllocateFallback) {
  for (const int err : {EOPNOTSUPP, ENOSYS}) {
    auto file = create(std::format("f{}", err));

    ASSERT_FALSE(file.write("abc", 3, 0));

    {
      Arm fa(g_fallocate, file.fd(), err, 1);
      Arm pw(g_pwrite, file.fd(), 0, 0);

      /* Not a whole chunk of zeros: the last write is short of it. */
      ASSERT_FALSE(file.allocate(3, (3 << 20) + 5)) << err;
      EXPECT_EQ(g_fallocate.m_calls, 1);
      EXPECT_EQ(g_pwrite.m_calls, 4) << "1 MB at a time";
    }

    EXPECT_EQ(*file.size(), (3 << 20) + 8);

    const auto contents = slurp(p(std::format("f{}", err)));

    EXPECT_EQ(contents.substr(0, 3), "abc");
    EXPECT_EQ(contents.find_first_not_of('\0', 3), std::string::npos);
  }
}

/** Out of space: returned, from fallocate() or from the zeros written in its place. */
TEST_F(FileTest, AllocateNoSpace) {
  auto file = create("f");

  {
    Arm arm(g_fallocate, file.fd(), ENOSPC, 1);

    EXPECT_EQ(file.allocate(0, 1 << 20), std::errc::no_space_on_device);
  }

  Arm fa(g_fallocate, file.fd(), EOPNOTSUPP, 1);
  Arm pw(g_pwrite, file.fd(), ENOSPC, 1, 0, 1);

  EXPECT_EQ(file.allocate(0, 3 << 20), std::errc::no_space_on_device);
  EXPECT_EQ(*file.size(), 1 << 20) << "what fits";
}

/* ----------------------------------------------------- close, move */

/** The destructor closes; close() closes once, a second is nothing; close() says what close(2) said. */
TEST_F(FileTest, Close) {
  int fd;

  {
    auto file = create("f");

    fd = file.fd();
    EXPECT_TRUE(is_open_fd(fd));
  }

  EXPECT_FALSE(is_open_fd(fd)) << "closed by the destructor";

  auto file = create("g");

  fd = file.fd();
  EXPECT_FALSE(file.close());
  EXPECT_FALSE(file.is_open());
  EXPECT_EQ(file.fd(), -1);
  EXPECT_FALSE(file.close()) << "again: nothing";

  /* Closed behind its back: the error of close(2), the descriptor is let go anyway. */
  auto other = create("h");

  ::close(other.fd());
  EXPECT_EQ(other.close(), std::errc::bad_file_descriptor);
  EXPECT_FALSE(other.is_open());
}

/** A move takes the descriptor; an assignment closes the one it replaces; release() gives it up. */
TEST_F(FileTest, Move) {
  auto a = create("a");
  const auto fd_a = a.fd();
  os::File b{std::move(a)};

  EXPECT_FALSE(a.is_open());
  EXPECT_EQ(b.fd(), fd_a);

  auto c = create("c");
  const auto fd_c = c.fd();

  c = std::move(b);
  EXPECT_FALSE(is_open_fd(fd_c)) << "replaced, closed";
  EXPECT_EQ(c.fd(), fd_a);

  /* Self assignment keeps it. */
  auto &self = c;

  c = std::move(self);
  EXPECT_EQ(c.fd(), fd_a);
  EXPECT_TRUE(is_open_fd(fd_a));

  const auto fd = c.release();

  EXPECT_EQ(fd, fd_a);
  EXPECT_FALSE(c.is_open());
  EXPECT_TRUE(is_open_fd(fd)) << "released, not closed";
  ::close(fd);

  /* A default one is closed, and closing it is nothing. */
  os::File none;

  EXPECT_FALSE(none.is_open());
  EXPECT_FALSE(none.close());
}

/* ------------------------------------------------------------- perf */

struct FilePerf : public FileTest {};

/** pwrite/pread of 4 KB and 16 KB in the page cache, and the fsync latency. */
TEST_F(FilePerf, Io) {
  auto file = create("f");
  const auto data = pattern(16384, 1);
  std::vector<std::byte> back(16384);

  ASSERT_FALSE(file.allocate(0, 1024 * 4096));

  auto w = ut::bench("file_write_4k", 200000, [&](uint64_t i) { (void)file.write(data.data(), 4096, off_t(i % 1024) * 4096); });
  auto r = ut::bench("file_read_4k", 200000, [&](uint64_t i) { (void)file.read(back.data(), 4096, off_t(i % 1024) * 4096); });
  auto w16 = ut::bench("file_write_16k", 50000, [&](uint64_t i) { (void)file.write(data.data(), 16384, off_t(i % 256) * 16384); });

  /* fsync() is the device's latency. */
  auto s = ut::bench("file_write_4k_sync", 50, [&](uint64_t i) {
    (void)file.write(data.data(), 4096, off_t(i % 1024) * 4096);
    (void)file.sync();
  });
  auto sz = ut::bench("file_size", 200000, [&](uint64_t) { ut::keep(*file.size()); });

  EXPECT_LT(w.ns_per_op(), 200000);
  EXPECT_LT(r.ns_per_op(), 200000);
  EXPECT_LT(w16.ns_per_op(), 400000);
  EXPECT_LT(s.ns_per_op(), 2e9);
  EXPECT_LT(sz.ns_per_op(), 100000);
}

/** allocate() of 64 MB, by fallocate() and by writing zeros. */
TEST_F(FilePerf, Allocate) {
  int i{};

  auto r = ut::bench("file_allocate_64m", 3, [&](uint64_t) {
    auto file = create(std::format("a{}", i++));
    (void)file.allocate(0, 64 << 20);
  });

  auto z = ut::bench("file_allocate_64m_zeros", 3, [&](uint64_t) {
    auto file = create(std::format("z{}", i++));
    Arm arm(g_fallocate, file.fd(), EOPNOTSUPP, 1);
    (void)file.allocate(0, 64 << 20);
  });

  std::cout << std::format("PERF allocate zeros {:.0f} MB/s\n", 64.0 / (z.ns_per_op() / 1e9));
  EXPECT_LT(r.ns_per_op(), 1e9);
  EXPECT_LT(z.ns_per_op(), 30e9);
}

/** 8 threads write 4 KB to one file. */
TEST_F(FilePerf, ConcurrentWrite) {
  auto file = create("f");
  const auto data = pattern(4096, 2);

  ASSERT_FALSE(file.allocate(0, 8 * 256 * 4096));

  auto r = ut::bench_mt("file_write_4k_mt", 8, 20000, [&](int t, uint64_t i) {
    (void)file.write(data.data(), 4096, off_t(t * 256 + int(i % 256)) * 4096);
  });

  EXPECT_LT(r.ns_per_op(), 200000);
}

}  // namespace
