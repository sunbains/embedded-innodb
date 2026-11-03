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

/** @file unit-tests/test_os.cc
The OS layer: files (os0file.cc), threads (os0thread.cc), large memory
blocks (os0proc.cc) and what test_sync.cc doesn't cover of os0sync.cc.

The error paths of the file functions are driven by fault injection: this
program defines pwrite(), pread() and fsync(), which the engine's calls bind
to, and which fail as told for one file descriptor (an "Arm"), otherwise
they are the system calls. ENOSPC, EINTR, EAGAIN, EIO, short reads and
writes are tested that way, without root or a full disk. */

#include "ut0env.h"
#include "ut0perf.h"

#include "mem0mem.h"
#include "os0file.h"
#include "os0proc.h"
#include "os0sync.h"
#include "os0thread.h"
#include "ut0byte.h"
#include "ut0mem.h"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <cstring>
#include <fstream>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <vector>

using namespace std::chrono_literals;

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

/* The engine's pwrite(), pread() and fsync() bind to these. */
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

namespace {

namespace fs = std::filesystem;

/** @return n bytes from seed. */
std::vector<byte> pattern(size_t n, uint32_t seed) {
  std::mt19937 rng(seed);
  std::vector<byte> v(n);

  for (auto &b : v) {
    b = byte(rng());
  }

  return v;
}

/** @return the contents of the file at path. */
std::string slurp(const fs::path &path) {
  std::ifstream in(path, std::ios::binary);

  return std::string(std::istreambuf_iterator<char>(in), {});
}

/** A scratch directory and a few helpers. */
struct OsFileTest : public ::testing::Test {
  /** @return the path of name in the scratch directory. */
  std::string p(const std::string &name) const { return (m_dir.m_path / name).string(); }

  /** @return a new file opened read-write with os_file_create(). */
  os_file_t create(const std::string &name, FlushMethod fm = SRV_UNIX_FSYNC) {
    bool ok{};
    const auto fh = os_file_create(p(name).c_str(), OS_FILE_CREATE, OS_FILE_NORMAL, fm, &ok);

    EXPECT_TRUE(ok) << name;
    EXPECT_GE(fh, 0) << name;

    return fh;
  }

  /** Write a file with plain C++. */
  void put(const std::string &name, const std::string &contents) {
    std::ofstream out(p(name), std::ios::binary);

    out << contents;
  }

  ut::TestDir m_dir;
};

using OsFileDeathTest = OsFileTest;

/* ------------------------------------------------------- create / open */

/** OS_FILE_CREATE makes a new file, rw-rw---- less the umask. */
TEST_F(OsFileTest, CreateSimpleNew) {
  bool ok{};
  const auto fh = os_file_create_simple(p("f").c_str(), OS_FILE_CREATE, OS_FILE_READ_WRITE, &ok);

  ASSERT_TRUE(ok);
  ASSERT_GE(fh, 0);

  struct stat st;

  ASSERT_EQ(fstat(fh, &st), 0);
  EXPECT_TRUE(S_ISREG(st.st_mode));
  EXPECT_EQ(st.st_size, 0);

  const auto mask = umask(0);

  umask(mask);
  EXPECT_EQ(st.st_mode & 0777, 0660U & ~mask);
  EXPECT_EQ(fcntl(fh, F_GETFL) & O_ACCMODE, O_RDWR);
  EXPECT_TRUE(os_file_close(fh));
}

/** OS_FILE_CREATE of a file that exists fails, without exiting. */
TEST_F(OsFileTest, CreateSimpleExists) {
  put("f", "x");

  bool ok{true};
  const auto fh = os_file_create_simple(p("f").c_str(), OS_FILE_CREATE, OS_FILE_READ_WRITE, &ok);

  EXPECT_FALSE(ok);
  EXPECT_EQ(fh, -1);
  EXPECT_EQ(slurp(p("f")), "x");
}

/** OS_FILE_OPEN read-only and read-write give the matching access mode. */
TEST_F(OsFileTest, CreateSimpleOpenAccess) {
  put("f", "abc");

  bool ok{};
  auto fh = os_file_create_simple(p("f").c_str(), OS_FILE_OPEN, OS_FILE_READ_ONLY, &ok);

  ASSERT_TRUE(ok);
  EXPECT_EQ(fcntl(fh, F_GETFL) & O_ACCMODE, O_RDONLY);
  /* Writes to it fail at the system level. */
  EXPECT_EQ(::pwrite(fh, "z", 1, 0), -1);
  EXPECT_EQ(errno, EBADF);
  EXPECT_TRUE(os_file_close(fh));

  fh = os_file_create_simple(p("f").c_str(), OS_FILE_OPEN, OS_FILE_READ_WRITE, &ok);
  ASSERT_TRUE(ok);
  EXPECT_EQ(fcntl(fh, F_GETFL) & O_ACCMODE, O_RDWR);
  /* OPEN doesn't truncate. */
  EXPECT_EQ(os_file_get_size_as_iblonglong(fh), 3);
  EXPECT_TRUE(os_file_close(fh));
}

/** os_file_create_simple() exits on a missing file: only "already exists",
"disk full" and path errors are returned to the caller. The header says the
error "can be retrieved with os_file_get_last_error"; callers that expect
the file may be missing must use the _no_error_handling variant. */
TEST_F(OsFileDeathTest, CreateSimpleOpenMissingIsFatal) {
  bool ok{};

  EXPECT_DEATH(
    (void)os_file_create_simple(p("missing").c_str(), OS_FILE_OPEN, OS_FILE_READ_ONLY, &ok), "Cannot continue operation"
  );
}

/** OS_FILE_CREATE_PATH creates the missing directories on the way. */
TEST_F(OsFileTest, CreateSimpleCreatePath) {
  bool ok{};
  const auto fh = os_file_create_simple(p("a/b/c/f").c_str(), OS_FILE_CREATE_PATH, OS_FILE_READ_WRITE, &ok);

  ASSERT_TRUE(ok);
  EXPECT_TRUE(fs::is_directory(p("a/b/c")));
  EXPECT_TRUE(fs::is_regular_file(p("a/b/c/f")));
  EXPECT_TRUE(os_file_close(fh));

  /* Again: the directories exist, the file too. */
  const auto fh2 = os_file_create_simple(p("a/b/c/f").c_str(), OS_FILE_CREATE_PATH, OS_FILE_READ_WRITE, &ok);

  EXPECT_FALSE(ok);
  EXPECT_EQ(fh2, -1);
}

/** A create mode the function doesn't know asserts. */
TEST_F(OsFileDeathTest, CreateSimpleBadModeAsserts) {
  bool ok{};

  EXPECT_DEATH((void)os_file_create_simple(p("f").c_str(), OS_FILE_OVERWRITE, OS_FILE_READ_WRITE, &ok), "");
  EXPECT_DEATH((void)os_file_create_simple_no_error_handling(p("f").c_str(), OS_FILE_OVERWRITE, OS_FILE_READ_WRITE, &ok), "");
}

/** The _no_error_handling variant returns all errors. */
TEST_F(OsFileTest, NoErrorHandlingErrors) {
  bool ok{true};

  EXPECT_EQ(os_file_create_simple_no_error_handling(p("missing").c_str(), OS_FILE_OPEN, OS_FILE_READ_ONLY, &ok), -1);
  EXPECT_FALSE(ok);

  put("f", "x");
  ok = true;
  EXPECT_EQ(os_file_create_simple_no_error_handling(p("f").c_str(), OS_FILE_CREATE, OS_FILE_READ_WRITE, &ok), -1);
  EXPECT_FALSE(ok);

  ok = true;
  EXPECT_EQ(os_file_create_simple_no_error_handling(p("nodir/f").c_str(), OS_FILE_CREATE, OS_FILE_READ_WRITE, &ok), -1);
  EXPECT_FALSE(ok);

  const auto fh = os_file_create_simple_no_error_handling(p("g").c_str(), OS_FILE_CREATE, OS_FILE_READ_WRITE, &ok);

  EXPECT_TRUE(ok);
  EXPECT_TRUE(os_file_close(fh));
}

/** @return the result of fn() run in a child process: 1 true, 0 false. */
template <typename F>
int in_child(F &&fn) {
  const auto pid = fork();

  if (pid == 0) {
    _exit(fn() ? 1 : 0);
  }

  int status{};

  waitpid(pid, &status, 0);

  return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
}

/** OPEN read-write takes an exclusive lock: another process can't open the
file read-write, it can read-only, and can after the close. */
TEST_F(OsFileTest, NoErrorHandlingLocks) {
  put("f", "x");

  const auto path = p("f");
  bool ok{};
  const auto fh = os_file_create_simple_no_error_handling(path.c_str(), OS_FILE_OPEN, OS_FILE_READ_WRITE, &ok);

  ASSERT_TRUE(ok);

  auto try_open = [&](ulint access) {
    return [&path, access] {
      bool ok2{};
      const auto fh2 = os_file_create_simple_no_error_handling(path.c_str(), OS_FILE_OPEN, access, &ok2);

      if (ok2) {
        close(fh2);
      }

      return ok2;
    };
  };

  EXPECT_EQ(in_child(try_open(OS_FILE_READ_WRITE)), 0);
  EXPECT_EQ(in_child(try_open(OS_FILE_READ_ONLY)), 1);

  EXPECT_TRUE(os_file_close(fh));

  EXPECT_EQ(in_child(try_open(OS_FILE_READ_WRITE)), 1);
}

/** The lock is a POSIX record lock: per process. A second read-write open
in the same process succeeds, and closing either descriptor drops the lock
for both (the first one is then unprotected). Pinned, as the engine must
not open a locked file twice. */
TEST_F(OsFileTest, NoErrorHandlingLockIsPerProcess) {
  put("f", "x");

  const auto path = p("f");
  bool ok{};
  const auto fh1 = os_file_create_simple_no_error_handling(path.c_str(), OS_FILE_OPEN, OS_FILE_READ_WRITE, &ok);

  ASSERT_TRUE(ok);

  const auto fh2 = os_file_create_simple_no_error_handling(path.c_str(), OS_FILE_OPEN, OS_FILE_READ_WRITE, &ok);

  ASSERT_TRUE(ok);
  EXPECT_TRUE(os_file_close(fh2));

  auto child = [&path] {
    bool ok2{};
    const auto fh = os_file_create_simple_no_error_handling(path.c_str(), OS_FILE_OPEN, OS_FILE_READ_WRITE, &ok2);

    if (ok2) {
      close(fh);
    }

    return ok2;
  };

  EXPECT_EQ(in_child(child), 1);
  EXPECT_TRUE(os_file_close(fh1));
}

/** os_file_create(): CREATE, OPEN, OPEN_RETRY, OVERWRITE, and their errors
returned without exiting. */
TEST_F(OsFileTest, CreateModes) {
  bool ok{};
  auto fh = os_file_create(p("f").c_str(), OS_FILE_CREATE, OS_FILE_NORMAL, SRV_UNIX_FSYNC, &ok);

  ASSERT_TRUE(ok);
  EXPECT_TRUE(os_file_write("f", fh, "hello", 5, 0));
  EXPECT_TRUE(os_file_close(fh));

  /* CREATE of an existing file. */
  ok = true;
  EXPECT_EQ(os_file_create(p("f").c_str(), OS_FILE_CREATE, OS_FILE_NORMAL, SRV_UNIX_FSYNC, &ok), -1);
  EXPECT_FALSE(ok);

  /* OPEN of a missing file. */
  ok = true;
  EXPECT_EQ(os_file_create(p("missing").c_str(), OS_FILE_OPEN, OS_FILE_NORMAL, SRV_UNIX_FSYNC, &ok), -1);
  EXPECT_FALSE(ok);

  /* CREATE in a missing directory. */
  ok = true;
  EXPECT_EQ(os_file_create(p("nodir/f").c_str(), OS_FILE_CREATE, OS_FILE_AIO, SRV_UNIX_FSYNC, &ok), -1);
  EXPECT_FALSE(ok);

  for (const auto mode : {OS_FILE_OPEN, OS_FILE_OPEN_RETRY, OS_FILE_OPEN_RAW}) {
    fh = os_file_create(p("f").c_str(), mode, OS_FILE_AIO, SRV_UNIX_FSYNC, &ok);
    ASSERT_TRUE(ok) << mode;
    EXPECT_EQ(os_file_get_size_as_iblonglong(fh), 5) << mode;
    EXPECT_EQ(fcntl(fh, F_GETFL) & O_ACCMODE, O_RDWR);
    EXPECT_TRUE(os_file_close(fh));
  }

  /* OVERWRITE truncates an existing file and creates a missing one. */
  fh = os_file_create(p("f").c_str(), OS_FILE_OVERWRITE, OS_FILE_NORMAL, SRV_UNIX_FSYNC, &ok);
  ASSERT_TRUE(ok);
  EXPECT_EQ(os_file_get_size_as_iblonglong(fh), 0);
  EXPECT_TRUE(os_file_close(fh));

  fh = os_file_create(p("g").c_str(), OS_FILE_OVERWRITE, OS_FILE_NORMAL, SRV_UNIX_FSYNC, &ok);
  ASSERT_TRUE(ok);
  EXPECT_TRUE(os_file_close(fh));
}

/** An unknown purpose or create mode asserts. */
TEST_F(OsFileDeathTest, CreateBadArgsAssert) {
  bool ok{};

  EXPECT_DEATH((void)os_file_create(p("f").c_str(), OS_FILE_CREATE, 0, SRV_UNIX_FSYNC, &ok), "");
  EXPECT_DEATH((void)os_file_create(p("f").c_str(), OS_FILE_CREATE_PATH, OS_FILE_NORMAL, SRV_UNIX_FSYNC, &ok), "");
}

/** SRV_UNIX_O_DIRECT sets O_DIRECT; aligned I/O works through it. */
TEST_F(OsFileTest, ODirect) {
  const auto fh = create("f", SRV_UNIX_O_DIRECT);

  ASSERT_NE(fcntl(fh, F_GETFL) & O_DIRECT, 0) << "the file system of TMPDIR doesn't do O_DIRECT?";

  auto mem = static_cast<byte *>(ut_new(3 * 4096));
  auto buf = static_cast<byte *>(ut_align(mem, 4096));

  std::memset(buf, 0x5A, 2 * 4096);
  EXPECT_TRUE(os_file_write("f", fh, buf, 2 * 4096, 4096));
  std::memset(buf, 0, 2 * 4096);
  EXPECT_TRUE(os_file_read(fh, buf, 2 * 4096, 4096));
  EXPECT_EQ(buf[0], 0x5A);
  EXPECT_EQ(buf[2 * 4096 - 1], 0x5A);
  EXPECT_TRUE(os_file_flush(fh));
  EXPECT_EQ(os_file_get_size_as_iblonglong(fh), 3 * 4096);

  ut_delete(mem);
  EXPECT_TRUE(os_file_close(fh));
}

/** Other flush methods don't set O_DIRECT. */
TEST_F(OsFileTest, NoODirect) {
  const auto fh = create("f", SRV_UNIX_FSYNC);

  EXPECT_EQ(fcntl(fh, F_GETFL) & O_DIRECT, 0);
  EXPECT_TRUE(os_file_close(fh));
}

/** An unaligned write to an O_DIRECT file fails with EINVAL, which is fatal. */
TEST_F(OsFileDeathTest, ODirectUnalignedWriteIsFatal) {
  const auto fh = create("f", SRV_UNIX_O_DIRECT);
  std::vector<byte> buf(4096 + 1);

  EXPECT_DEATH((void)os_file_write("f", fh, buf.data() + 1, 100, 1), "Cannot continue operation");
  EXPECT_TRUE(os_file_close(fh));
}

/** os_file_set_nocache() on a bad descriptor logs and continues. */
TEST_F(OsFileTest, SetNocacheBadFd) {
  os_file_set_nocache(-1, "nofile", "open");
}

/* --------------------------------------------------- delete / rename / close */

/** delete removes a file; a missing file is an error for os_file_delete()
and success for os_file_delete_if_exists(); a directory is an error. */
TEST_F(OsFileTest, Delete) {
  put("f", "x");

  EXPECT_TRUE(os_file_delete(p("f").c_str()));
  EXPECT_FALSE(fs::exists(p("f")));
  EXPECT_FALSE(os_file_delete(p("f").c_str()));

  put("g", "x");
  EXPECT_TRUE(os_file_delete_if_exists(p("g").c_str()));
  EXPECT_FALSE(fs::exists(p("g")));
  EXPECT_TRUE(os_file_delete_if_exists(p("g").c_str()));

  fs::create_directory(p("d"));
  EXPECT_FALSE(os_file_delete(p("d").c_str()));
  EXPECT_FALSE(os_file_delete_if_exists(p("d").c_str()));
  EXPECT_TRUE(fs::is_directory(p("d")));
}

/** Deleting an open file works; the open handle still reads it. */
TEST_F(OsFileTest, DeleteOpen) {
  const auto fh = create("f");

  EXPECT_TRUE(os_file_write("f", fh, "data", 4, 0));
  EXPECT_TRUE(os_file_delete(p("f").c_str()));

  char buf[4];

  EXPECT_TRUE(os_file_read(fh, buf, 4, 0));
  EXPECT_EQ(std::string(buf, 4), "data");
  EXPECT_TRUE(os_file_close(fh));
}

/** rename moves a file, replaces an existing target, fails on a missing
source, a missing target directory, and a directory as the target. */
TEST_F(OsFileTest, Rename) {
  put("a", "contents");

  EXPECT_TRUE(os_file_rename(p("a").c_str(), p("b").c_str()));
  EXPECT_FALSE(fs::exists(p("a")));
  EXPECT_EQ(slurp(p("b")), "contents");

  put("c", "other");
  EXPECT_TRUE(os_file_rename(p("b").c_str(), p("c").c_str()));
  EXPECT_EQ(slurp(p("c")), "contents");

  EXPECT_FALSE(os_file_rename(p("missing").c_str(), p("x").c_str()));
  EXPECT_FALSE(os_file_rename(p("c").c_str(), p("nodir/x").c_str()));

  fs::create_directory(p("d"));
  fs::create_directory(p("d/sub"));
  EXPECT_FALSE(os_file_rename(p("c").c_str(), p("d").c_str()));
  EXPECT_EQ(slurp(p("c")), "contents");

  /* Into another directory. */
  EXPECT_TRUE(os_file_rename(p("c").c_str(), p("d/sub/c").c_str()));
  EXPECT_EQ(slurp(p("d/sub/c")), "contents");

  /* Onto itself. */
  EXPECT_TRUE(os_file_rename(p("d/sub/c").c_str(), p("d/sub/c").c_str()));
}

/** Rename an open file: the handle keeps working. */
TEST_F(OsFileTest, RenameOpen) {
  const auto fh = create("a");

  EXPECT_TRUE(os_file_rename(p("a").c_str(), p("b").c_str()));
  EXPECT_TRUE(os_file_write("b", fh, "xyz", 3, 0));
  EXPECT_TRUE(os_file_close(fh));
  EXPECT_EQ(slurp(p("b")), "xyz");
}

/** Closing a bad descriptor is fatal (EBADF is not a handled error). */
TEST_F(OsFileDeathTest, CloseBadFdIsFatal) {
  EXPECT_DEATH((void)os_file_close(-1), "Cannot continue operation");
}

/* ------------------------------------------------------------ sizes */

/** The size of a new file is 0, then the end of the last write. */
TEST_F(OsFileTest, GetSize) {
  const auto fh = create("f");
  off_t size{-1};

  EXPECT_TRUE(os_file_get_size(fh, &size));
  EXPECT_EQ(size, 0);

  EXPECT_TRUE(os_file_write("f", fh, "abc", 3, 10));
  EXPECT_EQ(os_file_get_size_as_iblonglong(fh), 13);

  /* Past 4 GB: a sparse file, the offsets are 64 bit. */
  const off_t big = (off_t(5) << 30) + 7;

  EXPECT_TRUE(os_file_write("f", fh, "z", 1, big));
  EXPECT_EQ(os_file_get_size_as_iblonglong(fh), big + 1);

  EXPECT_TRUE(os_file_close(fh));
}

/** A bad descriptor: false, and -1. */
TEST_F(OsFileTest, GetSizeBadFd) {
  off_t size{};

  EXPECT_FALSE(os_file_get_size(-1, &size));
  EXPECT_EQ(os_file_get_size_as_iblonglong(-1), -1);
}

/** os_file_set_size() asserts below a page. */
TEST_F(OsFileDeathTest, SetSizeBelowPageAsserts) {
  const auto fh = create("f");

  EXPECT_DEATH((void)os_file_set_size("f", fh, UNIV_PAGE_SIZE - 1), "");
  EXPECT_TRUE(os_file_close(fh));
}

/** os_file_set_size() extends to exactly the size, with zeros, at any size. */
TEST_F(OsFileTest, SetSizeExact) {
  for (const off_t size : {off_t(UNIV_PAGE_SIZE), off_t(UNIV_PAGE_SIZE + 1), off_t(3 * UNIV_PAGE_SIZE + 5),
                           off_t(64 * UNIV_PAGE_SIZE), off_t(64 * UNIV_PAGE_SIZE + 1), off_t(5 << 20) + 3}) {
    const auto name = std::format("f{}", size);
    const auto fh = create(name);

    ASSERT_TRUE(os_file_set_size(name.c_str(), fh, size));
    EXPECT_EQ(os_file_get_size_as_iblonglong(fh), size);

    const auto s = slurp(p(name));

    EXPECT_EQ(s.size(), size_t(size));
    EXPECT_EQ(s.find_first_not_of('\0'), std::string::npos) << size;
    EXPECT_TRUE(os_file_close(fh));
  }
}

/** os_file_set_size() appends after what's there, and doesn't shrink. */
TEST_F(OsFileTest, SetSizeKeepsAndDoesNotShrink) {
  const auto fh = create("f");
  const std::string head(100, '\xAB');

  EXPECT_TRUE(os_file_write("f", fh, head.data(), head.size(), 0));
  EXPECT_TRUE(os_file_set_size("f", fh, 2 * UNIV_PAGE_SIZE));

  const auto s = slurp(p("f"));

  EXPECT_EQ(s.size(), 2 * UNIV_PAGE_SIZE);
  EXPECT_EQ(s.substr(0, 100), head);
  EXPECT_EQ(s.find_first_not_of('\0', 100), std::string::npos);

  /* Smaller than the file: nothing to do. */
  EXPECT_TRUE(os_file_set_size("f", fh, UNIV_PAGE_SIZE));
  EXPECT_EQ(os_file_get_size_as_iblonglong(fh), off_t(2 * UNIV_PAGE_SIZE));
  EXPECT_TRUE(os_file_close(fh));
}

/** os_file_set_size() on a full disk returns false (and frees its buffer). */
TEST_F(OsFileTest, SetSizeDiskFull) {
  const auto fh = create("f");

  {
    Arm arm(g_pwrite, fh, ENOSPC, 1, 0, 2);

    EXPECT_FALSE(os_file_set_size("f", fh, 200 * UNIV_PAGE_SIZE));
    EXPECT_EQ(g_pwrite.m_calls, 3);
  }

  /* The two writes that passed are there. */
  EXPECT_EQ(os_file_get_size_as_iblonglong(fh), off_t(2 * 64 * UNIV_PAGE_SIZE));
  EXPECT_TRUE(os_file_close(fh));
}

/** os_file_set_size() completes through short writes. */
TEST_F(OsFileTest, SetSizeShortWrites) {
  const auto fh = create("f");

  {
    Arm arm(g_pwrite, fh, 0, 0, 1000);

    EXPECT_TRUE(os_file_set_size("f", fh, 3 * UNIV_PAGE_SIZE + 1));
  }

  EXPECT_EQ(os_file_get_size_as_iblonglong(fh), off_t(3 * UNIV_PAGE_SIZE + 1));
  EXPECT_TRUE(os_file_close(fh));
}

/** With RLIMIT_FSIZE the write past the limit fails with EFBIG, which is not
a handled error: fatal. (The real ENOSPC is tested by injection.) */
TEST_F(OsFileDeathTest, FileSizeLimitIsFatal) {
  const auto fh = create("f");

  EXPECT_DEATH(
    {
      signal(SIGXFSZ, SIG_IGN);

      struct rlimit rl;

      rl.rlim_cur = rl.rlim_max = 64 * 1024;
      setrlimit(RLIMIT_FSIZE, &rl);
      (void)os_file_set_size("f", fh, 8 * UNIV_PAGE_SIZE);
    },
    "Cannot continue operation"
  );

  EXPECT_TRUE(os_file_close(fh));
}

/* ------------------------------------------------------------ flush */

/** os_file_flush() of a good file succeeds. */
TEST_F(OsFileTest, Flush) {
  const auto fh = create("f");

  EXPECT_TRUE(os_file_write("f", fh, "x", 1, 0));
  EXPECT_TRUE(os_file_flush(fh));
  EXPECT_TRUE(os_file_close(fh));
}

/** fsync() failing with ENOLCK is retried after 0.2 s. */
TEST_F(OsFileTest, FlushRetriesEnolck) {
  const auto fh = create("f");
  const auto start = std::chrono::steady_clock::now();

  {
    Arm arm(g_fsync, fh, ENOLCK, 2);

    EXPECT_TRUE(os_file_flush(fh));
    EXPECT_EQ(g_fsync.m_calls, 3);
  }

  EXPECT_GE(std::chrono::steady_clock::now() - start, 390ms);
  EXPECT_TRUE(os_file_close(fh));
}

/** A failed fsync() is fatal: the data may not be on disk. An unknown
error (EIO) exits in the error handler, a "handled" one (ENOSPC) in
os_file_flush() itself. */
TEST_F(OsFileDeathTest, FlushErrorIsFatal) {
  const auto fh = create("f");

  EXPECT_DEATH(
    {
      Arm arm(g_fsync, fh, EIO, 1);
      (void)os_file_flush(fh);
    },
    "Cannot continue operation"
  );

  EXPECT_DEATH(
    {
      Arm arm(g_fsync, fh, ENOSPC, 1);
      (void)os_file_flush(fh);
    },
    "flush did not succeed"
  );

  EXPECT_DEATH((void)os_file_flush(-1), "");
  EXPECT_TRUE(os_file_close(fh));
}

/* ------------------------------------------------------- read / write */

/** Writes read back at offsets around the alignments and 4 GB, of sizes 1
to 1 MB. */
TEST_F(OsFileTest, RoundTrip) {
  const auto fh = create("f");
  uint32_t seed{};

  for (const off_t off : {off_t(0), off_t(1), off_t(4095), off_t(4096), off_t(16383), (off_t(1) << 32) - 1, off_t(1) << 32,
                          off_t(5) << 30}) {
    for (const size_t n : {1UL, 7UL, 4096UL, 16385UL, 1UL << 20}) {
      const auto data = pattern(n, ++seed);
      std::vector<byte> back(n);

      ASSERT_TRUE(os_file_write("f", fh, data.data(), n, off)) << off << " " << n;
      ASSERT_TRUE(os_file_read(fh, back.data(), n, off)) << off << " " << n;
      ASSERT_EQ(back, data) << off << " " << n;
      ASSERT_TRUE(os_file_read_no_error_handling(fh, back.data(), n, off));
      ASSERT_EQ(back, data) << off << " " << n;
    }
  }

  EXPECT_TRUE(os_file_close(fh));
}

/** 0 bytes: both succeed and nothing changes. */
TEST_F(OsFileTest, ZeroBytes) {
  const auto fh = create("f");
  char c{'q'};

  EXPECT_TRUE(os_file_write("f", fh, &c, 0, 100));
  EXPECT_EQ(os_file_get_size_as_iblonglong(fh), 0);
  EXPECT_TRUE(os_file_read(fh, &c, 0, 0));
  EXPECT_TRUE(os_file_read_no_error_handling(fh, &c, 0, 0));
  EXPECT_EQ(c, 'q');
  EXPECT_TRUE(os_file_close(fh));
}

/** A write past the end leaves a hole that reads as zeros. */
TEST_F(OsFileTest, Hole) {
  const auto fh = create("f");

  EXPECT_TRUE(os_file_write("f", fh, "end", 3, 1 << 20));

  std::vector<byte> buf(1 << 20, 0xFF);

  EXPECT_TRUE(os_file_read(fh, buf.data(), buf.size(), 0));
  EXPECT_EQ(std::count(buf.begin(), buf.end(), 0), 1 << 20);
  EXPECT_TRUE(os_file_close(fh));
}

/** One large write and read (16 MB). */
TEST_F(OsFileTest, Large) {
  const auto fh = create("f");
  const auto data = pattern(16 << 20, 77);
  std::vector<byte> back(data.size());

  EXPECT_TRUE(os_file_write("f", fh, data.data(), data.size(), 3));
  EXPECT_TRUE(os_file_read(fh, back.data(), back.size(), 3));
  EXPECT_EQ(back, data);
  EXPECT_TRUE(os_file_close(fh));
}

/** EINTR and EAGAIN from pwrite() are retried. */
TEST_F(OsFileTest, WriteRetriesEintrEagain) {
  const auto fh = create("f");
  const auto data = pattern(10000, 1);

  for (const int err : {EINTR, EAGAIN}) {
    Arm arm(g_pwrite, fh, err, 3);

    EXPECT_TRUE(os_file_write("f", fh, data.data(), data.size(), 0)) << err;
    EXPECT_EQ(g_pwrite.m_calls, 4);
  }

  EXPECT_EQ(os_file_get_size_as_iblonglong(fh), 10000);
  EXPECT_TRUE(os_file_close(fh));
  EXPECT_EQ(slurp(p("f")), std::string(data.begin(), data.end()));
}

/** Short writes continue where they stopped. */
TEST_F(OsFileTest, WriteShort) {
  const auto fh = create("f");
  const auto data = pattern(10007, 2);

  for (const size_t max : {1UL, 7UL, 4096UL}) {
    Arm arm(g_pwrite, fh, 0, 0, max);

    EXPECT_TRUE(os_file_write("f", fh, data.data(), data.size(), 5)) << max;
    EXPECT_EQ(size_t(g_pwrite.m_calls), (data.size() + max - 1) / max);
  }

  std::vector<byte> back(data.size());

  EXPECT_TRUE(os_file_read(fh, back.data(), back.size(), 5));
  EXPECT_EQ(back, data);
  EXPECT_TRUE(os_file_close(fh));
}

/** A full disk: os_file_write() returns false, without exiting; after a
partial write the part written is in the file. */
TEST_F(OsFileTest, WriteDiskFull) {
  const auto fh = create("f");
  const auto data = pattern(10000, 3);

  {
    Arm arm(g_pwrite, fh, ENOSPC, 1);

    EXPECT_FALSE(os_file_write("f", fh, data.data(), data.size(), 0));
  }

  EXPECT_EQ(os_file_get_size_as_iblonglong(fh), 0);

  {
    Arm arm(g_pwrite, fh, ENOSPC, 1, 4000, 1);

    EXPECT_FALSE(os_file_write("f", fh, data.data(), data.size(), 0));
    EXPECT_EQ(g_pwrite.m_calls, 2);
  }

  EXPECT_EQ(os_file_get_size_as_iblonglong(fh), 4000);

  /* The disk has room again. */
  EXPECT_TRUE(os_file_write("f", fh, data.data(), data.size(), 0));
  EXPECT_TRUE(os_file_close(fh));
}

/** An I/O error on write is fatal. */
TEST_F(OsFileDeathTest, WriteEioIsFatal) {
  const auto fh = create("f");

  EXPECT_DEATH(
    {
      Arm arm(g_pwrite, fh, EIO, 1);
      (void)os_file_write("f", fh, "x", 1, 0);
    },
    "Cannot continue operation"
  );

  EXPECT_DEATH((void)os_file_write("bad", -1, "x", 1, 0), "Cannot continue operation");
  EXPECT_TRUE(os_file_close(fh));
}

/** EINTR, EAGAIN and short reads from pread() are retried and continued. */
TEST_F(OsFileTest, ReadRetriesAndShort) {
  const auto fh = create("f");
  const auto data = pattern(9999, 4);

  ASSERT_TRUE(os_file_write("f", fh, data.data(), data.size(), 0));

  for (const int err : {EINTR, EAGAIN}) {
    for (const size_t max : {0UL, 1UL, 1000UL}) {
      std::vector<byte> back(data.size());
      Arm arm(g_pread, fh, err, 2, max);

      EXPECT_TRUE(os_file_read(fh, back.data(), back.size(), 0)) << err << " " << max;
      EXPECT_EQ(back, data);

      std::fill(back.begin(), back.end(), 0);
      g_pread.m_fail = 2;
      EXPECT_TRUE(os_file_read_no_error_handling(fh, back.data(), back.size(), 0)) << err << " " << max;
      EXPECT_EQ(back, data);
    }
  }

  EXPECT_TRUE(os_file_close(fh));
}

/** A read error: os_file_read() exits, os_file_read_no_error_handling()
returns false. */
TEST_F(OsFileDeathTest, ReadErrors) {
  const auto fh = create("f");
  char buf[16];

  EXPECT_TRUE(os_file_write("f", fh, "0123456789abcdef", 16, 0));
  EXPECT_DEATH((void)os_file_read(-1, buf, 16, 0), "Cannot continue operation");

  EXPECT_FALSE(os_file_read_no_error_handling(-1, buf, 16, 0));

  {
    Arm arm(g_pread, fh, EIO, 1);

    EXPECT_FALSE(os_file_read_no_error_handling(fh, buf, 16, 0));
  }

  EXPECT_DEATH(
    {
      Arm arm(g_pread, fh, EIO, 1);
      (void)os_file_read(fh, buf, 16, 0);
    },
    "Cannot continue operation"
  );

  EXPECT_TRUE(os_file_close(fh));
}

/* BUG: os_file_read() and os_file_read_no_error_handling() (os/os0file.cc:
611 and 639) loop "while (n > 0)" and treat pread() returning 0 (end of
file) as progress: a read that reaches past the end of the file spins
forever at 100% CPU. Fil reads page 0 of every data file with
os_file_read(fh, page, UNIV_PAGE_SIZE, 0) (fil/fil0fil.cc:312, 904, 1559,
1731): a data file shorter than a page (truncated, or created and not yet
extended when the process died) hangs the start-up instead of failing.
The InnoDB original returned false when ret != n. Fix, in both loops:
     if (n_bytes == -1) {
       ...
     }
+
+    if (n_bytes == 0) {
+      log_err(std::format("Short read at offset {}: {} bytes past the end of the file", off, n));
+      return false;
+    }
Each case runs in a child that is killed by an alarm if it hangs. */
TEST_F(OsFileDeathTest, ReadPastEof) {
  put("f", std::string(100, 'x'));

  bool ok{};
  const auto fh = os_file_create_simple(p("f").c_str(), OS_FILE_OPEN, OS_FILE_READ_ONLY, &ok);

  ASSERT_TRUE(ok);

  std::vector<byte> buf(UNIV_PAGE_SIZE);

  /* Partly past the end, wholly past the end, and an empty file. */
  for (const off_t off : {off_t(0), off_t(50), off_t(100), off_t(1000)}) {
    EXPECT_EXIT(
      {
        alarm(2);
        _exit(os_file_read(fh, buf.data(), buf.size(), off) ? 1 : 0);
      },
      ::testing::ExitedWithCode(0),
      ""
    ) << off;

    EXPECT_EXIT(
      {
        alarm(2);
        _exit(os_file_read_no_error_handling(fh, buf.data(), buf.size(), off) ? 1 : 0);
      },
      ::testing::ExitedWithCode(0),
      ""
    ) << off;
  }

  EXPECT_TRUE(os_file_close(fh));
}

/** A read that ends exactly at the end of the file works. */
TEST_F(OsFileTest, ReadToEof) {
  put("f", "0123456789");

  bool ok{};
  const auto fh = os_file_create_simple(p("f").c_str(), OS_FILE_OPEN, OS_FILE_READ_ONLY, &ok);
  char buf[10];

  ASSERT_TRUE(ok);
  EXPECT_TRUE(os_file_read(fh, buf, 10, 0));
  EXPECT_EQ(std::string(buf, 10), "0123456789");
  EXPECT_TRUE(os_file_read(fh, buf, 3, 7));
  EXPECT_EQ(std::string(buf, 3), "789");
  EXPECT_TRUE(os_file_close(fh));
}

/** os_file_read_string() reads at most size - 1 bytes from the start and
terminates; size 0 writes nothing. */
TEST_F(OsFileTest, ReadString) {
  put("f", "hello world");

  auto f = fopen(p("f").c_str(), "r");

  ASSERT_NE(f, nullptr);

  char buf[32];

  /* From the start, whatever the position. */
  fseek(f, 5, SEEK_SET);
  os_file_read_string(f, buf, 6);
  EXPECT_STREQ(buf, "hello");

  os_file_read_string(f, buf, sizeof(buf));
  EXPECT_STREQ(buf, "hello world");

  os_file_read_string(f, buf, 1);
  EXPECT_STREQ(buf, "");

  buf[0] = 'Z';
  os_file_read_string(f, buf, 0);
  EXPECT_EQ(buf[0], 'Z');

  fclose(f);
}

/** Many threads write disjoint blocks of one file through one descriptor,
then read them back concurrently: the positional calls share no offset. */
TEST_F(OsFileTest, ConcurrentDisjointBlocks) {
  constexpr int N_THREADS = 16;
  constexpr int N_BLOCKS = 64;
  constexpr size_t BLOCK = 4096;
  const auto fh = create("f");
  std::atomic<int> bad{};
  std::vector<std::thread> threads;

  for (int t = 0; t < N_THREADS; ++t) {
    threads.emplace_back([&, t] {
      for (int b = 0; b < N_BLOCKS; ++b) {
        const auto data = pattern(BLOCK, uint32_t(t * N_BLOCKS + b));
        const off_t off = off_t(b * N_THREADS + t) * BLOCK;

        if (!os_file_write("f", fh, data.data(), BLOCK, off)) {
          ++bad;
        }

        /* lseek(SEEK_END) on the shared descriptor meanwhile. */
        if (os_file_get_size_as_iblonglong(fh) < off_t(BLOCK)) {
          ++bad;
        }
      }
    });
  }

  for (auto &t : threads) {
    t.join();
  }

  threads.clear();

  for (int t = 0; t < N_THREADS; ++t) {
    threads.emplace_back([&, t] {
      std::vector<byte> back(BLOCK);

      for (int b = N_BLOCKS - 1; b >= 0; --b) {
        const off_t off = off_t(b * N_THREADS + t) * BLOCK;

        if (!os_file_read(fh, back.data(), BLOCK, off) || back != pattern(BLOCK, uint32_t(t * N_BLOCKS + b))) {
          ++bad;
        }
      }
    });
  }

  for (auto &t : threads) {
    t.join();
  }

  EXPECT_EQ(bad, 0);
  EXPECT_EQ(os_file_get_size_as_iblonglong(fh), off_t(N_THREADS * N_BLOCKS * BLOCK));
  EXPECT_TRUE(os_file_close(fh));
}

/** Readers of the written part while a writer appends: they always see
complete, stable blocks. */
TEST_F(OsFileTest, ConcurrentReadWhileAppend) {
  constexpr size_t BLOCK = 4096;
  constexpr int N = 2000;
  const auto fh = create("f");
  std::atomic<int> written{};
  std::atomic<int> bad{};
  std::vector<std::thread> readers;

  for (int r = 0; r < 4; ++r) {
    readers.emplace_back([&, r] {
      std::vector<byte> back(BLOCK);
      std::mt19937 rng(r);

      while (written.load() < N) {
        const int w = written.load();

        if (w == 0) {
          continue;
        }

        const int b = int(rng() % w);

        if (!os_file_read(fh, back.data(), BLOCK, off_t(b) * BLOCK) || back[0] != byte(b) || back[BLOCK - 1] != byte(b)) {
          ++bad;
        }
      }
    });
  }

  std::vector<byte> block(BLOCK);

  for (int b = 0; b < N; ++b) {
    std::fill(block.begin(), block.end(), byte(b));
    ASSERT_TRUE(os_file_write("f", fh, block.data(), BLOCK, off_t(b) * BLOCK));
    written = b + 1;
  }

  for (auto &t : readers) {
    t.join();
  }

  EXPECT_EQ(bad, 0);
  EXPECT_TRUE(os_file_close(fh));
}

/** Threads create, write, rename and delete their own files in one
directory at once. */
TEST_F(OsFileTest, ConcurrentCreateDelete) {
  std::atomic<int> bad{};
  std::vector<std::thread> threads;

  for (int t = 0; t < 8; ++t) {
    threads.emplace_back([&, t] {
      for (int i = 0; i < 100; ++i) {
        const auto a = p(std::format("t{}-{}", t, i));
        const auto b = a + ".renamed";
        bool ok{};
        const auto fh = os_file_create(a.c_str(), OS_FILE_CREATE, OS_FILE_NORMAL, SRV_UNIX_FSYNC, &ok);

        if (!ok || !os_file_write(a.c_str(), fh, "abc", 3, 0) || !os_file_close(fh) || !os_file_rename(a.c_str(), b.c_str()) ||
            !os_file_delete(b.c_str())) {
          ++bad;
        }
      }
    });
  }

  for (auto &t : threads) {
    t.join();
  }

  EXPECT_EQ(bad, 0);
  EXPECT_TRUE(fs::is_empty(m_dir.m_path));
}

/* ------------------------------------------------------- status, dirs */

/** os_file_status(): the type, and "doesn't exist" for missing paths and
paths through a file. stat() follows symbolic links, so SYM_LINK is never
reported: a link reports its target. */
TEST_F(OsFileTest, Status) {
  put("f", "x");
  fs::create_directory(p("d"));
  fs::create_symlink(p("f"), p("link"));
  fs::create_symlink(p("missing"), p("dangling"));
  ASSERT_EQ(mkfifo(p("fifo").c_str(), 0600), 0);

  auto status = [&](const std::string &name, bool &exists) {
    OsFileType type{OS_FILE_TYPE_UNKNOWN};

    exists = false;
    EXPECT_TRUE(os_file_status(p(name).c_str(), &exists, &type)) << name;

    return type;
  };

  bool exists{};

  EXPECT_EQ(status("f", exists), REGULAR_FILE);
  EXPECT_TRUE(exists);
  EXPECT_EQ(status("d", exists), DIRECTORY);
  EXPECT_TRUE(exists);
  EXPECT_EQ(status("link", exists), REGULAR_FILE);
  EXPECT_TRUE(exists);
  EXPECT_EQ(status("fifo", exists), OS_FILE_TYPE_UNKNOWN);
  EXPECT_TRUE(exists);

  status("missing", exists);
  EXPECT_FALSE(exists);
  status("dangling", exists);
  EXPECT_FALSE(exists);
  status("f/child", exists);
  EXPECT_FALSE(exists);
}

/** A path that can't be stat()ed for lack of permission: false. */
TEST_F(OsFileTest, StatusNoPermission) {
  if (geteuid() == 0) {
    GTEST_SKIP() << "root ignores the permissions";
  }

  fs::create_directory(p("d"));
  put("d/f", "x");
  fs::permissions(p("d"), fs::perms::none);

  bool exists{};
  OsFileType type{};
  OsFileStat st{};

  EXPECT_FALSE(os_file_status(p("d/f").c_str(), &exists, &type));
  EXPECT_FALSE(os_file_get_status(p("d/f").c_str(), &st));

  fs::permissions(p("d"), fs::perms::owner_all);
}

/** os_file_get_status(): type, size and times; false for a missing path. */
TEST_F(OsFileTest, GetStatus) {
  put("f", "12345");
  fs::create_directory(p("d"));

  OsFileStat st{};
  const auto now = time(nullptr);

  ASSERT_TRUE(os_file_get_status(p("f").c_str(), &st));
  EXPECT_EQ(st.type, REGULAR_FILE);
  EXPECT_EQ(st.size, 5);
  EXPECT_LE(std::abs(st.mtime - now), 5);
  EXPECT_LE(std::abs(st.ctime - now), 5);

  ASSERT_TRUE(os_file_get_status(p("d").c_str(), &st));
  EXPECT_EQ(st.type, DIRECTORY);

  EXPECT_FALSE(os_file_get_status(p("missing").c_str(), &st));
  EXPECT_FALSE(os_file_get_status(p("f/x").c_str(), &st));
}

/** os_file_dirname(), the examples of the header comment. "/usr/" gives
"/usr", not "/" as the comment says: trailing slashes are not stripped.
The only caller, os_file_create_subdirs_if_needed(), is fine with it. */
TEST_F(OsFileTest, Dirname) {
  const std::vector<std::pair<const char *, const char *>> cases{
    {"/usr/lib", "/usr"}, {"/usr/", "/usr"}, {"usr", "."}, {"/", "/"}, {".", "."}, {"..", "."},
    {"a/b/c", "a/b"},     {"", "."},         {"/a", "/"},  {"//a", "/"}, {"a//b", "a/"},
  };

  for (const auto &[path, expected] : cases) {
    auto d = os_file_dirname(path);

    EXPECT_STREQ(d, expected) << path;
    mem_free(d);
  }
}

/** os_file_create_subdirs_if_needed(): creates every missing directory of
the path, but not the last component. */
TEST_F(OsFileTest, CreateSubdirs) {
  EXPECT_TRUE(os_file_create_subdirs_if_needed(p("a/b/c/file").c_str()));
  EXPECT_TRUE(fs::is_directory(p("a/b/c")));
  EXPECT_FALSE(fs::exists(p("a/b/c/file")));

  /* All there. */
  EXPECT_TRUE(os_file_create_subdirs_if_needed(p("a/b/c/file").c_str()));

  /* No directory component; the root. */
  EXPECT_TRUE(os_file_create_subdirs_if_needed("file"));
  EXPECT_TRUE(os_file_create_subdirs_if_needed("/file"));

  /* Through a regular file: an error, returned. */
  put("f", "x");
  EXPECT_FALSE(os_file_create_subdirs_if_needed(p("f/x/y").c_str()));
}

/** os_file_create_directory(): new, existing, existing with fail_if_exists. */
TEST_F(OsFileTest, CreateDirectory) {
  EXPECT_TRUE(os_file_create_directory(p("d").c_str(), true));
  EXPECT_TRUE(fs::is_directory(p("d")));
  EXPECT_TRUE(os_file_create_directory(p("d").c_str(), false));
  EXPECT_FALSE(os_file_create_directory(p("d").c_str(), true));

  const auto mask = umask(0);

  umask(mask);
  EXPECT_EQ(uint32_t(fs::status(p("d")).permissions()), 0770U & ~mask);

  /* A file in the way: EEXIST, and it's not a directory. */
  put("f", "x");
  EXPECT_FALSE(os_file_create_directory(p("f").c_str(), true));
  /* fail_if_exists = false and a file of that name: mkdir() says EEXIST
  and the call succeeds, though there is no directory. */
  EXPECT_TRUE(os_file_create_directory(p("f").c_str(), false));
  EXPECT_FALSE(fs::is_directory(p("f")));
}

/** A missing parent (ENOENT) is fatal. */
TEST_F(OsFileDeathTest, CreateDirectoryMissingParentIsFatal) {
  EXPECT_DEATH((void)os_file_create_directory(p("no/such/dir").c_str(), false), "Cannot continue operation");
}

/** os_file_get_last_error() maps errno. */
TEST(OsFileErrTest, LastError) {
  const std::vector<std::pair<int, ulint>> cases{
    {ENOSPC, OS_FILE_DISK_FULL},   {ENOENT, OS_FILE_NOT_FOUND},   {EEXIST, OS_FILE_ALREADY_EXISTS},
    {EXDEV, OS_FILE_PATH_ERROR},   {ENOTDIR, OS_FILE_PATH_ERROR}, {EISDIR, OS_FILE_PATH_ERROR},
    {EIO, 100 + ulint(EIO)},       {EACCES, 100 + ulint(EACCES)}, {EBADF, 100 + ulint(EBADF)},
  };

  for (const auto &[err, expected] : cases) {
    errno = err;
    EXPECT_EQ(os_file_get_last_error(false), expected) << err;
    errno = err;
    EXPECT_EQ(os_file_get_last_error(true), expected) << err;
  }
}

/** get_path_status() and to_string(perms). */
TEST_F(OsFileTest, PathHelpers) {
  put("f", "x");
  fs::create_directory(p("d"));
  ASSERT_EQ(mkfifo(p("fifo").c_str(), 0600), 0);

  EXPECT_STREQ(get_path_status(p("f")), "is a regular file");
  EXPECT_STREQ(get_path_status(p("d")), "is a directory");
  EXPECT_STREQ(get_path_status(p("missing")), "does not exist");
  EXPECT_STREQ(get_path_status(p("fifo")), "is a named IPC pipe");
  EXPECT_STREQ(get_path_status("/dev/null"), "is a character device");

  using fs::perms;

  EXPECT_EQ(to_string(perms::none), "---------");
  EXPECT_EQ(to_string(perms::all), "rwxrwxrwx");
  EXPECT_EQ(to_string(perms::owner_read | perms::owner_write | perms::group_read), "rw-r-----");
}

/* BUG: os_file_create_tmpfile() (os/os0file.cc:214) is documented to create
the file "in the configured temporary directory", but row_create_tempfile()
(row/row0misc.cc:38) calls tmpfile(3), which glibc always creates in
P_tmpdir (/tmp), ignoring $TMPDIR and any configuration; the prefix
argument is ignored too. The merge sort files of index builds
(row/row0merge.cc:1615, 1862) land in /tmp whatever the configuration.
Fix: take the directory from the configuration (or $TMPDIR) and use
open(dir, O_TMPFILE | O_RDWR | O_EXCL, 0600), falling back to mkstemp() +
unlink() where O_TMPFILE isn't supported.
Not run: it would create a file in /tmp. */
TEST_F(OsFileTest, TmpfileInTmpdir) {
  auto f = os_file_create_tmpfile();

  ASSERT_NE(f, nullptr);

  char link[PATH_MAX]{};
  const auto proc = std::format("/proc/self/fd/{}", fileno(f));

  ASSERT_GT(readlink(proc.c_str(), link, sizeof(link) - 1), 0);
  EXPECT_EQ(std::string(link).rfind(fs::temp_directory_path().string(), 0), 0UL) << link;
  fclose(f);
}

/* ------------------------------------------------------------ threads */

struct ThreadArg {
  std::atomic<int> m_ran{};
  os_thread_id_t m_self{};
  size_t m_guard{};
  size_t m_stack{};
};

void *thread_fn(void *p) {
  auto arg = static_cast<ThreadArg *>(p);

  arg->m_self = os_thread_get_curr_id();

  pthread_attr_t attr;

  if (pthread_getattr_np(pthread_self(), &attr) == 0) {
    pthread_attr_getguardsize(&attr, &arg->m_guard);
    pthread_attr_getstacksize(&attr, &arg->m_stack);
    pthread_attr_destroy(&attr);
  }

  ++arg->m_ran;

  return p;
}

/** os_thread_create() runs f(arg) in a joinable thread whose id it returns. */
TEST(OsThreadTest, CreateRunsAndJoins) {
  ThreadArg arg;
  os_thread_id_t id{};
  const auto t = os_thread_create(thread_fn, &arg, &id);

  EXPECT_TRUE(os_thread_eq(t, id));

  void *ret{};

  ASSERT_EQ(pthread_join(t, &ret), 0);
  EXPECT_EQ(ret, &arg);
  EXPECT_EQ(arg.m_ran, 1);
  EXPECT_TRUE(os_thread_eq(arg.m_self, id));
  EXPECT_FALSE(os_thread_eq(arg.m_self, os_thread_get_curr_id()));
  EXPECT_TRUE(os_thread_eq(os_thread_get_curr_id(), os_thread_get_curr()));
  EXPECT_NE(os_thread_pf(arg.m_self), os_thread_pf(os_thread_get_curr_id()));
}

/** The thread id out parameter is optional. */
TEST(OsThreadTest, NullIdOut) {
  ThreadArg arg;
  const auto t = os_thread_create(thread_fn, &arg, nullptr);

  ASSERT_EQ(pthread_join(t, nullptr), 0);
  EXPECT_EQ(arg.m_ran, 1);
}

/** The threads have the default stack size. */
TEST(OsThreadTest, StackSize) {
  ThreadArg arg, ref;
  const auto t = os_thread_create(thread_fn, &arg, nullptr);

  ASSERT_EQ(pthread_join(t, nullptr), 0);

  std::thread st([&] { thread_fn(&ref); });

  st.join();

  EXPECT_EQ(arg.m_stack, ref.m_stack);
  EXPECT_GE(arg.m_stack, size_t(256 * 1024));
}

/* BUG: os_thread_create() (os/os0thread.cc:49) passes a memset() zero
pthread_attr_t instead of one from pthread_attr_init(): glibc then creates
the thread with a guard size of 0, i.e. no guard page below the stack, so a
stack overflow in an engine thread silently overwrites the memory next to it
instead of faulting. (A zeroed attr is also not a valid attr object per
POSIX.) Fix:
-  memset(&attr, 0x0, sizeof(attr));
-
-  auto ret = pthread_create(&pthread, &attr, f, arg);
+  pthread_attr_init(&attr);
+
+  auto ret = pthread_create(&pthread, &attr, f, arg);
+
+  pthread_attr_destroy(&attr);
Also: log_fatal("pthread_create returned %d", ret, ...) prints "%d" as is. */
TEST(OsThreadTest, GuardPage) {
  ThreadArg arg, ref;
  const auto t = os_thread_create(thread_fn, &arg, nullptr);

  ASSERT_EQ(pthread_join(t, nullptr), 0);

  std::thread st([&] { thread_fn(&ref); });

  st.join();

  EXPECT_GT(ref.m_guard, 0UL);
  EXPECT_EQ(arg.m_guard, ref.m_guard);
}

/** @return the virtual size of this process in KB. */
size_t vm_size_kb() {
  std::ifstream in("/proc/self/status");
  std::string line;

  while (std::getline(in, line)) {
    if (line.rfind("VmSize:", 0) == 0) {
      return std::stoul(line.substr(7));
    }
  }

  return 0;
}

/* BUG: os_thread_create() makes joinable threads, but nothing in the engine
joins them or detaches them (Srv::create_thread(), srv/srv0srv.cc:1545, the
only caller, drops the id; there is no os_thread_join()). A joinable thread
that exits keeps its stack (8 MB of address space, plus the TLS and the
descriptor) until it is joined: every start and shutdown of a System leaks
the stacks of its background threads, which an embedded engine that is
started and stopped in a long-lived process (or in these tests) accumulates.
Fix: detach them (pthread_attr_setdetachstate(&attr,
PTHREAD_CREATE_DETACHED) after pthread_attr_init()), the engine already
waits for them with Srv::n_background_threads(); or add os_thread_join()
and join them at shutdown. */
TEST(OsThreadTest, ExitedThreadsAreReclaimed) {
  constexpr int N = 32;
  ThreadArg args[N];

  /* Warm up the allocator's own caches. */
  {
    ThreadArg a;

    pthread_join(os_thread_create(thread_fn, &a, nullptr), nullptr);
  }

  const auto before = vm_size_kb();

  for (auto &a : args) {
    pthread_detach(os_thread_create(thread_fn, &a, nullptr));
  }

  for (auto &a : args) {
    while (a.m_ran.load() == 0) {
      std::this_thread::sleep_for(1ms);
    }
  }

  std::this_thread::sleep_for(50ms);

  const auto after = vm_size_kb();

  /* Each kept stack is ~8 MB: allow far less than one per thread. */
  EXPECT_LT(after - before, size_t(N) * 1024) << "VmSize grew by " << (after - before) << " KB for " << N << " exited threads";
}

/** Many threads in a row and many at once, each runs exactly once. */
TEST(OsThreadTest, Many) {
  for (int i = 0; i < 500; ++i) {
    ThreadArg arg;

    ASSERT_EQ(pthread_join(os_thread_create(thread_fn, &arg, nullptr), nullptr), 0);
    ASSERT_EQ(arg.m_ran, 1);
  }

  constexpr int N = 128;
  std::vector<ThreadArg> args(N);
  std::vector<os_thread_t> threads;
  std::set<ulint> ids;

  for (auto &a : args) {
    threads.push_back(os_thread_create(thread_fn, &a, nullptr));
  }

  for (size_t i = 0; i < threads.size(); ++i) {
    ASSERT_EQ(pthread_join(threads[i], nullptr), 0);
    EXPECT_EQ(args[i].m_ran, 1);
    ids.insert(os_thread_pf(args[i].m_self));
  }

  /* Live at the same time: all ids differ. */
  EXPECT_EQ(ids.size(), size_t(N));
}

/** Threads that create threads at once. */
TEST(OsThreadTest, CreateFromManyThreads) {
  std::atomic<int> ran{};
  std::vector<std::thread> creators;

  for (int c = 0; c < 8; ++c) {
    creators.emplace_back([&] {
      for (int i = 0; i < 50; ++i) {
        ThreadArg arg;
        const auto t = os_thread_create(thread_fn, &arg, nullptr);

        pthread_join(t, nullptr);
        ran += arg.m_ran;
      }
    });
  }

  for (auto &t : creators) {
    t.join();
  }

  EXPECT_EQ(ran, 400);
}

/** os_thread_sleep() sleeps at least the time, 0 returns at once; yield returns. */
TEST(OsThreadTest, SleepYield) {
  auto start = std::chrono::steady_clock::now();

  os_thread_sleep(20000);
  EXPECT_GE(std::chrono::steady_clock::now() - start, 20ms);

  start = std::chrono::steady_clock::now();
  os_thread_sleep(0);
  os_thread_yield();
  EXPECT_LT(std::chrono::steady_clock::now() - start, 1s);
}

/* ---------------------------------------------------------- large memory */

/** os_mem_alloc_large() rounds up to the system page, zero filled, writable. */
TEST(OsProcTest, AllocLarge) {
  const ulint page = ulint(getpagesize());

  for (const ulint n : {1UL, page - 1, page, page + 1, 10 * page + 3, 64UL << 20}) {
    ulint size = n;
    auto p = static_cast<byte *>(os_mem_alloc_large(&size));

    ASSERT_NE(p, nullptr) << n;
    EXPECT_EQ(size, ut_calc_align(n, page)) << n;
    EXPECT_EQ(ut_align_offset(p, page), 0UL);

    /* Zero filled, and every page writable. */
    for (ulint i = 0; i < size; i += page) {
      ASSERT_EQ(p[i], 0);
      p[i] = 1;
    }

    p[size - 1] = 2;
    os_mem_free_large(p, size);
  }
}

/** 0 bytes and an impossible size: nullptr, the size rounded. */
TEST(OsProcTest, AllocLargeEdge) {
  ulint size{0};

  EXPECT_EQ(os_mem_alloc_large(&size), nullptr);
  EXPECT_EQ(size, 0UL);

  size = ulint(1) << 50;
  EXPECT_EQ(os_mem_alloc_large(&size), nullptr);
}

/** os_mem_free_large() of a bad range logs, doesn't crash. */
TEST(OsProcTest, FreeLargeBad) {
  os_mem_free_large(reinterpret_cast<void *>(1), 4096);
}

/** os_proc_get_number() is the process id. */
TEST(OsProcTest, ProcNumber) {
  EXPECT_EQ(os_proc_get_number(), ulint(getpid()));
}

/* -------------------------------------------- os0sync, beyond test_sync */

/** An OS mutex has its own event; unlocking one that isn't locked asserts. */
TEST(OsMutexTest, UnlockUnlockedAsserts) {
  auto m = os_mutex_create(nullptr);

  ASSERT_NE(m, nullptr);
  EXPECT_DEATH(os_mutex_exit(m), "");

  os_mutex_enter(m);
  os_mutex_exit(m);
  os_mutex_destroy(m);
}

/** The lock excludes another thread while held. */
TEST(OsMutexTest, Excludes) {
  auto m = os_mutex_create(nullptr);
  std::atomic<bool> got{};

  os_mutex_enter(m);

  std::thread t([&] {
    os_mutex_enter(m);
    got = true;
    os_mutex_exit(m);
  });

  std::this_thread::sleep_for(20ms);
  EXPECT_FALSE(got);
  os_mutex_exit(m);
  t.join();
  EXPECT_TRUE(got);
  os_mutex_destroy(m);
}

/* BUG: OsMutex::lock() (os/os0sync.cc:60) checks m_count == 1 after
std::mutex::lock() to catch a thread that locks the mutex it holds, as the
comment on m_count says, but a recursive std::mutex::lock() deadlocks (or is
undefined) before the check is reached: the check can never fire. Fix:
record the owner and assert before locking:
+  std::atomic<std::thread::id> m_owner{};
   void lock() {
+    ut_a(m_owner.load(std::memory_order_relaxed) != std::this_thread::get_id());
     static_cast<std::mutex *>(m_ptr)->lock();
+    m_owner.store(std::this_thread::get_id(), std::memory_order_relaxed);
   ...
   void unlock() {
+    m_owner.store({}, std::memory_order_relaxed);
In a child: today it hangs until the alarm. */
TEST(OsMutexTest, RecursiveLockAsserts) {
  EXPECT_DEATH(
    {
      alarm(2);
      auto m = os_mutex_create(nullptr);
      os_mutex_enter(m);
      os_mutex_enter(m);
    },
    "Assertion failure"
  );
}

struct OsEventTest : public ::testing::Test {
  void SetUp() override { m_event = os_event_create(nullptr); }
  void TearDown() override { os_event_free(m_event); }

  CondVar *m_event{};
};

/** wait_time() with a timeout of 0: at once, exceeded unless set. */
TEST_F(OsEventTest, WaitTimeZero) {
  const auto start = std::chrono::steady_clock::now();

  EXPECT_EQ(m_event->wait_time(std::chrono::microseconds(0), 0), OS_SYNC_TIME_EXCEEDED);
  EXPECT_LT(std::chrono::steady_clock::now() - start, 1s);

  os_event_set(m_event);
  EXPECT_EQ(m_event->wait_time(std::chrono::microseconds(0), 0), 0UL);
}

/** wait_time(max) waits for the set(): the "infinite" wait of row0pread.cc. */
TEST_F(OsEventTest, WaitTimeInfinite) {
  const auto sig = os_event_reset(m_event);
  const auto start = std::chrono::steady_clock::now();

  std::thread t([&] {
    std::this_thread::sleep_for(30ms);
    os_event_set(m_event);
  });

  EXPECT_EQ(m_event->wait_time(std::chrono::microseconds::max(), sig), 0UL);
  EXPECT_GE(std::chrono::steady_clock::now() - start, 25ms);
  t.join();
}

/* BUG: CondVar::wait_time() (os/os0sync.cc:126) computes
system_clock::now() + timeout in system_clock's int64 nanoseconds. Any
timeout other than microseconds::max() for which that overflows (more than
~234 years, e.g. microseconds(2^62) used as "forever") wraps to a time in
the past, and so does a negative one: microseconds(OS_SYNC_INFINITE_TIME),
the "infinite" constant os0sync.h offers, is microseconds(-1). Both return
OS_SYNC_TIME_EXCEEDED at once instead of waiting. The callers today pass
microseconds::max() (row/row0pread.cc:835, 904), which is special-cased.
The wait is also on the wall clock: a clock step lengthens or shortens it.
Fix: wait on steady_clock, saturate, and treat negative as infinite:
-  std::chrono::time_point<std::chrono::system_clock> timepoint;
-  if (timeout != std::chrono::microseconds::max()) {
-    timepoint = std::chrono::system_clock::now() + timeout;
-  } else {
-    timepoint = std::chrono::time_point<std::chrono::system_clock>::max();
-  }
+  using Clock = std::chrono::steady_clock;
+  const auto now = Clock::now();
+  const auto left = std::chrono::duration_cast<std::chrono::microseconds>(Clock::time_point::max() - now);
+  const auto timepoint = timeout.count() >= 0 && timeout < left ? now + timeout : Clock::time_point::max(); */
TEST_F(OsEventTest, WaitTimeHugeTimeout) {
  for (const auto timeout : {std::chrono::microseconds(int64_t(1) << 62), std::chrono::microseconds(OS_SYNC_INFINITE_TIME)}) {
    const auto sig = os_event_reset(m_event);
    const auto start = std::chrono::steady_clock::now();

    std::thread t([&] {
      std::this_thread::sleep_for(30ms);
      os_event_set(m_event);
    });

    EXPECT_EQ(m_event->wait_time(timeout, sig), 0UL) << timeout.count();
    EXPECT_GE(std::chrono::steady_clock::now() - start, 25ms) << timeout.count();
    t.join();
  }
}

/** Two threads hand a token back and forth with reset()/wait_low()/set():
no wake-up is lost (a lost one shows as a 10 s timeout). */
TEST(OsEventStress, PingPong) {
  auto ping = os_event_create(nullptr);
  auto pong = os_event_create(nullptr);
  constexpr int N = 20000;
  std::atomic<int> turn{0};
  std::atomic<int> timeouts{};

  auto player = [&](CondVar *mine, CondVar *theirs, int me) {
    for (int i = 0; i < N; ++i) {
      for (;;) {
        const auto sig = os_event_reset(mine);

        if (turn.load() % 2 == me) {
          break;
        }

        if (mine->wait_time(std::chrono::microseconds(10000000), sig) != 0) {
          ++timeouts;
          return;
        }
      }

      ++turn;
      os_event_set(theirs);
    }
  };

  std::thread a(player, ping, pong, 0);
  std::thread b(player, pong, ping, 1);

  a.join();
  b.join();

  EXPECT_EQ(timeouts, 0);
  EXPECT_EQ(turn, 2 * N);

  os_event_free(ping);
  os_event_free(pong);
}

/** Many waiters and a setter that races with resets by the waiters: every
waiter sees every generation. */
TEST(OsEventStress, ManyWaitersGenerations) {
  auto ev = os_event_create(nullptr);
  constexpr int N_WAITERS = 8;
  constexpr int N_GEN = 2000;
  std::atomic<int> gen{0};
  std::atomic<int> seen_all{};
  std::atomic<int> timeouts{};
  std::vector<std::thread> waiters;

  for (int w = 0; w < N_WAITERS; ++w) {
    waiters.emplace_back([&] {
      int last{};

      while (last < N_GEN) {
        const auto sig = os_event_reset(ev);

        if (gen.load() > last) {
          last = gen.load();
          continue;
        }

        if (ev->wait_time(std::chrono::microseconds(10000000), sig) != 0) {
          ++timeouts;
          return;
        }
      }

      ++seen_all;
    });
  }

  for (int g = 1; g <= N_GEN; ++g) {
    gen = g;
    os_event_set(ev);

    if (g % 64 == 0) {
      std::this_thread::yield();
    }
  }

  for (auto &t : waiters) {
    t.join();
  }

  EXPECT_EQ(timeouts, 0);
  EXPECT_EQ(seen_all, N_WAITERS);
  os_event_free(ev);
}

}  // namespace

/* ------------------------------------------------------------- perf */

namespace {

struct FilePerf : public OsFileTest {};

/** pwrite/pread of 4 KB in the page cache, and the fsync latency. */
TEST_F(FilePerf, Io) {
  const auto fh = create("f");
  const auto data = pattern(16384, 1);
  std::vector<byte> back(16384);

  ASSERT_TRUE(os_file_set_size("f", fh, 1024 * 4096));

  auto w = ut::bench("os_file_write_4k", 200000, [&](uint64_t i) {
    (void)os_file_write("f", fh, data.data(), 4096, off_t(i % 1024) * 4096);
  });
  auto r = ut::bench("os_file_read_4k", 200000, [&](uint64_t i) {
    (void)os_file_read(fh, back.data(), 4096, off_t(i % 1024) * 4096);
  });
  auto w16 = ut::bench("os_file_write_16k", 50000, [&](uint64_t i) {
    (void)os_file_write("f", fh, data.data(), 16384, off_t(i % 256) * 16384);
  });
  /* fsync() is the device's latency: ~0.2 s on a busy SATA SSD, of which
the mtime journal commit is 3/4 (fdatasync() is ~4x faster there). */
  auto s = ut::bench("os_file_write_4k_flush", 50, [&](uint64_t i) {
    (void)os_file_write("f", fh, data.data(), 4096, off_t(i % 1024) * 4096);
    (void)os_file_flush(fh);
  });
  auto sz = ut::bench("os_file_get_size", 200000, [&](uint64_t) { ut::keep(os_file_get_size_as_iblonglong(fh)); });

  EXPECT_LT(w.ns_per_op(), 200000);
  EXPECT_LT(r.ns_per_op(), 200000);
  EXPECT_LT(w16.ns_per_op(), 400000);
  EXPECT_LT(s.ns_per_op(), 2e9);
  EXPECT_LT(sz.ns_per_op(), 100000);
  EXPECT_TRUE(os_file_close(fh));
}

/** os_file_set_size() MB/s, 64 MB. */
TEST_F(FilePerf, SetSize) {
  int i{};
  auto r = ut::bench("os_file_set_size_64m", 3, [&](uint64_t) {
    const auto name = std::format("s{}", i++);
    const auto fh = create(name);
    (void)os_file_set_size(name.c_str(), fh, 64 << 20);
    (void)os_file_close(fh);
    (void)os_file_delete(p(name).c_str());
  });

  std::cout << std::format("PERF os_file_set_size {:.0f} MB/s\n", 64.0 / (r.ns_per_op() / 1e9));
  EXPECT_LT(r.ns_per_op(), 30e9);
}

/** 8 threads pwrite 4 KB to one descriptor. */
TEST_F(FilePerf, ConcurrentWrite) {
  const auto fh = create("f");
  const auto data = pattern(4096, 2);

  ASSERT_TRUE(os_file_set_size("f", fh, 8 * 256 * 4096));

  auto r = ut::bench_mt("os_file_write_4k_mt", 8, 20000, [&](int t, uint64_t i) {
    (void)os_file_write("f", fh, data.data(), 4096, off_t(t * 256 + i % 256) * 4096);
  });

  EXPECT_LT(r.ns_per_op(), 200000);
  EXPECT_TRUE(os_file_close(fh));
}

struct ThreadPerf : public ::testing::Test {};

/** os_thread_create() + pthread_join(). */
TEST_F(ThreadPerf, CreateJoin) {
  ThreadArg arg;
  auto r = ut::bench("os_thread_create_join", 5000, [&](uint64_t) { pthread_join(os_thread_create(thread_fn, &arg, nullptr), nullptr); });

  EXPECT_LT(r.ns_per_op(), 2e6);
}

struct OsProcPerf : public ::testing::Test {};

/** os_mem_alloc_large() + touch + free of 1 MB. */
TEST_F(OsProcPerf, AllocLarge) {
  auto r = ut::bench("os_mem_alloc_large_1m", 2000, [&](uint64_t) {
    ulint n = 1 << 20;
    auto p = static_cast<byte *>(os_mem_alloc_large(&n));
    p[0] = 1;
    os_mem_free_large(p, n);
  });

  EXPECT_LT(r.ns_per_op(), 2e6);
}

struct OsMutexPerf : public ::testing::Test {};

/** os_mutex_enter()/exit(), alone and with 4 threads. */
TEST_F(OsMutexPerf, EnterExit) {
  auto m = os_mutex_create(nullptr);
  auto r1 = ut::bench("os_mutex_uncontended", 5000000, [&](uint64_t) {
    os_mutex_enter(m);
    os_mutex_exit(m);
  });
  auto r4 = ut::bench_mt("os_mutex_contended", 4, 500000, [&](int, uint64_t) {
    os_mutex_enter(m);
    os_mutex_exit(m);
  });

  EXPECT_LT(r1.ns_per_op(), 2000);
  EXPECT_LT(r4.ns_per_op(), 50000);
  os_mutex_destroy(m);
}

}  // namespace
