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

/** @file unit-tests/test_aio.cc
Aio: the asynchronous I/O handler of the data files.

Most tests drive an Aio instance of their own on a scratch file: they submit
requests and reap the completions themselves, the way the I/O handler
threads of a System do. The queue ids of reap() are global: the log queue is
0, then the read queues, then the write queues. */

#include "ut0env.h"
#include "ut0perf.h"

#include "fil0types.h"
#include "os0aio.h"
#include "ut0mpmcbq.h"

#include <fcntl.h>
#include <unistd.h>

#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <set>
#include <thread>
#include <vector>

namespace {

/** An aligned I/O buffer. */
struct Buf {
  explicit Buf(size_t n) : m_n(n), m_ptr(static_cast<byte *>(std::aligned_alloc(4096, (n + 4095) / 4096 * 4096))) {
    memset(m_ptr, 0, n);
  }

  ~Buf() { std::free(m_ptr); }

  Buf(const Buf &) = delete;
  Buf &operator=(const Buf &) = delete;

  /** Fill with a pattern derived from seed. */
  void fill(uint32_t seed) {
    for (size_t i{}; i < m_n; ++i) {
      m_ptr[i] = byte((seed * 131 + i * 7 + (i >> 9)) & 0xff);
    }
  }

  /** @return true if the contents are the pattern of seed. */
  [[nodiscard]] bool is(uint32_t seed) const {
    for (size_t i{}; i < m_n; ++i) {
      if (m_ptr[i] != byte((seed * 131 + i * 7 + (i >> 9)) & 0xff)) {
        return false;
      }
    }
    return true;
  }

  size_t m_n;
  byte *m_ptr;
};

/** A scratch file, its FilNode and an Aio of its own. */
class AioFileTest : public ::testing::Test {
 public:
  /** Size of the scratch file. */
  static constexpr size_t FILE_SIZE = 32 * 1024 * 1024;

 protected:

  void SetUp() override { open(1, 1); }

  void TearDown() override { close(); }

  void open(ulint n_read, ulint n_write) {
    m_n_read = n_read;
    m_n_write = n_write;

    m_path = m_dir.m_path / "aio.dat";
    m_fd = ::open(m_path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
    ASSERT_GE(m_fd, 0);
    ASSERT_EQ(::ftruncate(m_fd, FILE_SIZE), 0);

    m_node.m_fh = m_fd;
    m_node.m_file_name = m_name.data();
    m_node.open = true;
    m_node.m_size_in_pages = FILE_SIZE / UNIV_PAGE_SIZE;
    m_node.m_magic_n = FIL_NODE_MAGIC_N;

    AioConfig config;

    config.m_n_read_threads = n_read;
    config.m_n_write_threads = n_write;

    m_aio = Aio::create(config);
    ASSERT_NE(m_aio, nullptr);
  }

  void close() {
    m_aio.reset();

    if (m_fd >= 0) {
      ::close(m_fd);
      m_fd = -1;
    }
  }

  /** @return the global queue id of read queue i. */
  [[nodiscard]] ulint read_queue(ulint i = 0) const { return 1 + i; }

  /** @return the global queue id of write queue i. */
  [[nodiscard]] ulint write_queue(ulint i = 0) const { return 1 + m_n_read + i; }

  /** @return an i/o context on the scratch file. */
  [[nodiscard]] IoCtx ctx(IoRequest req, void *msg = nullptr) {
    return IoCtx{.m_batch = false, .m_fil_node = &m_node, .m_msg = msg, .m_io_request = req};
  }

  /** Submit one async request and reap it from queue_id.
  @return the completed context */
  IoCtx async_io(IoRequest req, ulint queue_id, void *buf, ulint n, off_t off, void *msg = nullptr) {
    EXPECT_EQ(m_aio->submit(ctx(req, msg), buf, n, off), DB_SUCCESS);

    IoCtx done{};

    EXPECT_EQ(m_aio->reap(queue_id, done), DB_SUCCESS);

    return done;
  }

  /** Read n bytes at off with pread, bypassing the handler. */
  void pread_all(void *buf, size_t n, off_t off) { ASSERT_EQ(::pread(m_fd, buf, n, off), ssize_t(n)); }

  ut::TestDir m_dir;
  ut::fs::path m_path;
  std::string m_name{"aio.dat"};
  int m_fd{-1};
  FilNode m_node{};
  ulint m_n_read{};
  ulint m_n_write{};
  std::unique_ptr<Aio> m_aio;
};

/** A handler is created from a configuration and is valid. */
TEST(AioTest, Create) {
  for (ulint n : {1UL, 2UL, 4UL, 8UL}) {
    AioConfig config;

    config.m_n_read_threads = n;
    config.m_n_write_threads = n;

    auto aio = Aio::create(config);

    ASSERT_NE(aio, nullptr) << n;
    EXPECT_FALSE(aio->to_string().empty());
  }
}

/** Different numbers of read and write queues. */
TEST(AioTest, Asymmetric) {
  AioConfig config;

  config.m_n_read_threads = 1;
  config.m_n_write_threads = 7;

  auto aio = Aio::create(config);

  ASSERT_NE(aio, nullptr);

  const auto s = aio->to_string();

  EXPECT_NE(s.find("log:"), std::string::npos) << s;
  EXPECT_NE(s.find("read:"), std::string::npos) << s;
  EXPECT_NE(s.find("write:"), std::string::npos) << s;
}

/** The classification of the request types. */
TEST(AioTest, RequestKinds) {
  struct Kind {
    IoRequest m_req;
    bool m_sync;
    bool m_log;
    bool m_read;
    const char *m_name;
  };

  const Kind kinds[] = {
    {IoRequest::Async_read, false, false, true, "Async_read"},
    {IoRequest::Async_write, false, false, false, "Async_write"},
    {IoRequest::Async_log_read, false, true, true, "Async_log_read"},
    {IoRequest::Async_log_write, false, true, false, "Async_log_write"},
    {IoRequest::Async_log_flush, false, true, false, "Async_log_flush"},
    {IoRequest::Sync_read, true, false, true, "Sync_read"},
    {IoRequest::Sync_write, true, false, false, "Sync_write"},
    {IoRequest::Sync_log_read, true, true, true, "Sync_log_read"},
    {IoRequest::Sync_log_write, true, true, false, "Sync_log_write"},
    {IoRequest::Sync_log_flush, true, true, false, "Sync_log_flush"},
    {IoRequest::None, false, false, false, "None"},
  };

  for (const auto &k : kinds) {
    IoCtx ctx{.m_io_request = k.m_req};

    EXPECT_EQ(ctx.is_sync_request(), k.m_sync) << k.m_name;
    EXPECT_EQ(ctx.is_log_request(), k.m_log) << k.m_name;
    EXPECT_EQ(ctx.is_read_request(), k.m_read) << k.m_name;
    EXPECT_STREQ(to_string(k.m_req), k.m_name);
  }
}

/** shutdown() turns a context into the shutdown marker, a default one isn't. */
TEST(AioTest, ShutdownCtx) {
  IoCtx ctx{};

  EXPECT_FALSE(ctx.is_shutdown()) << "m_ret is -1 by default";

  FilNode node{};

  ctx = IoCtx{.m_ret = 10, .m_batch = true, .m_fil_node = &node, .m_msg = &node, .m_io_request = IoRequest::Async_read};
  ctx.shutdown();

  EXPECT_TRUE(ctx.is_shutdown());
  EXPECT_EQ(ctx.m_fil_node, nullptr);
  EXPECT_EQ(ctx.m_msg, nullptr);
  EXPECT_FALSE(ctx.m_batch);
}

using AioSysTest = ut::SysTest;

/** The System's handler exists before start up. */
TEST_F(AioSysTest, Owned) {
  ASSERT_NE(m_sys->m_aio, nullptr);
  EXPECT_FALSE(m_sys->m_aio->to_string().empty());
}

/** A synchronous write is on disk when submit returns, a synchronous read
returns the data; nothing is queued for a reaper. */
TEST_F(AioFileTest, SyncWriteRead) {
  for (size_t n : {512UL, 4096UL, 16384UL, 65536UL, 1048576UL}) {
    for (off_t off : {off_t(0), off_t(512), off_t(16384 * 7), off_t(FILE_SIZE - n)}) {
      Buf out(n);
      Buf in(n);
      Buf disk(n);

      out.fill(uint32_t(n + off));

      ASSERT_EQ(m_aio->submit(ctx(IoRequest::Sync_write), out.m_ptr, n, off), DB_SUCCESS);
      pread_all(disk.m_ptr, n, off);
      EXPECT_TRUE(disk.is(uint32_t(n + off))) << n << "@" << off;

      ASSERT_EQ(m_aio->submit(ctx(IoRequest::Sync_read), in.m_ptr, n, off), DB_SUCCESS);
      EXPECT_TRUE(in.is(uint32_t(n + off))) << n << "@" << off;
    }
  }

  EXPECT_NE(m_aio->to_string().find("sqes: 0"), std::string::npos) << "sync i/o goes around io_uring";
}

/** An async write completes on the write queue with the context it was
submitted with and the number of bytes written. */
TEST_F(AioFileTest, AsyncWrite) {
  Buf out(UNIV_PAGE_SIZE);
  Buf disk(UNIV_PAGE_SIZE);
  int msg{};

  out.fill(7);

  auto done = async_io(IoRequest::Async_write, write_queue(), out.m_ptr, UNIV_PAGE_SIZE, 3 * UNIV_PAGE_SIZE, &msg);

  EXPECT_EQ(done.m_ret, int(UNIV_PAGE_SIZE));
  EXPECT_EQ(done.m_msg, &msg);
  EXPECT_EQ(done.m_fil_node, &m_node);
  EXPECT_EQ(done.m_io_request, IoRequest::Async_write);
  EXPECT_FALSE(done.is_shutdown());

  pread_all(disk.m_ptr, UNIV_PAGE_SIZE, 3 * UNIV_PAGE_SIZE);
  EXPECT_TRUE(disk.is(7));
}

/** An async read completes on the read queue with the data. */
TEST_F(AioFileTest, AsyncRead) {
  Buf out(UNIV_PAGE_SIZE);
  Buf in(UNIV_PAGE_SIZE);

  out.fill(9);
  ASSERT_EQ(::pwrite(m_fd, out.m_ptr, UNIV_PAGE_SIZE, 5 * UNIV_PAGE_SIZE), ssize_t(UNIV_PAGE_SIZE));

  auto done = async_io(IoRequest::Async_read, read_queue(), in.m_ptr, UNIV_PAGE_SIZE, 5 * UNIV_PAGE_SIZE);

  EXPECT_EQ(done.m_ret, int(UNIV_PAGE_SIZE));
  EXPECT_EQ(done.m_io_request, IoRequest::Async_read);
  EXPECT_TRUE(in.is(9));
}

/** Async writes and reads of several sizes and alignments round trip. */
TEST_F(AioFileTest, AsyncSizes) {
  for (size_t n : {512UL, 1024UL, 4096UL, 16384UL, 65536UL, 1048576UL, 4194304UL}) {
    for (off_t off : {off_t(0), off_t(512), off_t(4096 * 3), off_t(FILE_SIZE - n)}) {
      Buf out(n);
      Buf in(n);

      out.fill(uint32_t(n ^ off));

      auto w = async_io(IoRequest::Async_write, write_queue(), out.m_ptr, n, off);
      EXPECT_EQ(w.m_ret, int(n)) << n << "@" << off;

      auto r = async_io(IoRequest::Async_read, read_queue(), in.m_ptr, n, off);
      EXPECT_EQ(r.m_ret, int(n)) << n << "@" << off;
      EXPECT_TRUE(in.is(uint32_t(n ^ off))) << n << "@" << off;
    }
  }
}

/** The last block of the file and the first. */
TEST_F(AioFileTest, FileEdges) {
  Buf a(512);
  Buf b(512);
  Buf in(512);

  a.fill(1);
  b.fill(2);

  EXPECT_EQ(async_io(IoRequest::Async_write, write_queue(), a.m_ptr, 512, 0).m_ret, 512);
  EXPECT_EQ(async_io(IoRequest::Async_write, write_queue(), b.m_ptr, 512, FILE_SIZE - 512).m_ret, 512);

  EXPECT_EQ(async_io(IoRequest::Async_read, read_queue(), in.m_ptr, 512, 0).m_ret, 512);
  EXPECT_TRUE(in.is(1));
  EXPECT_EQ(async_io(IoRequest::Async_read, read_queue(), in.m_ptr, 512, FILE_SIZE - 512).m_ret, 512);
  EXPECT_TRUE(in.is(2));
}

/** A write past the end of the file extends it. */
TEST_F(AioFileTest, WriteExtends) {
  Buf out(UNIV_PAGE_SIZE);

  out.fill(3);

  EXPECT_EQ(async_io(IoRequest::Async_write, write_queue(), out.m_ptr, UNIV_PAGE_SIZE, FILE_SIZE).m_ret, int(UNIV_PAGE_SIZE));
  EXPECT_EQ(ut::fs::file_size(m_path), FILE_SIZE + UNIV_PAGE_SIZE);
}

/** Many requests in flight on one queue: each completes once, with its own
message, whatever the completion order. */
TEST_F(AioFileTest, ManyInFlight) {
  constexpr size_t N = 200;
  std::vector<std::unique_ptr<Buf>> bufs;
  std::vector<int> msgs(N);

  for (size_t i{}; i < N; ++i) {
    bufs.push_back(std::make_unique<Buf>(UNIV_PAGE_SIZE));
    bufs[i]->fill(uint32_t(i));
    ASSERT_EQ(m_aio->submit(ctx(IoRequest::Async_write, &msgs[i]), bufs[i]->m_ptr, UNIV_PAGE_SIZE, off_t(i * UNIV_PAGE_SIZE)), DB_SUCCESS);
  }

  std::set<void *> seen;

  for (size_t i{}; i < N; ++i) {
    IoCtx done{};

    ASSERT_EQ(m_aio->reap(write_queue(), done), DB_SUCCESS);
    EXPECT_EQ(done.m_ret, int(UNIV_PAGE_SIZE));
    EXPECT_TRUE(seen.insert(done.m_msg).second) << "completed twice";
  }

  EXPECT_EQ(seen.size(), N);

  for (size_t i{}; i < N; ++i) {
    memset(bufs[i]->m_ptr, 0, UNIV_PAGE_SIZE);
    ASSERT_EQ(m_aio->submit(ctx(IoRequest::Async_read, &msgs[i]), bufs[i]->m_ptr, UNIV_PAGE_SIZE, off_t(i * UNIV_PAGE_SIZE)), DB_SUCCESS);
  }

  for (size_t i{}; i < N; ++i) {
    IoCtx done{};

    ASSERT_EQ(m_aio->reap(read_queue(), done), DB_SUCCESS);
    EXPECT_EQ(done.m_ret, int(UNIV_PAGE_SIZE));
  }

  for (size_t i{}; i < N; ++i) {
    EXPECT_TRUE(bufs[i]->is(uint32_t(i))) << i;
  }
}

/** More requests than the handler has slots (256): submit waits for the
reaper to free slots instead of failing. */
TEST_F(AioFileTest, MoreThanSlots) {
  constexpr size_t N = 2000;
  Buf out(4096);
  std::atomic<size_t> reaped{};

  out.fill(5);

  std::thread reaper([&] {
    while (reaped.load() < N) {
      IoCtx done{};

      ASSERT_EQ(m_aio->reap(write_queue(), done), DB_SUCCESS);
      ASSERT_EQ(done.m_ret, 4096);
      ++reaped;
    }
  });

  for (size_t i{}; i < N; ++i) {
    ASSERT_EQ(m_aio->submit(ctx(IoRequest::Async_write), out.m_ptr, 4096, off_t((i % 4096) * 4096)), DB_SUCCESS);
  }

  reaper.join();

  EXPECT_EQ(reaped.load(), N);
  m_aio->wait_for_pending_ops(aio::WRITE);
}

/** Log requests go to the log queue, 0. */
TEST_F(AioFileTest, LogQueue) {
  Buf out(4096);
  Buf in(4096);

  out.fill(11);

  auto w = async_io(IoRequest::Async_log_write, aio::LOG, out.m_ptr, 4096, 4096);

  EXPECT_EQ(w.m_ret, 4096);
  EXPECT_EQ(w.m_io_request, IoRequest::Async_log_write);

  auto r = async_io(IoRequest::Async_log_read, aio::LOG, in.m_ptr, 4096, 4096);

  EXPECT_EQ(r.m_ret, 4096);
  EXPECT_TRUE(in.is(11));
}

/** wait_for_pending_ops() returns at once when nothing is pending. */
TEST_F(AioFileTest, WaitIdle) {
  m_aio->wait_for_pending_ops(aio::LOG);
  m_aio->wait_for_pending_ops(aio::READ);
  m_aio->wait_for_pending_ops(aio::WRITE);
}

/** wait_for_pending_ops() waits until the last request is reaped. */
TEST_F(AioFileTest, WaitPending) {
  constexpr size_t N = 64;
  Buf out(4096);
  std::atomic<size_t> reaped{};

  out.fill(1);

  for (size_t i{}; i < N; ++i) {
    ASSERT_EQ(m_aio->submit(ctx(IoRequest::Async_write), out.m_ptr, 4096, off_t(i * 4096)), DB_SUCCESS);
  }

  std::thread reaper([&] {
    for (size_t i{}; i < N; ++i) {
      IoCtx done{};

      std::this_thread::sleep_for(std::chrono::microseconds(200));
      ASSERT_EQ(m_aio->reap(write_queue(), done), DB_SUCCESS);
      ++reaped;
    }
  });

  m_aio->wait_for_pending_ops(aio::WRITE);

  /* All were reaped; the reaper may not have counted the last one yet. */
  EXPECT_GE(reaped.load() + 1, N);

  reaper.join();
  EXPECT_EQ(reaped.load(), N);
}

/** After shutdown() a reap on every queue returns the shutdown marker, and
a second shutdown() is harmless. */
TEST_F(AioFileTest, Shutdown) {
  close();
  open(2, 3);

  Buf out(4096);

  out.fill(1);
  EXPECT_EQ(async_io(IoRequest::Async_write, write_queue(), out.m_ptr, 4096, 0).m_ret, 4096) << "an idle handler submits to its first queue";

  m_aio->shutdown();

  for (ulint q{}; q < 1 + m_n_read + m_n_write; ++q) {
    IoCtx done{};

    ASSERT_EQ(m_aio->reap(q, done), DB_SUCCESS);
    EXPECT_TRUE(done.is_shutdown()) << q;
  }

  m_aio->shutdown();
}

/** Run n_threads submitters, each writing then reading back its own pages,
with a reaper thread on each read and write queue: every request must
complete exactly once and the data read back must be the data written.
@return the number of bad completions or bad pages */
size_t multi_queue(AioFileTest &test, Aio &aio, FilNode &node, ulint n_read, ulint n_write, int n_threads) {
  const int N_THREADS = n_threads;
  constexpr size_t N_PER_THREAD = 300;
  constexpr size_t PAGE = 4096;

  std::atomic<size_t> n_writes{};
  std::atomic<size_t> n_reads{};
  std::atomic<size_t> bad{};

  std::vector<std::thread> reapers;

  for (ulint q{1}; q < 1 + n_read + n_write; ++q) {
    reapers.emplace_back([&, q] {
      for (;;) {
        IoCtx done{};

        if (aio.reap(q, done) != DB_SUCCESS) {
          ++bad;
          return;
        }

        if (done.is_shutdown()) {
          return;
        }

        if (done.m_ret != int(PAGE)) {
          ++bad;
        }

        auto flag = static_cast<std::atomic<int> *>(done.m_msg);

        flag->store(1, std::memory_order_release);

        if (done.is_read_request()) {
          ++n_reads;
        } else {
          ++n_writes;
        }
      }
    });
  }

  std::vector<std::thread> submitters;

  for (int t = 0; t < N_THREADS; ++t) {
    submitters.emplace_back([&, t] {
      std::vector<std::unique_ptr<Buf>> bufs;
      std::vector<std::atomic<int>> flags(N_PER_THREAD);

      for (size_t i{}; i < N_PER_THREAD; ++i) {
        const off_t off = off_t((t * N_PER_THREAD + i) * PAGE);

        bufs.push_back(std::make_unique<Buf>(PAGE));
        bufs[i]->fill(uint32_t(t * 100000 + i));
        flags[i] = 0;
        EXPECT_EQ(aio.submit(IoCtx{.m_fil_node = &node, .m_msg = &flags[i], .m_io_request = IoRequest::Async_write}, bufs[i]->m_ptr, PAGE, off), DB_SUCCESS);
      }

      for (auto &f : flags) {
        while (f.load(std::memory_order_acquire) == 0) {
          std::this_thread::yield();
        }
      }

      for (size_t i{}; i < N_PER_THREAD; ++i) {
        const off_t off = off_t((t * N_PER_THREAD + i) * PAGE);

        memset(bufs[i]->m_ptr, 0, PAGE);
        flags[i] = 0;
        EXPECT_EQ(aio.submit(IoCtx{.m_fil_node = &node, .m_msg = &flags[i], .m_io_request = IoRequest::Async_read}, bufs[i]->m_ptr, PAGE, off), DB_SUCCESS);
      }

      for (auto &f : flags) {
        while (f.load(std::memory_order_acquire) == 0) {
          std::this_thread::yield();
        }
      }

      for (size_t i{}; i < N_PER_THREAD; ++i) {
        if (!bufs[i]->is(uint32_t(t * 100000 + i))) {
          ++bad;
        }
      }
    });
  }

  for (auto &t : submitters) {
    t.join();
  }

  aio.wait_for_pending_ops(aio::READ);
  aio.wait_for_pending_ops(aio::WRITE);
  aio.shutdown();

  for (auto &t : reapers) {
    t.join();
  }

  EXPECT_EQ(n_writes.load(), N_THREADS * N_PER_THREAD);
  EXPECT_EQ(n_reads.load(), N_THREADS * N_PER_THREAD);

  (void)test;
  return bad.load();
}

/** One submitter, reapers on several read and write queues: requests are
spread over the queues and each completes exactly once with its data. */
TEST_F(AioFileTest, MultiQueue) {
  close();
  open(4, 4);

  EXPECT_EQ(multi_queue(*this, *m_aio, m_node, m_n_read, m_n_write, 1), 0UL);
}

/** A pool of objects, each taken and given back, as the free slots of Handler: never more
in it than its capacity, and enqueue() may still say full for a moment. Each give back must
get in by trying again: the objects are all there at the end, and none is there twice. */
TEST(AioPool, GiveBackRetries) {
  constexpr size_t N = 4;
  constexpr int OPS = 200000;

  BoundedChannel<size_t> pool(N);

  for (size_t i{}; i < N; ++i) {
    ASSERT_TRUE(pool.enqueue(i));
  }

  std::atomic<uint64_t> n_retries{};
  std::vector<std::thread> threads;

  for (size_t t{}; t < 2 * N; ++t) {
    threads.emplace_back([&] {
      for (int i{}; i < OPS; ++i) {
        size_t v{};

        while (!pool.dequeue(v)) {
          std::this_thread::yield();
        }

        while (!pool.enqueue(v)) {
          n_retries.fetch_add(1, std::memory_order_relaxed);
          std::this_thread::yield();
        }
      }
    });
  }

  for (auto &t : threads) {
    t.join();
  }

  std::set<size_t> seen;
  size_t v{};

  while (pool.dequeue(v)) {
    EXPECT_TRUE(seen.insert(v).second) << v;
  }

  EXPECT_EQ(seen.size(), N);
  std::printf("AioPool: %lu give backs had to retry\n", static_cast<unsigned long>(n_retries.load()));
}

/* Regression: Aio::submit() is not thread safe. Handler::Queue::submit() calls
io_uring_get_sqe() and io_uring_submit() on a queue's ring without a lock,
but several threads submit to the same queue (get_queue_for_submit() picks
the least loaded queue, the same one for threads that race). The SQ ring is
single producer: one thread's io_uring_submit() returns 2, having submitted
the other's SQE too, and the other's returns 0; both are fatal ("io_uring_submit
failed"). Same race with the reaper's resubmission of partial i/o.
See os/os0aio.cc Handler::Queue::submit(). */
TEST_F(AioFileTest, ConcurrentSubmit) {
  close();
  open(4, 4);

  EXPECT_EQ(multi_queue(*this, *m_aio, m_node, m_n_read, m_n_write, 8), 0UL);
}

/* Regression: a read that ends past the end of the file never completes correctly.
io_uring returns a short count (here half the request) and Queue::reap()
resubmits the rest without io_uring_cqe_seen() on the first completion, so
the next io_uring_wait_cqe() returns the same CQE again and its length is
counted twice; the resubmitted SQE then completes later (res 0, EOF) for a
slot that has been freed. A read wholly past EOF (res 0) resubmits forever.
See os/os0aio.cc Handler::Queue::reap(). */
TEST_F(AioFileTest, ShortRead) {
  Buf in(2 * 4096);

  auto done = async_io(IoRequest::Async_read, read_queue(), in.m_ptr, 2 * 4096, FILE_SIZE - 4096);

  /* The bytes that exist are read, the result says how many. */
  EXPECT_EQ(done.m_ret, 4096);

  /* No stray completion is left behind on the queue: the next request on
  the same queue completes as itself, with its data. */
  Buf out(4096);
  Buf back(4096);
  int msg{};

  out.fill(1);
  ASSERT_EQ(::pwrite(m_fd, out.m_ptr, 4096, 0), 4096);

  auto r = async_io(IoRequest::Async_read, read_queue(), back.m_ptr, 4096, 0, &msg);

  EXPECT_EQ(r.m_msg, &msg);
  EXPECT_EQ(r.m_ret, 4096);
  EXPECT_TRUE(back.is(1));
}

/* Regression: a failed request is not reported as failed. A write to a file opened
read-only completes with cqe->res == -EBADF; Queue::reap() only checks
res != -1 and then adds the negative value to the slot's offset and length,
so the "remaining" length grows and the request is resubmitted forever.
See os/os0aio.cc Handler::Queue::reap(). */
TEST_F(AioFileTest, ErrorResult) {
  const auto ro = ::open(m_path.c_str(), O_RDONLY);

  ASSERT_GE(ro, 0);

  FilNode node = m_node;

  node.m_fh = ro;

  Buf out(4096);

  out.fill(1);
  ASSERT_EQ(m_aio->submit(IoCtx{.m_fil_node = &node, .m_io_request = IoRequest::Async_write}, out.m_ptr, 4096, 0), DB_SUCCESS);

  IoCtx done{};

  ASSERT_EQ(m_aio->reap(write_queue(), done), DB_SUCCESS);
  EXPECT_LT(done.m_ret, 0) << "the error is returned to the caller";

  ::close(ro);
}

/** Reads and writes in parallel by many threads, through the handler: the
data written is read back. */
class AioIoTest : public ut::DbTest {
 protected:
  void SetUp() override {
    m_config = ut::make_config(m_dir.m_path);
    m_config.m_buf_pool.m_size = 8 * 1024 * 1024;
    m_config.m_aio.m_n_read_threads = 2;
    m_config.m_aio.m_n_write_threads = 2;
    start();
  }
};

TEST_F(AioIoTest, ReadWrite) {
  ut::Rows rows;

  for (uint32_t k = 0; k < 3000; ++k) {
    rows[k] = ut::Row{k, k, ut::blob(k, 3000)};
  }

  load(rows);

  std::vector<std::thread> threads;

  for (int t = 0; t < 4; ++t) {
    threads.emplace_back([&] {
      auto trx = begin();

      EXPECT_EQ(scan(trx), rows);
      commit(trx);
    });
  }

  for (auto &t : threads) {
    t.join();
  }

  EXPECT_FALSE(m_sys->m_aio->to_string().empty());
}

}  // namespace

namespace {

using AioPerf = AioFileTest;

/** Report n i/o operations of bytes each done in secs. */
void report_io(const std::string &name, int threads, uint64_t n, size_t bytes, double secs) {
  ut::report(ut::BenchResult{name, threads, n, secs});
  ut::report(ut::BenchResult{name + "_MBps", threads, uint64_t(double(n * bytes) / (1024.0 * 1024.0)), secs});
}

/** Async i/o of size bytes at queue depth qd, n requests in all.
@return seconds */
double run_qd(AioFileTest &t, Aio &aio, IoRequest req, ulint queue_id, size_t size, size_t qd, size_t n, FilNode &node) {
  std::vector<std::unique_ptr<Buf>> bufs;

  for (size_t i{}; i < qd; ++i) {
    bufs.push_back(std::make_unique<Buf>(size));
    bufs[i]->fill(uint32_t(i));
  }

  const size_t n_slots = AioFileTest::FILE_SIZE / size;
  const auto start = std::chrono::steady_clock::now();

  for (size_t done{}; done < n; done += qd) {
    for (size_t i{}; i < qd; ++i) {
      const off_t off = off_t(((done + i) * 7919 % n_slots) * size);

      (void)aio.submit(IoCtx{.m_fil_node = &node, .m_io_request = req}, bufs[i]->m_ptr, size, off);
    }

    for (size_t i{}; i < qd; ++i) {
      IoCtx ctx{};

      (void)aio.reap(queue_id, ctx);
      ut::keep(ctx);
    }
  }

  (void)t;
  const std::chrono::duration<double> secs = std::chrono::steady_clock::now() - start;

  return secs.count();
}

/** Synchronous 16K writes, through the handler. */
TEST_F(AioPerf, SyncWrite16K) {
  Buf out(UNIV_PAGE_SIZE);
  const size_t n_slots = FILE_SIZE / UNIV_PAGE_SIZE;

  out.fill(1);

  auto r = ut::bench("aio_sync_write_16k", 20000, [&](uint64_t i) {
    (void)m_aio->submit(ctx(IoRequest::Sync_write), out.m_ptr, UNIV_PAGE_SIZE, off_t((i * 7919 % n_slots) * UNIV_PAGE_SIZE));
  });

  EXPECT_LT(r.ns_per_op(), 1e6);
}

/** Async 16K writes at queue depth 1 (submit, reap): the round trip. */
TEST_F(AioPerf, AsyncWrite16KQd1) {
  const size_t n = 20000;
  const auto secs = run_qd(*this, *m_aio, IoRequest::Async_write, write_queue(), UNIV_PAGE_SIZE, 1, n, m_node);

  report_io("aio_write_16k_qd1", 1, n, UNIV_PAGE_SIZE, secs);
  EXPECT_LT(secs * 1e9 / n, 2e6);
}

/** Async 16K writes at queue depth 64: IOPS. */
TEST_F(AioPerf, AsyncWrite16KQd64) {
  const size_t n = 64 * 1000;
  const auto secs = run_qd(*this, *m_aio, IoRequest::Async_write, write_queue(), UNIV_PAGE_SIZE, 64, n, m_node);

  report_io("aio_write_16k_qd64", 1, n, UNIV_PAGE_SIZE, secs);
  EXPECT_LT(secs * 1e9 / n, 1e6);
}

/** Async 16K reads at queue depth 64: IOPS (from the page cache). */
TEST_F(AioPerf, AsyncRead16KQd64) {
  (void)run_qd(*this, *m_aio, IoRequest::Async_write, write_queue(), 1048576, 8, FILE_SIZE / 1048576, m_node);

  const size_t n = 64 * 2000;
  const auto secs = run_qd(*this, *m_aio, IoRequest::Async_read, read_queue(), UNIV_PAGE_SIZE, 64, n, m_node);

  report_io("aio_read_16k_qd64", 1, n, UNIV_PAGE_SIZE, secs);
  EXPECT_LT(secs * 1e9 / n, 1e6);
}

/** Async 1M writes and reads at queue depth 8: bandwidth. */
TEST_F(AioPerf, Bandwidth1M) {
  const size_t n = 8 * 200;
  const auto w = run_qd(*this, *m_aio, IoRequest::Async_write, write_queue(), 1048576, 8, n, m_node);

  report_io("aio_write_1m_qd8", 1, n, 1048576, w);

  const auto r = run_qd(*this, *m_aio, IoRequest::Async_read, read_queue(), 1048576, 8, n, m_node);

  report_io("aio_read_1m_qd8", 1, n, 1048576, r);

  EXPECT_LT(w * 1e9 / n, 50e6);
  EXPECT_LT(r * 1e9 / n, 50e6);
}

/** 16K reads at queue depth 128 from one submitter over 4 read queues,
each with a reaper thread of its own, the way a System runs. One submitter:
concurrent submitters crash, see ConcurrentSubmit. */
TEST_F(AioPerf, MultiQueueRead16K) {
  close();
  open(4, 4);

  /* Fill the file around the handler: with 4 write queues the submits are
  spread over them, a single reaper would wait forever. */
  {
    Buf mb(1048576);

    mb.fill(3);

    for (size_t off{}; off < FILE_SIZE; off += mb.m_n) {
      ASSERT_EQ(::pwrite(m_fd, mb.m_ptr, mb.m_n, off_t(off)), ssize_t(mb.m_n));
    }
  }

  constexpr size_t QD = 128;
  constexpr size_t N = QD * 1000;

  std::atomic<size_t> completed{};
  std::vector<std::thread> reapers;

  for (ulint q{}; q < m_n_read; ++q) {
    reapers.emplace_back([&, q] {
      for (;;) {
        IoCtx done{};

        (void)m_aio->reap(read_queue(q), done);

        if (done.is_shutdown()) {
          return;
        }

        completed.fetch_add(1, std::memory_order_release);
      }
    });
  }

  std::vector<std::unique_ptr<Buf>> bufs;

  for (size_t i{}; i < QD; ++i) {
    bufs.push_back(std::make_unique<Buf>(UNIV_PAGE_SIZE));
  }

  const auto start = std::chrono::steady_clock::now();

  for (size_t done{}; done < N; done += QD) {
    for (size_t i{}; i < QD; ++i) {
      const off_t off = off_t(((done + i) * 7919 % (FILE_SIZE / UNIV_PAGE_SIZE)) * UNIV_PAGE_SIZE);

      (void)m_aio->submit(ctx(IoRequest::Async_read), bufs[i]->m_ptr, UNIV_PAGE_SIZE, off);
    }

    while (completed.load(std::memory_order_acquire) < done + QD) {
      std::this_thread::yield();
    }
  }

  const std::chrono::duration<double> secs = std::chrono::steady_clock::now() - start;

  m_aio->wait_for_pending_ops(aio::READ);
  m_aio->shutdown();

  for (auto &t : reapers) {
    t.join();
  }

  report_io("aio_read_16k_qd128_4q", 1, N, UNIV_PAGE_SIZE, secs.count());
  EXPECT_LT(secs.count() * 1e9 / double(N), 1e6);
}

}  // namespace
