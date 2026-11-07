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

/** @file unit-tests/test_systrx.cc
System transactions: all or nothing across a failure and a crash. */

#include "ut0env.h"

#include "buf0buf.h"
#include "fsp0fsp.h"
#include "log0log.h"
#include "mach0data.h"
#include "mtr0log.h"
#include "mtr0systrx.h"
#include "srv0sys.h"

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <chrono>
#include <random>
#include <thread>
#include <vector>

namespace {

/** Where the test keeps its words, past the segment header. */
constexpr ulint WORD = FSEG_PAGE_DATA + FSEG_HEADER_SIZE;

constexpr ulint N_THREADS = 8;

class SysTrxTest : public ut::DbTest {
 protected:
  void SetUp() override {
    ut::DbTest::SetUp();

    Mtr mtr;

    mtr.start();
    m_sys->m_fsp->latch(SYS_TABLESPACE, &mtr);

    auto block = m_sys->m_fsp->fseg_create(SYS_TABLESPACE, nullptr, FSEG_PAGE_DATA, &mtr);

    ASSERT_NE(block, nullptr);

    m_page_no = block->get_page_no();

    for (ulint i{}; i <= N_THREADS; ++i) {
      mlog_write_ulint(block->get_frame() + WORD + 4 * i, 0, MLOG_4BYTES, &mtr);
    }

    mtr.commit(*m_sys);
  }

  [[nodiscard]] BufBlock *page(Mtr &mtr, page_no_t page_no) {
    BufPool::Request req{
      .m_rw_latch = RW_X_LATCH,
      .m_page_id = {SYS_TABLESPACE, page_no},
      .m_mode = BUF_GET,
      .m_file = __FILE__,
      .m_line = __LINE__,
      .m_mtr = &mtr
    };

    return m_sys->m_buf_pool->get(req, nullptr);
  }

  [[nodiscard]] uint32_t word() {
    Mtr mtr;

    mtr.start();

    const auto v = mach_read_from_4(page(mtr, m_page_no)->get_frame() + WORD);

    mtr.commit(*m_sys);

    return uint32_t(v);
  }

  /** @return the arguments of a compensation on the word. */
  [[nodiscard]] std::array<byte, 14> word_args(uint32_t v) const {
    std::array<byte, 14> a{};

    mach_write_to_4(a.data(), SYS_TABLESPACE);
    mach_write_to_4(a.data() + 4, m_page_no);
    mach_write_to_2(a.data() + 8, WORD);
    mach_write_to_4(a.data() + 10, v);

    return a;
  }

  /** A step: set the word to v, compensate with kind and arg. */
  void set_word(SysTrx &trx, uint32_t v, SysTrxKind kind, uint32_t arg, bool last = false) {
    Mtr mtr;

    mtr.start();
    mlog_write_ulint(page(mtr, m_page_no)->get_frame() + WORD, v, MLOG_4BYTES, &mtr);

    const auto args = word_args(arg);

    trx.step(&mtr, kind, args.data(), args.size());

    if (last) {
      trx.commit(&mtr);
    } else {
      trx.commit_step(&mtr);
    }
  }

  /** @return pages used by the segment. */
  [[nodiscard]] ulint used() {
    Mtr mtr;
    ulint n{};

    mtr.start();
    m_sys->m_fsp->latch(SYS_TABLESPACE, &mtr);
    (void)m_sys->m_fsp->fseg_n_reserved_pages(page(mtr, m_page_no)->get_frame() + FSEG_PAGE_DATA, &n, &mtr);
    mtr.commit(*m_sys);

    return n;
  }

  /** A step: allocate a page of the segment, freed if the system transaction doesn't finish. */
  void alloc(SysTrx &trx) {
    Mtr mtr;

    mtr.start();
    m_sys->m_fsp->latch(SYS_TABLESPACE, &mtr);

    const auto header = page(mtr, m_page_no)->get_frame() + FSEG_PAGE_DATA;
    const auto block = m_sys->m_fsp->fseg_alloc_free_page(header, 0, FSP_NO_DIR, &mtr);

    ASSERT_NE(block, nullptr);

    std::array<byte, 14> a{};

    mach_write_to_4(a.data(), SYS_TABLESPACE);
    mach_write_to_4(a.data() + 4, m_page_no);
    mach_write_to_2(a.data() + 8, FSEG_PAGE_DATA);
    mach_write_to_4(a.data() + 10, block->get_page_no());

    trx.step(&mtr, SysTrxKind::FREE_PAGE, a.data(), a.size());
    trx.commit_step(&mtr);
  }

  /** Log that has nothing to do with the system transaction. */
  void other_log() {
    Mtr mtr;

    mtr.start();
    mlog_write_ulint(page(mtr, m_page_no)->get_frame() + WORD + 4, 3, MLOG_4BYTES, &mtr);
    mtr.commit(*m_sys);
  }

  void run_threads(std::atomic<bool> &stop, std::vector<std::thread> &threads);

  [[nodiscard]] ulint counted();

  page_no_t m_page_no{FIL_NULL};
};

/** Commit keeps every step, across a normal restart too. */
TEST_F(SysTrxTest, Commit) {
  {
    SysTrx trx(*m_sys);

    set_word(trx, 1, SysTrxKind::WRITE4, 0);
    set_word(trx, 2, SysTrxKind::WRITE4, 1, true);
  }

  EXPECT_EQ(word(), 2U);
  EXPECT_EQ(m_sys->m_systrx->n_active(), 0UL);

  restart();
  EXPECT_EQ(word(), 2U);
}

/** A rollback compensates, latest step first. */
TEST_F(SysTrxTest, Rollback) {
  SysTrx trx(*m_sys);

  set_word(trx, 1, SysTrxKind::WRITE4, 0);
  set_word(trx, 2, SysTrxKind::WRITE4, 1);
  EXPECT_EQ(word(), 2U);

  trx.rollback();
  EXPECT_EQ(word(), 0U) << "the first step's compensation ran last";
  EXPECT_EQ(m_sys->m_systrx->n_active(), 0UL);
}

/** A rollback with no steps writes nothing and ends it. */
TEST_F(SysTrxTest, RollbackEmpty) {
  SysTrx trx(*m_sys);

  trx.rollback();
  EXPECT_EQ(m_sys->m_systrx->n_active(), 0UL);
}

/** A crash between steps compensates, in order. */
TEST_F(SysTrxTest, CrashBetween) {
  crash([this] {
    /* Never finished: the child dies in it, with the destructor not run. */
    auto trx = new SysTrx(*m_sys);

    set_word(*trx, 1, SysTrxKind::WRITE4, 0);
    set_word(*trx, 2, SysTrxKind::WRITE4, 1);
    m_sys->m_log->buffer_flush_to_disk();
  });

  EXPECT_EQ(word(), 0U);
  EXPECT_EQ(m_sys->m_systrx->n_active(), 0UL);
}

/** A crash after the commit is durable keeps it. */
TEST_F(SysTrxTest, CrashAfterCommit) {
  crash([this] {
    {
      SysTrx trx(*m_sys);

      set_word(trx, 1, SysTrxKind::WRITE4, 0);
      set_word(trx, 2, SysTrxKind::WRITE4, 1, true);
    }

    m_sys->m_log->buffer_flush_to_disk();
  });

  EXPECT_EQ(word(), 2U);
}

/** A lost tail of the log takes the steps with it: nothing to compensate for. */
TEST_F(SysTrxTest, CrashLostSteps) {
  crash([this] {
    auto trx = new SysTrx(*m_sys);

    set_word(*trx, 1, SysTrxKind::WRITE4, 7);
    /* Not flushed: if some of it got out, the compensation makes the word 7 or it stays 0. */
  });

  const auto v = word();

  EXPECT_TRUE(v == 0 || v == 7) << v;
}

/** After replace(), a crash finishes forward instead of undoing. */
TEST_F(SysTrxTest, CrashForward) {
  crash([this] {
    auto trx = new SysTrx(*m_sys);

    set_word(*trx, 1, SysTrxKind::WRITE4, 0);

    Mtr mtr;

    mtr.start();
    mlog_write_ulint(page(mtr, m_page_no)->get_frame() + WORD, 2, MLOG_4BYTES, &mtr);

    const auto args = word_args(9);

    trx->replace(&mtr, SysTrxKind::WRITE4, args.data(), args.size());
    trx->commit_step(&mtr);

    m_sys->m_log->buffer_flush_to_disk();
  });

  EXPECT_EQ(word(), 9U) << "finished forward, the first step's compensation dropped";
}

/** A crash right after recovery doesn't compensate twice. */
TEST_F(SysTrxTest, CrashTwice) {
  crash([this] {
    auto trx = new SysTrx(*m_sys);

    set_word(*trx, 100, SysTrxKind::ADD4, 5);
    m_sys->m_log->buffer_flush_to_disk();
  });

  EXPECT_EQ(word(), 105U);

  crash([this] { m_sys->m_log->buffer_flush_to_disk(); });

  EXPECT_EQ(word(), 105U);

  restart();
  EXPECT_EQ(word(), 105U);
}

/** A crash in the middle of a rollback finishes it, without repeating what's done. */
TEST_F(SysTrxTest, CrashInRollback) {
  crash([this] {
    auto trx = new SysTrx(*m_sys);

    set_word(*trx, 1000, SysTrxKind::ADD4, 10);
    set_word(*trx, 1000, SysTrxKind::ADD4, 100);

    trx->rollback_step();
    EXPECT_EQ(word(), 1100U);

    m_sys->m_log->buffer_flush_to_disk();
  });

  EXPECT_EQ(word(), 1110U) << "each compensation exactly once";
}

/** A checkpoint goes past an active system transaction, which carries its state along. */
TEST_F(SysTrxTest, Checkpoint) {
  crash([this] {
    auto trx = new SysTrx(*m_sys);

    set_word(*trx, 1, SysTrxKind::WRITE4, 0);

    const auto lsn = m_sys->m_log->get_lsn();

    other_log();

    m_sys->m_log->make_checkpoint_at(LSN_MAX, true);

    ASSERT_GT(m_sys->m_log->m_last_checkpoint_lsn, lsn) << "the step is before the checkpoint";
  });

  EXPECT_EQ(word(), 0U) << "and it was still compensated for";
}

/** Steps on both sides of checkpoints: all compensated for, once. */
TEST_F(SysTrxTest, CheckpointSteps) {
  crash([this] {
    auto trx = new SysTrx(*m_sys);

    set_word(*trx, 1000, SysTrxKind::ADD4, 1);
    m_sys->m_log->make_checkpoint_at(LSN_MAX, true);

    set_word(*trx, 1000, SysTrxKind::ADD4, 10);
    m_sys->m_log->make_checkpoint_at(LSN_MAX, true);
    m_sys->m_log->make_checkpoint_at(LSN_MAX, true);

    set_word(*trx, 1000, SysTrxKind::ADD4, 100);
    m_sys->m_log->buffer_flush_to_disk();
  });

  EXPECT_EQ(word(), 1111U);
}

/** A compensation before a checkpoint isn't repeated after it. */
TEST_F(SysTrxTest, CheckpointRollback) {
  crash([this] {
    auto trx = new SysTrx(*m_sys);

    set_word(*trx, 1000, SysTrxKind::ADD4, 10);
    set_word(*trx, 1000, SysTrxKind::ADD4, 100);

    trx->rollback_step();
    m_sys->m_log->make_checkpoint_at(LSN_MAX, true);
  });

  EXPECT_EQ(word(), 1110U);
}

/** What replace() dropped doesn't come back at a checkpoint. */
TEST_F(SysTrxTest, CheckpointForward) {
  crash([this] {
    auto trx = new SysTrx(*m_sys);

    set_word(*trx, 1, SysTrxKind::WRITE4, 0);

    Mtr mtr;

    mtr.start();
    mlog_write_ulint(page(mtr, m_page_no)->get_frame() + WORD, 2, MLOG_4BYTES, &mtr);

    const auto args = word_args(9);

    trx->replace(&mtr, SysTrxKind::WRITE4, args.data(), args.size());
    trx->commit_step(&mtr);

    m_sys->m_log->make_checkpoint_at(LSN_MAX, true);
  });

  EXPECT_EQ(word(), 9U);
}

/** A checkpoint after the commit has nothing to carry. */
TEST_F(SysTrxTest, CheckpointCommitted) {
  crash([this] {
    {
      SysTrx trx(*m_sys);

      set_word(trx, 1, SysTrxKind::WRITE4, 0);
      set_word(trx, 2, SysTrxKind::WRITE4, 1, true);
    }

    m_sys->m_log->make_checkpoint_at(LSN_MAX, true);
  });

  EXPECT_EQ(word(), 2U);
}

/** A crash of recovery after a checkpoint: the state is carried by the restart too. */
TEST_F(SysTrxTest, CheckpointCrashTwice) {
  crash([this] {
    auto trx = new SysTrx(*m_sys);

    set_word(*trx, 1000, SysTrxKind::ADD4, 10);
    m_sys->m_log->make_checkpoint_at(LSN_MAX, true);
    set_word(*trx, 1000, SysTrxKind::ADD4, 100);
    m_sys->m_log->buffer_flush_to_disk();
  });

  EXPECT_EQ(word(), 1110U);

  crash([this] { m_sys->m_log->make_checkpoint_at(LSN_MAX, true); });

  EXPECT_EQ(word(), 1110U);
}

/** A long one runs through many checkpoints: the checkpoint keeps moving, so the log
behind it can go, and the end state is still right. */
TEST_F(SysTrxTest, LongRunning) {
  SysTrx trx(*m_sys);
  auto last = m_sys->m_log->m_last_checkpoint_lsn;

  for (uint32_t i{1}; i <= 50; ++i) {
    set_word(trx, i, SysTrxKind::ADD4, 1);
    m_sys->m_log->make_checkpoint_at(LSN_MAX, true);

    ASSERT_GT(m_sys->m_log->m_last_checkpoint_lsn, last) << i;
    last = m_sys->m_log->m_last_checkpoint_lsn;
  }

  EXPECT_EQ(m_sys->m_systrx->n_active(), 1UL);

  trx.rollback();

  EXPECT_EQ(word(), 100U) << "the last step set 50, then 50 compensations of +1";
  EXPECT_EQ(m_sys->m_systrx->n_active(), 0UL);
}

/** Threads run system transactions: allocate a page, then count it in their word. Some
roll back. The checkpoints run all the time. A page in use is one that's counted. */
void SysTrxTest::run_threads(std::atomic<bool> &stop, std::vector<std::thread> &threads) {
  for (ulint t{}; t < N_THREADS; ++t) {
    threads.emplace_back([this, t, &stop] {
      std::mt19937 rng(uint32_t(t + 1));

      while (!stop.load(std::memory_order_relaxed)) {
        SysTrx trx(*m_sys);

        alloc(trx);

        std::this_thread::yield();

        Mtr mtr;

        mtr.start();

        const auto ptr = page(mtr, m_page_no)->get_frame() + WORD + 4 * (t + 1);

        mlog_write_ulint(ptr, (mach_read_from_4(ptr) + 1) & 0xFFFFFFFFUL, MLOG_4BYTES, &mtr);

        auto args = word_args(0xFFFFFFFFU);

        mach_write_to_2(args.data() + 8, WORD + 4 * (t + 1));

        trx.step(&mtr, SysTrxKind::ADD4, args.data(), args.size());

        if (rng() % 3 == 0) {
          trx.commit_step(&mtr);
          trx.rollback();
        } else {
          trx.commit(&mtr);
        }
      }
    });
  }

  threads.emplace_back([this, &stop] {
    while (!stop.load(std::memory_order_relaxed)) {
      m_sys->m_log->make_checkpoint_at(LSN_MAX, true);
      std::this_thread::yield();
    }
  });
}

/** @return the sum of the words of the threads. */
ulint SysTrxTest::counted() {
  Mtr mtr;
  ulint n{};

  mtr.start();

  const auto frame = page(mtr, m_page_no)->get_frame();

  for (ulint t{}; t < N_THREADS; ++t) {
    n += mach_read_from_4(frame + WORD + 4 * (t + 1));
  }

  mtr.commit(*m_sys);

  return n;
}

TEST_F(SysTrxTest, Threads) {
  const auto before = used();
  std::atomic<bool> stop{};
  std::vector<std::thread> threads;

  run_threads(stop, threads);

  std::this_thread::sleep_for(std::chrono::seconds(3));

  stop = true;

  for (auto &t : threads) {
    t.join();
  }

  EXPECT_GT(counted(), 0UL);
  EXPECT_EQ(used() - before, counted());
  EXPECT_EQ(m_sys->m_systrx->n_active(), 0UL);
  EXPECT_TRUE(m_sys->m_fsp->validate(SYS_TABLESPACE));
}

/** The same killed at some moment: whatever was active is compensated for. */
TEST_F(SysTrxTest, ThreadsCrash) {
  const auto before = used();

  for (int round{}; round < 3; ++round) {
    crash([this, round] {
      /* They run on until the kill. */
      auto stop = new std::atomic<bool>{};
      auto threads = new std::vector<std::thread>;

      run_threads(*stop, *threads);

      std::this_thread::sleep_for(std::chrono::milliseconds(500 + 300 * round));

      m_sys->m_log->buffer_flush_to_disk();
    });

    EXPECT_EQ(used() - before, counted()) << round;
    EXPECT_EQ(m_sys->m_systrx->n_active(), 0UL);
    EXPECT_TRUE(m_sys->m_fsp->validate(SYS_TABLESPACE));
  }

  EXPECT_GT(counted(), 0UL);
}

/** A page allocated then not used goes back to the segment on a crash. */
TEST_F(SysTrxTest, AllocCrash) {
  const auto before = used();

  crash([this] {
    auto trx = new SysTrx(*m_sys);

    alloc(*trx);
    alloc(*trx);
    m_sys->m_log->buffer_flush_to_disk();
  });

  EXPECT_EQ(used(), before);
  EXPECT_TRUE(m_sys->m_fsp->validate(SYS_TABLESPACE));
}

/** The same on a failure with no crash. */
TEST_F(SysTrxTest, AllocRollback) {
  const auto before = used();
  SysTrx trx(*m_sys);

  alloc(trx);
  EXPECT_EQ(used(), before + 1);

  trx.rollback();
  EXPECT_EQ(used(), before);
}

}  // namespace
