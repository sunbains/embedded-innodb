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

/** @file unit-tests/test_wal.cc
The WAL file as recovery sees it: LogWalBackend::read() returns exactly the
bytes that were appended and flushed, from any LSN, across partial blocks and
big appends, in a linear log and in one that wraps around its file; space
before a checkpoint goes back to the file system; the checkpoint file keeps
the latest valid checkpoint. */

#include "log0wal.h"

#include <gtest/gtest.h>

#include <sys/stat.h>

#include <filesystem>
#include <fstream>
#include <random>
#include <vector>

namespace {

namespace fs = std::filesystem;

/** A WAL configuration in a fresh directory. */
struct WalTest : public ::testing::Test {
  void SetUp() override {
    /* TEST_WAL_DIR picks the base directory, TEST_WAL_KEEP keeps the files for inspection. */
    const auto base = ::getenv("TEST_WAL_DIR") != nullptr ? fs::path(::getenv("TEST_WAL_DIR")) : fs::temp_directory_path();

    m_dir = base / std::format("test_wal_{}_{}", ::getpid(), ::testing::UnitTest::GetInstance()->current_test_info()->name());
    fs::remove_all(m_dir);
    fs::create_directories(m_dir);

    m_config.m_log_dir = m_dir.string();
    m_config.m_n_blocks = 16;
    m_config.m_pool_size = 4;
  }

  void TearDown() override {
    if (::getenv("TEST_WAL_KEEP") == nullptr) {
      fs::remove_all(m_dir);
    }
  }

  /** Appends random records, flushing at random points, and returns the bytes. */
  std::vector<byte> append(LogWalBackend &wal, ulint n_bytes, ulint max_rec_len) {
    std::vector<byte> bytes;
    std::uniform_int_distribution<ulint> len_dist(1, max_rec_len);
    std::uniform_int_distribution<int> byte_dist(0, 255);
    std::uniform_int_distribution<int> flush_dist(0, 9);

    while (bytes.size() < n_bytes) {
      std::vector<byte> rec(len_dist(m_rng));

      for (auto &b : rec) {
        b = byte(byte_dist(m_rng));
      }

      EXPECT_NE(wal.append(rec.data(), rec.size()), 0U);
      bytes.insert(bytes.end(), rec.begin(), rec.end());

      if (flush_dist(m_rng) == 0) {
        EXPECT_TRUE(wal.flush(IB_UINT64_T_MAX, true));
      }
    }

    EXPECT_TRUE(wal.flush(IB_UINT64_T_MAX, true));

    return bytes;
  }

  /** Reads the log from lsn: returns the bytes and sets end to where reading stopped. */
  std::vector<byte> read(lsn_t lsn, lsn_t &end) const {
    std::vector<byte> bytes;

    end = LogWalBackend::read(m_config, lsn, [&](const byte *data, ulint len) {
      bytes.insert(bytes.end(), data, data + len);
      return true;
    });

    return bytes;
  }

  fs::path m_dir;
  LogWalBackend::Config m_config;
  std::mt19937_64 m_rng{42};
};

}  // namespace

/** Small records with flushes in between rewrite the partial last block. */
TEST_F(WalTest, RoundTrip) {
  const auto start = LogWalBackend::align_start_lsn(m_config, 16384);

  LogWalBackend wal(m_config, start);

  ASSERT_TRUE(wal.open());
  wal.start_io();

  const auto bytes = append(wal, 300 * 1024, 3000);

  lsn_t end{};

  EXPECT_EQ(read(start, end), bytes);
  EXPECT_EQ(end, wal.get_lsn());
  EXPECT_EQ(end, start + bytes.size());

  /* From inside a block: the suffix. */
  const lsn_t skip = 12345;

  EXPECT_EQ(read(start + skip, end), std::vector<byte>(bytes.begin() + skip, bytes.end()));
  EXPECT_EQ(end, wal.get_lsn());
}

/** A record group bigger than wal::Log::append() takes is split. */
TEST_F(WalTest, BigAppend) {
  const auto start = LogWalBackend::align_start_lsn(m_config, 1);

  LogWalBackend wal(m_config, start);

  ASSERT_TRUE(wal.open());
  wal.start_io();

  std::vector<byte> rec(100 * 1024);

  for (ulint i{}; i < rec.size(); ++i) {
    rec[i] = byte(i * 7);
  }

  EXPECT_EQ(wal.append(rec.data(), rec.size()), start + rec.size());
  EXPECT_TRUE(wal.flush(IB_UINT64_T_MAX, true));

  lsn_t end{};

  EXPECT_EQ(read(start, end), rec);
  EXPECT_EQ(end, start + rec.size());
}

/** The log wraps around a small file: the newest capacity of log is readable,
older log is overwritten and reading it stops at once. */
TEST_F(WalTest, Wrap) {
  m_config.m_max_file_size = 64 * 4096;

  /* Log bytes in the file: 64 blocks of 4080 bytes of data. */
  const lsn_t capacity = 64 * 4080;
  const auto start = LogWalBackend::align_start_lsn(m_config, 1);

  LogWalBackend wal(m_config, start);

  ASSERT_TRUE(wal.open());
  wal.start_io();

  const auto bytes = append(wal, 4 * capacity, 2000);
  const auto from = wal.get_lsn() - capacity / 2;

  lsn_t end{};

  EXPECT_EQ(read(from, end), std::vector<byte>(bytes.end() - ptrdiff_t(capacity / 2), bytes.end()));
  EXPECT_EQ(end, wal.get_lsn());

  EXPECT_TRUE(read(start, end).empty());
  EXPECT_EQ(end, start);
}

/** A linear log gives back the space before a checkpoint; the log after it stays readable. */
TEST_F(WalTest, Discard) {
  const auto start = LogWalBackend::align_start_lsn(m_config, 1);

  LogWalBackend wal(m_config, start);

  ASSERT_TRUE(wal.open());
  wal.start_io();

  const auto bytes = append(wal, 2 * 1024 * 1024, 2000);
  const auto path = m_dir / m_config.m_log_file;

  struct stat before;
  ASSERT_EQ(::stat(path.c_str(), &before), 0);

  const auto checkpoint = start + bytes.size() - 100000;

  wal.discard_up_to(checkpoint);

  struct stat after;
  ASSERT_EQ(::stat(path.c_str(), &after), 0);

  EXPECT_EQ(after.st_size, before.st_size);
  EXPECT_LT(after.st_blocks, before.st_blocks);

  lsn_t end{};

  EXPECT_EQ(read(checkpoint, end), std::vector<byte>(bytes.end() - 100000, bytes.end()));
  EXPECT_EQ(end, wal.get_lsn());

  /* The discarded log reads as no log. */
  EXPECT_TRUE(read(start, end).empty());
}

/** Log that was never written ends the read where the written log ends. */
TEST_F(WalTest, EndOfLog) {
  const auto start = LogWalBackend::align_start_lsn(m_config, 1);

  {
    LogWalBackend wal(m_config, start);

    ASSERT_TRUE(wal.open());
    wal.start_io();

    std::vector<byte> rec(5000, byte(1));

    EXPECT_NE(wal.append(rec.data(), rec.size()), 0U);
    EXPECT_TRUE(wal.flush(IB_UINT64_T_MAX, true));
  }

  lsn_t end{};

  EXPECT_EQ(read(start, end).size(), 5000U);
  EXPECT_EQ(end, start + 5000);

  /* Past the end: nothing. */
  EXPECT_TRUE(read(start + 5000, end).empty());
  EXPECT_EQ(end, start + 5000);
}

/** The newer of the two slots wins; a torn slot leaves the other one. */
TEST_F(WalTest, Checkpoint) {
  uint64_t no{};
  lsn_t lsn{};

  EXPECT_FALSE(LogWalBackend::read_checkpoint(m_config, no, lsn));

  ASSERT_TRUE(LogWalBackend::write_checkpoint(m_config, 1, 1000));
  ASSERT_TRUE(LogWalBackend::write_checkpoint(m_config, 2, 2000));

  ASSERT_TRUE(LogWalBackend::read_checkpoint(m_config, no, lsn));
  EXPECT_EQ(no, 2U);
  EXPECT_EQ(lsn, 2000U);

  /* Checkpoint 2 is in slot 0: tear it. */
  {
    std::fstream f(m_dir / LogWalBackend::CHECKPOINT_FILE, std::ios::in | std::ios::out | std::ios::binary);
    f.seekp(9);
    f.put(char(0x55));
  }

  ASSERT_TRUE(LogWalBackend::read_checkpoint(m_config, no, lsn));
  EXPECT_EQ(no, 1U);
  EXPECT_EQ(lsn, 1000U);
}
