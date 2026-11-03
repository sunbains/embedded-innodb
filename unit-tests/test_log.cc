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

/** @file unit-tests/test_log.cc
Log: the LSN, the redo written per change, checkpoints, and the WAL file
under it (redo.wal): its block layout, the end of the log after a torn or
corrupt tail, and the space given back before a checkpoint.

LogWalTest drives a Log of an init()ed System directly, with a WAL in a
directory of its own and no tablespace: Log::init_wal_backend() starts it as
startup does. LogTest is a whole started instance. */

#include "ut0env.h"
#include "ut0perf.h"

#include "buf0buf.h"
#include "fil0fil.h"
#include "log0log.h"
#include "mtr0log.h"
#include "mtr0mtr.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <fstream>
#include <random>
#include <thread>
#include <vector>

namespace {

using LogTest = ut::DbTest;

/** Each change advances the LSN, by about the size of its redo. */
TEST_F(LogTest, LsnGrows) {
  auto &log = *m_sys->m_log;
  const auto start = log.get_lsn();
  auto lsn = start;

  constexpr uint32_t N = 40;

  for (uint32_t k = 0; k < N; ++k) {
    auto trx = begin();

    ASSERT_EQ(insert(trx, ut::Row{k, k, ut::blob(k, 1000)}), DB_SUCCESS);
    commit(trx);

    const auto now = log.get_lsn();

    EXPECT_GT(now, lsn);
    lsn = now;
  }

  /* The row, its undo and the commit, and now and then a page split that
  copies half a page: a few times the row on average. A reused
  mini-transaction wrote the redo of its previous use again. */
  EXPECT_LT((lsn - start) / N, 4000UL);
}


/** Size of a WAL block on disk. */
constexpr ulint BLOCK_SIZE = 4096;

/** Header of a WAL block: block number with the flush bit, checkpoint
number, first record group, data length; all in network byte order. */
constexpr ulint HDR_SIZE = 12;
constexpr ulint HDR_BLOCK_NO = 0;
constexpr ulint HDR_CHECKPOINT_NO = 4;
constexpr ulint HDR_LEN = 10;
constexpr uint32_t FLUSH_BIT = 0x80000000;

/** Log bytes in a block: the block less its header and the CRC32C trailer. */
constexpr ulint DATA_SIZE = BLOCK_SIZE - HDR_SIZE - 4;

/** CRC-32C (Castagnoli), bitwise, as a reference independent of the WAL code. */
uint32_t crc32c(const byte *p, size_t n) {
  uint32_t crc = 0xFFFFFFFF;

  while (n-- > 0) {
    crc ^= *p++;

    for (int k = 0; k < 8; ++k) {
      crc = (crc >> 1) ^ (0x82F63B78 & (0 - (crc & 1)));
    }
  }

  return ~crc;
}

/** @return the bytes of the file at path. */
std::vector<byte> read_file(const ut::fs::path &path) {
  std::ifstream f(path, std::ios::binary);

  return std::vector<byte>(std::istreambuf_iterator<char>(f), std::istreambuf_iterator<char>());
}

/** Overwrite n bytes at off in the file at path. */
void write_file(const ut::fs::path &path, off_t off, const void *data, size_t n) {
  const auto fd = ::open(path.c_str(), O_WRONLY);

  ASSERT_NE(fd, -1);
  ASSERT_EQ(::pwrite(fd, data, n, off), ssize_t(n));
  ::close(fd);
}

/** @return the allocated bytes of the file at path. */
off_t allocated(const ut::fs::path &path) {
  struct stat st {};

  EXPECT_EQ(::stat(path.c_str(), &st), 0);

  return off_t(st.st_blocks) * 512;
}

/** The bytes of data stored inside the file size, holes excluded. Unlike
st_blocks this does not count what the file system preallocates past the
end of a growing file (XFS speculative preallocation, up to the file size). */
off_t data_bytes(const ut::fs::path &path) {
  const auto fd = ::open(path.c_str(), O_RDONLY);

  EXPECT_NE(fd, -1);

  const auto size = ::lseek(fd, 0, SEEK_END);
  off_t total{};
  off_t off{};

  while (off < size) {
    const auto data = ::lseek(fd, off, SEEK_DATA);

    if (data == -1) {
      break;
    }

    auto hole = ::lseek(fd, data, SEEK_HOLE);

    if (hole == -1) {
      hole = size;
    }

    total += hole - data;
    off = hole;
  }

  ::close(fd);

  return total;
}

/** @return n bytes of a pattern that depends on seed and the position. */
std::vector<byte> pattern(ulint n, uint32_t seed) {
  std::vector<byte> v(n);
  uint32_t x = seed * 2654435761u + 7;

  for (auto &b : v) {
    x = x * 1103515245u + 12345u;
    b = byte(x >> 24);
  }

  return v;
}

/** The Log of an init()ed System with a running WAL in a directory of its
own; no tablespace, no buffer pool page is dirty. */
class LogWalTest : public ::testing::Test {
 protected:
  void SetUp() override {
    m_config = ut::make_config(m_dir.m_path);
    m_sys = std::make_unique<System>();

    ASSERT_EQ(m_sys->init(m_config), DB_SUCCESS);

    m_log = m_sys->m_log.get();
    m_log->init_wal_backend(m_log->wal_config());
    m_wal = m_log->m_wal_backend.get();
    m_start = m_log->get_lsn();
  }

  void TearDown() override {
    if (m_sys != nullptr) {
      m_log->shutdown();
      m_sys.reset();
    }
  }

  /** Append data as one mini-transaction does when its log fits in one
  block of its buffer. @return the end LSN */
  lsn_t append(const std::vector<byte> &data) {
    lsn_t start{};

    m_log->acquire();

    const auto end = m_log->reserve_and_write_fast(const_cast<byte *>(data.data()), data.size(), &start);

    m_log->release();

    EXPECT_EQ(end, start + data.size());

    return end;
  }

  /** Append data in pieces of piece bytes, as a big mini-transaction log. */
  lsn_t append_pieces(const std::vector<byte> &data, ulint piece) {
    const auto start = m_log->reserve_and_open(data.size());

    for (ulint off = 0; off < data.size(); off += piece) {
      m_log->write_low(data.data() + off, std::min(piece, data.size() - off));
    }

    const auto end = m_log->close();

    m_log->release();

    EXPECT_EQ(end, start + data.size());

    return end;
  }

  /** @return the log read from the WAL file from lsn; end is where it stopped. */
  std::vector<byte> read(lsn_t lsn, lsn_t &end) const {
    std::vector<byte> v;

    end = LogWalBackend::read(m_log->wal_config(), lsn, [&](const byte *data, ulint len) {
      v.insert(v.end(), data, data + len);
      return true;
    });

    return v;
  }

  /** @return the path of redo.wal. */
  ut::fs::path wal_path() const { return m_dir.m_path / "redo.wal"; }

  /** @return the file offset of the block that holds lsn. */
  static off_t block_off(lsn_t lsn) { return off_t(lsn / DATA_SIZE * BLOCK_SIZE); }

  ut::TestDir m_dir;
  Config m_config;
  std::unique_ptr<System> m_sys;
  Log *m_log{};
  LogWalBackend *m_wal{};
  lsn_t m_start{};
};

/** The WAL starts at a block boundary past the LSN of a new database, with
the first checkpoint, number 1, at its start. */
TEST_F(LogWalTest, StartAligned) {
  EXPECT_EQ(LogWalBackend::align_start_lsn(m_log->wal_config(), 1), DATA_SIZE);
  EXPECT_EQ(m_start % DATA_SIZE, 0UL);
  EXPECT_GE(m_start, 8192UL);
  EXPECT_EQ(m_wal->get_lsn(), m_start);
  EXPECT_EQ(m_log->m_last_checkpoint_lsn, m_start);

  lsn_t lsn{};
  uint64_t no{};

  ASSERT_TRUE(LogWalBackend::read_checkpoint(m_log->wal_config(), no, lsn));
  EXPECT_EQ(lsn, m_start);
  EXPECT_EQ(no, 1UL);
  EXPECT_EQ(m_log->m_next_checkpoint_no, 2UL);
}

/** align_start_lsn() rounds up to the next block boundary, and leaves one alone. */
TEST_F(LogWalTest, AlignStartLsn) {
  const auto config = m_log->wal_config();

  EXPECT_EQ(LogWalBackend::align_start_lsn(config, 0), 0UL);
  EXPECT_EQ(LogWalBackend::align_start_lsn(config, DATA_SIZE), DATA_SIZE);
  EXPECT_EQ(LogWalBackend::align_start_lsn(config, DATA_SIZE + 1), 2 * DATA_SIZE);
  EXPECT_EQ(LogWalBackend::align_start_lsn(config, 5 * DATA_SIZE - 1), 5 * DATA_SIZE);
}

/** reserve_and_write_fast() starts at the current LSN and moves it by the
length: the LSN counts log bytes, the block headers are not in it. */
TEST_F(LogWalTest, FastAppendLsn) {
  auto lsn = m_start;

  for (ulint len : {1UL, 7UL, 100UL, DATA_SIZE - 108, DATA_SIZE, 3 * DATA_SIZE + 5, 20000UL}) {
    lsn_t start{};
    auto data = pattern(len, uint32_t(len));

    m_log->acquire();

    const auto end = m_log->reserve_and_write_fast(data.data(), len, &start);

    m_log->release();

    EXPECT_EQ(start, lsn);
    EXPECT_EQ(end, lsn + len);
    EXPECT_EQ(m_log->get_lsn(), end);
    EXPECT_EQ(m_wal->get_lsn(), end);

    lsn = end;
  }
}

/** reserve_and_open() takes the log mutex and returns the start LSN;
write_low() pieces are appended at close() as one record group. */
TEST_F(LogWalTest, OpenWriteClose) {
  const auto data = pattern(10000, 1);
  const auto start = m_log->reserve_and_open(data.size());

#ifdef UNIV_DEBUG
  EXPECT_TRUE(mutex_own(&m_log->m_mutex));
#endif /* UNIV_DEBUG */

  EXPECT_EQ(start, m_start);

  m_log->write_low(data.data(), 3000);

  /* Nothing is appended before close(). */
  EXPECT_EQ(m_log->get_lsn(), m_start);

  m_log->write_low(data.data() + 3000, 7000);

  EXPECT_EQ(m_log->close(), m_start + data.size());
  EXPECT_EQ(m_log->get_lsn(), m_start + data.size());

  m_log->release();

  m_log->buffer_flush_to_disk();

  lsn_t end{};

  EXPECT_EQ(read(m_start, end), data);
  EXPECT_EQ(end, m_start + data.size());
}

/** An open with nothing written closes at the same LSN. */
TEST_F(LogWalTest, OpenCloseEmpty) {
  (void)m_log->reserve_and_open(0);
  EXPECT_EQ(m_log->close(), m_start);
  m_log->release();

  EXPECT_EQ(m_log->get_lsn(), m_start);
}

/** A record group bigger than one append of the WAL (MAX_APPEND) stays
contiguous. */
TEST_F(LogWalTest, BigGroup) {
  const auto data = pattern(5 * LogWalBackend::MAX_APPEND + 17, 2);

  EXPECT_EQ(append_pieces(data, 1000), m_start + data.size());

  m_log->buffer_flush_to_disk();

  lsn_t end{};

  EXPECT_EQ(read(m_start, end), data);
}

/** Both append paths mixed, flushed at random points: the WAL holds exactly
the bytes appended, in order, readable from any LSN. */
TEST_F(LogWalTest, MixedReadBack) {
  std::mt19937 rng(11);
  std::vector<byte> all;

  for (int i = 0; i < 400; ++i) {
    const auto data = pattern(1 + rng() % (rng() % 10 == 0 ? 20000 : 300), uint32_t(i));

    if (rng() % 3 == 0) {
      (void)append_pieces(data, 1 + rng() % 500);
    } else {
      (void)append(data);
    }

    all.insert(all.end(), data.begin(), data.end());

    if (rng() % 20 == 0) {
      m_log->write_up_to(m_log->get_lsn(), rng() % 2 == 0);
    }
  }

  m_log->buffer_flush_to_disk();

  EXPECT_EQ(m_log->get_lsn(), m_start + all.size());

  lsn_t end{};

  EXPECT_EQ(read(m_start, end), all);
  EXPECT_EQ(end, m_log->get_lsn());

  for (lsn_t skip : {lsn_t(1), lsn_t(DATA_SIZE - 1), lsn_t(DATA_SIZE), lsn_t(all.size() / 2), lsn_t(all.size() - 1)}) {
    EXPECT_EQ(read(m_start + skip, end), std::vector<byte>(all.begin() + ptrdiff_t(skip), all.end())) << skip;
  }
}

/** write_up_to() without sync writes, with sync makes durable; asking again
for what is already durable does no I/O. */
TEST_F(LogWalTest, WriteUpTo) {
  const auto lsn = append(pattern(5000, 3));

  EXPECT_LT(m_wal->get_flushed_lsn(), lsn);
  EXPECT_LT(m_wal->get_synced_lsn(), lsn);

  const auto syncs = m_wal->m_stats.m_n_syncs.load();

  m_log->write_up_to(lsn, false);

  EXPECT_GE(m_wal->get_flushed_lsn(), lsn);
  EXPECT_LT(m_wal->get_synced_lsn(), lsn);
  EXPECT_EQ(m_wal->m_stats.m_n_syncs.load(), syncs);

  m_log->write_up_to(lsn, true);

  EXPECT_GE(m_wal->get_synced_lsn(), lsn);
  EXPECT_EQ(m_wal->m_stats.m_n_syncs.load(), syncs + 1);

  const auto writes = m_wal->m_stats.m_n_writes.load();

  m_log->write_up_to(lsn, true);
  m_log->write_up_to(lsn - 100, true);
  m_log->write_up_to(lsn, false);

  EXPECT_EQ(m_wal->m_stats.m_n_syncs.load(), syncs + 1);
  EXPECT_EQ(m_wal->m_stats.m_n_writes.load(), writes);
}

/** A write up to an LSN in the middle of the log writes all of it: the
target is the end of what was appended. */
TEST_F(LogWalTest, WriteUpToMiddle) {
  const auto mid = append(pattern(3000, 4));
  const auto end_lsn = append(pattern(3000, 5));

  m_log->write_up_to(mid, true);

  EXPECT_GE(m_wal->get_synced_lsn(), mid);
  EXPECT_LE(m_wal->get_synced_lsn(), end_lsn);

  lsn_t end{};

  EXPECT_GE(read(m_start, end).size(), mid - m_start);
}

/** Log appended but not written is not in the file. */
TEST_F(LogWalTest, NotWrittenNotRead) {
  (void)append(pattern(500, 6));

  lsn_t end{};

  EXPECT_TRUE(read(m_start, end).empty());
  EXPECT_EQ(end, m_start);

  m_log->write_up_to(m_log->get_lsn(), false);

  EXPECT_EQ(read(m_start, end).size(), 500UL);
}

/** Durable log survives the Log: a new reader sees it after shutdown. */
TEST_F(LogWalTest, ShutdownWritesAll) {
  const auto data = pattern(12345, 7);

  (void)append(data);

  m_log->shutdown();

  lsn_t end{};

  EXPECT_EQ(read(m_start, end), data);
  EXPECT_EQ(end, m_start + data.size());

  m_sys.reset();
}

/** Each checkpoint has the next number; the checkpoint file has two slots
written alternately: the latest one wins. */
TEST_F(LogWalTest, CheckpointNumbers) {
  const auto config = m_log->wal_config();
  uint64_t prev{1};

  for (int i = 0; i < 5; ++i) {
    const auto lsn = append(pattern(100, uint32_t(i)));

    ASSERT_TRUE(m_log->write_checkpoint(lsn));

    lsn_t ckpt{};
    uint64_t no{};

    ASSERT_TRUE(LogWalBackend::read_checkpoint(config, no, ckpt));
    EXPECT_EQ(no, prev + 1);
    EXPECT_EQ(ckpt, lsn);
    EXPECT_EQ(m_log->m_last_checkpoint_lsn, lsn);

    /* The slot of number no is at (no % 2) * 512, both stay valid. */
    const auto file = read_file(m_dir.m_path / LogWalBackend::CHECKPOINT_FILE);

    ASSERT_EQ(file.size(), 1024UL);
    EXPECT_EQ(mach_read_from_8(file.data() + (no % 2) * 512), no);
    EXPECT_EQ(mach_read_from_8(file.data() + (no % 2) * 512 + 8), lsn);
    EXPECT_EQ(mach_read_from_8(file.data() + ((no + 1) % 2) * 512), no - 1);

    prev = no;
  }
}

/** read_checkpoint() sets the number of the next checkpoint and the last
checkpoint LSN from the file. */
TEST_F(LogWalTest, ReadCheckpoint) {
  const auto lsn = append(pattern(100, 8));

  ASSERT_TRUE(m_log->write_checkpoint(lsn));

  m_log->m_next_checkpoint_no = 0;
  m_log->m_last_checkpoint_lsn = 0;

  lsn_t ckpt{};
  uint64_t no{};

  ASSERT_TRUE(m_log->read_checkpoint(ckpt, no));
  EXPECT_EQ(ckpt, lsn);
  EXPECT_EQ(no, 2UL);
  EXPECT_EQ(m_log->m_next_checkpoint_no, 3UL);
  EXPECT_EQ(m_log->m_last_checkpoint_lsn, lsn);
}

/** A torn write of the newer slot leaves the older checkpoint. */
TEST_F(LogWalTest, CheckpointSlotTorn) {
  const auto lsn = append(pattern(100, 9));

  ASSERT_TRUE(m_log->write_checkpoint(lsn));

  /* Checkpoint 2 is in slot 0: flip a byte of its LSN. */
  const byte x = 0x5A;

  write_file(m_dir.m_path / LogWalBackend::CHECKPOINT_FILE, 12, &x, 1);

  lsn_t ckpt{};
  uint64_t no{};

  ASSERT_TRUE(m_log->read_checkpoint(ckpt, no));
  EXPECT_EQ(no, 1UL);
  EXPECT_EQ(ckpt, m_start);
}

/** Log::checkpoint() with no dirty page moves the checkpoint to the current
LSN; without write_always it does not write one that would not move. */
TEST_F(LogWalTest, CheckpointCleanPool) {
  const auto lsn = append(pattern(300, 10));

  ASSERT_TRUE(m_log->checkpoint(false));
  EXPECT_EQ(m_log->m_last_checkpoint_lsn, lsn);
  EXPECT_EQ(m_log->m_next_checkpoint_no, 3UL);

  /* Nothing new: no checkpoint written. */
  ASSERT_TRUE(m_log->checkpoint(false));
  EXPECT_EQ(m_log->m_next_checkpoint_no, 3UL);

  /* Forced: written at the same LSN. */
  ASSERT_TRUE(m_log->checkpoint(true));
  EXPECT_EQ(m_log->m_next_checkpoint_no, 4UL);
  EXPECT_EQ(m_log->m_last_checkpoint_lsn, lsn);
}

/** write_checkpoint() makes the log up to the checkpoint durable first. */
TEST_F(LogWalTest, CheckpointSyncsLog) {
  const auto lsn = append(pattern(3000, 11));

  ASSERT_LT(m_wal->get_synced_lsn(), lsn);
  ASSERT_TRUE(m_log->write_checkpoint(lsn));
  EXPECT_GE(m_wal->get_synced_lsn(), lsn);
}

/** The block layout on disk: block n at n * 4096; a 12 byte header in
network byte order (block number, checkpoint number, first record group,
data length), 4080 bytes of log, the CRC32C of the data area in host byte
order. */
TEST_F(LogWalTest, BlockLayout) {
  const auto data = pattern(2 * DATA_SIZE + 1000, 12);

  (void)append(data);
  m_log->buffer_flush_to_disk();

  const auto file = read_file(wal_path());
  const auto first = m_start / DATA_SIZE;

  ASSERT_GE(file.size(), (first + 3) * BLOCK_SIZE);

  for (ulint i = 0; i < 3; ++i) {
    const auto block = file.data() + (first + i) * BLOCK_SIZE;
    const auto block_no = uint32_t(mach_read_from_4(block + HDR_BLOCK_NO));
    const auto len = mach_read_from_2(block + HDR_LEN);

    EXPECT_EQ(block_no & ~FLUSH_BIT, first + i) << i;
    EXPECT_EQ(len, i < 2 ? DATA_SIZE : 1000UL) << i;
    EXPECT_EQ(memcmp(block + HDR_SIZE, data.data() + i * DATA_SIZE, len), 0) << i;

    uint32_t crc;

    memcpy(&crc, block + HDR_SIZE + DATA_SIZE, sizeof(crc));
    EXPECT_EQ(crc, crc32c(block + HDR_SIZE, DATA_SIZE)) << i;
  }

  /* The last block of a write carries the flush bit. */
  EXPECT_NE(mach_read_from_4(file.data() + (first + 2) * BLOCK_SIZE) & FLUSH_BIT, 0UL);

  /* Nothing past the last block. */
  EXPECT_EQ(file.size(), (first + 3) * BLOCK_SIZE);
}

/** The last, partial block is written again, whole, as it fills up. */
TEST_F(LogWalTest, PartialBlockRewrite) {
  const auto a = pattern(100, 13);
  const auto b = pattern(200, 14);

  (void)append(a);
  m_log->buffer_flush_to_disk();

  auto file = read_file(wal_path());
  const auto off = block_off(m_start);

  EXPECT_EQ(mach_read_from_2(file.data() + off + HDR_LEN), 100UL);

  (void)append(b);
  m_log->buffer_flush_to_disk();

  file = read_file(wal_path());

  EXPECT_EQ(mach_read_from_2(file.data() + off + HDR_LEN), 300UL);
  EXPECT_EQ(memcmp(file.data() + off + HDR_SIZE, a.data(), 100), 0);
  EXPECT_EQ(memcmp(file.data() + off + HDR_SIZE + 100, b.data(), 200), 0);

  lsn_t end{};
  auto all = a;

  all.insert(all.end(), b.begin(), b.end());
  EXPECT_EQ(read(m_start, end), all);
}

/** Three full blocks and a partial one on disk, for the torn tail tests.
@return the bytes */
std::vector<byte> four_blocks() {
  return pattern(3 * DATA_SIZE + 500, 15);
}

/** A block whose data area does not match its CRC ends the log before it:
the blocks after it are not read either. */
TEST_F(LogWalTest, TornTailCrc) {
  const auto data = four_blocks();

  (void)append(data);
  m_log->shutdown();

  /* One bit in the data of the third block. */
  const auto off = block_off(m_start) + 2 * BLOCK_SIZE + HDR_SIZE + 77;
  auto file = read_file(wal_path());
  const byte flipped = file[off] ^ 0x01;

  write_file(wal_path(), off, &flipped, 1);

  lsn_t end{};

  EXPECT_EQ(read(m_start, end), std::vector<byte>(data.begin(), data.begin() + 2 * DATA_SIZE));
  EXPECT_EQ(end, m_start + 2 * DATA_SIZE);

  m_sys.reset();
}

/** A corrupt CRC trailer, or bytes past the data length of the last block
(they are covered by the CRC), end the log too. */
TEST_F(LogWalTest, TornTailTrailer) {
  const auto data = four_blocks();

  (void)append(data);
  m_log->shutdown();

  const auto last = block_off(m_start) + 3 * BLOCK_SIZE;
  const byte junk = 0xAA;

  write_file(wal_path(), last + HDR_SIZE + 600, &junk, 1);

  lsn_t end{};

  EXPECT_EQ(read(m_start, end).size(), 3 * DATA_SIZE);

  write_file(wal_path(), last - 1, &junk, 1);

  EXPECT_EQ(read(m_start, end).size(), 2 * DATA_SIZE);

  m_sys.reset();
}

/** A file cut in the middle of a block ends the log at the last whole block. */
TEST_F(LogWalTest, TornTailTruncated) {
  const auto data = four_blocks();

  (void)append(data);
  m_log->shutdown();

  ut::fs::resize_file(wal_path(), block_off(m_start) + 3 * BLOCK_SIZE + 100);

  lsn_t end{};

  EXPECT_EQ(read(m_start, end), std::vector<byte>(data.begin(), data.begin() + 3 * DATA_SIZE));
  EXPECT_EQ(end, m_start + 3 * DATA_SIZE);

  ut::fs::resize_file(wal_path(), block_off(m_start) + BLOCK_SIZE - 1);

  EXPECT_TRUE(read(m_start, end).empty());
  EXPECT_EQ(end, m_start);

  m_sys.reset();
}

/** A block with the number of another block (a stale or misdirected write),
or a zeroed header, ends the log. */
TEST_F(LogWalTest, TornTailBlockNo) {
  const auto data = four_blocks();

  (void)append(data);
  m_log->shutdown();

  const auto file = read_file(wal_path());
  const auto b0 = block_off(m_start);

  /* Block 0 copied over block 1: a valid block at the wrong place. */
  write_file(wal_path(), b0 + BLOCK_SIZE, file.data() + b0, BLOCK_SIZE);

  lsn_t end{};

  EXPECT_EQ(read(m_start, end).size(), DATA_SIZE);

  /* A zeroed first block: no log at all. */
  const std::vector<byte> zeros(BLOCK_SIZE);

  write_file(wal_path(), b0, zeros.data(), zeros.size());

  EXPECT_TRUE(read(m_start, end).empty());
  EXPECT_EQ(end, m_start);

  m_sys.reset();
}

/** A data length that is impossible (more than a block holds) is corruption. */
TEST_F(LogWalTest, TornTailBadLength) {
  const auto data = four_blocks();

  (void)append(data);
  m_log->shutdown();

  byte len[2];

  mach_write_to_2(len, DATA_SIZE + 1);
  write_file(wal_path(), block_off(m_start) + BLOCK_SIZE + HDR_LEN, len, 2);

  lsn_t end{};

  EXPECT_EQ(read(m_start, end).size(), DATA_SIZE);

  m_sys.reset();
}

/** Garbage written after the end of the log is not log. */
TEST_F(LogWalTest, GarbageAfterEnd) {
  const auto data = pattern(DATA_SIZE + 10, 16);

  (void)append(data);
  m_log->shutdown();

  const auto junk = pattern(4 * BLOCK_SIZE, 17);

  write_file(wal_path(), block_off(m_start) + 2 * BLOCK_SIZE, junk.data(), junk.size());

  lsn_t end{};

  EXPECT_EQ(read(m_start, end), data);
  EXPECT_EQ(end, m_start + data.size());

  m_sys.reset();
}

/** A checkpoint gives back the space of the WAL before the block that holds
it: the file keeps its size, those blocks read as zeros, the log from the
checkpoint on is intact. */
TEST_F(LogWalTest, PunchHole) {
  const auto data = pattern(512 * DATA_SIZE, 18);

  (void)append(data);
  m_log->buffer_flush_to_disk();

  const auto size = ut::fs::file_size(wal_path());
  const auto before = allocated(wal_path());

  /* In the middle of a block. */
  const auto ckpt = m_start + 400 * DATA_SIZE + 123;

  ASSERT_TRUE(m_log->write_checkpoint(ckpt));

  EXPECT_EQ(ut::fs::file_size(wal_path()), size);
  EXPECT_LT(allocated(wal_path()), before - off_t(300 * BLOCK_SIZE));

  const auto file = read_file(wal_path());

  /* Everything before the checkpoint's block is a hole. */
  EXPECT_TRUE(std::all_of(file.begin(), file.begin() + block_off(ckpt), [](byte b) { return b == 0; }));

  /* The block that holds the checkpoint stays: recovery reads from it. */
  EXPECT_EQ(mach_read_from_4(file.data() + block_off(ckpt)) & ~FLUSH_BIT, ckpt / DATA_SIZE);

  lsn_t end{};

  EXPECT_EQ(read(ckpt, end), std::vector<byte>(data.begin() + ptrdiff_t(ckpt - m_start), data.end()));
  EXPECT_EQ(end, m_start + data.size());

  /* The discarded log reads as no log. */
  EXPECT_TRUE(read(m_start, end).empty());
}

/** Successive checkpoints punch only the new range; an older checkpoint
LSN gives back nothing. */
TEST_F(LogWalTest, PunchHoleIncremental) {
  const auto data = pattern(300 * DATA_SIZE, 19);

  (void)append(data);
  m_log->buffer_flush_to_disk();

  const auto full = allocated(wal_path());

  ASSERT_TRUE(m_log->write_checkpoint(m_start + 100 * DATA_SIZE));

  const auto after1 = allocated(wal_path());

  ASSERT_TRUE(m_log->write_checkpoint(m_start + 200 * DATA_SIZE));

  const auto after2 = allocated(wal_path());

  EXPECT_LT(after1, full);
  EXPECT_LT(after2, after1);

  /* A checkpoint behind the last one: the WAL does not move back. */
  m_wal->discard_up_to(m_start + 50 * DATA_SIZE);
  EXPECT_EQ(allocated(wal_path()), after2);

  lsn_t end{};

  EXPECT_EQ(read(m_start + 200 * DATA_SIZE, end).size(), 100 * DATA_SIZE);
}


/* Regression: the log of a run that recovery did not use is read back as part of
the next run's log. After a crash, recovery stops at the end of the last
complete record group, recv_recovered_lsn; the blocks after it may still be
valid (the first part of a multi-block group written when a log buffer
filled, or blocks after a torn one). Log::init_wal_backend() (log0log.cc:197)
starts the new WAL at align_start_lsn(recovered_lsn) in the same file and
LogWalBackend::open() (log0wal.cc:115) keeps the file as it is: the old blocks
after the new start keep their block numbers and checksums.
LogWalBackend::read() accepts any block with the expected number and a
good CRC (the checkpoint number field of the header is not set nor
checked), so when the new log ends at a block boundary the next recovery
reads on into the old blocks and parses them as new log: a corrupt log at
best, stale changes applied at worst. Upstream truncated the log after the
recovered LSN (recv_truncate_group()). Here: the old log is 4 blocks and a
bit, recovery stopped 100 bytes into it, the new log fills one block. */
TEST_F(LogWalTest, RestartInsideOldLog) {
  const auto old_log = pattern(4 * DATA_SIZE + 100, 21);

  (void)append(old_log);
  m_log->shutdown();

  /* Recovery found the log complete up to m_start + 100: startup continues
  the log there, as recv_recovery_from_checkpoint_start() and start_wal() do. */
  m_log->m_lsn = m_start + 100;
  m_log->init_wal_backend(m_log->wal_config());

  const auto new_start = m_log->get_lsn();
  const auto new_log = pattern(DATA_SIZE, 22);

  ASSERT_EQ(new_start, m_start + DATA_SIZE);

  (void)append(new_log);
  m_log->buffer_flush_to_disk();

  lsn_t end{};
  const auto log = read(new_start, end);

  EXPECT_EQ(log.size(), new_log.size());
  EXPECT_TRUE(log == new_log);
  EXPECT_EQ(end, new_start + DATA_SIZE);
}

/** Threads appending framed records, each under the log mutex: the log is
the records one after the other, none torn or interleaved, and the LSN is
the sum of their lengths. */
TEST_F(LogWalTest, ThreadsAppend) {
  constexpr int THREADS = 8;
  constexpr int N = 500;
  std::vector<std::thread> threads;
  std::atomic<ulint> total{};

  for (int t = 0; t < THREADS; ++t) {
    threads.emplace_back([&, t] {
      std::mt19937 rng(t);

      for (int i = 0; i < N; ++i) {
        /* thread, sequence number, length, then length bytes of t. */
        const ulint len = 1 + rng() % 600;
        std::vector<byte> rec(8 + len, byte(t));

        mach_write_to_2(rec.data(), t);
        mach_write_to_4(rec.data() + 2, i);
        mach_write_to_2(rec.data() + 6, len);

        (void)append(rec);
        total += rec.size();
      }
    });
  }

  for (auto &t : threads) {
    t.join();
  }

  EXPECT_EQ(m_log->get_lsn(), m_start + total);

  m_log->buffer_flush_to_disk();

  lsn_t end{};
  const auto log = read(m_start, end);

  ASSERT_EQ(log.size(), total.load());

  std::array<int, THREADS> next{};
  ulint off{};

  while (off < log.size()) {
    const auto t = mach_read_from_2(log.data() + off);
    const auto seq = mach_read_from_4(log.data() + off + 2);
    const auto len = mach_read_from_2(log.data() + off + 6);

    ASSERT_LT(t, ulint(THREADS));
    ASSERT_EQ(seq, ulint(next[t])) << "thread " << t;
    const auto first = log.begin() + ptrdiff_t(off + 8);

    ASSERT_TRUE(std::all_of(first, first + ptrdiff_t(len), [&](byte b) { return b == t; }));

    ++next[t];
    off += 8 + len;
  }

  for (auto n : next) {
    EXPECT_EQ(n, N);
  }
}

/** A mini-transaction that changed a page appends its framed log at
commit: the LSN moves by its size, m_start_lsn/m_end_lsn bound it, and the
WAL holds exactly those bytes. */
TEST_F(LogWalTest, MtrCommit) {
  auto block = m_sys->m_buf_pool->block_alloc();
  auto frame = block->get_frame();

  memset(frame, 0, UNIV_PAGE_SIZE);
  mach_write_to_4(frame + FIL_PAGE_SPACE_ID, 3);
  mach_write_to_4(frame + FIL_PAGE_OFFSET, 77);
  mach_write_to_2(frame + FIL_PAGE_TYPE, FIL_PAGE_TYPE_SYS);

  std::vector<byte> expected;

  for (int n : {1, 3, 200}) {
    Mtr mtr;

    mtr.start();
    mtr.memo_push(block, MTR_MEMO_PAGE_X_FIX);

    for (int i = 0; i < n; ++i) {
      mlog_write_ulint(frame + FIL_PAGE_DATA + 4 * i, 0x10000 + i, MLOG_4BYTES, &mtr);
    }

    const auto size = mtr.m_log.get_data_size() + (n > 1 ? 1 : 0);
    const auto before = m_log->get_lsn();

    /* The page is not really in the buffer pool: drop the memo by hand. */
    mtr.m_memo.clear();
    mtr.commit(*m_sys);

    EXPECT_EQ(mtr.m_start_lsn, before) << n;
    EXPECT_EQ(mtr.m_end_lsn, before + size) << n;
    EXPECT_EQ(m_log->get_lsn(), before + size) << n;

    for (auto b = mtr.m_log.get_first_block(); b != nullptr; b = mtr.m_log.get_next_block(b)) {
      expected.insert(expected.end(), mtr.m_log.get_data(b), mtr.m_log.get_data(b) + mtr.m_log.get_used(b));
    }
  }

  m_log->buffer_flush_to_disk();

  lsn_t end{};

  EXPECT_EQ(read(m_start, end), expected);

  m_sys->m_buf_pool->block_free(block);
}

/** A mini-transaction that logs nothing (MTR_LOG_NONE, or no change)
does not move the LSN. */
TEST_F(LogWalTest, MtrNoLog) {
  auto block = m_sys->m_buf_pool->block_alloc();
  auto frame = block->get_frame();

  mach_write_to_4(frame + FIL_PAGE_SPACE_ID, 3);
  mach_write_to_4(frame + FIL_PAGE_OFFSET, 78);

  Mtr mtr;

  mtr.start();
  mtr.memo_push(block, MTR_MEMO_PAGE_X_FIX);
  (void)mtr.set_log_mode(MTR_LOG_NONE);
  mlog_write_ulint(frame + FIL_PAGE_DATA, 5, MLOG_4BYTES, &mtr);
  mtr.m_memo.clear();
  mtr.commit(*m_sys);

  EXPECT_EQ(m_log->get_lsn(), m_start);

  mtr.start();
  mtr.commit(*m_sys);

  EXPECT_EQ(m_log->get_lsn(), m_start);

  m_sys->m_buf_pool->block_free(block);
}

/** peek_lsn() fails while another thread holds the log mutex. */
TEST_F(LogWalTest, PeekLsn) {
  lsn_t lsn{};

  EXPECT_TRUE(m_log->peek_lsn(&lsn));
  EXPECT_EQ(lsn, m_start);

  m_log->acquire();

  std::thread([&] { EXPECT_FALSE(m_log->peek_lsn(&lsn)); }).join();

  m_log->release();
}

/** A commit is durable when it returns (m_flush_at_trx_commit = 1): the
WAL is synced up to the LSN of the commit. */
TEST_F(LogTest, CommitDurable) {
  auto &log = *m_sys->m_log;

  for (uint32_t k = 0; k < 20; ++k) {
    auto trx = begin();

    ASSERT_EQ(insert(trx, ut::Row{k, k, "d"}), DB_SUCCESS);
    commit(trx);

    EXPECT_GE(log.m_wal_backend->get_synced_lsn(), log.get_lsn() - 1000) << k;
  }

  /* All of it, by now, at the latest at the next write. */
  log.buffer_flush_to_disk();
  EXPECT_EQ(log.m_wal_backend->get_synced_lsn(), log.m_wal_backend->get_lsn());
}

/** A checkpoint after flushing every page is at the end of the log, and the
WAL before it is given back. */
TEST_F(LogTest, CheckpointAtEnd) {
  auto &log = *m_sys->m_log;
  ut::Rows rows;

  for (uint32_t k = 0; k < 300; ++k) {
    rows[k] = ut::Row{k, k, ut::blob(k, 3000)};
  }

  load(rows);

  const auto path = m_dir.m_path / "redo.wal";
  const auto lsn = log.get_lsn();

  log.make_checkpoint_at(IB_UINT64_T_MAX, true);

  lsn_t ckpt{};
  uint64_t no{};

  ASSERT_TRUE(log.read_checkpoint(ckpt, no));
  EXPECT_GE(ckpt, lsn);
  EXPECT_LE(ckpt, log.get_lsn());

  /* Close to a megabyte of log, all before the checkpoint: it reads as
  zeros and most of it takes no space. Count the data inside the file
  size: st_blocks also counts what XFS preallocates past the end. */
  const auto size = off_t(ut::fs::file_size(path));
  const auto hole = off_t(ckpt / DATA_SIZE * BLOCK_SIZE);
  const auto file = read_file(path);

  EXPECT_GT(hole, 500000);
  EXPECT_TRUE(std::all_of(file.begin(), file.begin() + hole, [](byte b) { return b == 0; }));
  EXPECT_LT(data_bytes(path), size - hole / 2);

  expect_rows(rows);
}

/** A checkpoint is written and read back. */
TEST_F(LogTest, Checkpoint) {
  auto &log = *m_sys->m_log;

  load({{1, ut::Row{1, 1, "x"}}});

  log.buffer_flush_to_disk();
  (void)log.checkpoint(true);

  lsn_t lsn{};
  uint64_t no{};

  ASSERT_TRUE(log.read_checkpoint(lsn, no));
  EXPECT_GT(lsn, 0UL);
  EXPECT_LE(lsn, log.get_lsn());

  const auto first = no;

  load({{2, ut::Row{2, 2, "y"}}});
  log.buffer_flush_to_disk();
  (void)log.checkpoint(true);

  ASSERT_TRUE(log.read_checkpoint(lsn, no));
  EXPECT_GT(no, first);
}

/** The LSN continues after a restart. */
TEST_F(LogTest, LsnAfterRestart) {
  load({{1, ut::Row{1, 1, ut::blob(1, 50000)}}});

  const auto before = m_sys->m_log->get_lsn();

  restart();

  EXPECT_GE(m_sys->m_log->get_lsn(), before);
  expect_rows({{1, ut::Row{1, 1, ut::blob(1, 50000)}}});
}

/** More redo than the checkpoint age: the log keeps up and the space
before the checkpoint is given back. */
TEST_F(LogTest, ManyCheckpoints) {
  ut::Rows rows;

  for (uint32_t k = 0; k < 200; ++k) {
    rows[k] = ut::Row{k, k, ut::blob(k, 20000)};
  }

  load(rows);

  for (int round = 0; round < 10; ++round) {
    auto trx = begin();

    for (auto &[k, row] : rows) {
      row.m_b = ut::blob(k + round, 20000);
      ASSERT_EQ(update(trx, k, row), DB_SUCCESS);
    }

    commit(trx);
  }

  expect_rows(rows);
  restart();
  expect_rows(rows);
}

}  // namespace

namespace {

using LogPerf = LogWalTest;

/** reserve_and_write_fast() of 64 and 512 byte groups under the log mutex. */
TEST_F(LogPerf, ReserveFast) {
  for (ulint len : {64UL, 512UL}) {
    auto data = pattern(len, 1);
    lsn_t start{};

    auto r = ut::bench(std::format("reserve_write_fast_{}", len), 2000000 / (len / 64), [&](uint64_t) {
      m_log->acquire();
      ut::keep(m_log->reserve_and_write_fast(data.data(), len, &start));
      m_log->release();
    });

    EXPECT_LT(r.ns_per_op(), 100000.0);
  }
}

/** reserve_and_open(), three write_low() pieces, close(). */
TEST_F(LogPerf, OpenWriteClose) {
  auto data = pattern(3 * 600, 2);

  auto r = ut::bench("open_3_pieces_close_1800", 300000, [&](uint64_t) {
    (void)m_log->reserve_and_open(data.size());

    for (int i = 0; i < 3; ++i) {
      m_log->write_low(data.data() + 600 * i, 600);
    }

    ut::keep(m_log->close());
    m_log->release();
  });

  EXPECT_LT(r.ns_per_op(), 200000.0);
}

/** 1 to 16 threads appending 128 byte groups: the log mutex serializes them. */
TEST_F(LogPerf, ReserveThreads) {
  auto data = pattern(128, 3);

  for (int n : {1, 2, 4, 8, 16}) {
    auto r = ut::bench_mt(std::format("reserve_write_fast_128_t{}", n), n, 100000, [&](int, uint64_t) {
      lsn_t start{};

      m_log->acquire();
      ut::keep(m_log->reserve_and_write_fast(data.data(), data.size(), &start));
      m_log->release();
    });

    EXPECT_LT(r.ns_per_op(), 200000.0);
  }
}

/** Throughput of the log in MB/s: appends of 4 KB, written without sync. */
TEST_F(LogPerf, WriteThroughput) {
  auto data = pattern(4096, 4);

  auto r = ut::bench("append_4k_write_every_64", 50000, [&](uint64_t i) {
    lsn_t start{};

    m_log->acquire();
    ut::keep(m_log->reserve_and_write_fast(data.data(), data.size(), &start));
    m_log->release();

    if (i % 64 == 63) {
      m_log->write_up_to(m_log->get_lsn(), false);
    }
  });

  std::printf("PERF LogPerf.WriteThroughput MB/s=%.1f\n", 4096.0 / r.ns_per_op() * 1e9 / 1e6);
  EXPECT_LT(r.ns_per_op(), 1000000.0);
}

/** write_up_to() with sync after each small append: the latency of a
durable commit, an fdatasync() of the WAL. */
TEST_F(LogPerf, WriteUpToSync) {
  auto data = pattern(200, 5);

  auto r = ut::bench("append_200_write_up_to_sync", 200, [&](uint64_t) {
    const auto lsn = append(data);

    m_log->write_up_to(lsn, true);
  });

  EXPECT_LT(r.ns_per_op(), 100e6);
}

/** Threads committing durably at once. */
TEST_F(LogPerf, WriteUpToSyncThreads) {
  auto data = pattern(200, 6);

  for (int n : {1, 4, 16}) {
    const auto syncs = m_wal->m_stats.m_n_syncs.load();

    auto r = ut::bench_mt(std::format("append_200_sync_t{}", n), n, 400 / n, [&](int, uint64_t) {
      const auto lsn = append(data);

      m_log->write_up_to(lsn, true);
    });

    const auto per_commit = double(m_wal->m_stats.m_n_syncs.load() - syncs) / double(r.m_ops);

    std::printf("PERF LogPerf.WriteUpToSyncThreads t%d syncs/commit=%.2f\n", n, per_commit);
    EXPECT_LT(r.ns_per_op(), 100e6);
  }
}

/* Regression: no group commit. Durable commits of concurrent threads each pay
an fdatasync() of their own, and waiters starve. LogWalBackend::flush()
(log/log0wal.cc:176) holds m_flush_mutex across the write and the fdatasync().
A thread whose log a sync in progress already covers still waits for the
mutex, and std::mutex has no hand-off: the thread that just synced appends
again, takes the mutex back first, and syncs again for its own new log, over
and over, while the covered waiters sit blocked (a trace of 8 threads: one
thread did 10 syncs in a row, each covering everybody's log, while 7 waited
11 ms). With 4 and 16 threads 0.98-0.99 syncs per commit. The fix: wait on a
condition variable for "synced >= my lsn or nobody flushing" instead of on
the mutex, so a finished sync releases every waiter it covered. */
TEST_F(LogPerf, GroupCommit) {
  auto data = pattern(200, 7);
  const auto syncs = m_wal->m_stats.m_n_syncs.load();

  auto r = ut::bench_mt("group_commit_t8", 8, 50, [&](int, uint64_t) {
    const auto lsn = append(data);

    m_log->write_up_to(lsn, true);
  });

  const auto per_commit = double(m_wal->m_stats.m_n_syncs.load() - syncs) / double(r.m_ops);

  std::printf("PERF LogPerf.GroupCommit syncs/commit=%.2f\n", per_commit);

  /* 8 committers: a sync covers about 8 commits; 0.5 is generous. */
  EXPECT_LT(per_commit, 0.5);
}

/** Mini-transaction commit with 10 records into the WAL. */
TEST_F(LogPerf, MtrCommit) {
  auto block = m_sys->m_buf_pool->block_alloc();
  auto frame = block->get_frame();

  mach_write_to_4(frame + FIL_PAGE_SPACE_ID, 3);
  mach_write_to_4(frame + FIL_PAGE_OFFSET, 99);

  auto r = ut::bench("mtr_10_records_commit_wal", 200000, [&](uint64_t i) {
    Mtr mtr;

    mtr.start();
    mtr.memo_push(block, MTR_MEMO_PAGE_X_FIX);

    for (int j = 0; j < 10; ++j) {
      mlog_write_ulint(frame + FIL_PAGE_DATA + 4 * j, uint32_t(i + j), MLOG_4BYTES, &mtr);
    }

    mtr.m_memo.clear();
    mtr.commit(*m_sys);
  });

  m_sys->m_buf_pool->block_free(block);
  EXPECT_LT(r.ns_per_op(), 200000.0);
}

}  // namespace
