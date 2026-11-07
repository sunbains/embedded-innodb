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
/** @file unit-tests/test_blink_churn.cc
The B-link tree under load at any page size: writers, scans and marks on a tree that goes up and down,
with crashes, and with both kinds of index in a database. */

#include "test_blink.h"

namespace {

/** The test to run while developing: about a minute, concurrent, and at any page size. With small pages
(IB_PAGE_SIZE_SHIFT) and long keys a few hundred rows make a tree 3 or 4 levels deep, so splits, merges
and root changes happen all the time and the limits of a page are easy to reach. Sizes come from the page,
the depth from growing the tree. What's chosen comes from one seed, printed: IB_TEST_SEED. How long:
IB_TEST_SECS. */
class BlinkChurn : public Blink {
 protected:
  static constexpr const char *NAME = "test/churn";
  static constexpr ulint KEY_COL_LEN{768};
  static constexpr ulint B_COL_LEN{8000};
  /** A run that goes on this long after it should have ended is hanging. */
  static constexpr auto WATCHDOG = 300s;

  static constexpr uint32_t N_WRITERS{8};
  static constexpr uint32_t N_STABLE{64};

  /* Fields of a clustered record. */
  static constexpr ulint F_KEY{0};
  static constexpr ulint F_B{3};

  /** Keys that are multiples of this are stable, the digit below it says whose the rest are. */
  static constexpr uint32_t SPAN{1024};

  /** Past a stable key: the keys below this are the long ones of the churn, the rest are the short
  ones of the hot page. */
  static constexpr uint32_t NORMAL{256};

  /** Digits at the start of a key: they order the keys, what's after them is padding. */
  static constexpr ulint N_DIGITS{10};

  void create_table() {
    api::TableSchema *tbl_sch{};
    api::IndexSchema *idx_sch{};

    ASSERT_EQ(api::TableSchema::create(NAME, &tbl_sch, IB_TBL_V1), DB_SUCCESS);
    ASSERT_EQ(tbl_sch->add_col("k", IB_VARCHAR, IB_COL_NONE, 0, KEY_COL_LEN), DB_SUCCESS);
    ASSERT_EQ(tbl_sch->add_col("b", IB_VARCHAR, IB_COL_NONE, 0, B_COL_LEN), DB_SUCCESS);
    ASSERT_EQ(tbl_sch->add_index("PRIMARY", &idx_sch), DB_SUCCESS);
    ASSERT_EQ(idx_sch->add_col("k", 0), DB_SUCCESS);
    ASSERT_EQ(idx_sch->set_clustered(), DB_SUCCESS);

    auto trx = begin();
    uint64_t table_id{};

    ASSERT_EQ(api::schema_lock_exclusive(*m_sys, trx), DB_SUCCESS);
    ASSERT_EQ(api::table_create(*m_sys, trx, tbl_sch, &table_id), DB_SUCCESS);
    commit(trx);

    api::TableSchema::destroy(*m_sys, tbl_sch);

    m_sys->m_dict->mutex_acquire();
    m_index = m_sys->m_dict->table_get(NAME)->get_first_index();
    m_sys->m_dict->mutex_release();
  }

  /** @return the largest record's worth of key and b together, that stays on the page. */
  [[nodiscard]] static ulint largest() noexcept { return page_get_free_space_of_empty() / 2 - 64; }

  /** @return the length of the key of k. Always the same for a k: short ones, and the longest the
  column and the page allow, that make the fan-out small. */
  [[nodiscard]] static ulint key_len(uint32_t k) noexcept {
    const auto longest = std::min(KEY_COL_LEN, blink::max_key_size() - 64);

    /* Short, for many records on the leaf that all the writers are on. */
    if (k % SPAN == 0 || k % SPAN >= NORMAL) {
      return N_DIGITS;
    }

    switch ((k * 2654435761U >> 24) % 4) {
      case 0:
        return longest;
      case 1:
        return longest - 1;
      case 2:
        return longest / 2;
      default:
        return N_DIGITS + (k * 2654435761U >> 20) % (longest - N_DIGITS);
    }
  }

  [[nodiscard]] static byte *key_bytes(uint32_t k, MemHeap *heap, ulint &len) noexcept {
    len = key_len(k);

    auto p = static_cast<byte *>(mem_heap_alloc(heap, len));

    memset(p, 'x', len);

    (void)std::format_to_n(reinterpret_cast<char *>(p), N_DIGITS, "{:010}", k);

    return p;
  }

  [[nodiscard]] DTuple *ckey(uint32_t k, MemHeap *heap) const {
    auto t = dtuple_create(heap, 1);
    ulint len;
    const auto p = key_bytes(k, heap, len);

    m_index->copy_types(t, 1);
    dfield_set_data(dtuple_get_nth_field(t, 0), p, len);
    dtuple_set_n_fields_cmp(t, 1);

    return t;
  }

  [[nodiscard]] DTuple *centry(uint32_t k, ulint blen, MemHeap *heap) const {
    const auto n = m_index->get_n_fields();
    auto t = dtuple_create(heap, n);
    ulint len;
    const auto p = key_bytes(k, heap, len);

    m_index->copy_types(t, n);
    dfield_set_data(dtuple_get_nth_field(t, F_KEY), p, len);

    auto sys = mem_heap_zalloc(heap, DATA_TRX_ID_LEN + DATA_ROLL_PTR_LEN);

    sys[5] = 1;
    dfield_set_data(dtuple_get_nth_field(t, 1), sys, DATA_TRX_ID_LEN);
    dfield_set_data(dtuple_get_nth_field(t, 2), sys + DATA_TRX_ID_LEN, DATA_ROLL_PTR_LEN);

    auto b = static_cast<byte *>(mem_heap_alloc(heap, std::max<ulint>(blen, 1)));

    memset(b, 'y', blen);
    dfield_set_data(dtuple_get_nth_field(t, F_B), b, blen);
    dtuple_set_n_fields_cmp(t, m_index->get_n_unique_in_tree());

    return t;
  }

  [[nodiscard]] uint32_t ckey_of(Rec rec, MemHeap *heap) const {
    const auto offs = PhyRec(m_index, rec).get_all_col_offsets(nullptr, &heap, Current_location());
    ulint len;
    const auto data = rec.get_nth_field(offs, F_KEY, &len);

    ut_a(len >= N_DIGITS);

    uint32_t k{};

    (void)std::from_chars(reinterpret_cast<const char *>(data.get()), reinterpret_cast<const char *>(data.get()) + N_DIGITS, k);

    return k;
  }

  /** Insert the record for k, by the batch or the normal way, as it takes.
  @return true if it went in. */
  [[nodiscard]] bool cput(uint32_t k, ulint blen, MemHeap *heap, bool batched) {
    const auto e = centry(k, blen, heap);
    IndexCursor cur{*m_sys};

    if (batched) {
      const auto err = cur.insert_batched(m_index, e, DML_FLAGS, nullptr);

      if (err != DB_FAIL) {
        return err == DB_SUCCESS;
      }

      m_n_refused.fetch_add(1, std::memory_order_relaxed);
    }

    Mtr mtr;
    BigRec big{};

    mtr.start();
    cur.seek_insert(m_index, e, Intent::Write, nullptr, &mtr);

    const auto err = cur.insert(Intent::Write, DML_FLAGS, e, &big, 0, nullptr, &mtr);

    cur.commit(&mtr);

    return err == DB_SUCCESS;
  }

  [[nodiscard]] bool cdel(uint32_t k, MemHeap *heap) const {
    Mtr mtr;
    IndexCursor cur{*m_sys};

    mtr.start();

    const auto found = cur.find(m_index, ckey(k, heap), Intent::Write, &mtr);

    if (found) {
      ut_a(cur.remove(Intent::Write, RB_NONE, &mtr) == DB_SUCCESS);
    }

    cur.commit(&mtr);

    return found;
  }

  [[nodiscard]] bool chas(uint32_t k, MemHeap *heap) const {
    Mtr mtr;
    IndexCursor cur{*m_sys};

    mtr.start();

    const auto found = cur.find(m_index, ckey(k, heap), Intent::Read, &mtr);

    cur.commit(&mtr);

    return found;
  }

  void fail(const std::string &what) {
    ++m_errors;

    std::fprintf(stderr, "%s\n", what.c_str());
  }

  /** The size of b at a limit of the page, for a key of this length: nothing, a byte, the largest
  record or a byte less, and what makes pages of a few records. */
  [[nodiscard]] static ulint boundary_size(std::mt19937 &rng, ulint klen) noexcept {
    const auto empty = page_get_free_space_of_empty();
    const auto top = largest() - klen;
    const auto fit = [&](ulint want) { return want > klen + 64 ? want - klen - 64 : ulint{0}; };

    switch (rng() % 8) {
      case 0:
        return 0;
      case 1:
        return 1;
      case 2:
        return top;
      case 3:
        return top - 1;
      case 4:
        return fit(empty / 3);
      case 5:
        return fit(empty / 4) + rng() % 3;
      case 6:
        return fit(empty / 8);
      default:
        return rng() % top;
    }
  }

  /** @return a key of this writer: right after a stable key, right before the next, or between. */
  [[nodiscard]] static uint32_t writer_key(std::mt19937 &rng, uint32_t w) noexcept {
    const auto base = uint32_t(rng() % N_STABLE + 1) * SPAN;
    const auto slots = (NORMAL - 1) / N_WRITERS;
    uint32_t slot;

    switch (rng() % 4) {
      case 0:
        slot = 0;
        break;
      case 1:
        slot = slots - 1;
        break;
      default:
        slot = uint32_t(rng() % slots);
        break;
    }

    return base + slot * N_WRITERS + 1 + w;
  }

  /** An operation of a writer while the writers are all after the keys past the stable key hot: mostly
  batched inserts on a page that also splits, and when going down merges, under the ones that wait. */
  void hot_op(std::mt19937 &rng, uint32_t w, uint32_t hot, bool grow, std::set<uint32_t> &keys, MemHeap *heap) {
    const auto slots = (SPAN - NORMAL - 1) / N_WRITERS;
    const auto k = hot + NORMAL + uint32_t(rng() % slots) * N_WRITERS + 1 + w;

    if (keys.count(k) == 0) {
      if (!cput(k, rng() % 16, heap, true)) {
        fail(std::format("hot insert of {} failed", k));
      }

      keys.insert(k);

    } else if (rng() % (grow ? 8 : 2) == 0) {
      if (!cdel(k, heap)) {
        fail(std::format("hot {} wasn't there to delete", k));
      }

      keys.erase(k);

    } else {
      /* A key that's there: a batch never decides that, it comes back with nothing done. */
      IndexCursor cur{*m_sys};

      if (const auto err = cur.insert_batched(m_index, centry(k, 8, heap), DML_FLAGS, nullptr); err != DB_FAIL) {
        fail(std::format("batched insert of {}, already there: {}", k, int(err)));
      }
    }
  }

  void writer_loop(uint32_t w, uint32_t seed, std::set<uint32_t> &keys) {
    std::mt19937 rng(seed + w);
    auto heap = mem_heap_create(UNIV_PAGE_SIZE * 2);

    while (!m_stop.load(std::memory_order_relaxed)) {
      /* Growing or shrinking, in turns, for the root to go up and down. */
      const bool grow = (m_phase.load(std::memory_order_relaxed) & 1) == 0;

      if (const auto hot = m_hot.load(std::memory_order_relaxed); hot != 0) {
        hot_op(rng, w, hot, grow, keys, heap);
        mem_heap_empty(heap);
        m_n_ops.fetch_add(1, std::memory_order_relaxed);
        continue;
      }

      auto k = writer_key(rng, w);

      /* What's there to delete, near where it was going: a random key mostly isn't, on a tree that's
      going down. */
      if (!grow && !keys.empty()) {
        const auto at = keys.lower_bound(k);

        k = at != keys.end() ? *at : *keys.begin();
      }

      if (keys.count(k) == 0) {
        if (grow || rng() % 8 == 0) {
          if (!cput(k, boundary_size(rng, key_len(k)), heap, rng() % 2 == 0)) {
            fail(std::format("insert of {} failed", k));
          }

          keys.insert(k);
        }
      } else if (!grow || rng() % 8 == 0) {
        if (!cdel(k, heap)) {
          fail(std::format("{} wasn't there to delete", k));
        }

        keys.erase(k);
      }

      mem_heap_empty(heap);
      m_n_ops.fetch_add(1, std::memory_order_relaxed);
    }

    mem_heap_free(heap);
  }

  /** A scan that holds nothing: all the stable keys, once each, in order. */
  void scan_loop(uint32_t id) {
    auto heap = mem_heap_create(16384);

    for (uint32_t pass{id}; !m_stop.load(std::memory_order_relaxed); ++pass) {
      const bool forward = pass % 2 == 0;
      uint32_t expected = forward ? SPAN : N_STABLE * SPAN;
      uint32_t last = forward ? 0 : UINT32_MAX;
      uint32_t n_seen{};
      Mtr mtr;
      IndexCursor cur{*m_sys};

      mtr.start();

      if (forward) {
        cur.seek_first(m_index, Intent::Snapshot, &mtr);
      } else {
        cur.seek_last(m_index, Intent::Snapshot, &mtr);
      }

      while (forward ? cur.next(&mtr) : cur.prev(&mtr)) {
        const auto k = ckey_of(cur.rec(), heap);

        mem_heap_empty(heap);

        if (forward ? k <= last : k >= last) {
          fail(std::format("scan out of order: {} then {}", last, k));
          break;
        }

        last = k;

        if (k % SPAN == 0) {
          if (k != expected) {
            fail(std::format("scan wanted stable key {}, got {}", expected, k));
            break;
          }

          ++n_seen;

          expected = forward ? expected + SPAN : expected - SPAN;
        }
      }

      cur.commit(&mtr);

      if (n_seen != N_STABLE) {
        fail(std::format("a scan saw {} stable keys", n_seen));
      }

      m_n_scans.fetch_add(1, std::memory_order_relaxed);
    }

    mem_heap_free(heap);
  }

  /** Point reads and in-place changes of stable records, by their group's bit alone. */
  void mark_loop(uint32_t id, uint32_t seed) {
    std::mt19937 rng(seed + 100 + id);
    auto heap = mem_heap_create(16384);

    while (!m_stop.load(std::memory_order_relaxed)) {
      const auto k = uint32_t(rng() % N_STABLE + 1) * SPAN;

      {
        Mtr mtr;
        IndexCursor cur{*m_sys};

        mtr.start();

        if (!cur.find(m_index, ckey(k, heap), Intent::Mark, &mtr)) {
          fail(std::format("stable key {} is gone", k));
        } else {
          /* Bytes of the record that a read has to see together. Nothing on disk: both go. */
          BufBlock::RecWrite write{cur.block()};
          auto rec = cur.rec();

          rec.set_deleted_flag(true);
          rec.set_deleted_flag(false);
        }

        cur.commit(&mtr);
      }

      if (!chas(k, heap)) {
        fail(std::format("stable key {} can't be read", k));
      }

      mem_heap_empty(heap);
      m_n_reads.fetch_add(1, std::memory_order_relaxed);
    }

    mem_heap_free(heap);
  }

  /** @return the level of the root. */
  [[nodiscard]] ulint root_level() const {
    return blink::read_node(*m_sys, m_index, m_index->get_page_no(), nullptr, 0, blink::Edge::FIRST, ULINT_MAX).m_level;
  }

  /** Watch the depth and turn the writers around so that it keeps going up and down, and put the
  writers on a hot page for some of the time. How deep is for the page size: the same long keys are a
  bigger fan-out on a bigger page.
  @param[in] each               Called on each turn. */
  template <typename Each>
  void drive(uint32_t secs, uint32_t seed, Each each) {
    const ulint deep = deep_level();
    const auto start = std::chrono::steady_clock::now();
    std::mt19937 hot_rng(seed + 200);

    for (uint32_t tick{}; std::chrono::steady_clock::now() - start < std::chrono::seconds(secs); ++tick) {
      std::this_thread::sleep_for(std::chrono::milliseconds(50));

      /* A fifth of a second on one page out of every 0.8: the rest of the time is for the depth. */
      if (tick % 16 == 0) {
        m_hot = uint32_t(hot_rng() % N_STABLE + 1) * SPAN;
      } else if (tick % 16 == 4) {
        m_hot = 0;
      }

      const auto level = root_level();

      m_low = std::min(m_low, level);
      m_high = std::max(m_high, level);

      if (const auto phase = m_phase.load(); phase % 2 == 0 ? level >= deep : level <= 1) {
        m_phase.store(phase + 1);
      }

      each();
    }
  }

  [[nodiscard]] static ulint deep_level() noexcept { return UNIV_PAGE_SIZE <= 4096 ? 3 : 2; }

  void open_churn() {
    m_sys->m_dict->mutex_acquire();
    m_index = m_sys->m_dict->table_get(NAME)->get_first_index();
    m_sys->m_dict->mutex_release();
  }

  void start_all(uint32_t seed, std::vector<std::set<uint32_t>> &models, std::vector<std::thread> &threads) {
    for (uint32_t w{}; w < N_WRITERS; ++w) {
      threads.emplace_back([this, w, seed, &models] { writer_loop(w, seed, models[w]); });
    }

    for (uint32_t i{}; i < 2; ++i) {
      threads.emplace_back([this, i] { scan_loop(i); });
      threads.emplace_back([this, i, seed] { mark_loop(i, seed); });
    }
  }

  /** @return the pages, on all the levels, that have a split to complete. */
  [[nodiscard]] size_t n_unlinked() {
    size_t n{};

    for (auto first = m_index->get_page_no(); first != FIL_NULL;) {
      page_no_t below{FIL_NULL};

      for (auto page_no = first; page_no != FIL_NULL;) {
        Mtr mtr;

        mtr.start();

        const auto page = m_sys->m_btree->block_get(m_index->get_space_id(), page_no, RW_B_LATCH, &mtr)->get_frame();

        n += page_right_unlinked(page) ? 1 : 0;
        page_no = Btree::page_get_next(page, &mtr);

        mtr.commit(*m_sys);
      }

      const auto step = blink::read_node(*m_sys, m_index, first, nullptr, 0, blink::Edge::FIRST, 0);

      if (step.m_kind == blink::NodeStep::CHILD) {
        below = step.m_page;
      }

      first = below;
    }

    return n;
  }

  /** @return all the keys, in the order of the tree. */
  [[nodiscard]] std::vector<uint32_t> all_keys() {
    std::vector<uint32_t> found;

    {
      Mtr mtr;
      IndexCursor cur{*m_sys};

      mtr.start();
      cur.seek_first(m_index, Intent::Read, &mtr);

      while (cur.next(&mtr)) {
        found.push_back(ckey_of(cur.rec(), m_heap));
        mem_heap_empty(m_heap);
      }

      cur.commit(&mtr);
    }

    return found;
  }

  ulint m_low{ULINT_UNDEFINED};
  ulint m_high{};
  std::atomic<bool> m_stop{};
  std::atomic<uint32_t> m_phase{};

  /** The stable key that the writers are all after, 0 for none. */
  std::atomic<uint32_t> m_hot{};
  std::atomic<uint64_t> m_n_refused{};
  std::atomic<uint64_t> m_n_ops{};
  std::atomic<uint64_t> m_n_scans{};
  std::atomic<uint64_t> m_n_reads{};
  std::atomic<uint64_t> m_errors{};
};

TEST_F(BlinkChurn, Run) {
  const auto env = [](const char *name, uint32_t otherwise) {
    const auto value = std::getenv(name);

    return value != nullptr ? uint32_t(std::strtoul(value, nullptr, 10)) : otherwise;
  };

  const auto seed = env("IB_TEST_SEED", std::random_device{}());
  const auto secs = env("IB_TEST_SECS", 60);

  std::printf("CHURN seed=%u secs=%u page=%lu\n", seed, secs, ulint(UNIV_PAGE_SIZE));

  ASSERT_NO_FATAL_FAILURE(create_table());

  for (uint32_t i{1}; i <= N_STABLE; ++i) {
    ASSERT_TRUE(cput(i * SPAN, 8, m_heap, false));
    mem_heap_empty(m_heap);
  }

  Watchdog dog{std::chrono::seconds(secs) + WATCHDOG};
  std::vector<std::set<uint32_t>> models(N_WRITERS);
  std::vector<std::thread> threads;

  start_all(seed, models, threads);

  const auto deep = deep_level();

  drive(secs, seed, [] {});

  m_stop = true;

  for (auto &thread : threads) {
    thread.join();
  }

  std::printf(
    "CHURN ops=%lu scans=%lu reads=%lu phases=%u height=%lu..%lu batched=%lu refused=%lu retries=%lu errors=%lu\n", ulint(m_n_ops.load()),
    ulint(m_n_scans.load()), ulint(m_n_reads.load()), m_phase.load(), m_low, m_high, ulint(blink::n_batched(*m_sys)), ulint(m_n_refused.load()),
    ulint(blink::n_node_retries(*m_sys)), ulint(m_errors.load())
  );

  EXPECT_EQ(m_errors.load(), 0UL) << "seed " << seed;
  EXPECT_GE(m_high, deep) << "a deep tree, seed " << seed;
  EXPECT_GE(m_phase.load(), 2U) << "the root went up and down, seed " << seed;
  EXPECT_GT(m_n_scans.load(), 0UL);

  if (m_sys->m_config.m_dict.m_insert_batch) {
    EXPECT_GT(blink::n_batched(*m_sys), 0UL) << "somebody did the work of somebody else, seed " << seed;
  }

  /* All of them, and nothing else, and the tree is what it should be. */
  std::set<uint32_t> expected;

  for (uint32_t i{1}; i <= N_STABLE; ++i) {
    expected.insert(i * SPAN);
  }

  for (const auto &model : models) {
    expected.insert(model.begin(), model.end());
  }

  std::vector<uint32_t> found;

  {
    Mtr mtr;
    IndexCursor cur{*m_sys};

    mtr.start();
    cur.seek_first(m_index, Intent::Read, &mtr);

    while (cur.next(&mtr)) {
      found.push_back(ckey_of(cur.rec(), m_heap));
      mem_heap_empty(m_heap);
    }

    cur.commit(&mtr);
  }

  EXPECT_EQ(found, std::vector<uint32_t>(expected.begin(), expected.end())) << "seed " << seed;
  EXPECT_TRUE(m_sys->am(m_index).validate(m_index, nullptr)) << "seed " << seed;
}

/** A kill in the middle of all of it, with splits between levels and pages going off the tree, in turns
with recovery: the tree is a tree, the stable keys are there, and writers can go on from what's there. The
keys a writer had can come back or not, what it does next goes by what's there. */
TEST_F(BlinkChurn, Crash) {
  const auto env = [](const char *name, uint32_t otherwise) {
    const auto value = std::getenv(name);

    return value != nullptr ? uint32_t(std::strtoul(value, nullptr, 10)) : otherwise;
  };

  const auto seed = env("IB_TEST_SEED", std::random_device{}());
  constexpr uint32_t N_ROUNDS{4};
  const auto secs = std::max<uint32_t>(env("IB_TEST_SECS", 60) / (N_ROUNDS + 1), 2);

  std::printf("CHURN crash seed=%u secs=%u page=%lu\n", seed, secs, ulint(UNIV_PAGE_SIZE));

  ASSERT_NO_FATAL_FAILURE(create_table());

  for (uint32_t i{1}; i <= N_STABLE; ++i) {
    ASSERT_TRUE(cput(i * SPAN, 8, m_heap, false));
    mem_heap_empty(m_heap);
  }

  /* Who a key belongs to is in the key. */
  const auto models_of = [&](const std::vector<uint32_t> &keys) {
    std::vector<std::set<uint32_t>> models(N_WRITERS);

    for (const auto k : keys) {
      if (k % SPAN != 0) {
        models[(k % SPAN - 1) % N_WRITERS].insert(k);
      }
    }

    return models;
  };

  const auto check = [&](uint32_t round) -> std::vector<uint32_t> {
    const auto keys = all_keys();
    uint32_t n_stable{};

    for (size_t i{}; i < keys.size(); ++i) {
      if (i > 0) {
        EXPECT_LT(keys[i - 1], keys[i]) << "round " << round << " seed " << seed;
      }

      n_stable += keys[i] % SPAN == 0 ? 1 : 0;
    }

    EXPECT_EQ(n_stable, N_STABLE) << "round " << round << " seed " << seed;
    EXPECT_TRUE(m_sys->am(m_index).validate(m_index, nullptr)) << "round " << round << " seed " << seed;

    return keys;
  };

  auto keys = check(0);

  for (uint32_t round{1}; round <= N_ROUNDS; ++round) {
    crash([&] {
      open_churn();

      /* This one's own: they go when it does. */
      auto models = new std::vector<std::set<uint32_t>>(models_of(keys));
      auto threads = new std::vector<std::thread>();
      Watchdog dog{std::chrono::seconds(secs) + WATCHDOG};
      uint32_t n{};

      start_all(seed + round, *models, *threads);

      /* What's been done up to about here is what the recovery has to find, a page in any state. */
      drive(secs, seed + round, [&] {
        if (++n % 4 == 0) {
          m_sys->m_log->buffer_flush_to_disk();
        }
      });

      if (m_errors.load() != 0) {
        ::_exit(3);
      }
    });

    open_churn();

    ASSERT_FALSE(HasFailure());

    keys = check(round);

    ASSERT_FALSE(HasFailure());

    /* What this run tests is only there if the kills land on it: to be able to see that. */
    std::printf("CHURN round=%u keys=%zu unlinked=%zu\n", round, keys.size(), n_unlinked());
  }

  /* And on from there: they have to find their way around what the crashes left. */
  auto models = models_of(keys);
  std::vector<std::thread> threads;
  Watchdog dog{std::chrono::seconds(secs) + WATCHDOG};

  start_all(seed, models, threads);

  drive(secs, seed, [] {});

  m_stop = true;

  for (auto &thread : threads) {
    thread.join();
  }

  EXPECT_EQ(m_errors.load(), 0UL) << "seed " << seed;

  std::set<uint32_t> expected;

  for (uint32_t i{1}; i <= N_STABLE; ++i) {
    expected.insert(i * SPAN);
  }

  for (const auto &model : models) {
    expected.insert(model.begin(), model.end());
  }

  EXPECT_EQ(check(N_ROUNDS + 1), std::vector<uint32_t>(expected.begin(), expected.end())) << "seed " << seed;
}

/** The catalog says what an index is, not the configuration: a database has both kinds, and each
goes on as what it is through restarts, a crash and a drop under either setting. */
class MixedAm : public BlinkChurn {
 protected:
  void SetUp() override {
    m_config = ut::make_config(m_dir.m_path);
    m_config.m_dict.m_index_am = IndexAmKind::BTREE;

    start();

    m_heap = mem_heap_create(16384);
  }

  void start_as(IndexAmKind kind) {
    stop();
    m_config.m_dict.m_index_am = kind;
    start(false);
  }

  void open_churn() {
    m_sys->m_dict->mutex_acquire();
    m_index = m_sys->m_dict->table_get(NAME)->get_first_index();
    m_sys->m_dict->mutex_release();
  }

  [[nodiscard]] bool test_table_is_blink() {
    m_sys->m_dict->mutex_acquire();

    const auto table = m_sys->m_dict->table_get(ut::TABLE);
    bool all{true};
    bool none{true};

    for (auto index : table->m_indexes) {
      all = all && index->is_blink();
      none = none && !index->is_blink();
    }

    m_sys->m_dict->mutex_release();

    ut_a(all || none);

    return all;
  }

  [[nodiscard]] bool sys_tables_are_blink() {
    m_sys->m_dict->mutex_acquire();

    const auto blink = m_sys->m_dict->m_sys_tables->get_first_index()->is_blink();

    m_sys->m_dict->mutex_release();

    return blink;
  }

  [[nodiscard]] std::set<uint32_t> churn_keys() {
    std::set<uint32_t> keys;
    Mtr mtr;
    IndexCursor cur{*m_sys};

    mtr.start();
    cur.seek_first(m_index, Intent::Read, &mtr);

    while (cur.next(&mtr)) {
      keys.insert(ckey_of(cur.rec(), m_heap));
      mem_heap_empty(m_heap);
    }

    cur.commit(&mtr);

    return keys;
  }

  void more_rows(ut::Rows &rows, uint32_t from, uint32_t n) {
    auto trx = begin();

    for (uint32_t k{from}; k < from + n; ++k) {
      const ut::Row row{k, k * 2, ut::blob(k)};

      ASSERT_EQ(insert(trx, row), DB_SUCCESS);
      rows[k] = row;
    }

    commit(trx);
  }

  void more_churn(std::set<uint32_t> &keys, uint32_t from, uint32_t n) {
    for (uint32_t k{from}; k < from + n; ++k) {
      ASSERT_TRUE(cput(k * 7 + 1, 40, m_heap, k % 2 == 0));
      mem_heap_empty(m_heap);
      keys.insert(k * 7 + 1);
    }
  }
};

TEST_F(MixedAm, Run) {
  ut::Rows rows;
  std::set<uint32_t> keys;

  ASSERT_FALSE(sys_tables_are_blink());
  ASSERT_FALSE(test_table_is_blink());
  ASSERT_NO_FATAL_FAILURE(more_rows(rows, 1, 500));

  /* New indexes are B-link, what was there isn't. */
  ASSERT_NO_FATAL_FAILURE(start_as(IndexAmKind::BLINK));
  EXPECT_FALSE(sys_tables_are_blink());
  EXPECT_FALSE(test_table_is_blink());
  expect_rows(rows);

  ASSERT_NO_FATAL_FAILURE(create_table());
  EXPECT_TRUE(m_index->is_blink());
  EXPECT_TRUE(m_sys->m_dict->m_sys_tables->get_first_index()->is_blink() == false);
  ASSERT_NO_FATAL_FAILURE(more_rows(rows, 1000, 500));
  ASSERT_NO_FATAL_FAILURE(more_churn(keys, 1, 400));
  expect_rows(rows);
  EXPECT_EQ(churn_keys(), keys);

  /* The setting back: they stay what they are. */
  ASSERT_NO_FATAL_FAILURE(start_as(IndexAmKind::BTREE));
  open_churn();
  EXPECT_FALSE(test_table_is_blink());
  EXPECT_TRUE(m_index->is_blink());
  expect_rows(rows);
  EXPECT_EQ(churn_keys(), keys);
  ASSERT_NO_FATAL_FAILURE(more_churn(keys, 1000, 400));
  ASSERT_NO_FATAL_FAILURE(more_rows(rows, 2000, 200));
  EXPECT_EQ(churn_keys(), keys);
  EXPECT_TRUE(m_sys->am(m_index).validate(m_index, nullptr));

  /* A crash with both in the log, recovery has no index to ask. */
  crash([&] {
    open_churn();

    auto trx = begin();

    for (uint32_t k{5000}; k < 5200; ++k) {
      ut_a(cput(k * 7 + 1, 40, m_heap, false));
      mem_heap_empty(m_heap);
    }

    commit(trx);
  });

  open_churn();
  EXPECT_TRUE(m_index->is_blink());
  expect_rows(rows);

  const auto after = churn_keys();

  EXPECT_TRUE(std::includes(after.begin(), after.end(), keys.begin(), keys.end())) << "what was there before the crash";
  EXPECT_TRUE(m_sys->am(m_index).validate(m_index, nullptr));

  /* A drop by what the catalog says, with the setting the other way. */
  auto trx = begin();

  ASSERT_EQ(api::schema_lock_exclusive(*m_sys, trx), DB_SUCCESS);
  ASSERT_EQ(api::table_drop(*m_sys, trx, NAME), DB_SUCCESS);
  commit(trx);

  ASSERT_NO_FATAL_FAILURE(start_as(IndexAmKind::BLINK));
  expect_rows(rows);

  m_sys->m_dict->mutex_acquire();
  EXPECT_EQ(m_sys->m_dict->table_get(NAME), nullptr);
  m_sys->m_dict->mutex_release();
}

}  // namespace
