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

/** @file mtr/mtr0systrx.cc
System transactions. */

#include "mtr0systrx.h"

#include "buf0buf.h"
#include "fil0fil.h"
#include "fsp0fsp.h"
#include "log0log.h"
#include "mach0data.h"
#include "mtr0log.h"
#include "srv0sys.h"

#include <algorithm>
#include <cstring>
#include <iterator>
#include <mutex>

namespace {

constexpr ulint ID_LEN = 8;

/** Id, kind, replace flag and the length of the arguments. */
constexpr ulint STEP_HDR_LEN = ID_LEN + 1 + 1 + 2;

/** Arguments of a compensation on a word of a page. */
constexpr ulint WORD_ARGS_LEN = 4 + 4 + 2 + 4;

/** Arguments of FREE_PAGE: space, segment header page, its offset, the page. */
constexpr ulint FREE_ARGS_LEN = 4 + 4 + 2 + 4;

/** Write a record with no body beyond the id. */
void log_id(ulint type, uint64_t id, Mtr *mtr) noexcept {
  auto ptr = mlog_open(mtr, 11 + ID_LEN);

  if (ptr != nullptr) {
    ptr = mlog_write_initial_log_record_for_file_op(MlogType(type), SYS_TABLESPACE, 0, ptr, mtr);
    mach_write_to_8(ptr, id);
    mlog_close(mtr, ptr + ID_LEN);
  }
}

/** Write a step. A first record with replace set is all of the state, for a restart. */
void log_step_rec(uint64_t id, SysTrxKind kind, const void *args, ulint len, bool replace, Mtr *mtr) noexcept {
  auto ptr = mlog_open(mtr, 11 + STEP_HDR_LEN + len);

  if (ptr != nullptr) {
    ptr = mlog_write_initial_log_record_for_file_op(MLOG_SYSTRX_STEP, SYS_TABLESPACE, 0, ptr, mtr);

    mach_write_to_8(ptr, id);
    ptr += ID_LEN;
    *ptr++ = byte(kind);
    *ptr++ = byte(replace);
    mach_write_to_2(ptr, len);
    ptr += 2;
    std::memcpy(ptr, args, len);

    mlog_close(mtr, ptr + len);
  }
}

[[nodiscard]] BufBlock *get_page(System &sys, space_id_t space, page_no_t page_no, Mtr *mtr) noexcept {
  BufPool::Request req{
    .m_rw_latch = RW_X_LATCH, .m_page_id = {space, page_no}, .m_mode = BUF_GET, .m_file = __FILE__, .m_line = __LINE__, .m_mtr = mtr
  };

  return sys.m_buf_pool->get(req, nullptr);
}

/** Apply a compensation in mtr. A tablespace that's gone has nothing to compensate for. */
void apply(System &sys, const SysTrxStep &step, Mtr *mtr) noexcept {
  const auto args = step.m_args.data();
  const auto space = space_id_t(mach_read_from_4(args));

  if (sys.m_fil->space_get_flags(space) == ULINT_UNDEFINED) {
    return;
  }

  const auto page_no = page_no_t(mach_read_from_4(args + 4));
  const auto offset = mach_read_from_2(args + 8);
  const auto value = mach_read_from_4(args + 10);

  switch (step.m_kind) {
    case SysTrxKind::FREE_PAGE: {
      ut_a(step.m_args.size() == FREE_ARGS_LEN);

      sys.m_fsp->latch(space, mtr);

      const auto header = get_page(sys, space, page_no, mtr)->get_frame() + offset;

      sys.m_fsp->fseg_free_page(header, space, page_no_t(value), mtr);

      return;
    }

    case SysTrxKind::WRITE4:
      ut_a(step.m_args.size() == WORD_ARGS_LEN);
      mlog_write_ulint(get_page(sys, space, page_no, mtr)->get_frame() + offset, value, MLOG_4BYTES, mtr);
      return;

#ifdef UNIT_TESTING
    case SysTrxKind::ADD4: {
      ut_a(step.m_args.size() == WORD_ARGS_LEN);

      const auto ptr = get_page(sys, space, page_no, mtr)->get_frame() + offset;

      mlog_write_ulint(ptr, (mach_read_from_4(ptr) + value) & 0xFFFFFFFFUL, MLOG_4BYTES, mtr);

      return;
    }
#endif /* UNIT_TESTING */
  }

  ut_error;
}

}  // namespace

size_t SysTrxSys::n_active() const noexcept {
  std::shared_lock<std::shared_mutex> guard(m_latch);

  return m_active.size();
}

lsn_t SysTrxSys::carry_forward() noexcept {
  lsn_t start{LSN_MAX};
  lsn_t end{};

  {
    std::unique_lock<std::shared_mutex> guard(m_latch);

    Mtr mtr;

    mtr.start();

    for (const auto &[id, steps] : m_active) {
      bool first{true};

      for (const auto &step : *steps) {
        log_step_rec(id, step.m_kind, step.m_args.data(), step.m_args.size(), first, &mtr);
        first = false;
      }
    }

    if (mtr.m_n_log_recs > 0) {
      start = m_sys.m_log->get_lsn();
      mtr.m_modifications = true;
      mtr.commit(m_sys);
      end = mtr.m_end_lsn;
    } else {
      mtr.commit(m_sys);
    }
  }

  if (end != 0) {
    /* The checkpoint is about to go past the earlier records. */
    m_sys.m_log->write_up_to(end, true);
  }

  return start;
}

uint64_t SysTrxSys::begin(std::vector<SysTrxStep> *steps) noexcept {
  std::unique_lock<std::shared_mutex> guard(m_latch);

  const auto id = m_next_id++;

  m_active.emplace(id, steps);

  return id;
}

void SysTrxSys::end(uint64_t id) noexcept {
  std::unique_lock<std::shared_mutex> guard(m_latch);

  m_active.erase(id);
}

byte *SysTrxSys::parse(byte *ptr, const byte *end_ptr, ulint type) noexcept {
  if (type != MLOG_SYSTRX_STEP) {
    return end_ptr < ptr + ID_LEN ? nullptr : ptr + ID_LEN;
  }

  if (end_ptr < ptr + STEP_HDR_LEN) {
    return nullptr;
  }

  const auto end = ptr + STEP_HDR_LEN + mach_read_from_2(ptr + ID_LEN + 2);

  return end_ptr < end ? nullptr : end;
}

void SysTrxSys::replay(ulint type, const byte *body) noexcept {
  const auto id = mach_read_from_8(body);

  m_next_id = std::max(m_next_id, id + 1);

  if (type == MLOG_SYSTRX_END) {
    m_recovered.erase(id);
  } else if (type == MLOG_SYSTRX_UNDONE) {
    /* The scan may have started after the step only if this one had ended. */
    if (auto it = m_recovered.find(id); it != m_recovered.end() && !it->second.empty()) {
      it->second.pop_back();
    }
  } else {
    ut_a(type == MLOG_SYSTRX_STEP);

    auto &steps = m_recovered[id];

    if (body[ID_LEN + 1] != 0) {
      steps.clear();
    }

    const auto args = body + STEP_HDR_LEN;

    steps.push_back({SysTrxKind(body[ID_LEN]), std::vector<byte>(args, args + mach_read_from_2(body + ID_LEN + 2))});
  }
}

void SysTrxSys::recover() noexcept {
  /* A checkpoint while this runs has to find them. */
  {
    std::unique_lock<std::shared_mutex> guard(m_latch);

    for (auto &[id, steps] : m_recovered) {
      m_active.emplace(id, &steps);
    }
  }

  /* Latest first: a later one may rely on an earlier one that's still active. */
  for (auto it = m_recovered.rbegin(); it != m_recovered.rend(); ++it) {
    compensate(it->first, it->second);
    end(it->first);
  }

  m_recovered.clear();
}

void SysTrxSys::compensate(uint64_t id, std::vector<SysTrxStep> &steps) noexcept {
  while (!steps.empty()) {
    compensate_one(id, steps);
  }
}

void SysTrxSys::compensate_one(uint64_t id, std::vector<SysTrxStep> &steps) noexcept {
  Mtr mtr;

  mtr.start();

  apply(m_sys, steps.back(), &mtr);

  /* In the same mini-transaction as the compensation: it happens once. */
  log_id(steps.size() == 1 ? MLOG_SYSTRX_END : MLOG_SYSTRX_UNDONE, id, &mtr);

  std::shared_lock<std::shared_mutex> guard(m_latch);

  mtr.commit(m_sys);

  steps.pop_back();
}

SysTrx::SysTrx(System &sys) noexcept : m_sys(sys), m_id(sys.m_systrx->begin(&m_steps)) {}

SysTrx::~SysTrx() noexcept { ut_a(m_done); }

void SysTrx::log_step(Mtr *mtr, SysTrxKind kind, const void *args, ulint len, bool replace) noexcept {
  ut_a(len <= ARGS_MAX);
  ut_ad(!m_done);

  log_step_rec(m_id, kind, args, len, replace, mtr);

  if (replace) {
    m_pending.clear();
    m_replace = true;
  }

  const auto p = static_cast<const byte *>(args);

  m_pending.push_back({kind, std::vector<byte>(p, p + len)});
}

void SysTrx::step(Mtr *mtr, SysTrxKind kind, const void *args, ulint len) noexcept {
  log_step(mtr, kind, args, len, false);
}

void SysTrx::replace(Mtr *mtr, SysTrxKind kind, const void *args, ulint len) noexcept {
  log_step(mtr, kind, args, len, true);
}

void SysTrx::commit_step(Mtr *mtr) noexcept {
  ut_ad(!m_done);

  std::shared_lock<std::shared_mutex> guard(m_sys.m_systrx->m_latch);

  mtr->commit(m_sys);

  if (m_replace) {
    m_steps.clear();
    m_replace = false;
  }

  std::move(m_pending.begin(), m_pending.end(), std::back_inserter(m_steps));
  m_pending.clear();
}

void SysTrx::commit(Mtr *mtr) noexcept {
  ut_ad(!m_done);

  const auto logged = !m_steps.empty() || !m_pending.empty();

  if (logged) {
    log_id(MLOG_SYSTRX_END, m_id, mtr);
  }

  {
    std::shared_lock<std::shared_mutex> guard(m_sys.m_systrx->m_latch);

    mtr->commit(m_sys);

    /* With the end record in the log: a checkpoint must not write these again. */
    m_steps.clear();
    m_pending.clear();
  }

  m_sys.m_systrx->end(m_id);

  m_done = true;
}

void SysTrx::rollback() noexcept {
  ut_ad(!m_done);
  ut_a(m_pending.empty());

  m_sys.m_systrx->compensate(m_id, m_steps);
  m_sys.m_systrx->end(m_id);

  m_done = true;
}
