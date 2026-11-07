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

/** @file include/mtr0systrx.h
System transactions: an update of several objects that takes effect entirely or
not at all, in steps that each release their latches.

A mini-transaction is atomic but holds its latches until it commits. A system
transaction is the level above it in a multi-level scheme: each step is a
mini-transaction that also logs how to compensate for the step, in the same
log (Lomet, MLR, 1992), so a crash can't separate the change from its
compensation. Restart repeats history with redo, then compensates for each
system transaction that has no end record, latest step first (ARIES, Mohan et
al., 1992). A compensation logs that it's done in its own mini-transaction and
is never undone itself: another crash doesn't apply it twice.

What a step has to obey, since other work can touch the same pages between steps:
 - It's a complete operation. Its compensation stays correct whatever ran after
   it: it's logical, or on something nothing else can reach, not a before image
   of a shared page (Weikum & Hasse, 1993).
 - A compensation can't fail: it never needs a new page or a reservation.
 - Whatever must not change between the steps is protected by the caller, at
   the level above. Latches aren't held across steps.
 - Its last step's mini-transaction is committed with commit_step() or commit(), for
   the state in memory and in the log to change together.

A system transaction can run for as long as it takes: it doesn't hold the log back.
A checkpoint writes the state of every active one again, after the checkpoint LSN,
so restart only reads from the last checkpoint and the log before it can go. No state
after the last checkpoint means it finished. The state grows with the steps:
replace() keeps it small for one that finishes forward. */

#pragma once

#include "innodb0types.h"

#include "srv0sys.h"

#include <cstddef>
#include <map>
#include <shared_mutex>
#include <vector>

class System;

struct Mtr;

/** What a compensation does. Logged, so the values can't change. */
enum class SysTrxKind : uint8_t {
  /** Free a page of a segment: an allocation nothing links to yet. */
  FREE_PAGE = 1,

  /** Write 4 bytes of a page that only this system transaction uses. */
  WRITE4 = 2,

#ifdef UNIT_TESTING
  /** Add to 4 bytes of a page: applying this twice shows. */
  ADD4 = 255,
#endif /* UNIT_TESTING */
};

/** A compensation for one step. */
struct SysTrxStep {
  SysTrxKind m_kind{};
  std::vector<byte> m_args{};
};

/** The active system transactions of an instance. */
class SysTrxSys {
 public:
  explicit SysTrxSys(System &sys) noexcept : m_sys(sys) {}

  /** Write the state of all the active system transactions to the log, durably, for a
  checkpoint to go past their earlier records.
  @return the LSN the checkpoint can't pass, LSN_MAX if there's no state to keep. */
  [[nodiscard]] lsn_t carry_forward() noexcept;

  /** @return the number of system transactions that haven't finished. */
  [[nodiscard]] size_t n_active() const noexcept;

  /** Size a log record, for the parser.
  @return the end of the record, nullptr if it isn't all there. */
  [[nodiscard]] static byte *parse(byte *ptr, const byte *end_ptr, ulint type) noexcept;

  /** Track a record found by the log scan. */
  void replay(ulint type, const byte *body) noexcept;

  /** After redo: compensate for the system transactions the crash cut short. */
  void recover() noexcept;

  /** Compensate for steps, latest first, each in its own mini-transaction.
  @param[in] id                 The system transaction.
  @param[in,out] steps          The steps; empty on return. */
  void compensate(uint64_t id, std::vector<SysTrxStep> &steps) noexcept;

  /** Compensate for the latest of steps, in a mini-transaction that logs it's done.
  @param[in] id                 The system transaction.
  @param[in,out] steps          The steps, not empty; without that one on return. */
  void compensate_one(uint64_t id, std::vector<SysTrxStep> &steps) noexcept;

 private:
  friend class SysTrx;

  /** @return a new id for steps, which carry_forward() reads. */
  [[nodiscard]] uint64_t begin(std::vector<SysTrxStep> *steps) noexcept;

  /** It's finished: its steps are empty and that's in the log. */
  void end(uint64_t id) noexcept;

  System &m_sys;

  /** A mini-transaction of a system transaction commits and changes its steps under this
  in shared mode. carry_forward() takes it exclusively: the log and the steps agree. */
  mutable std::shared_mutex m_latch;

  /** The id for the next system transaction, protected by m_latch. */
  uint64_t m_next_id{1};

  /** The steps of the active ones, protected by m_latch. */
  std::map<uint64_t, std::vector<SysTrxStep> *> m_active;

  /** What the log scan found still pending. Only used in recovery. */
  std::map<uint64_t, std::vector<SysTrxStep>> m_recovered;
};

/** One system transaction: step() and commit_step() as many times as it takes, then
commit() or rollback(). Only the thread that runs it calls these. */
class SysTrx {
 public:
  explicit SysTrx(System &sys) noexcept;

  ~SysTrx() noexcept;

  SysTrx(const SysTrx &) = delete;
  SysTrx &operator=(const SysTrx &) = delete;

  /**
   * Log a compensation in the mini-transaction that does the step, which then commits
   * with commit_step() or commit().
   *
   * @param[in,out] mtr         The mini-transaction of the step.
   * @param[in] kind            What the compensation does.
   * @param[in] args            Its arguments.
   * @param[in] len             Their length, at most ARGS_MAX.
   */
  void step(Mtr *mtr, SysTrxKind kind, const void *args, ulint len) noexcept;

  /**
   * Log a compensation that replaces all the earlier ones: from this step on, finishing
   * forward is what a crash does (a half split gets completed, not undone).
   */
  void replace(Mtr *mtr, SysTrxKind kind, const void *args, ulint len) noexcept;

  /** Commit the mini-transaction of a step. */
  void commit_step(Mtr *mtr) noexcept;

  /** End it in the mini-transaction of the last step, and commit that. */
  void commit(Mtr *mtr) noexcept;

  /** Compensate for the steps done so far. */
  void rollback() noexcept;

  /** Compensate for the latest step only: rollback() can stop between steps, on a crash. */
  void rollback_step() noexcept { m_sys.m_systrx->compensate_one(m_id, m_steps); }

  /** The longest compensation arguments. */
  static constexpr ulint ARGS_MAX = 64;

 private:
  void log_step(Mtr *mtr, SysTrxKind kind, const void *args, ulint len, bool replace) noexcept;

  System &m_sys;

  /** Before m_id: a checkpoint can read them as soon as the id is given out. */
  std::vector<SysTrxStep> m_steps;

  uint64_t m_id;
  bool m_done{};

  /** Steps in the mini-transaction that hasn't committed yet. */
  std::vector<SysTrxStep> m_pending;

  /** The pending ones replace the earlier steps. */
  bool m_replace{};
};
