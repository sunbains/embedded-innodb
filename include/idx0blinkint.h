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

/** @file include/idx0blinkint.h
What the files of the B-link tree share, not for anyone else: idx0blink.cc, idx0blinkread.cc. */

#pragma once

#include "innodb0types.h"

#include "buf0buf.h"
#include "dict0types.h"
#include "idx0blink.h"
#include "mach0data.h"
#include "page0cur.h"
#include "page0page.h"

namespace blink {

/** @return true if the page isn't on the tree of index any more. */
[[nodiscard]] inline bool is_dead(const page_t *page, const Index *index) noexcept {
  return mach_read_from_8(page + PAGE_HEADER + PAGE_INDEX_ID) != index->m_id;
}

/** @return the mode for a search on a non-leaf page, for the leaf mode. */
[[nodiscard]] inline ulint node_mode(ulint mode) noexcept {
  switch (mode) {
    case PAGE_CUR_GE:
      return PAGE_CUR_L;
    case PAGE_CUR_G:
      return PAGE_CUR_LE;
    default:
      ut_ad(mode == PAGE_CUR_L || mode == PAGE_CUR_LE);
      return mode;
  }
}

/* A read with no latch races with the writer it then fails to validate against, on purpose. */
#ifdef __SANITIZE_THREAD__
extern "C" void AnnotateIgnoreReadsBegin(const char *file, int line);
extern "C" void AnnotateIgnoreReadsEnd(const char *file, int line);

struct RacyReads {
  RacyReads() noexcept { AnnotateIgnoreReadsBegin(__FILE__, __LINE__); }
  ~RacyReads() noexcept { AnnotateIgnoreReadsEnd(__FILE__, __LINE__); }
};
#else
struct RacyReads {};
#endif /* __SANITIZE_THREAD__ */

/** @return true if a search in this mode has to go on to the right sibling of a page. */
[[nodiscard]] bool past_high_key(const Index *index, const page_t *page, const DTuple *key, ulint mode) noexcept;

/** Count a read of a page that had to go again, for tests. */
void node_retried(System &sys) noexcept;

/**
 * Run a read of the frame of a block that's buffer fixed by the caller, until it has seen the page
 * as it was at one time. It takes no latch and waits for nothing but a writer to be gone: nothing is
 * held here that anyone could be waiting for.
 *
 * @return the version it read at.
 * @param[in] read  Reads the frame, whatever is on it. false if it can't be what it should be: that
 *                  means something only when the version says it was stable, and then the page is
 *                  corrupt, the same as for the assertions of a search under a latch.
 */
template <typename Read>
uint64_t read_stable(System &sys, const BufBlock *block, Read read) noexcept {
  /* A split going up takes longer than it's worth spinning for: then it sleeps. */
  constexpr int N_SPINS = 32;

  for (int i{};; ++i) {
    const auto version = block->read_begin();

    if ((version & 1) == 0) {
      bool ok;

      {
        [[maybe_unused]] RacyReads racy;

        ok = read();
      }

      if (block->read_is_valid(version)) {
        ut_a(ok);

        return version;
      }
    }

    node_retried(sys);

    if (i < N_SPINS) {
      UT_RELAX_CPU();
    } else {
      block->read_wait();
    }
  }
}

}  // namespace blink
