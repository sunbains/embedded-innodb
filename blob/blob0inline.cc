/****************************************************************************
Copyright (c) 1994, 2010, Innobase Oy. All Rights Reserved.
Copyright (c) 2024 Sunny Bains. All rights reserved.

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

/** @file blob/blob0inline.cc
The default store: a field is a chain of pages of the index's leaf segment, see blob0inline.h.

A write and a free are in the caller's mini-transaction, all the pages of the field with the
record's page: one atomic change, logged with the plain page writes (MLOG_nBYTES and
MLOG_WRITE_STRING) and the segment's own records. No mini-transaction of the store's, no nesting.
The pages of a field being written or freed are reachable only through the record, which the
caller holds X: nobody else waits on them.

The latches: the caller's, the record's page among them, then the tablespace latch (prepare()),
then the pages of the field. Page allocation in a split takes them in the same order. A read
latches one page at a time, S, in a mini-transaction of its own that holds nothing else. */

#include "blob0inline.h"

#include "btr0btr.h"
#include "buf0buf.h"
#include "dict0dict.h"
#include "fil0fil.h"
#include "fsp0fsp.h"
#include "mach0data.h"
#include "mtr0log.h"
#include "mtr0mtr.h"
#include "srv0sys.h"

#include <algorithm>

namespace {

struct InlineStore final : BlobStore {
  explicit InlineStore(System &sys) noexcept : m_sys(sys) {}

  void prepare(const Index *index, Mtr *mtr, bool space_latched) noexcept override {
    ut_a(mtr != nullptr);

    if (!space_latched) {
      m_sys.m_fsp->latch(index->get_space_id(), mtr);
    }
  }

  DbErr write(const Index *index, const byte *data, ulint len, uint64_t near, byte *id, Mtr *mtr) noexcept override {
    ut_a(len > 0);
    ut_a(mtr != nullptr);

    const auto space_id = index->get_space_id();
    const auto n_pages = this->n_pages(len);
    ulint n_reserved{};

    /* All the pages of the field before any of them: out of space, nothing has changed. */
    if (!m_sys.m_fsp->reserve_free_extents(&n_reserved, space_id, n_pages / FSP_EXTENT_SIZE + 2, FSP_NORMAL, mtr)) {
      return DB_OUT_OF_FILE_SPACE;
    }

    page_t *prev{};
    page_no_t first{FIL_NULL};
    page_no_t hint = page_no_t(near) + 1;

    for (ulint done{}; done < len;) {
      auto block = m_sys.m_btree->page_alloc(index, hint, FSP_NO_DIR, 0, mtr);

      /* The extents are reserved. */
      ut_a(block != nullptr);

      const auto page_no = block->get_page_no();
      const auto page = block->get_frame();
      const auto part = std::min(len - done, BTR_BLOB_PART_MAX);
      const auto hdr = page + FIL_PAGE_DATA;

      mlog_write_ulint(page + FIL_PAGE_TYPE, FIL_PAGE_TYPE_BLOB, MLOG_2BYTES, mtr);
      mlog_write_string(hdr + BTR_BLOB_HDR_SIZE, data + done, part, mtr);
      mlog_write_ulint(hdr + BTR_BLOB_HDR_PART_LEN, part, MLOG_4BYTES, mtr);
      mlog_write_ulint(hdr + BTR_BLOB_HDR_NEXT_PAGE_NO, FIL_NULL, MLOG_4BYTES, mtr);

      /* The page before is in mtr, X: linked in the same change. */
      if (prev != nullptr) {
        mlog_write_ulint(prev + FIL_PAGE_DATA + BTR_BLOB_HDR_NEXT_PAGE_NO, page_no, MLOG_4BYTES, mtr);
      } else {
        first = page_no;
      }

      prev = page;
      hint = page_no + 1;
      done += part;
    }

    m_sys.m_fil->space_release_free_extents(space_id, n_reserved);

    mach_write_to_4(id + BTR_EXTERN_SPACE_ID, space_id);
    mach_write_to_4(id + BTR_EXTERN_PAGE_NO, first);
    mach_write_to_4(id + BTR_EXTERN_OFFSET, FIL_PAGE_DATA);

    return DB_SUCCESS;
  }

  ulint read(const byte *id, byte *buf, ulint len) noexcept override {
    const space_id_t space_id = mach_read_from_4(id + BTR_EXTERN_SPACE_ID);
    page_no_t page_no = mach_read_from_4(id + BTR_EXTERN_PAGE_NO);
    ulint offset = mach_read_from_4(id + BTR_EXTERN_OFFSET);
    ulint copied{};

    while (copied < len && page_no != FIL_NULL) {
      Mtr mtr;

      mtr.start();

      const auto page = get(space_id, page_no, RW_S_LATCH, &mtr);
      const auto hdr = page + offset;
      const auto part = mach_read_from_4(hdr + BTR_BLOB_HDR_PART_LEN);
      const auto n = std::min<ulint>(part, len - copied);

      memcpy(buf + copied, hdr + BTR_BLOB_HDR_SIZE, n);
      copied += n;
      page_no = mach_read_from_4(hdr + BTR_BLOB_HDR_NEXT_PAGE_NO);

      mtr.commit(m_sys);

      /* After the first page the header is at the start of the data. */
      offset = FIL_PAGE_DATA;
    }

    return copied;
  }

  void free(const Index *index, const byte *id, Mtr *mtr) noexcept override {
    ut_a(mtr != nullptr);

    const space_id_t space_id = mach_read_from_4(id + BTR_EXTERN_SPACE_ID);

    ut_a(space_id == index->get_space_id());

    /* The whole chain in mtr: it and the reference that the caller zeroes are one change. */
    for (page_no_t page_no = mach_read_from_4(id + BTR_EXTERN_PAGE_NO); page_no != FIL_NULL;) {
      BufBlock *block;

      (void)get(space_id, page_no, RW_X_LATCH, mtr, &block);

      page_no = mach_read_from_4(block->get_frame() + FIL_PAGE_DATA + BTR_BLOB_HDR_NEXT_PAGE_NO);

      /* Level 0: the pages are of the leaf segment, see write(). */
      m_sys.m_btree->page_free_low(index, block, 0, mtr);
    }
  }

  ulint n_pages(ulint len) const noexcept override { return (len + BTR_BLOB_PART_MAX - 1) / BTR_BLOB_PART_MAX; }

  /** A page of a chain, latched in mtr, checked for what it is. */
  [[nodiscard]] const page_t *get(space_id_t space_id, page_no_t page_no, ulint latch, Mtr *mtr, BufBlock **out = nullptr) noexcept {
    BufPool::Request req{
      .m_rw_latch = latch, .m_page_id = {space_id, page_no}, .m_mode = BUF_GET, .m_file = __FILE__, .m_line = __LINE__, .m_mtr = mtr
    };

    auto block = m_sys.m_buf_pool->get(req, nullptr);

    buf_block_dbg_add_level();

    const auto page = block->get_frame();

    if (unlikely(Fil::page_get_type(page) != FIL_PAGE_TYPE_BLOB || page_get_space_id(page) != space_id || page_get_page_no(page) != page_no)) {
      log_fatal(std::format("not a page of a stored field: type {} space {} page {}", ulint(Fil::page_get_type(page)), space_id, page_no));
    }

    if (out != nullptr) {
      *out = block;
    }

    return page;
  }

  System &m_sys;
};

}  // namespace

std::unique_ptr<BlobStore> blob_store_inline(System &sys) { return std::make_unique<InlineStore>(sys); }
