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

#include "btr0btr.h"
#include "btr0blob.h"
#include "buf0lru.h"
#include "mtr0mtr.h"
#include "mtr0log.h"
#include "row0types.h"
#include "row0upd.h"

const byte field_ref_zero[BTR_EXTERN_FIELD_REF_SIZE] = {};

ulint Blob::get_externally_stored_len(rec_t *rec, const ulint *offsets) noexcept {
  ulint local_len{};
  ulint total_extern_len{};

  auto n_fields = rec_offs_n_fields(offsets);

  for (ulint i = 0; i < n_fields; ++i) {
    if (rec_offs_nth_extern(offsets, i)) {

      auto data = (byte *)rec_get_nth_field(rec, offsets, i, &local_len);

      local_len -= BTR_EXTERN_FIELD_REF_SIZE;

      auto extern_len = mach_read_from_4(data + local_len + BTR_EXTERN_LEN + 4);

      total_extern_len += ut_calc_align(extern_len, UNIV_PAGE_SIZE);
    }
  }

  return total_extern_len / UNIV_PAGE_SIZE;
}

void Blob::set_ownership_of_extern_field(rec_t *rec, const Index *index, const ulint *offsets, ulint i, bool val, mtr_t *mtr) noexcept {
  ulint local_len;

  auto data = (byte *)rec_get_nth_field(rec, offsets, i, &local_len);

  ut_a(local_len >= BTR_EXTERN_FIELD_REF_SIZE);

  local_len -= BTR_EXTERN_FIELD_REF_SIZE;

  auto byte_val = mach_read_from_1(data + local_len + BTR_EXTERN_LEN);

  if (val) {
    byte_val = byte_val & (~BTR_EXTERN_OWNER_FLAG);
  } else {
    byte_val = byte_val | BTR_EXTERN_OWNER_FLAG;
  }

  if (likely(mtr != nullptr)) {
    mlog_write_ulint(data + local_len + BTR_EXTERN_LEN, byte_val, MLOG_1BYTE, mtr);
  } else {
    mach_write_to_1(data + local_len + BTR_EXTERN_LEN, byte_val);
  }
}

void Blob::mark_extern_inherited_fields(rec_t *rec, Index *index, const ulint *offsets, const upd_t *update, mtr_t *mtr) noexcept {
  ut_ad(rec_offs_validate(rec, nullptr, offsets));

  if (!rec_offs_any_extern(offsets)) {

    return;
  }

  auto n = rec_offs_n_fields(offsets);

  for (ulint i = 0; i < n; i++) {
    if (rec_offs_nth_extern(offsets, i)) {

      /* Check it is not in updated fields */

      if (update) {
        for (ulint j = 0; j < Row_update::upd_get_n_fields(update); j++) {
          if (update->get_nth_field(j)->m_field_no == i) {

            continue;
          }
        }
      }

      set_ownership_of_extern_field(rec, index, offsets, i, false, mtr);
    }
  }
}

void Blob::mark_dtuple_inherited_extern(DTuple *entry, const upd_t *update) noexcept {
  for (ulint i = 0; i < dtuple_get_n_fields(entry); i++) {
    auto dfield = dtuple_get_nth_field(entry, i);

    if (!dfield_is_ext(dfield)) {
      continue;
    }

    /* Check if it is in updated fields */

    for (ulint j = 0; j < Row_update::upd_get_n_fields(update); j++) {
      if (update->get_nth_field(j)->m_field_no == i) {

        continue;
      }
    }

    auto len = dfield_get_len(dfield);
    auto data = (byte *)dfield_get_data(dfield);

    data[len - BTR_EXTERN_FIELD_REF_SIZE + BTR_EXTERN_LEN] |= BTR_EXTERN_INHERITED_FLAG;
  }
}

void Blob::unmark_extern_fields(rec_t *rec, const Index *index, const ulint *offsets, mtr_t *mtr) noexcept {
  const auto n = rec_offs_n_fields(offsets);

  if (!rec_offs_any_extern(offsets)) {

    return;
  }

  for (ulint i = 0; i < n; ++i) {
    if (rec_offs_nth_extern(offsets, i)) {
      set_ownership_of_extern_field(rec, index, offsets, i, true, mtr);
    }
  }
}

void Blob::unmark_dtuple_extern_fields(DTuple *entry) noexcept {
  for (ulint i = 0; i < dtuple_get_n_fields(entry); ++i) {
    auto dfield = dtuple_get_nth_field(entry, i);

    if (dfield_is_ext(dfield)) {
      auto data = (byte *)dfield_get_data(dfield);
      auto len = dfield_get_len(dfield);

      data[len - BTR_EXTERN_FIELD_REF_SIZE + BTR_EXTERN_LEN] &= ~BTR_EXTERN_OWNER_FLAG;
    }
  }
}

ulint Blob::push_update_extern_fields(DTuple *tuple, const upd_t *update, mem_heap_t *heap) noexcept {
  ulint n;
  ulint n_pushed = 0;
  const upd_field_t *uf;

  n = Row_update::upd_get_n_fields(update);

  for (uf = update->m_fields; n--; ++uf) {
    if (dfield_is_ext(&uf->m_new_val)) {
      auto field = dtuple_get_nth_field(tuple, uf->m_field_no);

      if (!dfield_is_ext(field)) {
        dfield_set_ext(field);
        n_pushed++;
      }

      switch (uf->m_orig_len) {
        byte *data;
        ulint len;
        byte *buf;
        case 0:
          break;
        case BTR_EXTERN_FIELD_REF_SIZE:
          /* Restore the original locally stored part of the column.  In the undo log,
          InnoDB writes a longer prefix of externally stored columns, so that column prefixes
          in secondary indexes can be reconstructed. */
          dfield_set_data(
            field, (byte *)dfield_get_data(field) + dfield_get_len(field) - BTR_EXTERN_FIELD_REF_SIZE, BTR_EXTERN_FIELD_REF_SIZE
          );
          dfield_set_ext(field);
          break;
        default:
          /* Reconstruct the original locally stored part of the column.  The data will have to be copied. */
          ut_a(uf->m_orig_len > BTR_EXTERN_FIELD_REF_SIZE);

          data = (byte *)dfield_get_data(field);
          len = dfield_get_len(field);

          buf = (byte *)mem_heap_alloc(heap, uf->m_orig_len);

          /* Copy the locally stored prefix. */
          memcpy(buf, data, uf->m_orig_len - BTR_EXTERN_FIELD_REF_SIZE);

          /* Copy the BLOB pointer. */
          memcpy(buf + uf->m_orig_len - BTR_EXTERN_FIELD_REF_SIZE, data + len - BTR_EXTERN_FIELD_REF_SIZE, BTR_EXTERN_FIELD_REF_SIZE);

          dfield_set_data(field, buf, uf->m_orig_len);
          dfield_set_ext(field);
      }
    }
  }

  return n_pushed;
}

ulint Blob::blob_get_part_len(const byte *blob_header) noexcept {
  return mach_read_from_4(blob_header + BTR_BLOB_HDR_PART_LEN);
}

ulint Blob::blob_get_next_page_no(const byte *blob_header) noexcept {
  return mach_read_from_4(blob_header + BTR_BLOB_HDR_NEXT_PAGE_NO);
}

void Blob::blob_free(Buf_block *block, mtr_t *mtr) noexcept {
  const auto space = block->get_space();
  const auto page_no = block->get_page_no();

  ut_ad(mtr->memo_contains(block, MTR_MEMO_PAGE_X_FIX));

  mtr->commit();

  mutex_enter(&m_fsp->m_buf_pool->m_mutex);
  mutex_enter(&block->m_mutex);

  /* Only free the block if it is still allocated to the same file page. */

  if (block->get_state() == BUF_BLOCK_FILE_PAGE && block->get_space() == space && block->get_page_no() == page_no) {

    auto block_status = m_fsp->m_buf_pool->m_LRU->free_block(&block->m_page, nullptr);
    ut_a(block_status == Buf_LRU::Block_status::FREED);
  }

  mutex_exit(&m_fsp->m_buf_pool->m_mutex);
  mutex_exit(&block->m_mutex);
}

db_err Blob::store_big_rec_extern_fields(
  const Index *index,
  Buf_block *rec_block,
  rec_t *rec,
  const ulint *offsets,
  big_rec_t *big_rec_vec,
  mtr_t *local_mtr __attribute__((unused))
) noexcept {

  ut_ad(rec_offs_validate(rec, index, offsets));
  ut_ad(local_mtr->memo_contains(index->get_lock(), MTR_MEMO_X_LOCK));
  ut_ad(local_mtr->memo_contains(rec_block, MTR_MEMO_PAGE_X_FIX));
  ut_ad(rec_block->get_frame() == page_align(rec));
  ut_a(index->is_clustered());

  const auto space_id = rec_block->get_space();
  const auto rec_page_no = rec_block->get_page_no();

  ut_a(m_fsp->m_fil->page_get_type(page_align(rec)) == FIL_PAGE_TYPE_INDEX);

  /* We have to create a file segment to the tablespace
  for each field and put the pointer to the field in rec */

  for (ulint i = 0; i < big_rec_vec->n_fields; i++) {
    rec_t *field_ref;

    ut_ad(rec_offs_nth_extern(offsets, big_rec_vec->fields[i].field_no));

    {
      ulint local_len;

      auto col_offset = rec_get_nth_field_offs(offsets, big_rec_vec->fields[i].field_no, &local_len);

      field_ref = rec + col_offset;

      ut_a(local_len >= BTR_EXTERN_FIELD_REF_SIZE);

      local_len -= BTR_EXTERN_FIELD_REF_SIZE;

      field_ref += local_len;
    }

    auto extern_len = big_rec_vec->fields[i].len;

    ut_a(extern_len > 0);

    page_no_t prev_page_no = FIL_NULL;

    for (;;) {
      mtr_t mtr;
      page_no_t hint_page_no;

      mtr.start();

      if (prev_page_no == FIL_NULL) {
        hint_page_no = 1 + rec_page_no;
      } else {
        hint_page_no = prev_page_no + 1;
      }

      auto block = m_btree->page_alloc(index, hint_page_no, FSP_NO_DIR, 0, &mtr);

      if (unlikely(block == nullptr)) {

        mtr.commit();

        return DB_OUT_OF_FILE_SPACE;
      }

      auto page_no = block->get_page_no();
      auto page = block->get_frame();

      if (prev_page_no != FIL_NULL) {

        Buf_pool::Request req {
          .m_rw_latch = RW_X_LATCH,
          .m_page_id = { space_id, prev_page_no },
	        .m_mode = BUF_GET,
	        .m_file = __FILE__,
	        .m_line = __LINE__,
	        .m_mtr = &mtr
        };

        auto prev_block = m_fsp->m_buf_pool->get(req, nullptr);

        buf_block_dbg_add_level(IF_SYNC_DEBUG(prev_block, SYNC_EXTERN_STORAGE));

        auto prev_page = prev_block->get_frame();

        mlog_write_ulint(prev_page + FIL_PAGE_DATA + BTR_BLOB_HDR_NEXT_PAGE_NO, page_no, MLOG_4BYTES, &mtr);
      }

      mlog_write_ulint(page + FIL_PAGE_TYPE, FIL_PAGE_TYPE_BLOB, MLOG_2BYTES, &mtr);

      ulint store_len;

      if (extern_len > (UNIV_PAGE_SIZE - FIL_PAGE_DATA - BTR_BLOB_HDR_SIZE - FIL_PAGE_DATA_END)) {
        store_len = UNIV_PAGE_SIZE - FIL_PAGE_DATA - BTR_BLOB_HDR_SIZE - FIL_PAGE_DATA_END;
      } else {
        store_len = extern_len;
      }

      mlog_write_string(
        page + FIL_PAGE_DATA + BTR_BLOB_HDR_SIZE,
        (const byte *)big_rec_vec->fields[i].data + big_rec_vec->fields[i].len - extern_len,
        store_len,
        &mtr
      );

      mlog_write_ulint(page + FIL_PAGE_DATA + BTR_BLOB_HDR_PART_LEN, store_len, MLOG_4BYTES, &mtr);

      mlog_write_ulint(page + FIL_PAGE_DATA + BTR_BLOB_HDR_NEXT_PAGE_NO, FIL_NULL, MLOG_4BYTES, &mtr);

      extern_len -= store_len;

      Buf_pool::Request req {
        .m_rw_latch = RW_X_LATCH,
        .m_page_id = { space_id, rec_page_no },
        .m_mode = BUF_GET,
        .m_file = __FILE__,
        .m_line = __LINE__,
        .m_mtr = &mtr
      };

      rec_block = m_fsp->m_buf_pool->get(req, nullptr);

      buf_block_dbg_add_level(IF_SYNC_DEBUG(rec_block, SYNC_NO_ORDER_CHECK));

      mlog_write_ulint(field_ref + BTR_EXTERN_LEN, 0, MLOG_4BYTES, &mtr);

      mlog_write_ulint(field_ref + BTR_EXTERN_LEN + 4, big_rec_vec->fields[i].len - extern_len, MLOG_4BYTES, &mtr);

      if (prev_page_no == FIL_NULL) {
        mlog_write_ulint(field_ref + BTR_EXTERN_SPACE_ID, space_id, MLOG_4BYTES, &mtr);

        mlog_write_ulint(field_ref + BTR_EXTERN_PAGE_NO, page_no, MLOG_4BYTES, &mtr);

        mlog_write_ulint(field_ref + BTR_EXTERN_OFFSET, FIL_PAGE_DATA, MLOG_4BYTES, &mtr);
      }

      prev_page_no = page_no;

      mtr.commit();

      if (extern_len == 0) {
        break;
      }
    }
  }

  return DB_SUCCESS;
}

void Blob::check_blob_fil_page_type(space_id_t space_id, page_no_t page_no, const page_t *page, bool read) noexcept {
  auto type = m_fsp->m_fil->page_get_type(page);

  ut_a(space_id == page_get_space_id(page));
  ut_a(page_no == page_get_page_no(page));

  if (unlikely(type != FIL_PAGE_TYPE_BLOB)) {
    ulint flags = m_fsp->m_fil->space_get_flags(space_id);

    if (likely((flags & DICT_TF_FORMAT_MASK) == DICT_TF_FORMAT_51)) {
      /* Old versions of InnoDB did not initialize
      FIL_PAGE_TYPE on BLOB pages.  Do not print
      anything about the type mismatch when reading
      a BLOB page that is in Antelope format.*/
      return;
    }

    log_fatal(std::format(
      "FIL_PAGE_TYPE={} on BLOB: {} space: {} page: {} flags: {}",
      (ulong) type,
      read ? "read" : "purge",
      space_id,
      page_no,
      flags
    ));
  }
}

void Blob::free_externally_stored_field(
  const Index *index,
  byte *field_ref,
  const rec_t *rec,
  const ulint *offsets,
  ulint i,
  trx_rb_ctx rb_ctx,
  mtr_t *local_mtr __attribute__((unused))
) noexcept {

#ifdef UNIV_DEBUG
  ut_ad(local_mtr->memo_contains(index->get_lock(), MTR_MEMO_X_LOCK));
  ut_ad(local_mtr->memo_contains_page(field_ref, MTR_MEMO_PAGE_X_FIX));
  ut_ad(!rec || rec_offs_validate(rec, index, offsets));

  if (rec != nullptr) {
    ulint local_len;
    const byte *f = rec_get_nth_field(rec, offsets, i, &local_len);

    ut_a(local_len >= BTR_EXTERN_FIELD_REF_SIZE);

    local_len -= BTR_EXTERN_FIELD_REF_SIZE;

    f += local_len;

    ut_ad(f == field_ref);
  }
#endif /* UNIV_DEBUG */

  if (unlikely(!memcmp(field_ref, field_ref_zero, BTR_EXTERN_FIELD_REF_SIZE))) {
    /* In the rollback of uncommitted transactions, we may
    encounter a clustered index record whose BLOBs have
    not been written.  There is nothing to free then. */
    ut_a(rb_ctx == RB_RECOVERY || rb_ctx == RB_RECOVERY_PURGE_REC);
    return;
  }

  auto space_id = mach_read_from_4(field_ref + BTR_EXTERN_SPACE_ID);

  if (unlikely(space_id != index->get_space_id())) {
    /* This must be an undo log record in the system tablespace,
    that is, in row_purge_upd_exist_or_extern().
    Currently, externally stored records are stored in the
    same tablespace as the referring records. */
    ut_ad(!page_get_space_id(page_align(field_ref)));
    ut_ad(!rec);
  }

  mtr_t mtr;

  for (;;) {
    mtr.start();

    auto ptr{page_align(field_ref)};

    Buf_pool::Request req {
      .m_rw_latch = RW_X_LATCH,
      .m_page_id = { page_get_space_id(ptr), page_get_page_no(ptr) },
      .m_mode = BUF_GET,
      .m_file = __FILE__,
      .m_line = __LINE__,
      .m_mtr = &mtr
    };

    IF_SYNC_DEBUG({
     auto rec_block = m_fsp->m_buf_pool->get(req, nullptr);
     buf_block_dbg_add_level(rec_block, SYNC_NO_ORDER_CHECK)
    });

    auto page_no = mach_read_from_4(field_ref + BTR_EXTERN_PAGE_NO);

    if (/* There is no external storage data */
        page_no == FIL_NULL
        /* This field does not own the externally stored field */
        || (mach_read_from_1(field_ref + BTR_EXTERN_LEN) & BTR_EXTERN_OWNER_FLAG)
        /* Rollback and inherited field */
        || ((rb_ctx == RB_NORMAL || rb_ctx == RB_RECOVERY) &&
            (mach_read_from_1(field_ref + BTR_EXTERN_LEN) & BTR_EXTERN_INHERITED_FLAG))) {

      /* Do not free */
      mtr.commit();

      return;
    }

    req.m_page_id.m_space_id = space_id;
    req.m_page_id.m_page_no = page_no;
    req.m_line = __LINE__;

    auto ext_block = m_fsp->m_buf_pool->get(req, nullptr);

    buf_block_dbg_add_level(IF_SYNC_DEBUG(ext_block, SYNC_EXTERN_STORAGE));

    auto page = ext_block->get_frame();

    check_blob_fil_page_type(space_id, page_no, page, false);

    auto next_page_no = mach_read_from_4(page + FIL_PAGE_DATA + BTR_BLOB_HDR_NEXT_PAGE_NO);

    /* We must supply the page level (= 0) as an argument
    because we did not store it on the page (we save the
    space overhead from an index page header. */

    m_btree->page_free_low(index, ext_block, 0, &mtr);

    mlog_write_ulint(field_ref + BTR_EXTERN_PAGE_NO, next_page_no, MLOG_4BYTES, &mtr);

    /* Zero out the BLOB length. If the server crashes during the execution of
    this function, trx_rollback_or_clean_all_recovered() could dereference the
    half-deleted BLOB, fetching a wrong prefix for the BLOB. */

    mlog_write_ulint(field_ref + BTR_EXTERN_LEN + 4, 0, MLOG_4BYTES, &mtr);

    /* Commit mtr and release the BLOB block to save memory. */
    blob_free(ext_block, &mtr);
  }
}

void Blob::free_externally_stored_fields(Index *index, rec_t *rec, const ulint *offsets, trx_rb_ctx rb_ctx, mtr_t *mtr) noexcept {
  ut_ad(rec_offs_validate(rec, index, offsets));
  ut_ad(mtr->memo_contains_page(rec, MTR_MEMO_PAGE_X_FIX));
  /* Free possible externally stored fields in the record */

  const auto n_fields = rec_offs_n_fields(offsets);

  for (ulint i = 0; i < n_fields; i++) {
    if (rec_offs_nth_extern(offsets, i)) {
      ulint len;
      auto data = const_cast<byte *>(reinterpret_cast<const byte *>(rec_get_nth_field(rec, offsets, i, &len)));

      ut_a(len >= BTR_EXTERN_FIELD_REF_SIZE);

      free_externally_stored_field(index, data + len - BTR_EXTERN_FIELD_REF_SIZE, rec, offsets, i, rb_ctx, mtr);
    }
  }
}

void Blob::free_updated_extern_fields(
  const Index *index,    
  rec_t *rec,             
  const ulint *offsets,   
  const upd_t *update,    
  trx_rb_ctx rb_ctx, 
  mtr_t *mtr
) noexcept {
  ut_ad(rec_offs_validate(rec, index, offsets));
  ut_ad(mtr->memo_contains_page(rec, MTR_MEMO_PAGE_X_FIX));

  /* Free possible externally stored fields in the record */

  const auto n_fields = Row_update::upd_get_n_fields(update);

  for (ulint i = 0; i < n_fields; i++) {
    auto ufield = update->get_nth_field(i);

    if (rec_offs_nth_extern(offsets, ufield->m_field_no)) {
      ulint len;
      auto data = (byte *)rec_get_nth_field(rec, offsets, ufield->m_field_no, &len);
      ut_a(len >= BTR_EXTERN_FIELD_REF_SIZE);

      free_externally_stored_field(index, data + len - BTR_EXTERN_FIELD_REF_SIZE, rec, offsets, ufield->m_field_no, rb_ctx, mtr);
    }
  }
}

ulint Blob::copy_blob_prefix(byte *buf, ulint len, space_id_t space_id, page_no_t page_no, ulint offset) noexcept{
  ulint copied_len = 0;

  for (;;) {
    mtr_t mtr;

    mtr.start();

    Buf_pool::Request req {
      .m_rw_latch = RW_S_LATCH,
      .m_page_id = { space_id, page_no },
      .m_mode = BUF_GET,
      .m_file = __FILE__,
      .m_line = __LINE__,
      .m_mtr = &mtr
    };

    auto block = m_fsp->m_buf_pool->get(req, nullptr);
    const auto page = block->get_frame();

    buf_block_dbg_add_level(IF_SYNC_DEBUG(block, SYNC_EXTERN_STORAGE));

    check_blob_fil_page_type(space_id, page_no, page, true);

    const auto blob_header = page + offset;
    auto part_len = blob_get_part_len(blob_header);
    auto copy_len = ut_min(part_len, len - copied_len);

    memcpy(buf + copied_len, blob_header + BTR_BLOB_HDR_SIZE, copy_len);
    copied_len += copy_len;

    page_no = blob_get_next_page_no(blob_header);

    mtr.commit();

    if (page_no == FIL_NULL || copy_len != part_len) {
      return copied_len;
    }

    /* On other BLOB pages except the first the BLOB header
    always is at the page data start: */

    offset = FIL_PAGE_DATA;

    ut_ad(copied_len <= len);
  }
}

ulint Blob::copy_externally_stored_field_prefix_low(byte *buf, ulint len, space_id_t space_id, page_no_t page_no, ulint offset) noexcept {
  if (unlikely(len == 0)) {
    return 0;
  }

  return copy_blob_prefix(buf, len, space_id, page_no, offset);
}

ulint Blob::copy_externally_stored_field_prefix(byte *buf, ulint len, const byte *data, ulint local_len) noexcept {
  ut_a(local_len >= BTR_EXTERN_FIELD_REF_SIZE);

  local_len -= BTR_EXTERN_FIELD_REF_SIZE;

  if (unlikely(local_len >= len)) {
    memcpy(buf, data, len);
    return len;
  }

  memcpy(buf, data, local_len);
  data += local_len;

  ut_a(memcmp(data, field_ref_zero, BTR_EXTERN_FIELD_REF_SIZE));

  if (!mach_read_from_4(data + BTR_EXTERN_LEN + 4)) {
    /* The externally stored part of the column has been
    (partially) deleted.  Signal the half-deleted BLOB
    to the caller. */

    return 0;

  } else {
    const space_id_t space_id = mach_read_from_4(data + BTR_EXTERN_SPACE_ID);
    const page_no_t page_no = mach_read_from_4(data + BTR_EXTERN_PAGE_NO);
    const ulint offset = mach_read_from_4(data + BTR_EXTERN_OFFSET);

    return local_len + copy_externally_stored_field_prefix_low(buf + local_len, len - local_len, space_id, page_no, offset);
  }
}

byte *Blob::copy_externally_stored_field(ulint *len, const byte *data, ulint local_len, mem_heap_t *heap) noexcept {
  ut_a(local_len >= BTR_EXTERN_FIELD_REF_SIZE);

  local_len -= BTR_EXTERN_FIELD_REF_SIZE;

  const space_id_t space_id = mach_read_from_4(data + local_len + BTR_EXTERN_SPACE_ID);
  const page_no_t page_no = mach_read_from_4(data + local_len + BTR_EXTERN_PAGE_NO);
  const auto offset = mach_read_from_4(data + local_len + BTR_EXTERN_OFFSET);

  /* Currently a BLOB cannot be bigger than 4 GB; we
  leave the 4 upper bytes in the length field unused */

  const auto extern_len = mach_read_from_4(data + local_len + BTR_EXTERN_LEN + 4);

  auto buf = static_cast<byte *>(mem_heap_alloc(heap, local_len + extern_len));

  memcpy(buf, data, local_len);

  *len = local_len + copy_externally_stored_field_prefix_low(buf + local_len, extern_len, space_id, page_no, offset);

  return buf;
}

byte *Blob::copy_externally_stored_field(const rec_t *rec, const ulint *offsets, ulint no, ulint *len, mem_heap_t *heap) noexcept {
  ut_a(rec_offs_nth_extern(offsets, no));

  /* An externally stored field can contain some initial
  data from the field, and in the last 20 bytes it has the
  space id, page number, and offset where the rest of the
  field data is stored, and the data length in addition to
  the data stored locally. We may need to store some data
  locally to get the local record length above the 128 byte
  limit so that field offsets are stored in two bytes, and
  the extern bit is available in those two bytes. */

  ulint local_len;

  auto data = static_cast<const byte *>(rec_get_nth_field(rec, offsets, no, &local_len));

  return copy_externally_stored_field(len, data, local_len, heap);
}