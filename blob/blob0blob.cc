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

/** @file blob/blob0blob.cc
The fields of a record stored apart from it: the references, who owns what, and the calls to the
store of the instance (blob0store.h) for the rest. */

#include "blob0blob.h"

#include "mach0data.h"
#include "mtr0log.h"
#include "mtr0mtr.h"
#include "row0upd.h"
#include "srv0sys.h"

#include <algorithm>
#include <array>
#include <cstring>

const byte field_ref_zero[BTR_EXTERN_FIELD_REF_SIZE] = {};

namespace {

/** @return the length of the field a reference refers to, 0 for nothing. */
[[nodiscard]] inline ulint ref_len(const byte *field_ref) noexcept { return mach_read_from_4(field_ref + BTR_EXTERN_LEN + 4); }

/** Write len bytes of a reference, 4 at a time: logged if it's on a page, mtr says. A record's page
takes MLOG_4BYTES, not MLOG_WRITE_STRING (recovery refuses a string on an index page). */
inline void ref_write(byte *ptr, const byte *src, ulint len, Mtr *mtr) noexcept {
  ut_ad(len % 4 == 0);

  if (mtr == nullptr) {
    memcpy(ptr, src, len);
    return;
  }

  for (ulint i{}; i < len; i += 4) {
    mlog_write_ulint(ptr + i, mach_read_from_4(src + i), MLOG_4BYTES, mtr);
  }
}

/** Write 4 bytes of a reference. */
inline void ref_write4(byte *ptr, ulint val, Mtr *mtr) noexcept {
  if (mtr != nullptr) {
    mlog_write_ulint(ptr, val, MLOG_4BYTES, mtr);
  } else {
    mach_write_to_4(ptr, val);
  }
}

/** @return the reference at the end of field i of rec. */
[[nodiscard]] inline byte *ref_of(Rec rec, const RecOffs &offsets, ulint i) noexcept {
  ulint local_len;
  auto data = const_cast<byte *>(rec.get_nth_field(offsets, i, &local_len).get());

  ut_a(local_len >= BTR_EXTERN_FIELD_REF_SIZE);

  return data + local_len - BTR_EXTERN_FIELD_REF_SIZE;
}

}  // namespace

void Blob::prepare(System &sys, const Index *index, Mtr *mtr, bool space_latched) noexcept {
  sys.blob_store().prepare(index, mtr, space_latched);
}

void Blob::set_ownership_of_extern_field(
  Rec rec, const Index *index, const ulint *offsets, ulint i, bool val, Mtr *mtr
) noexcept {
  ulint local_len;

  auto data = (byte *)rec.get_nth_field(offsets, i, &local_len).get();

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

void Blob::mark_extern_inherited_fields(Rec rec, const Index *index, const RecOffs &offsets, const Upd *update, Mtr *mtr) noexcept {
  ut_ad(rec.offs_validate(nullptr, offsets));

  if (!offsets.any_extern()) {

    return;
  }

  auto n = offsets.n_fields();

  for (ulint i = 0; i < n; i++) {
    /* A field the update replaces is not inherited: the record keeps it. */
    if (rec_offs_nth_extern(offsets, i) && (update == nullptr || update->get_field(i) == nullptr)) {
      set_ownership_of_extern_field(rec, index, offsets, i, false, mtr);
    }
  }
}

void Blob::mark_dtuple_inherited_extern(DTuple *entry, const Upd *update) noexcept {
  for (ulint i = 0; i < dtuple_get_n_fields(entry); i++) {
    auto dfield = dtuple_get_nth_field(entry, i);

    /* A field the update replaces is not inherited. */
    if (!dfield_is_ext(dfield) || update->get_field(i) != nullptr) {
      continue;
    }

    auto len = dfield_get_len(dfield);
    auto data = (byte *)dfield_get_data(dfield);

    data[len - BTR_EXTERN_FIELD_REF_SIZE + BTR_EXTERN_LEN] |= BTR_EXTERN_INHERITED_FLAG;
  }
}

void Blob::unmark_extern_fields(Rec rec, const Index *index, const RecOffs &offsets, Mtr *mtr) noexcept {
  const auto n = offsets.n_fields();

  if (!offsets.any_extern()) {

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

ulint Blob::push_update_extern_fields(DTuple *tuple, const Upd *update, MemHeap *heap) noexcept {
  ulint n;
  ulint n_pushed = 0;
  const UpdField *uf;

  n = RowUpdate::upd_get_n_fields(update);

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
          memcpy(
            buf + uf->m_orig_len - BTR_EXTERN_FIELD_REF_SIZE, data + len - BTR_EXTERN_FIELD_REF_SIZE, BTR_EXTERN_FIELD_REF_SIZE
          );

          dfield_set_data(field, buf, uf->m_orig_len);
          dfield_set_ext(field);
      }
    }
  }

  return n_pushed;
}


ulint Blob::get_externally_stored_len(System &sys, Rec rec, const RecOffs &offsets) noexcept {
  ulint n{};

  for (ulint i{}; i < offsets.n_fields(); ++i) {
    if (rec_offs_nth_extern(offsets, i)) {
      n += sys.blob_store().n_pages(ref_len(ref_of(rec, offsets, i)));
    }
  }

  return n;
}

bool Blob::free_ref(System &sys, const Index *index, const byte *field_ref, TrxRbCtx rb_ctx, Mtr *mtr) noexcept {
  /* Nothing to free: a record whose fields a crash, or the lack of space, kept from being stored, or
  a field that's freed already. */
  if (ref_len(field_ref) == 0) {
    return false;
  }

  const auto flags = mach_read_from_1(field_ref + BTR_EXTERN_LEN);

  /* Not the owner's to free; nor an inherited field in a rollback. */
  if ((flags & BTR_EXTERN_OWNER_FLAG) || ((rb_ctx == RB_NORMAL || rb_ctx == RB_RECOVERY) && (flags & BTR_EXTERN_INHERITED_FLAG))) {
    return false;
  }

  sys.blob_store().free(index, field_ref, mtr);

  return true;
}

void Blob::free_externally_stored_field(System &sys, const Index *index, byte *field_ref, TrxRbCtx rb_ctx, Mtr *mtr) noexcept {
  if (free_ref(sys, index, field_ref, rb_ctx, mtr)) {
    /* In the same change as the free: a reader, or a free again after a crash, sees nothing. */
    ref_write4(field_ref + BTR_EXTERN_LEN + 4, 0, mtr);
  }
}

void Blob::free_externally_stored_fields(System &sys, const Index *index, Rec rec, const RecOffs &offsets, TrxRbCtx rb_ctx, Mtr *mtr) noexcept {
  ut_ad(rec.offs_validate(index, offsets));

  for (ulint i{}; i < offsets.n_fields(); ++i) {
    if (rec_offs_nth_extern(offsets, i)) {
      free_externally_stored_field(sys, index, ref_of(rec, offsets, i), rb_ctx, mtr);
    }
  }
}

void Blob::free_updated_extern_fields(
  System &sys, const Index *index, Rec rec, const RecOffs &offsets, const Upd *update, TrxRbCtx rb_ctx, Mtr *mtr
) noexcept {
  ut_ad(rec.offs_validate(index, offsets));

  for (ulint i{}; i < RowUpdate::upd_get_n_fields(update); ++i) {
    const auto field_no = update->get_nth_field(i)->m_field_no;

    if (rec_offs_nth_extern(offsets, field_no)) {
      free_externally_stored_field(sys, index, ref_of(rec, offsets, field_no), rb_ctx, mtr);
    }
  }
}

DbErr Blob::store_big_rec_extern_fields(
  System &sys, const Index *index, Rec rec, const RecOffs &offsets, const BigRec &big_rec, uint64_t near, Mtr *mtr
) noexcept {
  ut_ad(rec.offs_validate(index, offsets));
  ut_a(index->is_clustered());

  auto &store = sys.blob_store();

  store.prepare(index, mtr, false);

  for (ulint i{}; i < big_rec.n_fields; ++i) {
    const auto &field = big_rec.fields[i];

    ut_ad(rec_offs_nth_extern(offsets, field.field_no));
    ut_a(field.len > 0);

    std::array<byte, BlobStore::ID_SIZE> id;

    if (const auto err = store.write(index, static_cast<const byte *>(field.data), field.len, near, id.data(), mtr); err != DB_SUCCESS) {
      /* The fields before this one are stored and referred to: the record is whole as far as it
      goes, a reference of length 0 refers to nothing. */
      return err;
    }

    auto field_ref = ref_of(rec, offsets, field.field_no);
    std::array<byte, 8> len{};

    /* Owned, not inherited: the flags are 0. */
    mach_write_to_4(len.data() + 4, field.len);

    ref_write(field_ref, id.data(), id.size(), mtr);
    ref_write(field_ref + BTR_EXTERN_LEN, len.data(), len.size(), mtr);
  }

  return DB_SUCCESS;
}

ulint Blob::copy_externally_stored_field_prefix(System &sys, byte *buf, ulint len, const byte *data, ulint local_len) noexcept {
  ut_a(local_len >= BTR_EXTERN_FIELD_REF_SIZE);

  local_len -= BTR_EXTERN_FIELD_REF_SIZE;

  if (unlikely(local_len >= len)) {
    memcpy(buf, data, len);
    return len;
  }

  memcpy(buf, data, local_len);
  data += local_len;

  const auto ext_len = ref_len(data);

  if (ext_len == 0) {
    /* Freed, or not stored yet: the caller is told by the 0. */
    return 0;
  }

  return local_len + sys.blob_store().read(data, buf + local_len, std::min(len - local_len, ext_len));
}

byte *Blob::copy_externally_stored_field(System &sys, ulint *len, const byte *data, ulint local_len, MemHeap *heap) noexcept {
  ut_a(local_len >= BTR_EXTERN_FIELD_REF_SIZE);

  local_len -= BTR_EXTERN_FIELD_REF_SIZE;

  const auto ext_len = ref_len(data + local_len);
  auto buf = static_cast<byte *>(mem_heap_alloc(heap, local_len + ext_len));

  memcpy(buf, data, local_len);

  *len = local_len + (ext_len == 0 ? 0 : sys.blob_store().read(data + local_len, buf + local_len, ext_len));

  return buf;
}

byte *Blob::copy_externally_stored_field(System &sys, const Rec rec, const RecOffs &offsets, ulint no, ulint *len, MemHeap *heap) noexcept {
  ut_a(offsets.nth_extern(no));

  ulint local_len;
  const auto data = static_cast<const byte *>(rec.get_nth_field(offsets, no, &local_len).get());

  return copy_externally_stored_field(sys, len, data, local_len, heap);
}
