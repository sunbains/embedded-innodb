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

/** @file include/blob0blob.h
The fields of a record stored apart from it, whatever the store (blob0store.h): the reference at
the end of such a field, who owns what it refers to, reading, freeing, and storing the fields a
record is too big for. Nothing here knows how a store keeps a field. */

#pragma once

#include "blob0store.h"
#include "btr0types.h"
#include "innodb0types.h"
#include "mem0types.h"
#include "rem0rec.h"
#include "row0types.h"
#include "trx0types.h"

class System;
struct Mtr;
struct Index;

/** The part of a reference every store has, after its own BlobStore::ID_SIZE bytes. @{ */

/** 8 bytes: the flags in the first, the length of the field the record doesn't keep in the last 4.
A length of 0 is a reference to nothing: not written yet, or freed. */
constexpr ulint BTR_EXTERN_LEN = BlobStore::ID_SIZE;

/** The most significant bit of the first byte of the length: set if this reference doesn't own the
field. Only the owner frees it, in purge. */
constexpr ulint BTR_EXTERN_OWNER_FLAG = 128;

/** The second most significant bit: the field is inherited from an earlier version of the row, a
rollback doesn't free it. */
constexpr ulint BTR_EXTERN_INHERITED_FLAG = 64;

/* @} */

static_assert(BTR_EXTERN_LEN + 8 == BTR_EXTERN_FIELD_REF_SIZE);

/** A reference to nothing, as dtuple_convert_big_rec() makes it before the field is stored. */
extern const byte field_ref_zero[BTR_EXTERN_FIELD_REF_SIZE];

struct Blob {
  Blob() = default;

  /**
   * Take what freeing and storing fields needs, in the caller's mini-transaction. Once per
   * mini-transaction, before free_*() and after the caller's own latches.
   *
   * @param[in] sys             The instance.
   * @param[in] index           The index of the records.
   * @param[in,out] mtr         The caller's mini-transaction.
   * @param[in] space_latched   mtr holds the tablespace latch of the index already.
   */
  void prepare(System &sys, const Index *index, Mtr *mtr, bool space_latched) noexcept;

  /**
   * Set or clear the owner flag of field i of rec.
   *
   * @param[in,out] mtr         Logs the change, nullptr if rec isn't on a page.
   */
  void set_ownership_of_extern_field(Rec rec, const Index *index, const ulint *offsets, ulint i, bool val, Mtr *mtr) noexcept;

  /**
   * Free the fields of rec that update gives new values: the rollback of an update that stored them.
   * mtr is prepared.
   */
  void free_updated_extern_fields(
    System &sys, const Index *index, Rec rec, const RecOffs &offsets, const Upd *update, TrxRbCtx rb_ctx, Mtr *mtr
  ) noexcept;

  /** Free the fields rec owns, before rec is removed. mtr is prepared. */
  void free_externally_stored_fields(System &sys, const Index *index, Rec rec, const RecOffs &offsets, TrxRbCtx rb_ctx, Mtr *mtr) noexcept;

  /** @return the pages the stored fields of rec take. */
  [[nodiscard]] ulint get_externally_stored_len(System &sys, Rec rec, const RecOffs &offsets) noexcept;

  /**
   * Copy a field and what it refers to.
   *
   * @param[in] data            The field in the record: a prefix, then the reference.
   * @param[in] local_len       Its length.
   * @param[out] len            The length of the copy.
   */
  [[nodiscard]] byte *copy_externally_stored_field(System &sys, ulint *len, const byte *data, ulint local_len, MemHeap *heap) noexcept;

  /** The same for field no of rec. */
  [[nodiscard]] byte *copy_externally_stored_field(System &sys, const Rec rec, const RecOffs &offsets, ulint no, ulint *len, MemHeap *heap) noexcept;

  /**
   * Copy the first len bytes of a field and what it refers to.
   *
   * @return the bytes copied, 0 if the field refers to nothing (it's freed, or not stored yet).
   */
  [[nodiscard]] ulint copy_externally_stored_field_prefix(System &sys, byte *buf, ulint len, const byte *data, ulint local_len) noexcept;

  /** Mark the fields of rec that the update doesn't replace as not owned by it. */
  void mark_extern_inherited_fields(Rec rec, const Index *index, const RecOffs &offsets, const Upd *update, Mtr *mtr) noexcept;

  /** Mark the stored fields of entry that the update doesn't replace as inherited. */
  void mark_dtuple_inherited_extern(DTuple *entry, const Upd *update) noexcept;

  /** Mark the stored fields of entry as owned. */
  void unmark_dtuple_extern_fields(DTuple *entry) noexcept;

  /** Mark the stored fields of rec as owned by it. */
  void unmark_extern_fields(Rec rec, const Index *index, const RecOffs &offsets, Mtr *mtr) noexcept;

  /**
   * Store the fields rec is too big for, and put their references in rec. Everything is in mtr, it
   * holds the page of rec: the fields and the references are one change. Prepares mtr itself.
   *
   * @param[in] near            Where rec is, for the store's locality; 0 if nowhere.
   * @param[in,out] mtr         The caller's mini-transaction, nullptr if rec isn't on a page.
   */
  [[nodiscard]] DbErr store_big_rec_extern_fields(
    System &sys, const Index *index, Rec rec, const RecOffs &offsets, const BigRec &big_rec, uint64_t near, Mtr *mtr
  ) noexcept;

  /**
   * Free what a reference refers to if it owns it, and make it refer to nothing.
   *
   * @param[in,out] field_ref   The reference, in a record or an undo record.
   * @param[in,out] mtr         Prepared, logs the change of field_ref; nullptr if it isn't on a page.
   */
  void free_externally_stored_field(System &sys, const Index *index, byte *field_ref, TrxRbCtx rb_ctx, Mtr *mtr) noexcept;

  /**
   * Free what a reference refers to if it owns it. The reference itself isn't changed: it's a copy,
   * of a record that doesn't refer to the field any more.
   *
   * @return true if it was freed.
   */
  [[nodiscard]] bool free_ref(System &sys, const Index *index, const byte *field_ref, TrxRbCtx rb_ctx, Mtr *mtr) noexcept;

  /** @return the number of fields of the update that are stored apart, added to tuple. */
  [[nodiscard]] ulint push_update_extern_fields(DTuple *tuple, const Upd *update, MemHeap *heap) noexcept;
};
