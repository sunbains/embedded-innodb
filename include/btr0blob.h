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

#pragma once

#include "btr0types.h"
#include "innodb0types.h"
#include "mem0types.h"
#include "page0types.h"
#include "rem0rec.h"
#include "row0types.h"
#include "trx0types.h"

struct Fil;
struct FSP;
struct mtr_t;
struct Btree;
struct Buf_pool;
struct Buf_block;
struct Index;

/** The structure of a BLOB part header */
/* @{ */
/*--------------------------------------*/

/** BLOB part len on this page */
constexpr ulint BTR_BLOB_HDR_PART_LEN = 0;

/** next BLOB part page no, FIL_NULL if none */
constexpr ulint BTR_BLOB_HDR_NEXT_PAGE_NO = 4;

/*--------------------------------------*/
/** Size of a BLOB  part header, in bytes */
constexpr ulint BTR_BLOB_HDR_SIZE = 8;

/* @} */

/** A BLOB field reference full of zero, for use in assertions and tests.
Initially, BLOB field references are set to zero, in
dtuple_convert_big_rec(). */
extern const byte field_ref_zero[BTR_EXTERN_FIELD_REF_SIZE];

/** The reference in a field for which data is stored on a different page.
  The reference is at the end of the 'locally' stored part of the field.
  'Locally' means storage in the index record.
  We store locally a long enough prefix of each column so that we can determine
  the ordering parts of each index record without looking into the externally
  stored part. */
/*-------------------------------------- @{ */

/** space id where stored */
constexpr ulint BTR_EXTERN_SPACE_ID = 0;

/** page no where stored */
constexpr ulint BTR_EXTERN_PAGE_NO = 4;

/** offset of BLOB header on that page */
constexpr ulint BTR_EXTERN_OFFSET = 8;

/** 8 bytes containing the length of the externally stored part of the
  BLOB. The 2 highest bits are reserved to the flags below. */
constexpr ulint BTR_EXTERN_LEN = 12;

/** The most significant bit of BTR_EXTERN_LEN (i.e., the most
  significant bit of the byte at smallest address) is set to 1 if this
  field does not 'own' the externally stored field; only the owner field
  is allowed to free the field in purge! */
constexpr ulint BTR_EXTERN_OWNER_FLAG = 128;

/** If the second most significant bit of BTR_EXTERN_LEN (i.e., the
  second most significant bit of the byte at smallest address) is 1 then
  it means that the externally stored field was inherited from an
  earlier version of the row.  In rollback we are not allowed to free an
  inherited external field. */
constexpr ulint BTR_EXTERN_INHERITED_FLAG = 64;

/* } */

struct Blob {

  /**
   * Constructor.
   *
   * @param[in] fsp             File space handler.
   */
  explicit Blob(FSP *fsp, Btree *btree) : m_fsp{fsp}, m_btree(btree) {}

  /**
   * Sets the ownership bit of an externally stored field in a record.
   *
   * @param[in,out] rec         Clustered index record.
   * @param[in] index           Index of the page.
   * @param[in] offsets         Array returned by Phy_rec::get_col_offsets().
   * @param[in] i               Field number.
   * @param[in] val             Value to set.
   * @param[in] mtr             Mini-transaction handle, or nullptr if not logged.
   */
  void set_ownership_of_extern_field(Rec rec, const Index *index, const ulint *offsets, ulint i, bool val, mtr_t *mtr) noexcept;

  /** Returns the length of a BLOB part stored on the header page.
   *
   * @return	part length
   */
  [[nodiscard]] ulint blob_get_part_len(const byte *blob_header) noexcept;

  /**
   * Returns the page number where the next BLOB part is stored.
   *
   * @return	page number or FIL_NULL if no more pages
   */
  [[nodiscard]] ulint blob_get_next_page_no(const byte *blob_header) noexcept;

  /** Deallocate a buffer block that was reserved for a BLOB part.
   *
   * @param[in] buf_pool        Buffer pool.
   * @param[in,out] block       Block to free.
   * @param[in,out] mtr         Min-transaction.
   */
  void blob_free(Buf_block *block, mtr_t *mtr) noexcept;

  /** Frees the externally stored fields for a record, if the field is mentioned
   * in the update vector.
   *
   * @param[in] index           The index of the record.
   * @param[in] rec             The record.
   * @param[in] offsets         The offsets of the record in the index.
   * @param[in] update          The update vector.
   * @param[in] rb_ctx          The rollback context.
   * @param[in] mtr             The mini-transaction handle which contains an X-latch to the record page and to the tree.
   */
  void free_updated_extern_fields(
    const Index *index, Rec rec, const ulint *offsets, const upd_t *update, trx_rb_ctx rb_ctx, mtr_t *mtr
  ) noexcept;

  /**
   * Frees the externally stored fields of a record in the given index.
   *
   * @param[in] index           The index of the data. The index tree MUST be X-latched.
   * @param[in] rec             The record to free the externally stored fields from.
   * @param[in] offsets         The offsets of the record in the index.
   * @param[in] rb_ctx          The rollback context.
   * @param[in] mtr             The mini-transaction handle which contains an X-latch to record page and to the index tree
   */
  void free_externally_stored_fields(Index *index, Rec rec, const ulint *offsets, trx_rb_ctx rb_ctx, mtr_t *mtr) noexcept;

  /**
   * Gets the externally stored size of a record, in units of a database page.
   *
   * @param[in] rec             The record for which to calculate the length of externally stored data.
   * @param[in] offsets         An array returned by Phy_rec::get_col_offsets().
   *
   * @return externally stored part, in units of a database page
   */
  [[nodiscard]] ulint get_externally_stored_len(Rec rec, const ulint *offsets) noexcept;

  /**
   * Check the FIL_PAGE_TYPE on a BLOB page.
   *
   * @param[in] fil             File interface.
   * @param[in] space_id        Tablespace ID
   * @param[in] page_no         Page number
   * @param[in] page            Page
   * @param[in] read            True if the page is being read, false if it is being purged
   */
  void check_blob_fil_page_type(space_id_t space_id, page_no_t page_no, const page_t *page, bool read) noexcept;

  /**
   * Copies the prefix of a BLOB.
   *
   * The clustered index record that points to this BLOB must be protected by a lock or a page latch.
   *
   * @param[in] fil            File interface.
   * @param[out] buf            The externally stored part of the field, or a prefix of it.
   * @param[in]  len            Length of buf, in bytes.
   * @param[in]  space_id       Space id of the BLOB pages.
   * @param[in]  page_no        Page number of the first BLOB page.
   * @param[in]  offset         Offset on the first BLOB page.
   *
   * @return Number of bytes written to buf.
   */
  [[nodiscard]] ulint copy_blob_prefix(byte *buf, ulint len, space_id_t space_id, page_no_t page_no, ulint offset) noexcept;

  /**
   * Copies the prefix of an externally stored field of a record.
   *
   * The clustered index record that points to this BLOB must be protected by a
   * lock or a page latch.
   *
   * @param[out] buf            The externally stored part of the field, or a prefix of it.
   * @param[in]  len            Length of buf, in bytes.
   * @param[in]  space_id       Space id of the first BLOB page.
   * @param[in]  page_no        Page number of the first BLOB page.
   * @param[in]  offset         Offset on the first BLOB page.
   *
   * @return Number of bytes written to buf.
   */
  [[nodiscard]] ulint copy_externally_stored_field_prefix_low(
    byte *buf, ulint len, space_id_t space_id, page_no_t page_no, ulint offset
  ) noexcept;

  /**
   * Copies an externally stored field of a record to mem heap.
   * The clustered index record must be protected by a lock or a page latch.
   *
   * @param[in] fil             File interface.
   * @param[out] len            Length of the whole field
   * @param[in]  data           'Internally' stored part of the field containing also the reference to the external part; must be protected by a lock or a page latch
   * @param[in]  local_len      Length of data
   * @param[in]  heap           Memory heap
   *
   * @return The whole field copied to heap
   */
  [[nodiscard]] byte *copy_externally_stored_field(ulint *len, const byte *data, ulint local_len, mem_heap_t *heap) noexcept;

  /**
   * Copies an externally stored field of a record to mem heap.
   * The clustered index record must be protected by a lock or a page latch.
   *
   * @param[in] rec       The record.
   * @param[in] offsets   The array returned by Phy_rec::get_col_offsets().
   * @param[in] no        The field number.
   * @param[in] len       The length of the field.
   * @param[in] heap      The memory heap.
   *
   * @return The whole field copied to heap
   */
  [[nodiscard]] byte *copy_externally_stored_field(
    const Rec rec, const ulint *offsets, ulint no, ulint *len, mem_heap_t *heap
  ) noexcept;

  /**
   * Marks not updated extern fields as not-owned by this record. The ownership
   * is transferred to the updated record which is inserted elsewhere in the
   * index tree. In purge only the owner of externally stored field is allowed
   * to free the field.
   *
   * @param[in] rec             Record
   * @param[in] index           Index of rec; the index tree MUST be X-latched
   * @param[in] offsets         Phy_rec::get_col_offsets(rec, index)
   * @param[in] update          Update entry
   * @param[in] mtr             Mtr
   */
  void mark_extern_inherited_fields(Rec rec, Index *index, const ulint *offsets, const upd_t *update, mtr_t *mtr) noexcept;

  /**
   * The complement of the previous function: in an update entry may inherit
   * some externally stored fields from a record. We must mark them as inherited
   * in entry, so that they are not freed in a rollback.
   *
   * @param[in,out] entry       Clustered index entry
   * @param[in] update          Update entry
   */
  void mark_dtuple_inherited_extern(DTuple *entry, const upd_t *update) noexcept;

  /**
   * Marks all extern fields in a dtuple as owned by the record.
   *
   * @param[in,out] entry         Clustered index entry
   */
  void unmark_dtuple_extern_fields(DTuple *entry) noexcept;

  /**
   * Stores the fields in big_rec_vec to the tablespace and puts pointers to them in rec.
   * The extern flags in rec will have to be set beforehand.
   * The fields are stored on pages allocated from leaf node file segment of the index tree.
   *
   * @param[in] index            Index of rec; the index tree MUST be X-latched
   * @param[in,out] rec_block    Block containing rec
   * @param[in] rec              Record
   * @param[in] offsets          Phy_rec::get_col_offsets(rec, index); the "external storage" flags in offsets
   *                             will not correspond to rec when this function returns
   * @param[in] big_rec_vec      Vector containing fields to be stored externally
   * @param[in] local_mtr        Mtr containing the latch to rec and to the tree
   *
   * @return  DB_SUCCESS or error
   */
  [[nodiscard]] db_err store_big_rec_extern_fields(
    const Index *index, Buf_block *rec_block, Rec rec, const ulint *offsets, big_rec_t big_rec_vec, mtr_t *local_mtr
  ) noexcept;

  /**
   * Frees the space in an externally stored field to the file space management if the field in data is owned the externally stored field,
   * in a rollback we may have the additional condition that the field must not be inherited.
   *
   * @param[in] index           Index of the data, the index tree MUST be X-latched;
   *                            if the tree height is 1, then also the root page must be X-latched!
   *                            (this is relevant in the case this function is called from purge
   *                            where 'data' is located on an undo log page, not an index page)
   * @param[in,out] field_ref   Field reference
   * @param[in] rec             Record containing field_ref or nullptr
   * @param[in] offsets         Phy_rec::get_col_offsets(rec, index), or nullptr
   * @param[in] i               Field number of field_ref; ignored if rec == nullptr
   * @param[in] rb_ctx          Rollback context
   * @param[in] local_mtr       Mtr containing the latch to data an an X-latch to the index tree
   */
  void free_externally_stored_field(
    const Index *index, byte *field_ref, const Rec rec, const ulint *offsets, ulint i, enum trx_rb_ctx rb_ctx, mtr_t *local_mtr
  ) noexcept;

  /**
   * Copies the prefix of an externally stored field of a record. The clustered index record must be protected by a lock or a page latch.
   *
   * @param[out] buf            Field, or a prefix of it
   * @param[in] len             Length of buf, in bytes
   * @param[in] data            'Internally' stored part of the field containing also the reference to the external part; must be protected by a lock or a page latch
   * @param[in] local_len       Length of data, in bytes
   *
   * @return the length of the copied field, or 0 if the column was being or has been deleted
   */
  [[nodiscard]] ulint copy_externally_stored_field_prefix(byte *buf, ulint len, const byte *data, ulint local_len) noexcept;

  /**
   * Copies an externally stored field of a record to mem heap.
   *
   * @param[in] rec             Record in a clustered index; must be protected by a lock or a page latch
   * @param[in] offsets         Array returned by Phy_rec::get_col_offsets()
   * @param[in] no              Field number
   * @param[out] len            Length of the field
   * @param[in] heap            Memory heap
   *
   * @return the field copied to heap
   */
  [[nodiscard]] byte *rec_copy_externally_stored_field(
    const Rec rec, const ulint *offsets, ulint no, ulint *len, mem_heap_t *heap
  ) noexcept;

  /**
   * Flags the data tuple fields that are marked as extern storage in the update vector.
   * We use this function to remember which fields we must mark as extern storage in a record inserted for an update.
   *
   * @param[in,out] tuple       Data tuple
   * @param[in] update          Update vector
   * @param[in] heap            Memory heap
   *
   * @return number of flagged external columns
   */
  [[nodiscard]] ulint push_update_extern_fields(DTuple *tuple, const upd_t *update, mem_heap_t *heap) noexcept;

  /**
   * Marks all extern fields in a record as owned by the record. This function
   * should be called if the delete mark of a record is removed: a not delete
   * marked record always owns all its extern fields.
   *
   * @param[in,out] rec         The record in a clustered index.
   * @param[in] index           The index of the page.
   * @param[in] offsets         The array returned by Phy_rec::get_col_offsets().
   * @param[in] mtr             The mtr, or nullptr if not logged.
   */
  void unmark_extern_fields(Rec rec, const Index *index, const ulint *offsets, mtr_t *mtr) noexcept;

  /**
   * The file space handler.
   */
  FSP *m_fsp{};

  /**
   * The B-tree handler.
   */
  Btree *m_btree{};
};
