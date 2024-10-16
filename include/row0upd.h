/****************************************************************************
Copyright (c) 1996, 2009, Innobase Oy. All Rights Reserved.
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

/** @file include/row0upd.h
Update of a row

Created 12/27/1996 Heikki Tuuri
*******************************************************/

#pragma once

#include "btr0types.h"
#include "data0data.h"
#include "dict0types.h"
#include "innodb0types.h"
#include "row0types.h"
#include "trx0types.h"
#include "btr0pcur.h"
#include "pars0types.h"
#include "que0types.h"
#include "mtr0log.h"
#include "row0row.h"
#include "trx0trx.h"
#include "trx0undo.h"

#ifndef UNIV_DEBUG
#define upd_get_nth_field(update, n) ((update)->fields + (n))
#endif /* !UNIV_DEBUG */

/** Writes into the redo log the values of trx id and roll ptr and enough info
to determine their positions within a clustered index record.
@return	new pointer to mlog */
byte *row_upd_write_sys_vals_to_log(
  dict_index_t *index, /*!< in: clustered index */
  trx_t *trx,          /*!< in: transaction */
  roll_ptr_t roll_ptr, /*!< in: roll ptr of the undo log record */
  byte *log_ptr,       /*!< pointer to a buffer of size > 20 opened
                         in mlog */
  mtr_t *mtr /*!< in: mtr */
  );

/** Sets the trx id or roll ptr field of a clustered index entry. */
void row_upd_index_entry_sys_field(
  const dtuple_t *entry, /*!< in: index entry, where the memory buffers
                           for sys fields are already allocated:
                           the function just copies the new values to
                           them */
  dict_index_t *index,   /*!< in: clustered index */
  ulint type,            /*!< in: DATA_TRX_ID or DATA_ROLL_PTR */
  uint64_t val /*!< in: value to write */
  );

/** Creates an update node for a query graph.
@return	own: update node */
upd_node_t *upd_node_create(mem_heap_t *heap); /*!< in: mem heap where created */

/** Writes to the redo log the new values of the fields occurring in the index. */
void row_upd_index_write_log(
  const upd_t *update, /*!< in: update vector */
  byte *log_ptr,       /*!< in: pointer to mlog buffer: must
                         contain at least MLOG_BUF_MARGIN bytes
                         of free space; the buffer is closed
                         within this function */
  mtr_t *mtr /*!< in: mtr into whose log to write */
  );

/** Returns true if row update changes size of some field in index or if some
field to be updated is stored externally in rec or update.
@return true if the update changes the size of some field in index or
the field is external in rec or update */
bool row_upd_changes_field_size_or_external(
  dict_index_t *index,  /*!< in: index */
  const ulint *offsets, /*!< in: Phy_rec::get_col_offsets(rec, index) */
  const upd_t *update /*!< in: update vector */
  );

/** Replaces the new column values stored in the update vector to the record
given. No field size changes are allowed. */
void row_upd_rec_in_place(
  rec_t *rec,           /*!< in/out: record where replaced */
  dict_index_t *index,  /*!< in: the index the record belongs to */
  const ulint *offsets, /*!< in: array returned by Phy_rec::get_col_offsets() */
  const upd_t *update /*!< in: update vector */
  );

/** Builds an update vector from those fields which in a secondary index entry
differ from a record that has the equal ordering fields. NOTE: we compare
the fields as binary strings!
@return	own: update vector of differing fields */
upd_t *row_upd_build_sec_rec_difference_binary(
  dict_index_t *index,   /*!< in: index */
  const dtuple_t *entry, /*!< in: entry to insert */
  const rec_t *rec,      /*!< in: secondary index record */
  trx_t *trx,            /*!< in: transaction */
  mem_heap_t *heap /*!< in: memory heap from which allocated */
  );

/** Builds an update vector from those fields, excluding the roll ptr and
trx id fields, which in an index entry differ from a record that has
the equal ordering fields. NOTE: we compare the fields as binary strings!
@return own: update vector of differing fields, excluding roll ptr and
trx id */
upd_t *row_upd_build_difference_binary(
  dict_index_t *index,   /*!< in: clustered index */
  const dtuple_t *entry, /*!< in: entry to insert */
  const rec_t *rec,      /*!< in: clustered index record */
  trx_t *trx,            /*!< in: transaction */
  mem_heap_t *heap /*!< in: memory heap from which allocated */
  );

/** Replaces the new column values stored in the update vector to the index
entry given. */
void row_upd_index_replace_new_col_vals_index_pos(
  dtuple_t *entry,     /*!< in/out: index entry where replaced;
                         the clustered index record must be
                         covered by a lock or a page latch to
                         prevent deletion (rollback or purge) */
  dict_index_t *index, /*!< in: index; NOTE that this may also be a
                         non-clustered index */
  const upd_t *update, /*!< in: an update vector built for the index so
                         that the field number in an upd_field is the
                         index position */
  bool order_only,
  /*!< in: if true, limit the replacement to
    ordering fields of index; note that this
    does not work for non-clustered indexes. */
  mem_heap_t *heap /*!< in: memory heap for allocating and copying the new values */
  );

/** Replaces the new column values stored in the update vector to the index
entry given. */
void row_upd_index_replace_new_col_vals(
  dtuple_t *entry,     /*!< in/out: index entry where replaced;
                         the clustered index record must be
                         covered by a lock or a page latch to
                         prevent deletion (rollback or purge) */
  dict_index_t *index, /*!< in: index; NOTE that this may also be a
                         non-clustered index */
  const upd_t *update, /*!< in: an update vector built for the
                         CLUSTERED index so that the field number in
                         an upd_field is the clustered index position */
  mem_heap_t *heap /*!< in: memory heap for allocating and copying the new values */
  );

/** Replaces the new column values stored in the update vector. */
void row_upd_replace(
  dtuple_t *row,             /*!< in/out: row where replaced,
                                      indexed by col_no;
                                      the clustered index record must be
                                      covered by a lock or a page latch to
                                      prevent deletion (rollback or purge) */
  row_ext_t **ext,           /*!< out, own: NULL, or externally
                                      stored column prefixes */
  const dict_index_t *index, /*!< in: clustered index */
  const upd_t *update,       /*!< in: an update vector built for
                                          the clustered index */
  mem_heap_t *heap /*!< in: memory heap */
  );

/** Checks if an update vector changes an ordering field of an index record.

This function is fast if the update vector is short or the number of ordering
fields in the index is small. Otherwise, this can be quadratic.
NOTE: we compare the fields as binary strings!
@return true if update vector changes an ordering field in the index record */
bool row_upd_changes_ord_field_binary(
  const dtuple_t *row, /*!< in: old value of row, or NULL if the
                          row and the data values in update are not
                          known when this function is called, e.g., at
                          compile time */
  dict_index_t *index, /*!< in: index of the record */
  const upd_t *update /*!< in: update vector for the row; NOTE: the field numbers in this MUST be clustered index positions! */
  );

/** Checks if an update vector changes an ordering field of an index record.
This function is fast if the update vector is short or the number of ordering
fields in the index is small. Otherwise, this can be quadratic.
NOTE: we compare the fields as binary strings!
@return true if update vector may change an ordering field in an index
record */
bool row_upd_changes_some_index_ord_field_binary(
  const dict_table_t *table, /*!< in: table */
  const upd_t *update /*!< in: update vector for the row */
  );

/** Updates a row in a table. This is a high-level function used
in SQL execution graphs.
@return	query thread to run next or NULL */
que_thr_t *row_upd_step(que_thr_t *thr); /*!< in: query thread */

/** Parses the log data of system field values.
@return	log data end or NULL */
byte *row_upd_parse_sys_vals(
  byte *ptr,        /*!< in: buffer */
  byte *end_ptr,    /*!< in: buffer end */
  ulint *pos,       /*!< out: TRX_ID position in record */
  trx_id_t *trx_id, /*!< out: trx id */
  roll_ptr_t *roll_ptr /*!< out: roll ptr */
  );

/** Updates the trx id and roll ptr field in a clustered index record in
database recovery. */
void row_upd_rec_sys_fields_in_recovery(
  rec_t *rec,           /*!< in/out: record */
  const ulint *offsets, /*!< in: array returned by Phy_rec::get_col_offsets() */
  ulint pos,            /*!< in: TRX_ID position in rec */
  trx_id_t trx_id,      /*!< in: transaction id */
  roll_ptr_t roll_ptr /*!< in: roll ptr of the undo log record */
  );

/** Parses the log data written by row_upd_index_write_log.
@return	log data end or NULL */
byte *row_upd_index_parse(
  byte *ptr,        /*!< in: buffer */
  byte *end_ptr,    /*!< in: buffer end */
  mem_heap_t *heap, /*!< in: memory heap where update
                                               vector is    built */
  upd_t **update_out /*!< out: update vector */
  );

/** Creates an query graph node of 'update' type to be used in the engine
interface.
@return	own: update node */
upd_node_t *row_create_update_node(
  dict_table_t *table, /*!< in: table to update */
  mem_heap_t *heap /*!< in: mem heap from which allocated */
  );

/* Update vector field */
struct upd_field_struct {
  uint16_t field_no; /*!< field number in an index, usually
                          the clustered index, but in updating
                          a secondary index record in btr0cur.c
                          this is the position in the secondary
                          index */
  uint16_t orig_len; /*!< original length of the locally
                          stored part of an externally stored
                          column, or 0 */
  que_node_t *exp;        /*!< expression for calculating a new
                          value: it refers to column values and
                          constants in the symbol table of the
                          query graph */
  dfield_t new_val;       /*!< new value for the column */
};

/* Update vector structure */
struct upd_struct {
  ulint info_bits;     /*!< new value of info bits to record;
                       default is 0 */
  ulint n_fields;      /*!< number of update fields */
  upd_field_t *fields; /*!< array of update fields */
};

/* Update node structure which also implements the delete operation
of a row */
struct upd_node_struct {
  que_common_t common; /*!< node type: QUE_NODE_UPDATE */
  bool is_delete;      /* true if delete, false if update */
  bool searched_update; /* true if searched update, false if positioned */
  bool in_client_interface;
  /* true if the update node was created
  for the client interface */
  dict_foreign_t *foreign;  /* NULL or pointer to a foreign key
                            constraint if this update node is used in
                            doing an ON DELETE or ON UPDATE operation */
  upd_node_t *cascade_node; /* NULL or an update node template which
                       is used to implement ON DELETE/UPDATE CASCADE
                       or ... SET NULL for foreign keys */
  mem_heap_t *cascade_heap; /* NULL or a mem heap where the cascade
                       node is created */
  sel_node_t *select;       /*!< query graph subtree implementing a base
                            table cursor: the rows returned will be
                            updated */
  Btree_pcursor *pcur;         /*!< persistent cursor placed on the clustered
                            index record which should be updated or
                            deleted; the cursor is stored in the graph
                            of 'select' field above, except in the case
                            of the client interface */
  dict_table_t *table;      /*!< table where updated */
  upd_t *update;            /*!< update vector for the row */
  ulint update_n_fields;
  /* when this struct is used to implement
  a cascade operation for foreign keys, we store
  here the size of the buffer allocated for use
  as the update vector */
  sym_node_list_t columns; /* symbol table nodes for the columns
                           to retrieve from the table */
  bool has_clust_rec_x_lock;
  /* true if the select which retrieves the
  records to update already sets an x-lock on
  the clustered record; note that it must always
  set at least an s-lock */
  ulint cmpl_info; /* information extracted during query
                 compilation; speeds up execution:
                 UPD_NODE_NO_ORD_CHANGE and
                 UPD_NODE_NO_SIZE_CHANGE, ORed */
  /*----------------------*/
  /* Local storage for this graph node */
  ulint state;         /*!< node execution state */
  dict_index_t *index; /*!< NULL, or the next index whose record should
                       be updated */
  dtuple_t *row;       /*!< NULL, or a copy (also fields copied to
                       heap) of the row to update; this must be reset
                       to NULL after a successful update */
  row_ext_t *ext;      /*!< NULL, or prefixes of the externally
                       stored columns in the old row */
  dtuple_t *upd_row;   /* NULL, or a copy of the updated row */
  row_ext_t *upd_ext;  /* NULL, or prefixes of the externally
                       stored columns in upd_row */
  mem_heap_t *heap;    /*!< memory heap used as auxiliary storage;
                       this must be emptied after a successful
                       update */
  /*----------------------*/
  sym_node_t *table_sym; /* table node in symbol table */
  que_node_t *col_assign_list;
  /* column assignment list */
  ulint magic_n;
};

constexpr ulint UPD_NODE_MAGIC_N = 1579975;

/* Node execution states */

/** Execution came to the node from a node above and if the field has_clust_rec_x_lock
is false, we should set an intention x-lock on the table */
constexpr ulint UPD_NODE_SET_IX_LOCK = 1;

/** Clustered index record should be updated */
constexpr ulint UPD_NODE_UPDATE_CLUSTERED = 2;

/** Clustered index record should be inserted, old record is already delete marked */
constexpr ulint UPD_NODE_INSERT_CLUSTERED = 3;

/** An ordering field of the clustered index record was changed, or this is a delete
operation: should update all the secondary index records */
constexpr ulint UPD_NODE_UPDATE_ALL_SEC = 4;

/** Secondary index entries should be looked at and updated if an ordering field changed */
constexpr ulint UPD_NODE_UPDATE_SOME_SEC = 5;

/** Compilation info flags: these must fit within 3 bits; see trx0rec.h */

/** No secondary index record will be changed in the update and no ordering field
of the clustered index */
constexpr ulint UPD_NODE_NO_ORD_CHANGE = 1;

/** No record field size will be changed in the update */
constexpr ulint UPD_NODE_NO_SIZE_CHANGE = 2;

/** Creates an update vector object.
@return	own: update vector object */
inline upd_t *upd_create(
  ulint n, /*!< in: number of fields */
  mem_heap_t *heap
) /*!< in: heap from which memory allocated */
{
  upd_t *update;

  update = (upd_t *)mem_heap_alloc(heap, sizeof(upd_t));

  update->info_bits = 0;
  update->n_fields = n;
  update->fields = (upd_field_t *)mem_heap_alloc(heap, sizeof(upd_field_t) * n);

  return (update);
}

/** Returns the number of fields in the update vector == number of columns
to be updated by an update vector.
@return	number of fields */
inline ulint upd_get_n_fields(const upd_t *update) /*!< in: update vector */
{
  ut_ad(update);

  return (update->n_fields);
}

#ifdef UNIV_DEBUG
/** Returns the nth field of an update vector.
@return	update vector field */
inline upd_field_t *upd_get_nth_field(
  const upd_t *update, /*!< in: update vector */
  ulint n
) /*!< in: field position in update vector */
{
  ut_ad(update);
  ut_ad(n < update->n_fields);

  return ((upd_field_t *)update->fields + n);
}
#endif /* UNIV_DEBUG */

/** Sets an index field number to be updated by an update vector field. */
inline void upd_field_set_field_no(
  upd_field_t *upd_field,   /*!< in: update vector field */
  ulint field_no,           /*!< in: field number in a clustered
                                       index */
  dict_index_t *dict_index, /*!< in: dict_index */
  trx_t *trx
) /*!< in: transaction */
{
  upd_field->field_no = field_no;
  upd_field->orig_len = 0;

  if (unlikely(field_no >= dict_index_get_n_fields(dict_index))) {
    ib_logger(ib_stream, "Error: trying to access field %lu in ", (ulong)field_no);
    dict_index_name_print(ib_stream, trx, dict_index);
    ib_logger(
      ib_stream,
      "\n"
      "but index only has %lu fields\n",
      (ulong)dict_index_get_n_fields(dict_index)
    );
  }

  dict_col_copy_type(dict_index_get_nth_col(dict_index, field_no), dfield_get_type(&upd_field->new_val));
}

/** Returns a field of an update vector by field_no.
@return	update vector field, or NULL */
inline const upd_field_t *upd_get_field_by_field_no(
  const upd_t *update, /*!< in: update vector */
  ulint no
) /*!< in: field_no */
{
  ulint i;
  for (i = 0; i < upd_get_n_fields(update); i++) {
    const upd_field_t *uf = upd_get_nth_field(update, i);

    if (uf->field_no == no) {

      return (uf);
    }
  }

  return (nullptr);
}

/** Updates the trx id and roll ptr field in a clustered index record when
a row is updated or marked deleted. */
inline void row_upd_rec_sys_fields(
  rec_t *rec,               /*!< in/out: record */
  dict_index_t *dict_index, /*!< in: clustered index */
  const ulint *offsets,     /*!< in: Phy_rec::get_col_offsets(rec, index) */
  trx_t *trx,               /*!< in: transaction */
  roll_ptr_t roll_ptr
) /*!< in: roll ptr of the undo log record */
{
#ifdef UNIV_SYNC_DEBUG
  if (!rw_lock_own(&btr_search_latch, RW_LOCK_EX)) {
    ut_ad(!buf_block_align(rec)->is_hashed);
  }
#endif /* UNIV_SYNC_DEBUG */

  ulint offset = dict_index->trx_id_offset;

  if (!offset) {
    offset = row_get_trx_id_offset(rec, dict_index, offsets);
  }

  static_assert(DATA_TRX_ID + 1 == DATA_ROLL_PTR, "error DATA_TRX_ID + 1 != DATA_ROLL_PTR");

  srv_trx_sys->write_trx_id(rec + offset, trx->m_id);
  trx_write_roll_ptr(rec + offset + DATA_TRX_ID_LEN, roll_ptr);
}
