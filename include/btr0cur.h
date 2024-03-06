/****************************************************************************
Copyright (c) 1994, 2010, Innobase Oy. All Rights Reserved.

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

/*** @file include/btr0cur.h
The index tree cursor

Created 10/16/1994 Heikki Tuuri
*******************************************************/

#pragma once

#include "btr0btr.h"
#include "dict0dict.h"
#include "innodb0types.h"
#include "page0cur.h"

/** Mode flags for btr_cur operations; these can be ORed */

/** Do no undo logging */
constexpr ulint BTR_NO_UNDO_LOG_FLAG = 1;

/** Do no record lock checking */
constexpr ulint BTR_NO_LOCKING_FLAG = 2;

/** Sys fields will be found from the   update vector or inserted entry */
constexpr ulint BTR_KEEP_SYS_FLAG = 4;

#include "que0types.h"
#include "row0types.h"

#define BTR_CUR_ADAPT
#define BTR_CUR_HASH_ADAPT

#ifdef UNIV_DEBUG

/*** Returns the page cursor component of a tree cursor.
@return	pointer to page cursor component */
inline page_cur_t *
btr_cur_get_page_cur(const btr_cur_t *cursor); /*!< in: tree cursor */
#else                                          /* UNIV_DEBUG */
#define btr_cur_get_page_cur(cursor) (&(cursor)->m_page_cur)
#endif /* UNIV_DEBUG */

/**
 * Searches to the nth level of the index tree.
 *
 * @param[in,out] index      The index to search in.
 * @param[in] level          The tree level of the search.
 * @param[in] tuple          The data tuple to search for.
 *                           Note that `n_fields_cmp` in the tuple must be set so
 *                           that it cannot get compared to the node ptr page
 *                           number field.
 * @param mode               The search mode.
 *                           Possible values are PAGE_CUR_L, PAGE_CUR_LE,
 *                           PAGE_CUR_GE, etc.
 *                           If the search is made using a unique prefix of a
 *                           record, mode should be PAGE_CUR_LE, not PAGE_CUR_GE,
 *                           as the latter may end up on the previous page of
 *                           the record.
 *                           Inserts should always be made using PAGE_CUR_LE to
 *                           search the position.
 * @param latch_mode         The latch mode.
 *                           Possible values are BTR_SEARCH_LEAF, BTR_INSERT,
 *                           BTR_ESTIMATE, etc.
 *                           The cursor's `left_block` is used to store a
 *                           pointer to the left neighbor page in the cases
 *                           BTR_SEARCH_PREV and BTR_MODIFY_PREV.
 *                           If `has_search_latch` is not 0, we may not have a
 *                           latch set on the cursor page, assuming the caller
 *                           uses their search latch to protect the record.
 * @param cursor             The tree cursor.
 *                           The cursor page is s- or x-latched.
 *                           Note that the caller may need to set their own
 *                           latch on the cursor page if `has_search_latch` is
 *                           not 0.
 * @param has_search_latch   The latch mode the caller currently has on
 *                           `btr_search_latch`.
 *                           Possible values are RW_S_LATCH or 0.
 * @param file               The name of the file where the function is called.
 * @param line               The line number where the function is called.
 * @param mtr                The mini-transaction handle.
 */
void btr_cur_search_to_nth_level(
    dict_index_t* index, 
    ulint level, 
    const dtuple_t* tuple, 
    ulint mode, 
    ulint latch_mode, 
    btr_cur_t* cursor, 
    ulint has_search_latch, 
    const char* file, 
    ulint line, 
    mtr_t* mtr);

 
void btr_cur_search_to_nth_level( dict_index_t *index, ulint level, const dtuple_t *tuple, ulint mode, ulint latch_mode, btr_cur_t *cursor, ulint has_search_latch, const char *file, ulint line, mtr_t *mtr);

/**
 * Opens a cursor at either end of an index.
 *
 * @param from_left   true if open to the low end, false if open to the high end
 * @param index       the index to open the cursor on
 * @param latch_mode  the latch mode to use
 * @param cursor      the cursor to open
 * @param file        the name of the file where the function is called
 * @param line        the line number where the function is called
 * @param mtr         the mini-transaction handle
 */
void btr_cur_open_at_index_side_func(
    bool from_left,
    dict_index_t *index,
    ulint latch_mode,
    btr_cur_t *cursor,
    const char *file,
    ulint line,
    mtr_t *mtr);

#define btr_cur_open_at_index_side(f, i, l, c, m) \
    btr_cur_open_at_index_side_func(f, i, l, c, __FILE__, __LINE__, m)

/**
 * Positions a cursor at a randomly chosen position within a B-tree.
 *
 * @param index      The index.
 * @param latch_mode The latch mode (BTR_SEARCH_LEAF, ...).
 * @param cursor     The B-tree cursor.
 * @param file       The file name where the function is called.
 * @param line       The line number where the function is called.
 * @param mtr        The mini-transaction handle.
 */
void btr_cur_open_at_rnd_pos_func(dict_index_t *index, ulint latch_mode,
                                  btr_cur_t *cursor, const char *file,
                                  ulint line, mtr_t *mtr);

#define btr_cur_open_at_rnd_pos(i, l, c, m) \
    btr_cur_open_at_rnd_pos_func(i, l, c, __FILE__, __LINE__, m)

/**
 * Tries to perform an insert to a page in an index tree, next to cursor.
 * It is assumed that mtr holds an x-latch on the page. The operation does
 * not succeed if there is too little space on the page. If there is just
 * one record on the page, the insert will always succeed; this is to
 * prevent trying to split a page with just one record.
 *
 * @param flags         in: undo logging and locking flags: if not zero, the parameters
 *                      index and thr should be specified
 * @param cursor        in: cursor on page after which to insert; cursor stays valid
 * @param entry         in/out: entry to insert
 * @param rec           out: pointer to inserted record if succeed
 * @param big_rec       out: big rec vector whose fields have to be stored externally by
 *                      the caller, or NULL
 * @param n_ext         in: number of externally stored columns
 * @param thr           in: query thread or NULL
 * @param mtr           in: mtr; if this function returns DB_SUCCESS on a leaf page of
 *                      a secondary index in a compressed tablespace, the mtr must be
 *                      committed before latching any further pages
 *
 * @return              DB_SUCCESS, DB_WAIT_LOCK, DB_FAIL, or error number
 */
db_err btr_cur_optimistic_insert(
    ulint flags,
    btr_cur_t *cursor,
    dtuple_t *entry,
    rec_t **rec,
    big_rec_t **big_rec,
    ulint n_ext,
    que_thr_t *thr,
    mtr_t *mtr);

/**
 * Performs an insert on a page of an index tree. It is assumed that mtr holds an x-latch
 * on the tree and on the cursor page. If the insert is made on the leaf level, to avoid
 * deadlocks, mtr must also own x-latches to brothers of page, if those brothers exist.
 *
 * @param flags     in: undo logging and locking flags: if not zero, the parameter thr should be
 *                  specified; if no undo logging is specified, then the caller must have reserved
 *                  enough free extents in the file space so that the insertion will certainly succeed
 * @param cursor    in: cursor after which to insert; cursor stays valid
 * @param entry     in/out: entry to insert
 * @param rec       out: pointer to inserted record if succeed
 * @param big_rec   out: big rec vector whose fields have to be stored externally by the caller, or NULL
 * @param n_ext     in: number of externally stored columns
 * @param thr       in: query thread or NULL
 * @param mtr       in: mtr
 *
 * @return          DB_SUCCESS or error number
 */
db_err btr_cur_pessimistic_insert(
    ulint flags,
    btr_cur_t *cursor,
    dtuple_t *entry,
    rec_t **rec,
    big_rec_t **big_rec,
    ulint n_ext,
    que_thr_t *thr,
    mtr_t *mtr);

/**
 * Updates a record when the update causes no size changes in its fields.
 *
 * @param flags   in: undo logging and locking flags
 * @param cursor  in: cursor on the record to update; cursor stays valid and positioned on the same record
 * @param update  in: update vector
 * @param cmpl_info  in: compiler info on secondary index updates
 * @param thr  in: query thread
 * @param mtr  in: mtr; must be committed before latching any further pages
 *
 * @return  DB_SUCCESS or error number
 */
db_err btr_cur_update_in_place(
    ulint flags,
    btr_cur_t *cursor,
    const upd_t *update,
    ulint cmpl_info,
    que_thr_t *thr,
    mtr_t *mtr);

/**
 * Tries to update a record on a page in an index tree. It is assumed that mtr
 * holds an x-latch on the page. The operation does not succeed if there is too
 * little space on the page or if the update would result in too empty a page,
 * so that tree compression is recommended.
 *
 * @param flags     in: undo logging and locking flags
 * @param cursor    in: cursor on the record to update; cursor stays valid and
 *                     positioned on the same record
 * @param update    in: update vector; this must also contain trx id and roll
 *                     ptr fields
 * @param cmpl_info in: compiler info on secondary index updates
 * @param thr       in: query thread
 * @param mtr       in: mtr; must be committed before latching any further pages
 *
 * @return          DB_SUCCESS, or DB_OVERFLOW if the updated record does not fit,
 *                  DB_UNDERFLOW if the page would become too empty.
 */
db_err btr_cur_optimistic_update(
    ulint flags,
    btr_cur_t *cursor,
    const upd_t *update,
    ulint cmpl_info,
    que_thr_t *thr,
    mtr_t *mtr);

/**
 * Performs an update of a record on a page of a tree. It is assumed that mtr holds an x-latch
 * on the tree and on the cursor page. If the update is made on the leaf level, to avoid deadlocks,
 * mtr must also own x-latches to brothers of page, if those brothers exist.
 *
 * @param flags         in: undo logging, locking, and rollback flags
 * @param cursor        in: cursor on the record to update
 * @param heap          in/out: pointer to memory heap, or NULL
 * @param big_rec       out: big rec vector whose fields have to be stored externally by the caller, or NULL
 * @param update        in: update vector; this is allowed also contain trx id and roll ptr fields,
 *                      but the values in update vector have no effect
 * @param cmpl_info     in: compiler info on secondary index updates
 * @param thr           in: query thread
 * @param mtr           in: mtr; must be committed before latching any further pages
 *
 * @return              DB_SUCCESS or error code
 */
db_err btr_cur_pessimistic_update(
    ulint flags,
    btr_cur_t *cursor,
    mem_heap_t **heap,
    big_rec_t **big_rec,
    const upd_t *update,
    ulint cmpl_info,
    que_thr_t *thr,
    mtr_t *mtr);

/**
 * Marks a clustered index record deleted. Writes an undo log record to undo log on this delete marking.
 * Writes in the trx id field the id of the deleting transaction, and in the roll ptr field pointer to the
 * undo log record created.
 *
 * @param flags   in: undo logging and locking flags
 * @param cursor  in: cursor
 * @param val     in: value to set
 * @param thr     in: query thread
 * @param mtr     in: mtr
 *
 * @return        DB_SUCCESS, DB_LOCK_WAIT, or error number
 */
db_err btr_cur_del_mark_set_clust_rec(
    ulint flags,
    btr_cur_t *cursor,
    bool val,
    que_thr_t *thr,
    mtr_t *mtr);

/**
 * Sets a secondary index record delete mark to true or false.
 *
 * @param flags   in: locking flag
 * @param cursor  in: cursor
 * @param val     in: value to set
 * @param thr     in: query thread
 * @param mtr     in: mtr
 *
 * @return        DB_SUCCESS, DB_LOCK_WAIT, or error number
 */
db_err btr_cur_del_mark_set_sec_rec(ulint flags, btr_cur_t *cursor, bool val, que_thr_t *thr, mtr_t *mtr);

/**
 * Removes the record on which the tree cursor is positioned. It is assumed that
 * the mtr has an x-latch on the page where the cursor is positioned, but no
 * latch on the whole tree.
 *
 * @param cursor    in: cursor on the record to delete; cursor stays valid: if
 *                  deletion succeeds, on function exit it points to the successor
 *                  of the deleted record
 * @param mtr       in: mtr; if this function returns true on a leaf page of a
 *                  secondary index, the mtr must be committed before latching
 *                  any further pages
 *
 * @return          true on success.
 */
bool btr_cur_optimistic_delete(btr_cur_t *cursor, mtr_t *mtr);

/**
 * Removes the record on which the tree cursor is positioned. Tries to compress the page if its fillfactor drops below a threshold
 * or if it is the only page on the level. It is assumed that mtr holds an x-latch on the tree and on the cursor page. To avoid deadlocks,
 * mtr must also own x-latches to brothers of page, if those brothers exist.
 *
 * @param err                  out: DB_SUCCESS or DB_OUT_OF_FILE_SPACE; the latter may occur because we may have to update node pointers
 *                             on upper levels, and in the case of variable length keys these may actually grow in size
 * @param has_reserved_extents in: true if the caller has already reserved enough free extents so that he knows that the operation will succeed
 * @param cursor               in: cursor on the record to delete; if compression does not occur, the cursor stays valid: it points to
 *                             successor of deleted record on function exit
 * @param rb_ctx               in: rollback context
 * @param mtr                  in: mtr
 *
 * @return                     true if compression occurred
 */
void btr_cur_pessimistic_delete(
    db_err *err,
    bool has_reserved_extents,
    btr_cur_t *cursor,
    enum trx_rb_ctx rb_ctx,
    mtr_t *mtr);

/**
 * Parses a redo log record of updating a record in-place.
 *
 * @param ptr     in: buffer
 * @param end_ptr in: buffer end
 * @param page    in/out: page or NULL
 * @param index   in: index corresponding to page
 *
 * @return        end of log record or NULL
 */
byte *btr_cur_parse_update_in_place(byte *ptr, byte *end_ptr, page_t *page, dict_index_t *index);

/**
 * Parses the redo log record for delete marking or unmarking of a clustered index record.
 *
 * @param ptr     in: buffer
 * @param end_ptr in: buffer end
 * @param page    in/out: page or NULL
 * @param index   in: index corresponding to page
 *
 * @return        end of log record or NULL
 */
byte *btr_cur_parse_del_mark_set_clust_rec(byte *ptr, byte *end_ptr, page_t *page, dict_index_t *index);

/**
 * Parses the redo log record for delete marking or unmarking of a secondary index record.
 *
 * @param ptr       in: buffer
 * @param end_ptr   in: buffer end
 * @param page      in/out: page or NULL
 *
 * @return          end of log record or NULL
 */
byte *btr_cur_parse_del_mark_set_sec_rec(byte *ptr, byte *end_ptr, page_t *page);

/**
 * Estimates the number of rows in a given index range.
 *
 * @param index    in: index
 * @param tuple1   in: range start, may also be empty tuple
 * @param mode1    in: search mode for range start
 * @param tuple2   in: range end, may also be empty tuple
 * @param mode2    in: search mode for range end
 *
 * @return         estimated number of rows
 */
int64_t btr_estimate_n_rows_in_range(dict_index_t *index, const dtuple_t *tuple1,
                                     ulint mode1, const dtuple_t *tuple2,
                                     ulint mode2);

/**
 * Estimates the number of different key values in a given index, for each n-column prefix of the index
 * where n <= dict_index_get_n_unique(index). The estimates are stored in the array index->stat_n_diff_key_vals.
 *
 * @param index in: index
 */
void btr_estimate_number_of_different_key_vals(
    dict_index_t *index);

/**
 * Marks not updated extern fields as not-owned by this record. The ownership
 * is transferred to the updated record which is inserted elsewhere in the
 * index tree. In purge only the owner of externally stored field is allowed
 * to free the field.
 *
 * @param rec      in: record
 * @param index    in: index of rec; the index tree MUST be X-latched
 * @param offsets  in: rec_get_offsets(rec, index)
 * @param update   in: update entry
 * @param mtr      in: mtr
 */
void btr_cur_mark_extern_inherited_fields(
    rec_t *rec,
    dict_index_t *index,
    const ulint *offsets,
    const upd_t *update,
    mtr_t *mtr);

/**
 * The complement of the previous function: in an update entry may inherit
 * some externally stored fields from a record. We must mark them as inherited
 * in entry, so that they are not freed in a rollback.
 *
 * @param entry   in/out: clustered index entry
 * @param update  in: update entry
 */
void btr_cur_mark_dtuple_inherited_extern(dtuple_t *entry, const upd_t *update);

/**
 * Marks all extern fields in a dtuple as owned by the record.
 *
 * @param entry in/out: clustered index entry
 */
void btr_cur_unmark_dtuple_extern_fields( dtuple_t *entry);

/**
 * Stores the fields in big_rec_vec to the tablespace and puts pointers to them in rec.
 * The extern flags in rec will have to be set beforehand.
 * The fields are stored on pages allocated from leaf node file segment of the index tree.
 *
 * @param index         in: index of rec; the index tree MUST be X-latched
 * @param rec_block     in/out: block containing rec
 * @param rec           in: record
 * @param offsets       in: rec_get_offsets(rec, index); the "external storage" flags in offsets
 *                        will not correspond to rec when this function returns
 * @param big_rec_vec   in: vector containing fields to be stored externally
 * @param local_mtr     in: mtr containing the latch to rec and to the tree
 *
 * @return              DB_SUCCESS or error
 */
db_err btr_store_big_rec_extern_fields(
    dict_index_t *index,
    buf_block_t *rec_block,
    rec_t *rec,
    const ulint *offsets,
    big_rec_t *big_rec_vec,
    mtr_t *local_mtr);

/**
 * Frees the space in an externally stored field to the file space management if the field in data is owned the externally stored field,
 * in a rollback we may have the additional condition that the field must not be inherited.
 *
 * @param index      in: index of the data, the index tree MUST be X-latched; if the tree height is 1, then also the root page must be X-latched! (this is relevant in the case this function is called from purge where 'data' is located on an undo log page, not an index page)
 * @param field_ref  in/out: field reference
 * @param rec        in: record containing field_ref or NULL
 * @param offsets    in: rec_get_offsets(rec, index), or NULL
 * @param i          in: field number of field_ref; ignored if rec == NULL
 * @param rb_ctx     in: rollback context
 * @param local_mtr  in: mtr containing the latch to data an an X-latch to the index tree
 */
void btr_free_externally_stored_field(
    dict_index_t *index,
    byte *field_ref,
    const rec_t *rec,
    const ulint *offsets,
    ulint i,
    enum trx_rb_ctx rb_ctx,
    mtr_t *local_mtr);

/**
 * Copies the prefix of an externally stored field of a record. The clustered index record must be protected by a lock or a page latch.
 *
 * @param buf        out: the field, or a prefix of it
 * @param len        in: length of buf, in bytes
 * @param data       in: 'internally' stored part of the field containing also the reference to the external part; must be protected by a lock or a page latch
 * @param local_len  in: length of data, in bytes
 *
 * @return the length of the copied field, or 0 if the column was being or has been deleted
 */
ulint btr_copy_externally_stored_field_prefix( byte *buf, ulint len, const byte *data, ulint local_len);

/**
 * Copies an externally stored field of a record to mem heap.
 *
 * @param rec     in: record in a clustered index; must be protected by a lock or a page latch
 * @param offsets in: array returned by rec_get_offsets()
 * @param no      in: field number
 * @param len     out: length of the field
 * @param heap    in: mem heap
 *
 * @return the field copied to heap
 */
byte *btr_rec_copy_externally_stored_field(
    const rec_t *rec,
    const ulint *offsets,
    ulint no,
    ulint *len,
    mem_heap_t *heap);

/**
 * Flags the data tuple fields that are marked as extern storage in the update vector.
 * We use this function to remember which fields we must mark as extern storage in a record inserted for an update.
 *
 * @param tuple     in/out: data tuple
 * @param update    in: update vector
 * @param heap      in: memory heap
 *
 * @return          number of flagged external columns
 */
ulint btr_push_update_extern_fields(
    dtuple_t *tuple,
    const upd_t *update,
    mem_heap_t *heap);

/*** Reset global configuration variables. */
void btr_cur_var_init();

/*######################################################################*/

/** In the pessimistic delete, if the page data size drops below this
limit, merging it to a neighbor is tried */
constexpr ulint BTR_CUR_PAGE_LOW_FILL_LIMIT = UNIV_PAGE_SIZE / 2;

/**
 * A slot in the path array. We store here info on a search path down the tree.
 * Each slot contains data on a single level of the tree.
 */
struct btr_path_t {
    /** Index of the record where the page cursor stopped on this level
    (index in alphabetical order); value ULINT_UNDEFINED denotes array end */
    ulint nth_rec; 

    /** Number of records on the page */
    ulint n_recs;
};

/** Size of path array (in slots) */
constexpr ulint BTR_PATH_ARRAY_N_SLOTS = 250;

/**
 * Values for the flag documenting the used search method.
 */
enum btr_cur_method {
    /** Successful shortcut using the hash index. */
    BTR_CUR_HASH = 1,

    /** Failure using hash, success using binary search: the misleading
    hash reference is stored in the field hash_node, and might be
    necessary to update. */
    BTR_CUR_HASH_FAIL,

    /** Success using the binary search. */
    BTR_CUR_BINARY
};

/** The tree cursor: the definition appears here only for the compiler
to know struct size! */
struct btr_cur_t {
    /** index where positioned */
    dict_index_t *m_index;
    
    /** page cursor */
    page_cur_t m_page_cur;
    
    /*!< this field is used to store a pointer to the left neighbor page, in the
     * cases BTR_SEARCH_PREV and BTR_MODIFY_PREV */
    buf_block_t *left_block;
    
    /** this field is only used when btr_cur_search_to_nth_level is called for an
     * index entry insertion: the calling query thread is passed here to be used
     * in the insert buffer */
    que_thr_t *thr;
    
    /** Search method used */
    enum btr_cur_method flag;
    
    /*!< Tree height if the search is done for a pessimistic insert or update
     * operation */
    ulint tree_height;
    
    /*!< If the search mode was PAGE_CUR_LE, the number of matched fields to the
     * the first user record to the right of the cursor record after
     * btr_cur_search_to_nth_level; for the mode PAGE_CUR_GE, the matched fields
     * to the first user record AT THE CURSOR or to the right of it; NOTE that the
     * up_match and low_match values may exceed the correct values for comparison
     * to the adjacent user record if that record is on a different leaf page!
     * (See the note in row_ins_duplicate_key.) */
    ulint up_match;
    
    /** number of matched bytes to the right at the time cursor positioned; only
     * used internally in searches: not defined after the search */
    ulint up_bytes;
    
    /** if search mode was PAGE_CUR_LE, the number of matched fields to the first
     * user record AT THE CURSOR or to the left of it after
     * btr_cur_search_to_nth_level; NOT defined for PAGE_CUR_GE or any other search
     * modes; see also the NOTE in up_match! */
    ulint low_match;
    
    /** number of matched bytes to the right at the time cursor positioned; only
     * used internally in searches: not defined after the search */
    ulint low_bytes;
    
    /** prefix length used in a hash search if hash_node != NULL */
    ulint n_fields;
    
    /** hash prefix bytes if hash_node != NULL */
    ulint n_bytes;
    
    /** fold value used in the search if flag is BTR_CUR_HASH */
    ulint fold;
    
    /** in estimating the number of rows in range, we store in this array
     * information of the path through the tree */
    btr_path_t *path_arr;
};

/** If pessimistic delete fails because of lack of file space, there
is still a good change of success a little later.  Try this many
times. */
constexpr ulint BTR_CUR_RETRY_DELETE_N_TIMES = 100;
/** If pessimistic delete fails because of lack of file space, there
is still a good change of success a little later.  Sleep this many
microseconds between retries. */
constexpr ulint BTR_CUR_RETRY_SLEEP_TIME = 50000;

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

#ifdef UNIV_DEBUG
/**
 * Returns the page cursor component of a tree cursor.
 * @param cursor The tree cursor.
 * @return Pointer to the page cursor component.
 */
inline page_cur_t* btr_cur_get_page_cur(const btr_cur_t* cursor) {
    return &((btr_cur_t*)cursor)->m_page_cur;
}
#endif /* UNIV_DEBUG */

/**
 * Returns the buffer block on which the tree cursor is positioned.
 * @param cursor The tree cursor.
 * @return Pointer to the buffer block.
 */
inline buf_block_t* btr_cur_get_block(btr_cur_t* cursor) {
    return page_cur_get_block(btr_cur_get_page_cur(cursor));
}

/**
 * Returns the record pointer of a tree cursor.
 * @param cursor The tree cursor.
 * @return Pointer to the record.
 */
inline rec_t* btr_cur_get_rec(btr_cur_t* cursor) {
    return page_cur_get_rec(&(cursor->m_page_cur));
}

/**
 * Invalidates a tree cursor by setting the record pointer to NULL.
 * @param cursor The tree cursor.
 */
inline void btr_cur_invalidate(btr_cur_t* cursor) {
    page_cur_invalidate(&(cursor->m_page_cur));
}

/**
 * Returns the page of a tree cursor.
 * @param cursor The tree cursor.
 * @return Pointer to the page.
 */
inline page_t* btr_cur_get_page(btr_cur_t* cursor) {
    return page_align(page_cur_get_rec(&(cursor->m_page_cur)));
}

/**
 * Returns the index of a cursor.
 * @param cursor The B-tree cursor.
 * @return The index.
 */
inline dict_index_t* btr_cur_get_index(btr_cur_t* cursor) {
    return cursor->m_index;
}

/**
 * Positions a tree cursor at a given record.
 * @param dict_index The dictionary index.
 * @param rec The record in the tree.
 * @param block The buffer block of the record.
 * @param cursor The output cursor.
 */
inline void btr_cur_position(dict_index_t* dict_index, rec_t* rec, buf_block_t* block, btr_cur_t* cursor) {
    ut_ad(page_align(rec) == block->frame);

    page_cur_position(rec, block, btr_cur_get_page_cur(cursor));

    cursor->m_index = dict_index;
}

/**
 * Checks if compressing an index page where a btr cursor is placed makes sense.
 * @param cursor The btr cursor.
 * @param mtr The minit-transaction
 * @return True if merge is recommended.
 */
inline bool btr_cur_compress_recommendation(btr_cur_t* cursor, mtr_t* mtr) {
    ut_ad(mtr_memo_contains(mtr, btr_cur_get_block(cursor), MTR_MEMO_PAGE_X_FIX));

    auto page = btr_cur_get_page(cursor);

    if (page_get_data_size(page) < BTR_CUR_PAGE_LOW_FILL_LIMIT ||
        (btr_page_get_next(page, mtr) == FIL_NULL &&
         btr_page_get_prev(page, mtr) == FIL_NULL)) {

        // The page fillfactor has dropped below a predefined minimum value
        // OR the level in the B-tree contains just one page: we recommend
        // merge if this is not the root page.
        return dict_index_get_page(cursor->m_index) != page_get_page_no(page);
    } else {
        return false;
    }
}

/**
 * Checks if the record on which the cursor is placed can be deleted without
 * making merge necessary (or, recommended).
 * @param cursor The btr cursor.
 * @param rec_size The size of the record.
 * @param mtr The mini-transaction
 * @return True if the record can be deleted without recommended merging.
 */
inline bool btr_cur_delete_will_underflow(btr_cur_t* cursor, ulint rec_size, mtr_t* mtr) {
    ut_ad(mtr_memo_contains(mtr, btr_cur_get_block(cursor), MTR_MEMO_PAGE_X_FIX));

    auto page = btr_cur_get_page(cursor);

    if ((page_get_data_size(page) - rec_size < BTR_CUR_PAGE_LOW_FILL_LIMIT) ||
        (btr_page_get_next(page, mtr) == FIL_NULL &&
         btr_page_get_prev(page, mtr) == FIL_NULL) ||
        page_get_n_recs(page) < 2) {

        // The page fillfactor will drop below a predefined minimum value,
        // OR the level in the B-tree contains just one page,
        // OR the page will become empty: we recommend merge if this is
        // not the root page.
        return dict_index_get_page(cursor->m_index) == page_get_page_no(page);
    } else {
        return true;
    }
}
