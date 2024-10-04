/****************************************************************************
Copyright (c) 1996, 2010, Innobase Oy. All Rights Reserved.
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

/** @file dict/dict0boot.c
Data dictionary creation and booting

Created 4/18/1996 Heikki Tuuri
*******************************************************/

#include "dict0boot.h"

#include "btr0btr.h"
#include "buf0flu.h"
#include "dict0crea.h"
#include "dict0load.h"
#include "log0recv.h"
#include "os0file.h"
#include "srv0srv.h"
#include "trx0trx.h"

dict_hdr_t *dict_hdr_get(mtr_t *mtr) {
  Buf_pool::Request req {
      .m_rw_latch = RW_X_LATCH,
      .m_page_id = { DICT_HDR_SPACE, DICT_HDR_PAGE_NO },
      .m_mode = BUF_GET,
      .m_file = __FILE__,
      .m_line = __LINE__,
      .m_mtr = mtr
    };

  auto block = srv_buf_pool->get(req, nullptr);
  auto header = DICT_HDR + block->get_frame();

  buf_block_dbg_add_level(IF_SYNC_DEBUG(block, SYNC_DICT_HEADER));

  return header;
}

uint64_t dict_hdr_get_new_id(ulint type) {
  ut_ad(type == DICT_HDR_TABLE_ID || type == DICT_HDR_INDEX_ID);

  mtr_t mtr;

  mtr.start();

  auto dict_hdr = dict_hdr_get(&mtr);

  auto id = mtr.read_uint64(dict_hdr + type) + 1;

  mlog_write_uint64(dict_hdr + type, id, &mtr);

  mtr.commit();

  return id;
}

void dict_hdr_flush_row_id() {
  mtr_t mtr;

  ut_ad(mutex_own(&(dict_sys->mutex)));

  auto id = dict_sys->row_id;

  mtr.start();

  auto dict_hdr = dict_hdr_get(&mtr);

  mlog_write_uint64(dict_hdr + DICT_HDR_ROW_ID, id, &mtr);

  mtr.commit();
}

/**
 * Creates the file page for the dictionary header. This function is called only at the database creation.
 *
 * @param mtr in: mtr
 *
 * @return true if succeed
 */
static bool dict_hdr_create(mtr_t *mtr) {

  ut_ad(mtr);

  /* Create the dictionary header file block in a new, allocated file
  segment in the system tablespace */
  auto block = srv_fsp->fseg_create(DICT_HDR_SPACE, 0, DICT_HDR + DICT_HDR_FSEG_HEADER, mtr);

  ut_a(DICT_HDR_PAGE_NO == block->get_page_no());

  auto dict_header = dict_hdr_get(mtr);

  /* Start counting row, table, index, and tree ids from DICT_HDR_FIRST_ID */
  mlog_write_uint64(dict_header + DICT_HDR_ROW_ID, DICT_HDR_FIRST_ID, mtr);

  mlog_write_uint64(dict_header + DICT_HDR_TABLE_ID, DICT_HDR_FIRST_ID, mtr);

  mlog_write_uint64(dict_header + DICT_HDR_INDEX_ID, DICT_HDR_FIRST_ID, mtr);

  /* Obsolete, but we must initialize it to 0 anyway. */
  mlog_write_uint64(dict_header + DICT_HDR_MIX_ID, DICT_HDR_FIRST_ID, mtr);

  /* Create the B-tree roots for the clustered indexes of the basic system tables */

  auto root_page_no = srv_btree_sys->create(
    DICT_CLUSTERED | DICT_UNIQUE, DICT_HDR_SPACE, DICT_TABLES_ID, dict_ind_redundant, mtr);

  if (root_page_no == FIL_NULL) {

    return false;
  }

  mlog_write_ulint(dict_header + DICT_HDR_TABLES, root_page_no, MLOG_4BYTES, mtr);

  root_page_no = srv_btree_sys->create(DICT_UNIQUE, DICT_HDR_SPACE, DICT_TABLE_IDS_ID, dict_ind_redundant, mtr);

  if (root_page_no == FIL_NULL) {

    return false;
  }

  mlog_write_ulint(dict_header + DICT_HDR_TABLE_IDS, root_page_no, MLOG_4BYTES, mtr);

  root_page_no = srv_btree_sys->create(DICT_CLUSTERED | DICT_UNIQUE, DICT_HDR_SPACE, DICT_COLUMNS_ID, dict_ind_redundant, mtr);

  if (root_page_no == FIL_NULL) {

    return (false);
  }

  mlog_write_ulint(dict_header + DICT_HDR_COLUMNS, root_page_no, MLOG_4BYTES, mtr);

  root_page_no = srv_btree_sys->create(DICT_CLUSTERED | DICT_UNIQUE, DICT_HDR_SPACE, DICT_INDEXES_ID, dict_ind_redundant, mtr);

  if (root_page_no == FIL_NULL) {

    return false;
  }

  mlog_write_ulint(dict_header + DICT_HDR_INDEXES, root_page_no, MLOG_4BYTES, mtr);

  root_page_no = srv_btree_sys->create(DICT_CLUSTERED | DICT_UNIQUE, DICT_HDR_SPACE, DICT_FIELDS_ID, dict_ind_redundant, mtr);

  if (root_page_no == FIL_NULL) {

    return false;
  }

  mlog_write_ulint(dict_header + DICT_HDR_FIELDS, root_page_no, MLOG_4BYTES, mtr);

  return true;
}

void dict_boot() {
  dict_table_t *table;
  mtr_t mtr;

  mtr.start();

  /* Create the hash tables etc. */
  dict_init();

  auto heap = mem_heap_create(450);

  mutex_enter(&dict_sys->mutex);

  /* Get the dictionary header */
  auto dict_hdr = dict_hdr_get(&mtr);

  /* Because we only write new row ids to disk-based data structure
  (dictionary header) when it is divisible by
  DICT_HDR_ROW_ID_WRITE_MARGIN, in recovery we will not recover
  the latest value of the row id counter. Therefore we advance
  the counter at the database startup to avoid overlapping values.
  Note that when a user after database startup first time asks for
  a new row id, then because the counter is now divisible by
  ..._MARGIN, it will immediately be updated to the disk-based
  header. */

  dict_sys->row_id = ut_uint64_align_up(mtr.read_uint64(dict_hdr + DICT_HDR_ROW_ID), DICT_HDR_ROW_ID_WRITE_MARGIN) +
                     DICT_HDR_ROW_ID_WRITE_MARGIN;

  /* Insert into the dictionary cache the descriptions of the basic
  system tables */
  /*-------------------------*/
  table = dict_mem_table_create("SYS_TABLES", DICT_HDR_SPACE, 8, 0);

  dict_mem_table_add_col(table, heap, "NAME", DATA_BINARY, 0, 0);
  dict_mem_table_add_col(table, heap, "ID", DATA_BINARY, 0, 0);
  /* ROW_FORMAT = (N_COLS >> 31) ? COMPACT : REDUNDANT */
  dict_mem_table_add_col(table, heap, "N_COLS", DATA_INT, 0, 4);
  /* TYPE is either DICT_TABLE_ORDINARY, or (TYPE & DICT_TF_COMPACT)
  and (TYPE & DICT_TF_FORMAT_MASK) are nonzero and TYPE = table->flags */
  dict_mem_table_add_col(table, heap, "TYPE", DATA_INT, 0, 4);
  dict_mem_table_add_col(table, heap, "MIX_ID", DATA_BINARY, 0, 0);
  /* MIX_LEN may contain additional table flags when
  ROW_FORMAT!=REDUNDANT.  Currently, these flags include
  DICT_TF2_TEMPORARY. */
  dict_mem_table_add_col(table, heap, "MIX_LEN", DATA_INT, 0, 4);
  dict_mem_table_add_col(table, heap, "CLUSTER_NAME", DATA_BINARY, 0, 0);
  dict_mem_table_add_col(table, heap, "SPACE", DATA_INT, 0, 4);

  table->id = DICT_TABLES_ID;

  dict_table_add_to_cache(table, heap);
  dict_sys->sys_tables = table;
  mem_heap_empty(heap);

  auto index = dict_mem_index_create("SYS_TABLES", "CLUST_IND", DICT_HDR_SPACE, DICT_UNIQUE | DICT_CLUSTERED, 1);

  dict_mem_index_add_field(index, "NAME", 0);

  index->id = DICT_TABLES_ID;

  auto err = dict_index_add_to_cache(table, index, mtr.read_ulint(dict_hdr + DICT_HDR_TABLES, MLOG_4BYTES), false);
  ut_a(err == DB_SUCCESS);

  /*-------------------------*/
  index = dict_mem_index_create("SYS_TABLES", "ID_IND", DICT_HDR_SPACE, DICT_UNIQUE, 1);
  dict_mem_index_add_field(index, "ID", 0);

  index->id = DICT_TABLE_IDS_ID;
  err = dict_index_add_to_cache(table, index, mtr.read_ulint(dict_hdr + DICT_HDR_TABLE_IDS, MLOG_4BYTES), false);
  ut_a(err == DB_SUCCESS);

  /*-------------------------*/
  table = dict_mem_table_create("SYS_COLUMNS", DICT_HDR_SPACE, 7, 0);

  dict_mem_table_add_col(table, heap, "TABLE_ID", DATA_BINARY, 0, 0);
  dict_mem_table_add_col(table, heap, "POS", DATA_INT, 0, 4);
  dict_mem_table_add_col(table, heap, "NAME", DATA_BINARY, 0, 0);
  dict_mem_table_add_col(table, heap, "MTYPE", DATA_INT, 0, 4);
  dict_mem_table_add_col(table, heap, "PRTYPE", DATA_INT, 0, 4);
  dict_mem_table_add_col(table, heap, "LEN", DATA_INT, 0, 4);
  dict_mem_table_add_col(table, heap, "PREC", DATA_INT, 0, 4);

  table->id = DICT_COLUMNS_ID;

  dict_table_add_to_cache(table, heap);
  dict_sys->sys_columns = table;
  mem_heap_empty(heap);

  index = dict_mem_index_create("SYS_COLUMNS", "CLUST_IND", DICT_HDR_SPACE, DICT_UNIQUE | DICT_CLUSTERED, 2);

  dict_mem_index_add_field(index, "TABLE_ID", 0);
  dict_mem_index_add_field(index, "POS", 0);

  index->id = DICT_COLUMNS_ID;
  err = dict_index_add_to_cache(table, index, mtr.read_ulint(dict_hdr + DICT_HDR_COLUMNS, MLOG_4BYTES), false);
  ut_a(err == DB_SUCCESS);

  /*-------------------------*/
  table = dict_mem_table_create("SYS_INDEXES", DICT_HDR_SPACE, 7, 0);

  dict_mem_table_add_col(table, heap, "TABLE_ID", DATA_BINARY, 0, 0);
  dict_mem_table_add_col(table, heap, "ID", DATA_BINARY, 0, 0);
  dict_mem_table_add_col(table, heap, "NAME", DATA_BINARY, 0, 0);
  dict_mem_table_add_col(table, heap, "N_FIELDS", DATA_INT, 0, 4);
  dict_mem_table_add_col(table, heap, "TYPE", DATA_INT, 0, 4);
  dict_mem_table_add_col(table, heap, "SPACE", DATA_INT, 0, 4);
  dict_mem_table_add_col(table, heap, "PAGE_NO", DATA_INT, 0, 4);

  /* The '+ 2' below comes from the fields DB_TRX_ID, DB_ROLL_PTR */

  static_assert(DICT_SYS_INDEXES_PAGE_NO_FIELD == 6 + 2, "error DICT_SYS_INDEXES_PAGE_NO_FIELD != 6 + 2");

  static_assert(DICT_SYS_INDEXES_SPACE_NO_FIELD == 5 + 2, "rror DICT_SYS_INDEXES_SPACE_NO_FIELD != 5 + 2");

  static_assert(DICT_SYS_INDEXES_TYPE_FIELD == 4 + 2, "error DICT_SYS_INDEXES_TYPE_FIELD != 4 + 2");

  static_assert(DICT_SYS_INDEXES_NAME_FIELD == 2 + 2, "error DICT_SYS_INDEXES_NAME_FIELD != 2 + 2");

  table->id = DICT_INDEXES_ID;
  dict_table_add_to_cache(table, heap);
  dict_sys->sys_indexes = table;
  mem_heap_empty(heap);

  index = dict_mem_index_create("SYS_INDEXES", "CLUST_IND", DICT_HDR_SPACE, DICT_UNIQUE | DICT_CLUSTERED, 2);

  dict_mem_index_add_field(index, "TABLE_ID", 0);
  dict_mem_index_add_field(index, "ID", 0);

  index->id = DICT_INDEXES_ID;
  err = dict_index_add_to_cache(table, index, mtr.read_ulint(dict_hdr + DICT_HDR_INDEXES, MLOG_4BYTES), false);
  ut_a(err == DB_SUCCESS);

  /*-------------------------*/
  table = dict_mem_table_create("SYS_FIELDS", DICT_HDR_SPACE, 3, 0);

  dict_mem_table_add_col(table, heap, "INDEX_ID", DATA_BINARY, 0, 0);
  dict_mem_table_add_col(table, heap, "POS", DATA_INT, 0, 4);
  dict_mem_table_add_col(table, heap, "COL_NAME", DATA_BINARY, 0, 0);

  table->id = DICT_FIELDS_ID;
  dict_table_add_to_cache(table, heap);
  dict_sys->sys_fields = table;
  mem_heap_free(heap);

  index = dict_mem_index_create("SYS_FIELDS", "CLUST_IND", DICT_HDR_SPACE, DICT_UNIQUE | DICT_CLUSTERED, 2);

  dict_mem_index_add_field(index, "INDEX_ID", 0);
  dict_mem_index_add_field(index, "POS", 0);

  index->id = DICT_FIELDS_ID;
  err = dict_index_add_to_cache(table, index, mtr.read_ulint(dict_hdr + DICT_HDR_FIELDS, MLOG_4BYTES), false);
  ut_a(err == DB_SUCCESS);

  mtr.commit();
  /*-------------------------*/

  /* Load definitions of other indexes on system tables */

  dict_load_sys_table(dict_sys->sys_tables);
  dict_load_sys_table(dict_sys->sys_columns);
  dict_load_sys_table(dict_sys->sys_indexes);
  dict_load_sys_table(dict_sys->sys_fields);

  mutex_exit(&dict_sys->mutex);
}

/** Inserts the basic system table data into themselves in the database
creation. */
static void dict_insert_initial_data() { }

void dict_create() {
  mtr_t mtr;

  mtr.start();

  dict_hdr_create(&mtr);

  mtr.commit();

  dict_boot();

  dict_insert_initial_data();
}
