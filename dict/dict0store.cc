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

/** @file dict/dict0store.cc
Data dictionary persistence functions.

Created 4/18/1996 Heikki Tuuri
Modified 2024-Oct-15 Sunny Bains
*******************************************************/

#include "btr0btr.h"
#include "buf0flu.h"
#include "btr0pcur.h"
#include "dict0dict.h"
#include "dict0load.h"
#include "ddl0ddl.h"
#include "log0recv.h"
#include "os0file.h"
#include "que0que.h"
#include "pars0pars.h"
#include "row0ins.h"
#include "srv0srv.h"
#include "trx0trx.h"

Dict_store::Dict_store(Dict *dict, Btree *btree) noexcept
  : m_dict{dict}, m_fsp{btree->m_fsp}, m_btree{btree} {}

void Dict_store::destroy(Dict_store *dict_store) noexcept {
  ut_delete(dict_store);
}

Dict_store::hdr_t *Dict_store::hdr_get(mtr_t *mtr) noexcept {
  Buf_pool::Request req {
      .m_rw_latch = RW_X_LATCH,
      .m_page_id = { DICT_HDR_SPACE, DICT_HDR_PAGE_NO },
      .m_mode = BUF_GET,
      .m_file = __FILE__,
      .m_line = __LINE__,
      .m_mtr = mtr
    };

  auto block = m_fsp->m_buf_pool->get(req, nullptr);
  auto header = DICT_HDR + block->get_frame();

  buf_block_dbg_add_level(IF_SYNC_DEBUG(block, SYNC_DICT_HEADER));

  return header;
}

Dict_id Dict_store::hdr_get_new_id(Dict_id_type type) noexcept {
  ut_ad(type == Dict_id_type::TABLE_ID || type == Dict_id_type::INDEX_ID);

  mtr_t mtr;

  mtr.start();

  auto hdr = hdr_get(&mtr);

  const Dict_id id = mtr.read_uint64(hdr + ulint(type)) + 1;

  mlog_write_uint64(hdr + ulint(type), id, &mtr);

  mtr.commit();

  return id;
}

void Dict_store::hdr_flush_row_id() noexcept {
  ut_ad(mutex_own(&m_dict->m_mutex));

  const auto id = m_row_id;

  mtr_t mtr;

  mtr.start();

  auto hdr = hdr_get(&mtr);

  mlog_write_uint64(hdr + DICT_HDR_ROW_ID, id, &mtr);

  mtr.commit();
}

bool Dict_store::bootstrap_system_tables(mtr_t *mtr) noexcept {

  /* Create the dictionary header file block in a new, allocated file
  segment in the system tablespace */
  auto block = m_fsp->fseg_create(DICT_HDR_SPACE, 0, DICT_HDR + DICT_HDR_FSEG_HEADER, mtr);

  ut_a(DICT_HDR_PAGE_NO == block->get_page_no());

  auto hdr = hdr_get(mtr);

  /* Start counting row, table, index, and tree ids from DICT_HDR_FIRST_ID */
  mlog_write_uint64(hdr + DICT_HDR_ROW_ID, DICT_HDR_FIRST_ID, mtr);

  mlog_write_uint64(hdr + ulint(Dict_id_type::TABLE_ID), DICT_HDR_FIRST_ID, mtr);

  mlog_write_uint64(hdr + ulint(Dict_id_type::INDEX_ID), DICT_HDR_FIRST_ID, mtr);

  /* Obsolete, but we must initialize it to 0 anyway. */
  mlog_write_uint64(hdr + DICT_HDR_MIX_ID, DICT_HDR_FIRST_ID, mtr);

  /* Create the B-tree roots for the clustered indexes of the basic system tables */

  auto root_page_no = m_btree->create(DICT_CLUSTERED | DICT_UNIQUE, DICT_HDR_SPACE, DICT_TABLES_ID, m_dict->m_dummy_index, mtr);

  if (root_page_no == FIL_NULL) {

    return false;
  }

  mlog_write_ulint(hdr + DICT_HDR_TABLES, root_page_no, MLOG_4BYTES, mtr);

  root_page_no = m_btree->create(DICT_UNIQUE, DICT_HDR_SPACE, DICT_TABLE_IDS_ID, m_dict->m_dummy_index, mtr);

  if (root_page_no == FIL_NULL) {

    return false;
  }

  mlog_write_ulint(hdr + DICT_HDR_TABLE_IDS, root_page_no, MLOG_4BYTES, mtr);

  root_page_no = m_btree->create(DICT_CLUSTERED | DICT_UNIQUE, DICT_HDR_SPACE, DICT_COLUMNS_ID, m_dict->m_dummy_index, mtr);

  if (root_page_no == FIL_NULL) {

    return false;
  }

  mlog_write_ulint(hdr + DICT_HDR_COLUMNS, root_page_no, MLOG_4BYTES, mtr);

  root_page_no = m_btree->create(DICT_CLUSTERED | DICT_UNIQUE, DICT_HDR_SPACE, DICT_INDEXES_ID, m_dict->m_dummy_index, mtr);

  if (root_page_no == FIL_NULL) {

    return false;
  }

  mlog_write_ulint(hdr + DICT_HDR_INDEXES, root_page_no, MLOG_4BYTES, mtr);

  root_page_no = m_btree->create(DICT_CLUSTERED | DICT_UNIQUE, DICT_HDR_SPACE, DICT_FIELDS_ID, m_dict->m_dummy_index, mtr);

  if (root_page_no == FIL_NULL) {

    return false;
  }

  mlog_write_ulint(hdr + DICT_HDR_FIELDS, root_page_no, MLOG_4BYTES, mtr);

  return true;
}

Dict_id Dict_store::sys_get_new_row_id() noexcept {
  m_dict->mutex_acquire();

  const auto id = m_row_id;

  if ((id % DICT_HDR_ROW_ID_WRITE_MARGIN) == 0) {

    hdr_flush_row_id();
  }

  ++m_row_id;

  m_dict->mutex_release();

  return id;
}

DTuple *Dict_store::create_sys_tables_tuple(const Table *table, mem_heap_t *heap) noexcept {
  auto sys_tables = m_dict->m_sys_tables;
  auto entry = dtuple_create(heap, 8 + DATA_N_SYS_COLS);

  sys_tables->copy_types(entry);

  /* 0: NAME -----------------------------*/
  auto dfield = dtuple_get_nth_field(entry, 0 /*NAME*/);

  dfield_set_data(dfield, table->m_name, strlen(table->m_name));

  /* 3: ID -------------------------------*/
  dfield = dtuple_get_nth_field(entry, 1 /*ID*/);

  auto ptr = static_cast<byte *>(mem_heap_alloc(heap, 8));

  mach_write_to_8(ptr, table->m_id);

  dfield_set_data(dfield, ptr, 8);

  /* 4: N_COLS ---------------------------*/
  dfield = dtuple_get_nth_field(entry, 2 /*N_COLS*/);

  ptr = static_cast<byte *>(mem_heap_alloc(heap, 4));
  mach_write_to_4(ptr, table->m_n_fields);
  dfield_set_data(dfield, ptr, 4);

  /* 5: TYPE -----------------------------*/
  dfield = dtuple_get_nth_field(entry, 3 /*TYPE*/);

  ptr = static_cast<byte *>(mem_heap_alloc(heap, 4));

  mach_write_to_4(ptr, DICT_TABLE_ORDINARY);

  dfield_set_data(dfield, ptr, 4);

  /* 6: MIX_ID (obsolete) ---------------------------*/
  dfield = dtuple_get_nth_field(entry, 4 /*MIX_ID*/);

  ptr = static_cast<byte *>(mem_heap_zalloc(heap, 8));

  dfield_set_data(dfield, ptr, 8);

  /* 7: MIX_LEN (additional flags) --------------------------*/

  dfield = dtuple_get_nth_field(entry, 5 /*MIX_LEN*/);

  ptr = static_cast<byte *>(mem_heap_alloc(heap, 4));
  mach_write_to_4(ptr, table->m_flags >> DICT_TF2_SHIFT);

  dfield_set_data(dfield, ptr, 4);

  /* 8: CLUSTER_NAME ---------------------*/
  dfield = dtuple_get_nth_field(entry, 6 /*CLUSTER_NAME*/);
  dfield_set_null(dfield); /* not supported */

  /* 9: SPACE ----------------------------*/
  dfield = dtuple_get_nth_field(entry, 7 /*SPACE*/);

  ptr = static_cast<byte *>(mem_heap_alloc(heap, 4));
  mach_write_to_4(ptr, table->m_space_id);

  dfield_set_data(dfield, ptr, 4);
  /*----------------------------------*/

  return entry;
}

DTuple *Dict_store::create_sys_columns_tuple(const Table *table, ulint i, mem_heap_t *heap) noexcept {
  const char *col_name;

  auto column = table->get_nth_col(i);
  auto sys_columns = m_dict->m_sys_columns;
  auto entry = dtuple_create(heap, 7 + DATA_N_SYS_COLS);

  sys_columns->copy_types(entry);

  /* 0: TABLE_ID -----------------------*/
  auto dfield = dtuple_get_nth_field(entry, 0 /*TABLE_ID*/);
  auto ptr = static_cast<byte *>(mem_heap_alloc(heap, 8));

  mach_write_to_8(ptr, table->m_id);

  dfield_set_data(dfield, ptr, 8);

  /* 1: POS ----------------------------*/
  dfield = dtuple_get_nth_field(entry, 1 /*POS*/);

  ptr = (byte *)mem_heap_alloc(heap, 4);
  mach_write_to_4(ptr, i);

  dfield_set_data(dfield, ptr, 4);
  /* 4: NAME ---------------------------*/
  dfield = dtuple_get_nth_field(entry, 2 /*NAME*/);

  col_name = table->get_col_name(i);
  dfield_set_data(dfield, col_name, strlen(col_name));

  /* 5: MTYPE --------------------------*/
  dfield = dtuple_get_nth_field(entry, 3 /*MTYPE*/);

  ptr = static_cast<byte *>(mem_heap_alloc(heap, 4));
  mach_write_to_4(ptr, column->mtype);

  dfield_set_data(dfield, ptr, 4);
  /* 6: PRTYPE -------------------------*/
  dfield = dtuple_get_nth_field(entry, 4 /*PRTYPE*/);

  ptr = static_cast<byte *>(mem_heap_alloc(heap, 4));
  mach_write_to_4(ptr, column->prtype);

  dfield_set_data(dfield, ptr, 4);
  /* 7: LEN ----------------------------*/
  dfield = dtuple_get_nth_field(entry, 5 /*LEN*/);

  ptr = static_cast<byte *>(mem_heap_alloc(heap, 4));
  mach_write_to_4(ptr, column->len);

  dfield_set_data(dfield, ptr, 4);
  /* 8: PREC ---------------------------*/
  dfield = dtuple_get_nth_field(entry, 6 /*PREC*/);

  ptr = static_cast<byte *>(mem_heap_alloc(heap, 4));
  mach_write_to_4(ptr, 0 /* unused */);

  dfield_set_data(dfield, ptr, 4);
  /*---------------------------------*/

  return entry;
}

db_err Dict_store::build_table_def_step(que_thr_t *thr, Table_node *node) noexcept {
  mtr_t mtr;

  ut_ad(mutex_own(&m_dict->m_mutex));

  auto table = node->m_table;

  table->m_id = hdr_get_new_id(Dict_id_type::TABLE_ID);

  thr_get_trx(thr)->table_id = table->m_id;

  if (srv_config.m_file_per_table) {
    /* We create a new single-table tablespace for the table.
    We initially let it be 4 pages:
    - page 0 is the fsp header and an extent descriptor page,
    - page 1 is an ibuf bitmap page,
    - page 2 is the first inode page,
    - page 3 will contain the root of the clustered index of the table we create here. */

    bool is_path;
    const char *path_or_name;
    space_id_t space_id{NULL_SPACE_ID};

    if (table->m_dir_path_of_temp_table != nullptr) {
      /* We place tables created with CREATE TEMPORARY TABLE in the configured tmp dir. */
      is_path = true;
      path_or_name = table->m_dir_path_of_temp_table;
    } else {
      is_path = false;
      path_or_name = table->m_name;
    }

    auto flags = table->m_flags & ~(~0UL << DICT_TF_BITS);

    auto fil = m_fsp->m_fil;

    auto err = fil->create_new_single_table_tablespace(&space_id, path_or_name, is_path, flags, FIL_IBD_FILE_INITIAL_SIZE);

    table->m_space_id = space_id;

    if (err != DB_SUCCESS) {

      return err;
    }

    mtr.start();

    m_fsp->header_init(table->m_space_id, FIL_IBD_FILE_INITIAL_SIZE, &mtr);

    mtr.commit();

  } else {
    /* Create in the system tablespace: disallow new features */
  }

  auto row = create_sys_tables_tuple(table, node->m_heap);

  row_ins_node_set_new_row(node->m_tab_def, row);

  return DB_SUCCESS;
}

db_err Dict_store::build_col_def_step(Table_node *node) noexcept {
  auto row = create_sys_columns_tuple(node->m_table, node->m_col_no, node->m_heap);
  row_ins_node_set_new_row(node->m_col_def, row);

  return DB_SUCCESS;
}

DTuple *Dict_store::create_sys_indexes_tuple(const Index *index, mem_heap_t *heap) noexcept {
  ut_ad(mutex_own(&m_dict->m_mutex));

  auto sys_indexes = m_dict->m_sys_indexes;
  auto table = index->m_table;
  auto entry = dtuple_create(heap, 7 + DATA_N_SYS_COLS);

  sys_indexes->copy_types(entry);

  /* 0: TABLE_ID -----------------------*/
  auto dfield = dtuple_get_nth_field(entry, 0 /*TABLE_ID*/);

  auto ptr = static_cast<byte *>(mem_heap_alloc(heap, 8));
  mach_write_to_8(ptr, table->m_id);

  dfield_set_data(dfield, ptr, 8);

  /* 1: ID ----------------------------*/
  dfield = dtuple_get_nth_field(entry, 1 /*ID*/);

  ptr = static_cast<byte *>(mem_heap_alloc(heap, 8));
  mach_write_to_8(ptr, index->m_id);

  dfield_set_data(dfield, ptr, 8);

  /* 4: NAME --------------------------*/
  dfield = dtuple_get_nth_field(entry, 2 /*NAME*/);

  dfield_set_data(dfield, index->m_name, strlen(index->m_name));

  /* 5: N_FIELDS ----------------------*/
  dfield = dtuple_get_nth_field(entry, 3 /*N_FIELDS*/);

  ptr = static_cast<byte *>(mem_heap_alloc(heap, 4));
  mach_write_to_4(ptr, index->m_n_fields);

  dfield_set_data(dfield, ptr, 4);

  /* 6: TYPE --------------------------*/
  dfield = dtuple_get_nth_field(entry, 4 /*TYPE*/);

  ptr = (byte *)mem_heap_alloc(heap, 4);
  mach_write_to_4(ptr, index->m_type);

  dfield_set_data(dfield, ptr, 4);

  /* 7: SPACE --------------------------*/
  static_assert(DICT_SYS_INDEXES_SPACE_NO_FIELD == 7, "error DICT_SYS_INDEXES_SPACE_NO_FIELD != 7");

  dfield = dtuple_get_nth_field(entry, 5 /*SPACE*/);

  ptr = static_cast<byte *>(mem_heap_alloc(heap, 4));
  mach_write_to_4(ptr, index->m_page_id.m_space_id);

  dfield_set_data(dfield, ptr, 4);

  /* 8: PAGE_NO --------------------------*/
  static_assert(DICT_SYS_INDEXES_PAGE_NO_FIELD == 8, "error DICT_SYS_INDEXES_PAGE_NO_FIELD != 8");

  dfield = dtuple_get_nth_field(entry, 6 /*PAGE_NO*/);

  ptr = static_cast<byte *>(mem_heap_alloc(heap, 4));
  mach_write_to_4(ptr, FIL_NULL);

  dfield_set_data(dfield, ptr, 4);

  return entry;
}

DTuple *Dict_store::create_sys_fields_tuple(const Index *index, ulint i, mem_heap_t *heap) noexcept {
  bool index_contains_column_prefix_field{};

  for (ulint i{}; i < index->m_n_fields; ++i) {
    if (index->get_nth_field(i)->m_prefix_len > 0) {
      index_contains_column_prefix_field = true;
      break;
    }
  }

  auto field = index->get_nth_field(i);
  auto sys_fields = m_dict->m_sys_fields;
  auto entry = dtuple_create(heap, 3 + DATA_N_SYS_COLS);

  sys_fields->copy_types(entry);

  /* 0: INDEX_ID -----------------------*/
  auto dfield = dtuple_get_nth_field(entry, 0 /*INDEX_ID*/);

  auto ptr = static_cast<byte *>(mem_heap_alloc(heap, 8));
  mach_write_to_8(ptr, index->m_id);

  dfield_set_data(dfield, ptr, 8);

  /* 1: POS + PREFIX LENGTH ----------------------------*/
  dfield = dtuple_get_nth_field(entry, 1 /*POS*/);

  ptr = static_cast<byte *>(mem_heap_alloc(heap, 4));

  if (index_contains_column_prefix_field) {
    /* If there are column prefix fields in the index, then
    we store the number of the field to the 2 HIGH bytes
    and the prefix length to the 2 low bytes, */

    mach_write_to_4(ptr, (i << 16) + field->m_prefix_len);

  } else {
    /* Else we store the number of the field to the 2 LOW bytes.
    This is to keep the storage format compatible with
    InnoDB versions < 4.0.14. */

    mach_write_to_4(ptr, i);
  }

  dfield_set_data(dfield, ptr, 4);

  /* 4: COL_NAME -------------------------*/
  dfield = dtuple_get_nth_field(entry, 2 /*COL_NAME*/);

  dfield_set_data(dfield, field->m_name, strlen(field->m_name));

  return entry;
}

DTuple *Dict_store::create_search_tuple(const DTuple *tuple, mem_heap_t *heap) noexcept {
  auto search_tuple = dtuple_create(heap, 2);
  auto field1 = dtuple_get_nth_field(tuple, 0);
  auto field2 = dtuple_get_nth_field(search_tuple, 0);

  dfield_copy(field2, field1);

  field1 = dtuple_get_nth_field(tuple, 1);
  field2 = dtuple_get_nth_field(search_tuple, 1);

  dfield_copy(field2, field1);

  ut_ad(dtuple_validate(search_tuple));

  return search_tuple;
}

db_err Dict_store::build_index_def_step(que_thr_t *thr, Index_node *node) noexcept {
  ut_ad(mutex_own(&m_dict->m_mutex));

  auto trx = thr_get_trx(thr);
  auto index = node->m_index;

  {
    auto table = m_dict->table_get(index->m_table->m_name);

    if (table == nullptr) {
      return DB_TABLE_NOT_FOUND;
    }

    if (index->m_table->m_space_id == NULL_SPACE_ID) {
      /* Another check for the dummy table, check that it doesn't have any user columns. */
      ut_a(index->m_table->m_n_cols == DATA_N_SYS_COLS);
      /* Destroy the dummy table created by the parse in pars_create_table(). */
      Table::destroy(index->m_table, Current_location());
    }

    index->m_table = table;
  }

  node->m_table = index->m_table;
  trx->table_id = index->m_table->m_id;

  ut_ad(!index->m_table->m_indexes.empty() || index->is_clustered());

  index->m_id = hdr_get_new_id(Dict_id_type::INDEX_ID);

  /* Inherit the space id from the table; we store all indexes of a
  table in the same tablespace */

  node->m_page_no = FIL_NULL;
  index->m_page_id.m_space_id = index->m_table->m_space_id;

  auto row = create_sys_indexes_tuple(index, node->m_heap);

  node->m_ind_row = row;

  row_ins_node_set_new_row(node->m_ind_def, row);

  /* Note that the index was created by this transaction. */
  index->m_trx_id = trx->m_id;

  return DB_SUCCESS;
}

db_err Dict_store::build_field_def_step(Index_node *node) noexcept {
  auto index = node->m_index;

  auto row = create_sys_fields_tuple(index, node->m_field_no, node->m_heap);

  row_ins_node_set_new_row(node->m_field_def, row);

  return DB_SUCCESS;
}

db_err Dict_store::create_index_tree_step(Index_node *node) noexcept {
  ut_ad(mutex_own(&m_dict->m_mutex));

  Btree_pcursor pcur(m_fsp, m_btree);

  auto index = node->m_index;
  auto sys_indexes = m_dict->m_sys_indexes;

  /* Run a mini-transaction in which the index tree is allocated for
  the index and its root address is written to the index entry in
  sys_indexes */

  mtr_t mtr;

  mtr.start();

  auto search_tuple = create_search_tuple(node->m_ind_row, node->m_heap);

  pcur.open(sys_indexes->m_indexes.front(), search_tuple, PAGE_CUR_L, BTR_MODIFY_LEAF, &mtr, Current_location());

  (void) pcur.move_to_next_user_rec(&mtr);

  node->m_page_no = m_btree->create(index->m_type, index->m_page_id.m_space_id, index->m_id, index, &mtr);

  page_rec_write_index_page_no(pcur.get_rec(), DICT_SYS_INDEXES_PAGE_NO_FIELD, node->m_page_no, &mtr);

  pcur.close();

  mtr.commit();

  if (node->m_page_no == FIL_NULL) {

    return DB_OUT_OF_FILE_SPACE;
  }

  return DB_SUCCESS;
}

void Dict_store::drop_index_tree(rec_t *rec, mtr_t *mtr) noexcept {
  ut_ad(mutex_own(&m_dict->m_mutex));

  ulint len;

  auto ptr = rec_get_nth_field(rec, DICT_SYS_INDEXES_PAGE_NO_FIELD, &len);

  ut_ad(len == 4);

  auto root_page_no = mtr->read_ulint(ptr, MLOG_4BYTES);

  if (root_page_no == FIL_NULL) {
    /* The tree has already been freed */
    return;
  }

  ptr = rec_get_nth_field(rec, DICT_SYS_INDEXES_SPACE_NO_FIELD, &len);

  ut_ad(len == 4);

  auto space_id = static_cast<space_id_t>(mtr->read_ulint(ptr, MLOG_4BYTES));

  if (m_fsp->m_fil->space_get_flags(space_id) == ULINT_UNDEFINED) {
    /* It is a single table tablespace and the .ibd file is
    missing: do nothing */
    return;
  }

  /* We free all the pages but the root page first; this operation
  may span several mini-transactions */

  m_btree->free_but_not_root(space_id, root_page_no);

  /* Then we free the root page in the same mini-transaction where
  we write FIL_NULL to the appropriate field in the SYS_INDEXES
  record: this mini-transaction marks the B-tree totally freed */

  m_btree->free_root(space_id, root_page_no, mtr);

  page_rec_write_index_page_no(rec, DICT_SYS_INDEXES_PAGE_NO_FIELD, FIL_NULL, mtr);
}

page_no_t Dict_store::truncate_index_tree(Table *table, space_id_t space_id, Btree_pcursor *pcur, mtr_t *mtr) noexcept {
  ut_ad(mutex_own(&m_dict->m_mutex));

  /* We never drop the system tablespace. */
  bool drop = space_id != SYS_TABLESPACE;

  ulint len;
  auto rec = pcur->get_rec();
  auto ptr = rec_get_nth_field(rec, DICT_SYS_INDEXES_PAGE_NO_FIELD, &len);
  ut_ad(len == 4);

  auto root_page_no = mtr->read_ulint(ptr, MLOG_4BYTES);

  if (drop && root_page_no == FIL_NULL) {
    /* The tree has been freed. */

    log_err(std::format("Trying to TRUNCATE a missing index of table {}!", table->m_name));
    drop = false;
  }

  ptr = rec_get_nth_field(rec, DICT_SYS_INDEXES_SPACE_NO_FIELD, &len);

  ut_ad(len == 4);

  if (drop) {
    space_id = mtr->read_ulint(ptr, MLOG_4BYTES);
  }

  auto fil = m_fsp->m_fil;

  if (fil->space_get_flags(space_id) == ULINT_UNDEFINED) {
    /* It is a single table tablespace and the .ibd file is
    missing: do nothing */

    log_err(std::format("Trying to TRUNCATE a missing .ibd file of table {}!", table->m_name));

    return FIL_NULL;
  }

  ptr = rec_get_nth_field(rec, DICT_SYS_INDEXES_TYPE_FIELD, &len);
  ut_ad(len == 4);

  auto type = mach_read_from_4(ptr);

  ptr = rec_get_nth_field(rec, 1, &len);
  ut_ad(len == 8);
  auto index_id = mach_read_from_8(ptr);

  if (drop) {
    /* We free all the pages but the root page first; this operation
    may span several mini-transactions */

    m_btree->free_but_not_root(space_id, root_page_no);

    /* Then we free the root page in the same mini-transaction where
    we create the b-tree and write its new root page number to the
    appropriate field in the SYS_INDEXES record: this mini-transaction
    marks the B-tree totally truncated */

    (void) m_btree->page_get(space_id, root_page_no, RW_X_LATCH, mtr);

    m_btree->free_root(space_id, root_page_no, mtr);
  }

  /* We will temporarily write FIL_NULL to the PAGE_NO field
  in SYS_INDEXES, so that the database will not get into an
  inconsistent state in case it crashes between the mtr.commit()
  below and the following mtr.commit() call. */
  page_rec_write_index_page_no(rec, DICT_SYS_INDEXES_PAGE_NO_FIELD, FIL_NULL, mtr);

  /* We will need to commit the mini-transaction in order to avoid
  deadlocks in the btr_create() call, because otherwise we would
  be freeing and allocating pages in the same mini-transaction. */
  pcur->store_position(mtr);

  mtr->commit();

  mtr->start();

  (void) pcur->restore_position(BTR_MODIFY_LEAF, mtr, Current_location());

  /* Find the index corresponding to this SYS_INDEXES record. */
  for (auto index : table->m_indexes) {
    if (index->m_id == index_id) {
      root_page_no = m_btree->create(type, space_id, index_id, index, mtr);
      index->m_page_id.m_page_no = root_page_no;

      return root_page_no;
    }
  }

  log_err(std::format("Index {} of table {} is missing from the data dictionary during TRUNCATE!", index_id, table->m_name));

  return FIL_NULL;
}

que_thr_t *Dict_store::create_table_step(que_thr_t *thr) noexcept {
  ut_ad(mutex_own(&m_dict->m_mutex));

  auto trx = thr_get_trx(thr);
  auto node = static_cast<Table_node *>(thr->run_node);

  ut_ad(que_node_get_type(node) == QUE_NODE_CREATE_TABLE);

  if (thr->prev_node == que_node_get_parent(node)) {
    node->m_state = TABLE_BUILD_TABLE_DEF;
  }

  auto clean_up = [&](db_err err) -> que_thr_t * {
    trx->error_state = err;

    if (err == DB_SUCCESS) {
      /* Ok: do nothing */

    } else if (err == DB_LOCK_WAIT) {

      return nullptr;

    } else {

      /* SQL error detected */
      return nullptr;
    }

    thr->run_node = que_node_get_parent(node);

    return thr;
  };

  if (node->m_state == TABLE_BUILD_TABLE_DEF) {

    /* DO THE CHECKS OF THE CONSISTENCY CONSTRAINTS HERE */

    auto err = build_table_def_step(thr, node);

    if (err != DB_SUCCESS) {

      return clean_up(err);
    }

    node->m_col_no = 0;
    node->m_state = TABLE_BUILD_COL_DEF;

    thr->run_node = node->m_tab_def;

    return thr;
  }

  if (node->m_state == TABLE_BUILD_COL_DEF) {

    if (node->m_col_no < node->m_table->m_n_fields) {

      auto err = build_col_def_step(node);

      if (err != DB_SUCCESS) {

        return clean_up(err);
      }

      ++node->m_col_no;

      thr->run_node = node->m_col_def;

      return thr;
    } else {
      node->m_state = TABLE_COMMIT_WORK;
    }
  }

  if (node->m_state == TABLE_COMMIT_WORK) {

    /* Table was correctly defined: do NOT commit the transaction
    (CREATE TABLE does NOT do an implicit commit of the current
    transaction) */

    node->m_state = TABLE_ADD_TO_CACHE;

    /* thr->run_node = node->commit_node;

    return(thr); */
  }

  if (node->m_state == TABLE_ADD_TO_CACHE) {
    m_dict->table_add_to_cache(node->m_table);
    return clean_up(DB_SUCCESS);
  } else {
    return clean_up(DB_ERROR);
  }
}

que_thr_t *Dict_store::create_index_step(que_thr_t *thr) noexcept {
  ut_ad(mutex_own(&m_dict->m_mutex));

  auto trx = thr_get_trx(thr);
  auto node = static_cast<Index_node *>(thr->run_node);

  ut_ad(que_node_get_type(node) == QUE_NODE_CREATE_INDEX);

  if (thr->prev_node == que_node_get_parent(node)) {
    node->m_state = INDEX_BUILD_INDEX_DEF;
  }

  auto clean_up = [&](db_err err) -> que_thr_t * {
    trx->error_state = err;

    if (err == DB_SUCCESS) {
      /* Ok: do nothing */

    } else if (err == DB_LOCK_WAIT) {

      return nullptr;
    } else {
      /* SQL error detected */

      return nullptr;
    }

    thr->run_node = que_node_get_parent(node);

    return thr;
  };

  for (;;) {
    switch (node->m_state) {
      case INDEX_BUILD_INDEX_DEF: {
        /* DO THE CHECKS OF THE CONSISTENCY CONSTRAINTS HERE */
        const auto err = build_index_def_step(thr, node);

        if (err != DB_SUCCESS) {

          return clean_up(err);
        }

        node->m_field_no = 0;
        node->m_state = INDEX_BUILD_FIELD_DEF;

        thr->run_node = node->m_ind_def;

        return thr;
      }

      case INDEX_BUILD_FIELD_DEF: {
        if (node->m_field_no < (node->m_index)->m_n_fields) {
          const auto err = build_field_def_step(node);

          if (err != DB_SUCCESS) {

            return clean_up(err);
          }

          
          ++node->m_field_no;
          thr->run_node = node->m_field_def;

          return thr;
        } else {
          node->m_state = INDEX_ADD_TO_CACHE;
          break;
        }
      }

      case INDEX_ADD_TO_CACHE: {
        const auto index_id = node->m_index->m_id;
        const auto err = m_dict->index_add_to_cache(node->m_index, FIL_NULL, true);

        node->m_index = m_dict->index_get_if_in_cache(index_id);
        ut_a(!node->m_index == (err != DB_SUCCESS));

        if (err != DB_SUCCESS) {

          return clean_up(err);
        }

        node->m_state = INDEX_CREATE_INDEX_TREE;
        break;
      }

      case INDEX_CREATE_INDEX_TREE: {

        auto err = create_index_tree_step(node);

        if (err != DB_SUCCESS) {
          m_dict->index_remove_from_cache(node->m_table, node->m_index);
          node->m_index = nullptr;

          return clean_up(err);
        }

        node->m_index->m_page_id.m_page_no = node->m_page_no;
        node->m_state = INDEX_COMMIT_WORK;
        break;
      }

      case INDEX_COMMIT_WORK: {

        /* Index was correctly defined: do NOT commit the transaction
        (CREATE INDEX does NOT currently do an implicit commit of
        the current transaction) */

        node->m_state = INDEX_CREATE_INDEX_TREE;

        /* thr->run_node = node->m_commit_node;

        return(thr); */
        return clean_up(DB_SUCCESS);
      }

      default:
        return clean_up(DB_ERROR);
    }
  }

  return clean_up(DB_SUCCESS);
}

db_err Dict_store::create_or_check_foreign_constraint_tables() noexcept {
  m_dict->mutex_acquire();

  auto sys_foreign = m_dict->table_get("SYS_FOREIGN");
  auto sys_foreign_cols = m_dict->table_get("SYS_FOREIGN_COLS");

  if (sys_foreign != nullptr && sys_foreign_cols != nullptr && sys_foreign->m_indexes.size() == 3 && sys_foreign_cols->m_indexes.size() == 1) {

    /* Foreign constraint system tables have already been created, and they are ok */

    if (!sys_foreign->m_cached) {
      Table::destroy(sys_foreign, Current_location());
    }

    if (!sys_foreign_cols->m_cached) {
      Table::destroy(sys_foreign_cols, Current_location());
    }

    m_dict->mutex_release();

    return DB_SUCCESS;
  }

  m_dict->mutex_release();

  auto trx = trx_allocate_for_client(nullptr);
  auto started = trx_start(trx, ULINT_UNDEFINED);
  ut_a(started);

  trx->m_op_info = "creating foreign key sys tables";

  m_dict->lock_data_dictionary(trx);

  if (sys_foreign != nullptr) {
    log_warn("Dropping incompletely created SYS_FOREIGN table");
    if (auto err = m_dict->m_ddl.drop_table("SYS_FOREIGN", trx, true); err != DB_SUCCESS) {
      log_warn("DROP table failed with error ", err , " while dropping table SYS_FOREIGN");
    }
    auto err_commit = trx_commit(trx);
    ut_a(err_commit == DB_SUCCESS);
  }

  if (sys_foreign_cols != nullptr) {
    log_warn("Dropping incompletely created SYS_FOREIGN_COLS table");
    if (auto err = m_dict->m_ddl.drop_table("SYS_FOREIGN_COLS", trx, true); err != DB_SUCCESS) {
      log_warn("DROP table failed with error ", err , " while dropping table SYS_FOREIGN_COLS");
    }
    auto err_commit = trx_commit(trx);
    ut_a(err_commit == DB_SUCCESS);
  }

  (void) trx_start_if_not_started(trx);

  log_info("Creating foreign key constraint system tables");

  /* NOTE: in dict_load_foreigns we use the fact that there are 2 secondary indexes on SYS_FOREIGN, and they
  are defined just like below */

  auto err = que_eval_sql(
    nullptr,
    "PROCEDURE CREATE_FOREIGN_SYS_TABLES_PROC () IS\n"
    "BEGIN\n"
    "CREATE TABLE SYS_FOREIGN(ID CHAR, FOR_NAME CHAR, REF_NAME CHAR, N_COLS INT);\n"
    "CREATE UNIQUE CLUSTERED INDEX ID_IND ON SYS_FOREIGN (ID);\n"
    "CREATE INDEX FOR_IND ON SYS_FOREIGN (FOR_NAME);\n"
    "CREATE INDEX REF_IND ON SYS_FOREIGN (REF_NAME);\n"
    "CREATE TABLE SYS_FOREIGN_COLS(ID CHAR, POS INT, FOR_COL_NAME CHAR, REF_COL_NAME CHAR);\n"
    "CREATE UNIQUE CLUSTERED INDEX ID_IND ON SYS_FOREIGN_COLS (ID, POS);\n"
    "COMMIT WORK;\n"
    "END;\n",
    false,
    trx
  );

  if (err != DB_SUCCESS) {
    log_err("Error ", (int)err, " in creation");

    ut_a(err == DB_OUT_OF_FILE_SPACE || err == DB_TOO_MANY_CONCURRENT_TRXS);

    log_err("Creation failed tablespace is full dropping incompletely created SYS_FOREIGN tables");

    (void) m_dict->m_ddl.drop_table("SYS_FOREIGN", trx, true);
    (void) m_dict->m_ddl.drop_table("SYS_FOREIGN_COLS", trx, true);

    auto err_commit = trx_commit(trx);
    ut_a(err_commit == DB_SUCCESS);

    err = DB_MUST_GET_MORE_FILE_SPACE;
  }

  m_dict->unlock_data_dictionary(trx);

  trx_free_for_client(trx);

  if (err == DB_SUCCESS) {
    log_info("Foreign key constraint system tables created");
  }

  return err;
}

db_err Dict_store::foreign_eval_sql(pars_info_t *info, const char *sql, Table *table, Foreign *foreign, trx_t *trx) noexcept {
  (void) trx_start_if_not_started(trx);

  auto err = que_eval_sql(info, sql, false, trx);

  if (err == DB_DUPLICATE_KEY) {
    mutex_enter(&m_dict->m_foreign_err_mutex);

    log_err(std::format(
      "Foreign key constraint creation for table {}. A foreign key constraint of name {} already exists."
      " (Note that internally InnoDB adds 'databasename' in front of the user-defined constraint name.)"
      " Note that InnoDB's FOREIGN KEY system tables store constraint names as case-insensitive, with the"
      " standard latin1_swedish_ci collation. If you create tables or databases whose names differ only in"
      " the character case, then collisions in constraint names can occur. Workaround: name your constraints"
      " explicitly with unique names.",
      table->m_name, foreign->m_id));

    mutex_exit(&m_dict->m_foreign_err_mutex);

    return err;
  }

  if (err != DB_SUCCESS) {
    log_err("Foreign key constraint creation failed: internal error number ", (ulong)err);
    log_err("Table SYS_FOREIGN not found in internal data dictionary");

    return err;
  } else {
    return DB_SUCCESS;
  }
}

db_err Dict_store::add_foreign_field_to_dictionary(ulint field_nr, Table *table, Foreign *foreign, trx_t *trx) noexcept {
  auto info = pars_info_create();

  pars_info_add_str_literal(info, "id", foreign->m_id);

  pars_info_add_int4_literal(info, "pos", field_nr);

  pars_info_add_str_literal(info, "for_col_name", foreign->m_foreign_col_names[field_nr]);

  pars_info_add_str_literal(info, "ref_col_name", foreign->m_referenced_col_names[field_nr]);

  return foreign_eval_sql(
    info,
    "PROCEDURE P () IS\n"
    "BEGIN\n"
    " INSERT INTO SYS_FOREIGN_COLS VALUES(:id, :pos, :for_col_name, :ref_col_name);\n"
    "END;\n",
    table,
    foreign,
    trx
  );
}

db_err Dict_store::add_foreign_to_dictionary(ulint *id_nr, Table *table, Foreign *foreign, trx_t *trx) noexcept {
  auto info = pars_info_create();

  if (foreign->m_id == nullptr) {
    /* Generate a new constraint id */
    ulint namelen = strlen(table->m_name);
    char *id = reinterpret_cast<char *>(mem_heap_alloc(foreign->m_heap, namelen + 20));

    /* no overflow if number < 1e13 */
    snprintf(id, namelen + 20, "%s_ibfk_%lu", table->m_name, (ulong)(*id_nr)++);
    foreign->m_id = id;
  }

  pars_info_add_str_literal(info, "id", foreign->m_id);

  pars_info_add_str_literal(info, "for_name", table->m_name);

  pars_info_add_str_literal(info, "ref_name", foreign->m_referenced_table_name);

  pars_info_add_int4_literal(info, "n_cols", foreign->m_n_fields + (foreign->m_type << 24));

  auto err = foreign_eval_sql(
    info,
    "PROCEDURE P () IS\n"
    "BEGIN\n"
    "INSERT INTO SYS_FOREIGN VALUES"
    "(:id, :for_name, :ref_name, :n_cols);\n"
    "END;\n",
    table,
    foreign,
    trx
  );

  if (err != DB_SUCCESS) {

    return err;
  }

  for (ulint i{}; i < foreign->m_n_fields; ++i) {
    const auto err = add_foreign_field_to_dictionary(i, table, foreign, trx);

    if (err != DB_SUCCESS) {

      return err;
    }
  }

  err = foreign_eval_sql(
    nullptr,
    "PROCEDURE P () IS\n"
    "BEGIN\n"
    " COMMIT WORK;\n"
    "END;\n",
    table,
    foreign,
    trx
  );

  return err;
}

db_err Dict_store::add_foreigns_to_dictionary(ulint start_id, Table *table, trx_t *trx) noexcept {
  ut_ad(mutex_own(&m_dict->m_mutex));

  if (m_dict->table_get("SYS_FOREIGN") == nullptr) {
    log_err("table SYS_FOREIGN not found in internal data dictionary");

    return DB_ERROR;
  }

  ulint number = start_id + 1;

  for (auto foreign : table->m_foreign_list) {
    const auto err = add_foreign_to_dictionary(&number, table, foreign, trx);

    if (err != DB_SUCCESS) {

      return err;
    }
  }

  return DB_SUCCESS;
}

Table_node *Table_node::create(Dict *dict, Table *table, mem_heap_t *heap, bool commit) noexcept {
  auto ptr = mem_heap_alloc(heap, sizeof(Table_node));
  auto node = new (ptr) Table_node();

  node->m_common.type = QUE_NODE_CREATE_TABLE;

  node->m_table = table;

  node->m_state = TABLE_BUILD_TABLE_DEF;
  node->m_heap = mem_heap_create(256);

  node->m_tab_def = row_ins_node_create(INS_DIRECT, dict->m_sys_tables, heap);
  node->m_tab_def->common.parent = node;

  node->m_col_def = row_ins_node_create(INS_DIRECT, dict->m_sys_columns, heap);
  node->m_col_def->common.parent = node;

  if (commit) {
    node->m_commit_node = commit_node_create(heap);
    node->m_commit_node->common.parent = node;
  } else {
    node->m_commit_node = nullptr;
  }

  return node;
}

Index_node *Index_node::create(Dict *dict, Index *index, mem_heap_t *heap, bool commit) noexcept {
  auto ptr = mem_heap_alloc(heap, sizeof(Index_node));
  auto node = new (ptr) Index_node();

  node->m_common.type = QUE_NODE_CREATE_INDEX;

  node->m_index = index;

  node->m_state = INDEX_BUILD_INDEX_DEF;
  node->m_page_no = FIL_NULL;
  node->m_heap = mem_heap_create(256);

  node->m_ind_def = row_ins_node_create(INS_DIRECT, dict->m_sys_indexes, heap);
  node->m_ind_def->common.parent = node;

  node->m_field_def = row_ins_node_create(INS_DIRECT, dict->m_sys_fields, heap);
  node->m_field_def->common.parent = node;

  if (commit) {
    node->m_commit_node = commit_node_create(heap);
    node->m_commit_node->common.parent = node;
  } else {
    node->m_commit_node = nullptr;
  }

  return node;
}

db_err Dict_store::create_instance() noexcept {
  mtr_t mtr;

  mtr.start();

  const auto err = !bootstrap_system_tables(&mtr) ? DB_ERROR : DB_SUCCESS;

  mtr.commit();

  return err;
}
