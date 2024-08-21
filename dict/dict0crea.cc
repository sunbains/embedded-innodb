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

/** @file dict/dict0crea.c
Database object creation

Created 1/8/1996 Heikki Tuuri
*******************************************************/

#include "dict0crea.h"

#include "btr0btr.h"
#include "btr0pcur.h"
#include "ddl0ddl.h"
#include "dict0boot.h"
#include "dict0dict.h"
#include "lock0lock.h"
#include "log0log.h"
#include "mach0data.h"
#include "page0page.h"
#include "pars0pars.h"
#include "que0que.h"
#include "row0ins.h"
#include "trx0roll.h"
#include "usr0sess.h"

/**
 * @brief Based on a table object, this function builds the entry to be inserted in the SYS_TABLES system table.
 * 
 * @param table The table object.
 * @param heap The memory heap from which the memory for the built tuple is allocated.
 * @return The tuple which should be inserted.
 */
static dtuple_t *dict_create_sys_tables_tuple(const dict_table_t *table, mem_heap_t *heap) {
  byte *ptr;

  ut_ad(table);
  ut_ad(heap);

  auto sys_tables = dict_sys->sys_tables;
  auto entry = dtuple_create(heap, 8 + DATA_N_SYS_COLS);

  dict_table_copy_types(entry, sys_tables);

  /* 0: NAME -----------------------------*/
  auto dfield = dtuple_get_nth_field(entry, 0 /*NAME*/);

  dfield_set_data(dfield, table->name, strlen(table->name));
  /* 3: ID -------------------------------*/
  dfield = dtuple_get_nth_field(entry, 1 /*ID*/);

  ptr = static_cast<byte *>(mem_heap_alloc(heap, 8));
  mach_write_to_8(ptr, table->id);

  dfield_set_data(dfield, ptr, 8);
  /* 4: N_COLS ---------------------------*/
  dfield = dtuple_get_nth_field(entry, 2 /*N_COLS*/);

  ptr = static_cast<byte *>(mem_heap_alloc(heap, 4));
  mach_write_to_4(ptr, table->n_def);
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
  mach_write_to_4(ptr, table->flags >> DICT_TF2_SHIFT);

  dfield_set_data(dfield, ptr, 4);
  /* 8: CLUSTER_NAME ---------------------*/
  dfield = dtuple_get_nth_field(entry, 6 /*CLUSTER_NAME*/);
  dfield_set_null(dfield); /* not supported */

  /* 9: SPACE ----------------------------*/
  dfield = dtuple_get_nth_field(entry, 7 /*SPACE*/);

  ptr = static_cast<byte *>(mem_heap_alloc(heap, 4));
  mach_write_to_4(ptr, table->space);

  dfield_set_data(dfield, ptr, 4);
  /*----------------------------------*/

  return entry;
}

/**
 * @brief Based on a table object, this function builds the entry to be inserted in the SYS_COLUMNS system table.
 * 
 * @param table The table object.
 * @param i The column number.
 * @param heap The memory heap from which the memory for the built tuple is allocated.
 * @return The tuple which should be inserted.
 */
static dtuple_t *dict_create_sys_columns_tuple(const dict_table_t *table, ulint i, mem_heap_t *heap) {
  const char *col_name;

  ut_ad(table);
  ut_ad(heap);

  auto column = dict_table_get_nth_col(table, i);
  auto sys_columns = dict_sys->sys_columns;
  auto entry = dtuple_create(heap, 7 + DATA_N_SYS_COLS);

  dict_table_copy_types(entry, sys_columns);

  /* 0: TABLE_ID -----------------------*/
  auto dfield = dtuple_get_nth_field(entry, 0 /*TABLE_ID*/);

  auto ptr = (byte *)mem_heap_alloc(heap, 8);
  mach_write_to_8(ptr, table->id);

  dfield_set_data(dfield, ptr, 8);
  /* 1: POS ----------------------------*/
  dfield = dtuple_get_nth_field(entry, 1 /*POS*/);

  ptr = (byte *)mem_heap_alloc(heap, 4);
  mach_write_to_4(ptr, i);

  dfield_set_data(dfield, ptr, 4);
  /* 4: NAME ---------------------------*/
  dfield = dtuple_get_nth_field(entry, 2 /*NAME*/);

  col_name = dict_table_get_col_name(table, i);
  dfield_set_data(dfield, col_name, strlen(col_name));
  /* 5: MTYPE --------------------------*/
  dfield = dtuple_get_nth_field(entry, 3 /*MTYPE*/);

  ptr = (byte *)mem_heap_alloc(heap, 4);
  mach_write_to_4(ptr, column->mtype);

  dfield_set_data(dfield, ptr, 4);
  /* 6: PRTYPE -------------------------*/
  dfield = dtuple_get_nth_field(entry, 4 /*PRTYPE*/);

  ptr = (byte *)mem_heap_alloc(heap, 4);
  mach_write_to_4(ptr, column->prtype);

  dfield_set_data(dfield, ptr, 4);
  /* 7: LEN ----------------------------*/
  dfield = dtuple_get_nth_field(entry, 5 /*LEN*/);

  ptr = (byte *)mem_heap_alloc(heap, 4);
  mach_write_to_4(ptr, column->len);

  dfield_set_data(dfield, ptr, 4);
  /* 8: PREC ---------------------------*/
  dfield = dtuple_get_nth_field(entry, 6 /*PREC*/);

  ptr = (byte *)mem_heap_alloc(heap, 4);
  mach_write_to_4(ptr, 0 /* unused */);

  dfield_set_data(dfield, ptr, 4);
  /*---------------------------------*/

  return entry;
}

/**
 * @brief Builds a table definition to insert.
 * 
 * @param thr The query thread.
 * @param node The table create node.
 * @return DB_SUCCESS or error code.
 */
static db_err dict_build_table_def_step(que_thr_t *thr, tab_node_t *node) {
  dict_table_t *table;
  dtuple_t *row;
  db_err err;
  ulint flags;
  const char *path_or_name;
  bool is_path;
  mtr_t mtr;

  ut_ad(mutex_own(&(dict_sys->mutex)));

  table = node->table;

  table->id = dict_hdr_get_new_id(DICT_HDR_TABLE_ID);

  thr_get_trx(thr)->table_id = table->id;

  if (srv_file_per_table) {
    /* We create a new single-table tablespace for the table.
    We initially let it be 4 pages:
    - page 0 is the fsp header and an extent descriptor page,
    - page 1 is an ibuf bitmap page,
    - page 2 is the first inode page,
    - page 3 will contain the root of the clustered index of the table we create here. */

    space_id_t space = 0; /* reset to zero for the call below */

    if (table->dir_path_of_temp_table != nullptr) {
      /* We place tables created with CREATE TEMPORARY
      TABLE in the configured tmp dir. */

      path_or_name = table->dir_path_of_temp_table;
      is_path = true;
    } else {
      path_or_name = table->name;
      is_path = false;
    }

    ut_ad(dict_table_get_format(table) <= DICT_TF_FORMAT_MAX);

    flags = table->flags & ~(~0UL << DICT_TF_BITS);
    err = srv_fil->create_new_single_table_tablespace(&space, path_or_name, is_path, flags, FIL_IBD_FILE_INITIAL_SIZE);
    table->space = space_id_t(space);

    if (err != DB_SUCCESS) {

      return err;
    }

    mtr_start(&mtr);

    fsp_header_init(table->space, FIL_IBD_FILE_INITIAL_SIZE, &mtr);

    mtr_commit(&mtr);
  } else {
    /* Create in the system tablespace: disallow new features */
  }

  row = dict_create_sys_tables_tuple(table, node->heap);

  row_ins_node_set_new_row(node->tab_def, row);

  return DB_SUCCESS;
}

/**
 * @brief Builds a column definition to insert.
 * 
 * @param node The table create node.
 * @return DB_SUCCESS
 */
static ulint dict_build_col_def_step(tab_node_t *node) {
  dtuple_t *row;

  row = dict_create_sys_columns_tuple(node->table, node->col_no, node->heap);
  row_ins_node_set_new_row(node->col_def, row);

  return DB_SUCCESS;
}

/**
 * @brief Based on an index object, this function builds the entry to be inserted in the SYS_INDEXES system table.
 * 
 * @param index The index.
 * @param heap The memory heap from which the memory for the built tuple is allocated.
 * @return The tuple which should be inserted.
 */
static dtuple_t *dict_create_sys_indexes_tuple(const dict_index_t *index, mem_heap_t *heap) {
  dict_table_t *sys_indexes;
  dict_table_t *table;
  dtuple_t *entry;
  dfield_t *dfield;
  byte *ptr;

  ut_ad(mutex_own(&(dict_sys->mutex)));
  ut_ad(index);
  ut_ad(heap);

  sys_indexes = dict_sys->sys_indexes;

  table = dict_table_get_low(index->table_name);

  entry = dtuple_create(heap, 7 + DATA_N_SYS_COLS);

  dict_table_copy_types(entry, sys_indexes);

  /* 0: TABLE_ID -----------------------*/
  dfield = dtuple_get_nth_field(entry, 0 /*TABLE_ID*/);

  ptr = (byte *)mem_heap_alloc(heap, 8);
  mach_write_to_8(ptr, table->id);

  dfield_set_data(dfield, ptr, 8);
  /* 1: ID ----------------------------*/
  dfield = dtuple_get_nth_field(entry, 1 /*ID*/);

  ptr = (byte *)mem_heap_alloc(heap, 8);
  mach_write_to_8(ptr, index->id);

  dfield_set_data(dfield, ptr, 8);
  /* 4: NAME --------------------------*/
  dfield = dtuple_get_nth_field(entry, 2 /*NAME*/);

  dfield_set_data(dfield, index->name, strlen(index->name));
  /* 5: N_FIELDS ----------------------*/
  dfield = dtuple_get_nth_field(entry, 3 /*N_FIELDS*/);

  ptr = (byte *)mem_heap_alloc(heap, 4);
  mach_write_to_4(ptr, index->n_fields);

  dfield_set_data(dfield, ptr, 4);
  /* 6: TYPE --------------------------*/
  dfield = dtuple_get_nth_field(entry, 4 /*TYPE*/);

  ptr = (byte *)mem_heap_alloc(heap, 4);
  mach_write_to_4(ptr, index->type);

  dfield_set_data(dfield, ptr, 4);
  /* 7: SPACE --------------------------*/

  static_assert(DICT_SYS_INDEXES_SPACE_NO_FIELD == 7, "error DICT_SYS_INDEXES_SPACE_NO_FIELD != 7");

  dfield = dtuple_get_nth_field(entry, 5 /*SPACE*/);

  ptr = (byte *)mem_heap_alloc(heap, 4);
  mach_write_to_4(ptr, index->space);

  dfield_set_data(dfield, ptr, 4);
  /* 8: PAGE_NO --------------------------*/

  static_assert(DICT_SYS_INDEXES_PAGE_NO_FIELD == 8, "error DICT_SYS_INDEXES_PAGE_NO_FIELD != 8");

  dfield = dtuple_get_nth_field(entry, 6 /*PAGE_NO*/);

  ptr = (byte *)mem_heap_alloc(heap, 4);
  mach_write_to_4(ptr, FIL_NULL);

  dfield_set_data(dfield, ptr, 4);
  /*--------------------------------*/

  return entry;
}

/**
 * @brief Based on an index object, this function builds the entry to be inserted in the SYS_FIELDS system table.
 * 
 * @param index - in: index
 * @param i - in: field number
 * @param heap - in: memory heap from which the memory for the built tuple is allocated
 * @return the tuple which should be inserted
 */
static dtuple_t *dict_create_sys_fields_tuple(const dict_index_t *index, ulint i, mem_heap_t *heap) {
  dict_table_t *sys_fields;
  dtuple_t *entry;
  dict_field_t *field;
  dfield_t *dfield;
  byte *ptr;
  bool index_contains_column_prefix_field = false;
  ulint j;

  ut_ad(index);
  ut_ad(heap);

  for (j = 0; j < index->n_fields; j++) {
    if (dict_index_get_nth_field(index, j)->prefix_len > 0) {
      index_contains_column_prefix_field = true;
      break;
    }
  }

  field = dict_index_get_nth_field(index, i);

  sys_fields = dict_sys->sys_fields;

  entry = dtuple_create(heap, 3 + DATA_N_SYS_COLS);

  dict_table_copy_types(entry, sys_fields);

  /* 0: INDEX_ID -----------------------*/
  dfield = dtuple_get_nth_field(entry, 0 /*INDEX_ID*/);

  ptr = (byte *)mem_heap_alloc(heap, 8);
  mach_write_to_8(ptr, index->id);

  dfield_set_data(dfield, ptr, 8);
  /* 1: POS + PREFIX LENGTH ----------------------------*/

  dfield = dtuple_get_nth_field(entry, 1 /*POS*/);

  ptr = (byte *)mem_heap_alloc(heap, 4);

  if (index_contains_column_prefix_field) {
    /* If there are column prefix fields in the index, then
    we store the number of the field to the 2 HIGH bytes
    and the prefix length to the 2 low bytes, */

    mach_write_to_4(ptr, (i << 16) + field->prefix_len);
  } else {
    /* Else we store the number of the field to the 2 LOW bytes.
    This is to keep the storage format compatible with
    InnoDB versions < 4.0.14. */

    mach_write_to_4(ptr, i);
  }

  dfield_set_data(dfield, ptr, 4);
  /* 4: COL_NAME -------------------------*/
  dfield = dtuple_get_nth_field(entry, 2 /*COL_NAME*/);

  dfield_set_data(dfield, field->name, strlen(field->name));
  /*---------------------------------*/

  return entry;
}

/**
 * @brief Creates the tuple with which the index entry is searched for writing the index tree root page number, if such a tree is created.
 * 
 * @param tuple - in: the tuple inserted in the SYS_INDEXES table
 * @param heap - in: memory heap from which the memory for the built tuple is allocated
 * @return the tuple for search
 */
static dtuple_t *dict_create_search_tuple(const dtuple_t *tuple, mem_heap_t *heap) {
  dtuple_t *search_tuple;
  const dfield_t *field1;
  dfield_t *field2;

  ut_ad(tuple && heap);

  search_tuple = dtuple_create(heap, 2);

  field1 = dtuple_get_nth_field(tuple, 0);
  field2 = dtuple_get_nth_field(search_tuple, 0);

  dfield_copy(field2, field1);

  field1 = dtuple_get_nth_field(tuple, 1);
  field2 = dtuple_get_nth_field(search_tuple, 1);

  dfield_copy(field2, field1);

  ut_ad(dtuple_validate(search_tuple));

  return search_tuple;
}

/** Builds an index definition row to insert.
@return	DB_SUCCESS or error code */
static ulint dict_build_index_def_step(
  que_thr_t *thr, /*!< in: query thread */
  ind_node_t *node
) /*!< in: index create node */
{
  dict_table_t *table;
  dict_index_t *index;
  dtuple_t *row;
  trx_t *trx;

  ut_ad(mutex_own(&(dict_sys->mutex)));

  trx = thr_get_trx(thr);

  index = node->index;

  table = dict_table_get_low(index->table_name);

  if (table == nullptr) {
    return DB_TABLE_NOT_FOUND;
  }

  trx->table_id = table->id;

  node->table = table;

  ut_ad((UT_LIST_GET_LEN(table->indexes) > 0) || dict_index_is_clust(index));

  index->id = dict_hdr_get_new_id(DICT_HDR_INDEX_ID);

  /* Inherit the space id from the table; we store all indexes of a
  table in the same tablespace */

  index->space = table->space;
  node->page_no = FIL_NULL;
  row = dict_create_sys_indexes_tuple(index, node->heap);
  node->ind_row = row;

  row_ins_node_set_new_row(node->ind_def, row);

  /* Note that the index was created by this transaction. */
  index->trx_id = trx->m_id;

  return DB_SUCCESS;
}

/** Builds a field definition row to insert.
@return	DB_SUCCESS */
static ulint dict_build_field_def_step(ind_node_t *node) /*!< in: index create node */
{
  dict_index_t *index;
  dtuple_t *row;

  index = node->index;

  row = dict_create_sys_fields_tuple(index, node->field_no, node->heap);

  row_ins_node_set_new_row(node->field_def, row);

  return DB_SUCCESS;
}

/**
 * @brief Creates an index tree for the index if it is not a member of a cluster.
 * @param node - in: index create node
 * @return DB_SUCCESS or DB_OUT_OF_FILE_SPACE
 */
static ulint dict_create_index_tree_step(ind_node_t *node) {
  dict_index_t *index;
  dict_table_t *sys_indexes;
  dtuple_t *search_tuple;
  btr_pcur_t pcur;
  mtr_t mtr;

  ut_ad(mutex_own(&(dict_sys->mutex)));

  index = node->index;

  sys_indexes = dict_sys->sys_indexes;

  /* Run a mini-transaction in which the index tree is allocated for
  the index and its root address is written to the index entry in
  sys_indexes */

  mtr_start(&mtr);

  search_tuple = dict_create_search_tuple(node->ind_row, node->heap);

  pcur.open(UT_LIST_GET_FIRST(sys_indexes->indexes), search_tuple, PAGE_CUR_L, BTR_MODIFY_LEAF, &mtr, Source_location{});

  pcur.move_to_next_user_rec(&mtr);

  node->page_no = btr_create(index->type, index->space, index->id, index, &mtr);

  /* printf("Created a new index tree in space %lu root page %lu\n",
  index->space, index->page_no); */

  page_rec_write_index_page_no(pcur.get_rec(), DICT_SYS_INDEXES_PAGE_NO_FIELD, node->page_no, &mtr);

  pcur.close();

  mtr_commit(&mtr);

  if (node->page_no == FIL_NULL) {

    return DB_OUT_OF_FILE_SPACE;
  }

  return DB_SUCCESS;
}

void dict_drop_index_tree(rec_t *rec, mtr_t *mtr) {
  ulint len;

  ut_ad(mutex_own(&(dict_sys->mutex)));
  auto ptr = rec_get_nth_field(rec, DICT_SYS_INDEXES_PAGE_NO_FIELD, &len);

  ut_ad(len == 4);

  page_no_t root_page_no = mtr_read_ulint(ptr, MLOG_4BYTES, mtr);

  if (root_page_no == FIL_NULL) {
    /* The tree has already been freed */

    return;
  }

  ptr = rec_get_nth_field(rec, DICT_SYS_INDEXES_SPACE_NO_FIELD, &len);

  ut_ad(len == 4);

  space_id_t space = mtr_read_ulint(ptr, MLOG_4BYTES, mtr);

  if (srv_fil->space_get_flags(space) == ULINT_UNDEFINED) {
    /* It is a single table tablespace and the .ibd file is
    missing: do nothing */

    return;
  }

  /* We free all the pages but the root page first; this operation
  may span several mini-transactions */

  btr_free_but_not_root(space, root_page_no);

  /* Then we free the root page in the same mini-transaction where
  we write FIL_NULL to the appropriate field in the SYS_INDEXES
  record: this mini-transaction marks the B-tree totally freed */

  btr_free_root(space, root_page_no, mtr);

  page_rec_write_index_page_no(rec, DICT_SYS_INDEXES_PAGE_NO_FIELD, FIL_NULL, mtr);
}

ulint dict_truncate_index_tree(dict_table_t *table, ulint space, btr_pcur_t *pcur, mtr_t *mtr) {
  bool drop = !space;
  ulint type;
  uint64_t index_id;
  ulint len;
  dict_index_t *index;

  ut_ad(mutex_own(&dict_sys->mutex));
  auto rec = pcur->get_rec();
  auto ptr = rec_get_nth_field(rec, DICT_SYS_INDEXES_PAGE_NO_FIELD, &len);

  ut_ad(len == 4);

  auto root_page_no = mtr_read_ulint(ptr, MLOG_4BYTES, mtr);

  if (drop && root_page_no == FIL_NULL) {
    /* The tree has been freed. */

    ut_print_timestamp(ib_stream);
    ib_logger(
      ib_stream,
      "  Trying to TRUNCATE"
      " a missing index of table %s!\n",
      table->name
    );
    drop = false;
  }

  ptr = rec_get_nth_field(rec, DICT_SYS_INDEXES_SPACE_NO_FIELD, &len);

  ut_ad(len == 4);

  if (drop) {
    space = mtr_read_ulint(ptr, MLOG_4BYTES, mtr);
  }

  if (srv_fil->space_get_flags(space) == ULINT_UNDEFINED) {
    /* It is a single table tablespace and the .ibd file is
    missing: do nothing */

    ut_print_timestamp(ib_stream);
    ib_logger(
      ib_stream,
      "  Trying to TRUNCATE"
      " a missing .ibd file of table %s!\n",
      table->name
    );

    return FIL_NULL;
  }

  ptr = rec_get_nth_field(rec, DICT_SYS_INDEXES_TYPE_FIELD, &len);
  ut_ad(len == 4);
  type = mach_read_from_4(ptr);

  ptr = rec_get_nth_field(rec, 1, &len);
  ut_ad(len == 8);
  index_id = mach_read_from_8(ptr);

  if (drop) {
    /* We free all the pages but the root page first; this operation
    may span several mini-transactions */

    btr_free_but_not_root(space, root_page_no);

    /* Then we free the root page in the same mini-transaction where
    we create the b-tree and write its new root page number to the
    appropriate field in the SYS_INDEXES record: this mini-transaction
    marks the B-tree totally truncated */

    btr_page_get(space, root_page_no, RW_X_LATCH, mtr);

    btr_free_root(space, root_page_no, mtr);
  }

  /* We will temporarily write FIL_NULL to the PAGE_NO field
  in SYS_INDEXES, so that the database will not get into an
  inconsistent state in case it crashes between the mtr_commit()
  below and the following mtr_commit() call. */
  page_rec_write_index_page_no(rec, DICT_SYS_INDEXES_PAGE_NO_FIELD, FIL_NULL, mtr);

  /* We will need to commit the mini-transaction in order to avoid
  deadlocks in the btr_create() call, because otherwise we would
  be freeing and allocating pages in the same mini-transaction. */
  pcur->store_position(mtr);

  mtr_commit(mtr);

  mtr_start(mtr);

  pcur->restore_position(BTR_MODIFY_LEAF, mtr, Source_location{});

  /* Find the index corresponding to this SYS_INDEXES record. */
  for (index = UT_LIST_GET_FIRST(table->indexes); index; index = UT_LIST_GET_NEXT(indexes, index)) {
    if (index->id == index_id) {
      root_page_no = btr_create(type, space, index_id, index, mtr);
      index->page = (unsigned int)root_page_no;
      return root_page_no;
    }
  }

  ut_print_timestamp(ib_stream);
  ib_logger(
    ib_stream,
    "  Index %lu %lu of table %s is missing\n"
    "from the data dictionary during TRUNCATE!\n",
    index_id,
    index_id,
    table->name
  );

  return FIL_NULL;
}

tab_node_t *tab_create_graph_create(dict_table_t *table, mem_heap_t *heap, bool commit) {
  tab_node_t *node;

  node = (tab_node_t *)mem_heap_alloc(heap, sizeof(tab_node_t));

  node->common.type = QUE_NODE_CREATE_TABLE;

  node->table = table;

  node->state = TABLE_BUILD_TABLE_DEF;
  node->heap = mem_heap_create(256);

  node->tab_def = row_ins_node_create(INS_DIRECT, dict_sys->sys_tables, heap);
  node->tab_def->common.parent = node;

  node->col_def = row_ins_node_create(INS_DIRECT, dict_sys->sys_columns, heap);
  node->col_def->common.parent = node;

  if (commit) {
    node->commit_node = commit_node_create(heap);
    node->commit_node->common.parent = node;
  } else {
    node->commit_node = nullptr;
  }

  return node;
}

ind_node_t *ind_create_graph_create(dict_index_t *index, mem_heap_t *heap, bool commit) {
  ind_node_t *node;

  node = (ind_node_t *)mem_heap_alloc(heap, sizeof(ind_node_t));

  node->common.type = QUE_NODE_CREATE_INDEX;

  node->index = index;

  node->state = INDEX_BUILD_INDEX_DEF;
  node->page_no = FIL_NULL;
  node->heap = mem_heap_create(256);

  node->ind_def = row_ins_node_create(INS_DIRECT, dict_sys->sys_indexes, heap);
  node->ind_def->common.parent = node;

  node->field_def = row_ins_node_create(INS_DIRECT, dict_sys->sys_fields, heap);
  node->field_def->common.parent = node;

  if (commit) {
    node->commit_node = commit_node_create(heap);
    node->commit_node->common.parent = node;
  } else {
    node->commit_node = nullptr;
  }

  return node;
}

que_thr_t *dict_create_table_step(que_thr_t *thr) {
  tab_node_t *node;
  auto err = DB_ERROR;
  trx_t *trx;

  ut_ad(thr);
  ut_ad(mutex_own(&(dict_sys->mutex)));

  trx = thr_get_trx(thr);

  node = (tab_node_t *)thr->run_node;

  ut_ad(que_node_get_type(node) == QUE_NODE_CREATE_TABLE);

  if (thr->prev_node == que_node_get_parent(node)) {
    node->state = TABLE_BUILD_TABLE_DEF;
  }

  if (node->state == TABLE_BUILD_TABLE_DEF) {

    /* DO THE CHECKS OF THE CONSISTENCY CONSTRAINTS HERE */

    err = (db_err)dict_build_table_def_step(thr, node);

    if (err != DB_SUCCESS) {

      goto function_exit;
    }

    node->state = TABLE_BUILD_COL_DEF;
    node->col_no = 0;

    thr->run_node = node->tab_def;

    return thr;
  }

  if (node->state == TABLE_BUILD_COL_DEF) {

    if (node->col_no < (node->table)->n_def) {

      err = (db_err)dict_build_col_def_step(node);

      if (err != DB_SUCCESS) {

        goto function_exit;
      }

      node->col_no++;

      thr->run_node = node->col_def;

      return thr;
    } else {
      node->state = TABLE_COMMIT_WORK;
    }
  }

  if (node->state == TABLE_COMMIT_WORK) {

    /* Table was correctly defined: do NOT commit the transaction
    (CREATE TABLE does NOT do an implicit commit of the current
    transaction) */

    node->state = TABLE_ADD_TO_CACHE;

    /* thr->run_node = node->commit_node;

    return(thr); */
  }

  if (node->state == TABLE_ADD_TO_CACHE) {

    dict_table_add_to_cache(node->table, node->heap);

    err = DB_SUCCESS;
  }

function_exit:
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
}

que_thr_t *dict_create_index_step(que_thr_t *thr) {
  ind_node_t *node;
  auto err = DB_ERROR;
  trx_t *trx;

  ut_ad(thr);
  ut_ad(mutex_own(&(dict_sys->mutex)));

  trx = thr_get_trx(thr);

  node = (ind_node_t *)thr->run_node;

  ut_ad(que_node_get_type(node) == QUE_NODE_CREATE_INDEX);

  if (thr->prev_node == que_node_get_parent(node)) {
    node->state = INDEX_BUILD_INDEX_DEF;
  }

  if (node->state == INDEX_BUILD_INDEX_DEF) {
    /* DO THE CHECKS OF THE CONSISTENCY CONSTRAINTS HERE */
    err = (db_err)dict_build_index_def_step(thr, node);

    if (err != DB_SUCCESS) {

      goto function_exit;
    }

    node->state = INDEX_BUILD_FIELD_DEF;
    node->field_no = 0;

    thr->run_node = node->ind_def;

    return thr;
  }

  if (node->state == INDEX_BUILD_FIELD_DEF) {

    if (node->field_no < (node->index)->n_fields) {

      err = (db_err)dict_build_field_def_step(node);

      if (err != DB_SUCCESS) {

        goto function_exit;
      }

      node->field_no++;

      thr->run_node = node->field_def;

      return thr;
    } else {
      node->state = INDEX_ADD_TO_CACHE;
    }
  }

  if (node->state == INDEX_ADD_TO_CACHE) {

    uint64_t index_id = node->index->id;

    err = (db_err)dict_index_add_to_cache(node->table, node->index, FIL_NULL, true);

    node->index = dict_index_get_if_in_cache_low(index_id);
    ut_a(!node->index == (err != DB_SUCCESS));

    if (err != DB_SUCCESS) {

      goto function_exit;
    }

    node->state = INDEX_CREATE_INDEX_TREE;
  }

  if (node->state == INDEX_CREATE_INDEX_TREE) {

    err = (db_err)dict_create_index_tree_step(node);

    if (err != DB_SUCCESS) {
      dict_index_remove_from_cache(node->table, node->index);
      node->index = nullptr;

      goto function_exit;
    }

    node->index->page = node->page_no;
    node->state = INDEX_COMMIT_WORK;
  }

  if (node->state == INDEX_COMMIT_WORK) {

    /* Index was correctly defined: do NOT commit the transaction
    (CREATE INDEX does NOT currently do an implicit commit of
    the current transaction) */

    node->state = INDEX_CREATE_INDEX_TREE;

    /* thr->run_node = node->commit_node;

    return(thr); */
  }

function_exit:
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
}

db_err dict_create_or_check_foreign_constraint_tables() {
  dict_table_t *table1;
  dict_table_t *table2;
  db_err err;
  trx_t *trx;
  int started;

  mutex_enter(&(dict_sys->mutex));

  table1 = dict_table_get_low("SYS_FOREIGN");
  table2 = dict_table_get_low("SYS_FOREIGN_COLS");

  if (table1 && table2 && UT_LIST_GET_LEN(table1->indexes) == 3 && UT_LIST_GET_LEN(table2->indexes) == 1) {

    /* Foreign constraint system tables have already been
    created, and they are ok */

    if (!table1->cached) {
      dict_mem_table_free(table1);
    }

    if (!table2->cached) {
      dict_mem_table_free(table2);
    }

    mutex_exit(&(dict_sys->mutex));

    return DB_SUCCESS;
  }

  mutex_exit(&(dict_sys->mutex));

  trx = trx_allocate_for_client(nullptr);
  started = trx_start(trx, ULINT_UNDEFINED);
  ut_a(started);

  trx->m_op_info = "creating foreign key sys tables";

  dict_lock_data_dictionary(trx);

  if (table1) {
    ib_logger(
      ib_stream,
      "dropping incompletely created"
      " SYS_FOREIGN table\n"
    );
    ddl_drop_table("SYS_FOREIGN", trx, true);
    auto err_commit = trx_commit(trx);
    ut_a(err_commit == DB_SUCCESS);
  }

  if (table2) {
    ib_logger(
      ib_stream,
      "dropping incompletely created"
      " SYS_FOREIGN_COLS table\n"
    );
    ddl_drop_table("SYS_FOREIGN_COLS", trx, true);
    auto err_commit = trx_commit(trx);
    ut_a(err_commit == DB_SUCCESS);
  }

  trx_start_if_not_started(trx);

  log_info("Creating foreign key constraint system tables");

  /* NOTE: in dict_load_foreigns we use the fact that
  there are 2 secondary indexes on SYS_FOREIGN, and they
  are defined just like below */

  /* NOTE: when designing InnoDB's foreign key support in 2001, we made
  an error and made the table names and the foreign key id of type
  'CHAR' (internally, really a VARCHAR). We should have made the type
  VARBINARY, like in other InnoDB system tables, to get a clean
  design. */

  err = que_eval_sql(
    nullptr,
    "PROCEDURE CREATE_FOREIGN_SYS_TABLES_PROC () IS\n"
    "BEGIN\n"
    "CREATE TABLE\n"
    "SYS_FOREIGN(ID CHAR, FOR_NAME CHAR,"
    " REF_NAME CHAR, N_COLS INT);\n"
    "CREATE UNIQUE CLUSTERED INDEX ID_IND"
    " ON SYS_FOREIGN (ID);\n"
    "CREATE INDEX FOR_IND"
    " ON SYS_FOREIGN (FOR_NAME);\n"
    "CREATE INDEX REF_IND"
    " ON SYS_FOREIGN (REF_NAME);\n"
    "CREATE TABLE\n"
    "SYS_FOREIGN_COLS(ID CHAR, POS INT,"
    " FOR_COL_NAME CHAR, REF_COL_NAME CHAR);\n"
    "CREATE UNIQUE CLUSTERED INDEX ID_IND"
    " ON SYS_FOREIGN_COLS (ID, POS);\n"
    "END;\n",
    false,
    trx
  );

  if (err != DB_SUCCESS) {
    ib_logger(ib_stream, "error %lu in creation\n", (ulong)err);

    ut_a(err == DB_OUT_OF_FILE_SPACE || err == DB_TOO_MANY_CONCURRENT_TRXS);

    ib_logger(
      ib_stream,
      "creation failed\n"
      "tablespace is full\n"
      "dropping incompletely created"
      " SYS_FOREIGN tables\n"
    );

    ddl_drop_table("SYS_FOREIGN", trx, true);
    ddl_drop_table("SYS_FOREIGN_COLS", trx, true);

    auto err_commit = trx_commit(trx);
    ut_a(err_commit == DB_SUCCESS);

    err = DB_MUST_GET_MORE_FILE_SPACE;
  }

  auto err_commit = trx_commit(trx);
  ut_a(err_commit == DB_SUCCESS);

  dict_unlock_data_dictionary(trx);

  trx_free_for_client(trx);

  if (err == DB_SUCCESS) {
    log_info("Foreign key constraint system tables created");
  }

  return err;
}

/**
 * @brief Evaluate the given foreign key SQL statement.
 * 
 * @param info info struct, or nullptr
 * @param sql SQL string to evaluate
 * @param table table
 * @param foreign foreign
 * @param trx transaction
 * @return error code or DB_SUCCESS
 */
static db_err dict_foreign_eval_sql(
  pars_info_t *info,
  const char *sql,
  dict_table_t *table,
  dict_foreign_t *foreign,
  trx_t *trx
) {
  trx_start_if_not_started(trx);

  auto err = que_eval_sql(info, sql, false, trx);

  if (err == DB_DUPLICATE_KEY) {
    mutex_enter(&dict_foreign_err_mutex);
    ut_print_timestamp(ib_stream);
    ib_logger(ib_stream, " Error in foreign key constraint creation for table ");
    ut_print_name(table->name);
    ib_logger(ib_stream, ".\nA foreign key constraint of name ");
    ut_print_name(foreign->id);
    ib_logger(
      ib_stream,
      "\nalready exists."
      " (Note that internally InnoDB adds 'databasename'\n"
      "in front of the user-defined constraint name.)\n"
      "Note that InnoDB's FOREIGN KEY system tables store\n"
      "constraint names as case-insensitive, with the\n"
      "standard latin1_swedish_ci collation. If you\n"
      "create tables or databases whose names differ only in\n"
      "the character case, then collisions in constraint\n"
      "names can occur. Workaround: name your constraints\n"
      "explicitly with unique names.\n"
    );

    mutex_exit(&dict_foreign_err_mutex);

    return err;
  }

  if (err != DB_SUCCESS) {
    ib_logger(
      ib_stream,
      "Foreign key constraint creation failed:\n"
      "internal error number %lu\n",
      (ulong)err
    );

    mutex_enter(&dict_foreign_err_mutex);
    ut_print_timestamp(ib_stream);
    ib_logger(
      ib_stream,
      " Internal error in foreign key constraint creation"
      " for table "
    );
    ut_print_name(table->name);
    ib_logger(
      ib_stream,
      ".\n"
      "See the .err log in the datadir for more "
      "information.\n"
    );
    mutex_exit(&dict_foreign_err_mutex);

    return err;
  }

  return DB_SUCCESS;
}

/**
 * @brief Add a single foreign key field definition to the data dictionary tables in the database.
 * 
 * @param field_nr foreign field number
 * @param table table
 * @param foreign foreign
 * @param trx transaction
 * @return error code or DB_SUCCESS
 */
static db_err dict_create_add_foreign_field_to_dictionary(
  ulint field_nr,
  dict_table_t *table,
  dict_foreign_t *foreign,
  trx_t *trx
) {
  pars_info_t *info = pars_info_create();

  pars_info_add_str_literal(info, "id", foreign->id);

  pars_info_add_int4_literal(info, "pos", field_nr);

  pars_info_add_str_literal(info, "for_col_name", foreign->foreign_col_names[field_nr]);

  pars_info_add_str_literal(info, "ref_col_name", foreign->referenced_col_names[field_nr]);

  return dict_foreign_eval_sql(
    info,
    "PROCEDURE P () IS\n"
    "BEGIN\n"
    "INSERT INTO SYS_FOREIGN_COLS VALUES"
    "(:id, :pos, :for_col_name, :ref_col_name);\n"
    "END;\n",
    table,
    foreign,
    trx
  );
}

/**
 * @brief Add a single foreign key definition to the data dictionary tables in the database.
 * We also generate names to constraints that were not named by the user.
 * A generated constraint has a name of the format databasename/tablename_ibfk_NUMBER,
 * where the numbers start from 1, and are given locally for this table, that is,
 * the number is not global, as in the old format constraints < 4.0.18 it used to be.
 * 
 * @param id_nr in/out: number to use in id generation; incremented if used
 * @param table in: table
 * @param foreign in: foreign
 * @param trx in: transaction
 * @return error code or DB_SUCCESS
 */
static db_err dict_create_add_foreign_to_dictionary(
  ulint *id_nr,
  dict_table_t *table,
  dict_foreign_t *foreign,
  trx_t *trx
) {
  ulint i;

  pars_info_t *info = pars_info_create();

  if (foreign->id == nullptr) {
    /* Generate a new constraint id */
    ulint namelen = strlen(table->name);
    auto id = (char *)mem_heap_alloc(foreign->heap, namelen + 20);
    /* no overflow if number < 1e13 */
    snprintf(id, namelen + 20, "%s_ibfk_%lu", table->name, (ulong)(*id_nr)++);
    foreign->id = id;
  }

  pars_info_add_str_literal(info, "id", foreign->id);

  pars_info_add_str_literal(info, "for_name", table->name);

  pars_info_add_str_literal(info, "ref_name", foreign->referenced_table_name);

  pars_info_add_int4_literal(info, "n_cols", foreign->n_fields + (foreign->type << 24));

  auto err = dict_foreign_eval_sql(
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

  for (i = 0; i < foreign->n_fields; i++) {
    err = dict_create_add_foreign_field_to_dictionary(i, table, foreign, trx);

    if (err != DB_SUCCESS) {

      return err;
    }
  }

  err = dict_foreign_eval_sql(
    nullptr,
    "PROCEDURE P () IS\n"
    "BEGIN\n"
    "COMMIT WORK;\n"
    "END;\n",
    table,
    foreign,
    trx
  );

  return err;
}

db_err dict_create_add_foreigns_to_dictionary(ulint start_id, dict_table_t *table, trx_t *trx) {
  dict_foreign_t *foreign;
  ulint number = start_id + 1;

  ut_ad(mutex_own(&(dict_sys->mutex)));

  if (nullptr == dict_table_get_low("SYS_FOREIGN")) {
    ib_logger(ib_stream, "table SYS_FOREIGN not found in internal data dictionary\n");

    return DB_ERROR;
  }

  for (foreign = UT_LIST_GET_FIRST(table->foreign_list); foreign; foreign = UT_LIST_GET_NEXT(foreign_list, foreign)) {

    auto err = dict_create_add_foreign_to_dictionary(&number, table, foreign, trx);

    if (err != DB_SUCCESS) {

      return err;
    }
  }

  return DB_SUCCESS;
}
