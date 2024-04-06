/****************************************************************************
Copyright (c) 1996, 2010, Innobase Oy. All Rights Reserved.

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

/** @file dict/dict0dict.c
Data dictionary system

Created 1/8/1996 Heikki Tuuri
***********************************************************************/

#include "dict0dict.h"

/** dummy index for ROW_FORMAT=REDUNDANT supremum and infimum records */
dict_index_t *dict_ind_redundant;

/** dummy index for ROW_FORMAT=COMPACT supremum and infimum records */
dict_index_t *dict_ind_compact;

#include "api0ucode.h"
#include "btr0btr.h"
#include "btr0cur.h"

#include "buf0buf.h"
#include "data0type.h"
#include "dict0boot.h"
#include "dict0crea.h"
#include "dict0mem.h"
#include "mach0data.h"
#include "page0page.h"
#include "pars0pars.h"
#include "pars0sym.h"
#include "que0que.h"
#include "rem0cmp.h"
#include "row0merge.h"
#include "srv0start.h"
#include "trx0undo.h"

#include <ctype.h>

/** the dictionary system */
dict_sys_t *dict_sys = nullptr;

/** @brief the data dictionary rw-latch protecting dict_sys

table create, drop, etc. reserve this in X-mode; implicit or
backround operations purge, rollback, foreign key checks reserve this
in S-mode; we cannot trust that the client protects implicit or background
operations a table drop since the client does not know about them; therefore
we need this; NOTE: a transaction which reserves this must keep book
on the mode in trx_t::m_dict_operation_lock_mode */
rw_lock_t dict_operation_lock{};

/** initial memory heap size when creating a table or index object */
constexpr ulint DICT_HEAP_SIZE = 100;

/** Buffer pool max size per table hash table fixed size in bytes */
constexpr ulint DICT_POOL_PER_TABLE_HASH = 512;

/** Buffer pool max size per data dictionary varying size in bytes */
constexpr ulint DICT_POOL_PER_VARYING = 4;

/** Identifies generated InnoDB foreign key names */
static char dict_ibfk[] = "_ibfk_";

/** Array of mutexes protecting dict_index_t::stat_n_diff_key_vals[] */
constexpr ulint DICT_INDEX_STAT_MUTEX_SIZE = 32;

mutex_t dict_index_stat_mutex[DICT_INDEX_STAT_MUTEX_SIZE];

/**
 * Tries to find column names for the index and sets the col field of the index.
 *
 * @param table - in: table
 * @param index - in: index
 *
 * @return true if the column names were found
 */
static bool dict_index_find_cols(dict_table_t *table, dict_index_t *index);

/**
 * @brief Builds the internal dictionary cache representation for a clustered index.
 *
 * @param table - in: table
 * @param index - in: user representation of a clustered index
 * @return own: the internal representation of the clustered index
 */
static dict_index_t *dict_index_build_internal_clust(
  const dict_table_t *table,
  dict_index_t *index
); 

/**
 * @brief Builds the internal dictionary cache representation for a non-clustered index, containing also system fields not defined by the user.
 *
 * @param table - in: table
 * @param index - in: user representation of a non-clustered index
 * @return own: the internal representation of the non-clustered index
 */
static dict_index_t *dict_index_build_internal_non_clust(
  const dict_table_t *table,
  dict_index_t *index
);

/** Removes a foreign constraint struct from the dictionary cache. */
static void dict_foreign_remove_from_cache(dict_foreign_t *foreign); /*!< in, own: foreign constraint */

/**
 * @brief Prints a column data.
 *
 * @param table - in: table
 * @param col - in: column
 */
static void dict_col_print_low(const dict_table_t *table, const dict_col_t *col);

/**
 * @brief Prints an index data.
 *
 * @param index - in: index
 */
static void dict_index_print_low(dict_index_t *index);

/**
 * @brief Prints a field data.
 *
 * @param field - in: field
 */
static void dict_field_print_low(const dict_field_t *field);

/**
 * @brief Frees a foreign key struct.
 *
 * @param foreign - in, own: foreign key struct
 */
static void dict_foreign_free(dict_foreign_t *foreign);

/* mutex protecting the foreign and unique error buffers */
mutex_t dict_foreign_err_mutex{};

void dict_var_init() {
  dict_sys = nullptr;
}

void dict_casedn_str(char *a) {
  ib_utf8_casedown(a);
}

bool dict_tables_have_same_db(const char *name1, const char *name2) {
  for (; *name1 == *name2; name1++, name2++) {
    if (*name1 == '/') {
      return true;
    }
    ut_a(*name1); /* the names must contain '/' */
  }
  return false;
}

const char *dict_remove_db_name(const char *name) {
  const char *s = strchr(name, '/');
  ut_a(s);

  return s + 1;
}

ulint dict_get_db_name_len(const char *name) {
  auto s = strchr(name, '/');
  ut_a(s);
  return s - name;
}

void dict_mutex_enter(void) {
  mutex_enter(&(dict_sys->mutex));
}

/** Get the mutex that protects index->stat_n_diff_key_vals[] */
#define GET_INDEX_STAT_MUTEX(index) (&dict_index_stat_mutex[ut_uint64_fold(index->id) % DICT_INDEX_STAT_MUTEX_SIZE])

void dict_index_stat_mutex_enter(const dict_index_t *index) {
  ut_ad(index != nullptr);
  ut_ad(index->magic_n == DICT_INDEX_MAGIC_N);
  ut_ad(index->cached);
  ut_ad(!index->to_be_dropped);

  mutex_enter(GET_INDEX_STAT_MUTEX(index));
}

void dict_index_stat_mutex_exit(const dict_index_t *index) {
  ut_ad(index != nullptr);
  ut_ad(index->magic_n == DICT_INDEX_MAGIC_N);
  ut_ad(index->cached);
  ut_ad(!index->to_be_dropped);

  mutex_exit(GET_INDEX_STAT_MUTEX(index));
}

void dict_mutex_exit(void) {
  mutex_exit(&(dict_sys->mutex));
}

void dict_table_decrement_handle_count(dict_table_t *table, bool dict_locked) {
  if (!dict_locked) {
    mutex_enter(&dict_sys->mutex);
  }

  ut_ad(mutex_own(&dict_sys->mutex));
  ut_a(table->n_handles_opened > 0);

  table->n_handles_opened--;

  if (!dict_locked) {
    mutex_exit(&dict_sys->mutex);
  }
}

void dict_table_increment_handle_count(dict_table_t *table, bool dict_locked) {
  if (!dict_locked) {
    mutex_enter(&dict_sys->mutex);
  }

  ut_ad(mutex_own(&dict_sys->mutex));

  table->n_handles_opened++;

  if (!dict_locked) {
    mutex_exit(&dict_sys->mutex);
  }
}

const char *dict_table_get_col_name(const dict_table_t *table, ulint col_nr) {
  ulint i;
  const char *s;

  ut_ad(table);
  ut_ad(col_nr < table->n_def);
  ut_ad(table->magic_n == DICT_TABLE_MAGIC_N);

  s = table->col_names;
  if (s) {
    for (i = 0; i < col_nr; i++) {
      s += strlen(s) + 1;
    }
  }

  return s;
}

int dict_table_get_col_no(const dict_table_t *table, const char *name) {
  ulint i;
  const char *s;

  ut_ad(table->magic_n == DICT_TABLE_MAGIC_N);

  s = table->col_names;

  if (s) {
    for (i = 0; i < table->n_def; i++, s += strlen(s) + 1) {
      if (strcmp(s, name) == 0) {
        return i;
      }
    }
  }

  return -1;
}

dict_index_t *dict_index_get_on_id_low(dict_table_t *table, uint64_t id) {
  dict_index_t *dict_index;

  dict_index = dict_table_get_first_index(table);

  while (dict_index) {
    if (id == dict_index->id) {
      return dict_index;
    }

    dict_index = dict_table_get_next_index(dict_index);
  }

  return nullptr;
}

ulint dict_index_get_nth_col_pos(const dict_index_t *dict_index, ulint n) {
  const dict_field_t *field;
  const dict_col_t *col;
  ulint pos;
  ulint n_fields;

  ut_ad(dict_index);
  ut_ad(dict_index->magic_n == DICT_INDEX_MAGIC_N);

  col = dict_table_get_nth_col(dict_index->table, n);

  if (dict_index_is_clust(dict_index)) {

    return dict_col_get_clust_pos(col, dict_index);
  }

  n_fields = dict_index_get_n_fields(dict_index);

  for (pos = 0; pos < n_fields; pos++) {
    field = dict_index_get_nth_field(dict_index, pos);

    if (col == field->col && field->prefix_len == 0) {

      return pos;
    }
  }

  return ULINT_UNDEFINED;
}

bool dict_index_contains_col_or_prefix(const dict_index_t *dict_index, ulint n) {
  const dict_field_t *field;
  const dict_col_t *col;
  ulint pos;
  ulint n_fields;

  ut_ad(dict_index);
  ut_ad(dict_index->magic_n == DICT_INDEX_MAGIC_N);

  if (dict_index_is_clust(dict_index)) {

    return true;
  }

  col = dict_table_get_nth_col(dict_index->table, n);

  n_fields = dict_index_get_n_fields(dict_index);

  for (pos = 0; pos < n_fields; pos++) {
    field = dict_index_get_nth_field(dict_index, pos);

    if (col == field->col) {

      return true;
    }
  }

  return false;
}

ulint dict_index_get_nth_field_pos(const dict_index_t *dict_index, const dict_index_t *index2, ulint n) {
  const dict_field_t *field;
  const dict_field_t *field2;
  ulint n_fields;
  ulint pos;

  ut_ad(dict_index);
  ut_ad(dict_index->magic_n == DICT_INDEX_MAGIC_N);

  field2 = dict_index_get_nth_field(index2, n);

  n_fields = dict_index_get_n_fields(dict_index);

  for (pos = 0; pos < n_fields; pos++) {
    field = dict_index_get_nth_field(dict_index, pos);

    if (field->col == field2->col && (field->prefix_len == 0 || (field->prefix_len >= field2->prefix_len && field2->prefix_len != 0))) {

      return pos;
    }
  }

  return ULINT_UNDEFINED;
}

dict_table_t *dict_table_get_on_id(ib_recovery_t recovery, uint64_t table_id, trx_t *trx) {
  dict_table_t *table;

  if (table_id <= DICT_FIELDS_ID || trx->m_dict_operation_lock_mode == RW_X_LATCH) {
    /* It is a system table which will always exist in the table
    cache: we avoid acquiring the dictionary mutex, because
    if we are doing a rollback to handle an error in TABLE
    CREATE, for example, we already have the mutex! */

    ut_ad(mutex_own(&(dict_sys->mutex)) || trx->m_dict_operation_lock_mode == RW_X_LATCH);

    return dict_table_get_on_id_low(recovery, table_id);
  }

  mutex_enter(&(dict_sys->mutex));

  table = dict_table_get_on_id_low(recovery, table_id);

  mutex_exit(&(dict_sys->mutex));

  return table;
}

ulint dict_table_get_nth_col_pos(const dict_table_t *table, ulint n) {
  return dict_index_get_nth_col_pos(dict_table_get_first_index(table), n);
}

bool dict_table_col_in_clustered_key(const dict_table_t *table, ulint n) {
  const dict_index_t *dict_index;
  const dict_field_t *field;
  const dict_col_t *col;
  ulint pos;
  ulint n_fields;

  ut_ad(table);

  col = dict_table_get_nth_col(table, n);

  dict_index = dict_table_get_first_index(table);

  n_fields = dict_index_get_n_unique(dict_index);

  for (pos = 0; pos < n_fields; pos++) {
    field = dict_index_get_nth_field(dict_index, pos);

    if (col == field->col) {

      return true;
    }
  }

  return false;
}

void dict_init() {
  dict_sys = (dict_sys_t *)mem_alloc(sizeof(dict_sys_t));

  mutex_create(&dict_sys->mutex, IF_DEBUG("dict_mutex",) IF_SYNC_DEBUG(SYNC_DICT,) Source_location{});

  dict_sys->table_hash = hash_create(buf_pool->get_curr_size() / (DICT_POOL_PER_TABLE_HASH * UNIV_WORD_SIZE));
  dict_sys->table_id_hash = hash_create(buf_pool->get_curr_size() / (DICT_POOL_PER_TABLE_HASH * UNIV_WORD_SIZE));
  dict_sys->size = 0;

  UT_LIST_INIT(dict_sys->table_LRU);

  rw_lock_create(&dict_operation_lock, SYNC_DICT_OPERATION);

  mutex_create(&dict_foreign_err_mutex, IF_DEBUG("dict_foreign_mutex",) IF_SYNC_DEBUG(SYNC_ANY_LATCH,) Source_location{});

  for (ulint i = 0; i < DICT_INDEX_STAT_MUTEX_SIZE; i++) {
    mutex_create(&dict_index_stat_mutex[i], IF_DEBUG("dict_index_stat_mutex",) IF_SYNC_DEBUG(SYNC_INDEX_TREE,) Source_location{});
  }
}

dict_table_t *dict_table_get(const char *table_name, bool ref_count) {
  dict_table_t *table;

  mutex_enter(&dict_sys->mutex);

  table = dict_table_get_low(table_name);

  if (ref_count && table != nullptr) {
    dict_table_increment_handle_count(table, true);
  }

  mutex_exit(&dict_sys->mutex);

  if (table != nullptr && !table->stat_initialized) {
    /* If table->ibd_file_missing == true, this will
    print an error message and return without doing
    anything. */
    dict_update_statistics(table);
  }

  return table;
}

dict_table_t *dict_table_get_using_id(ib_recovery_t recovery, uint64_t table_id, bool ref_count) {
  dict_table_t *table;

  ut_ad(mutex_own(&dict_sys->mutex));

  table = dict_table_get_on_id_low(recovery, table_id);

  if (ref_count && table != nullptr) {
    dict_table_increment_handle_count(table, true);
  }

  return table;
}

void dict_table_add_system_columns(dict_table_t *table, mem_heap_t *heap) {
  ut_ad(table);
  ut_ad(table->n_def == table->n_cols - DATA_N_SYS_COLS);
  ut_ad(table->magic_n == DICT_TABLE_MAGIC_N);
  ut_ad(!table->cached);

  /* NOTE: the system columns MUST be added in the following order
  (so that they can be indexed by the numerical value of DATA_ROW_ID,
  etc.) and as the last columns of the table memory object.
  The clustered index will not always physically contain all
  system columns. */

  dict_mem_table_add_col(table, heap, "DB_ROW_ID", DATA_SYS, DATA_ROW_ID | DATA_NOT_NULL, DATA_ROW_ID_LEN);

  /* This check reminds that if a new system column is added to
  the program, it should be dealt with here */
  static_assert(DATA_ROW_ID == 0, "error DATA_ROW_ID != 0");
  static_assert(DATA_TRX_ID == 1, "error DATA_TRX_ID != 1");
  static_assert(DATA_ROLL_PTR == 2, "error DATA_ROLL_PTR != 2");
  static_assert(DATA_N_SYS_COLS == 3, "error DATA_N_SYS_COLS != 3");

  dict_mem_table_add_col(table, heap, "DB_TRX_ID", DATA_SYS, DATA_TRX_ID | DATA_NOT_NULL, DATA_TRX_ID_LEN);

  dict_mem_table_add_col(table, heap, "DB_ROLL_PTR", DATA_SYS, DATA_ROLL_PTR | DATA_NOT_NULL, DATA_ROLL_PTR_LEN);
}

void dict_table_add_to_cache(dict_table_t *table, mem_heap_t *heap) {
  ulint fold;
  ulint id_fold;
  ulint i;
  ulint row_len;

  /* The lower limit for what we consider a "big" row */
  constexpr ulint BIG_ROW_SIZE = 1024;

  ut_ad(mutex_own(&(dict_sys->mutex)));

  dict_table_add_system_columns(table, heap);

  table->cached = true;

  fold = ut_fold_string(table->name);
  id_fold = ut_uint64_fold(table->id);

  row_len = 0;
  for (i = 0; i < table->n_def; i++) {
    ulint col_len = dict_col_get_max_size(dict_table_get_nth_col(table, i));

    row_len += col_len;

    /* If we have a single unbounded field, or several gigantic
    fields, mark the maximum row size as BIG_ROW_SIZE. */
    if (row_len >= BIG_ROW_SIZE || col_len >= BIG_ROW_SIZE) {
      row_len = BIG_ROW_SIZE;

      break;
    }
  }

  table->big_rows = row_len >= BIG_ROW_SIZE;

  /* Look for a table with the same name: error if such exists */
  {
    dict_table_t *table2;
    HASH_SEARCH(
      name_hash, dict_sys->table_hash, fold, dict_table_t *, table2, ut_ad(table2->cached), strcmp(table2->name, table->name) == 0
    );
    ut_a(table2 == nullptr);

#ifdef UNIV_DEBUG
    /* Look for the same table pointer with a different name */
    HASH_SEARCH_ALL(name_hash, dict_sys->table_hash, dict_table_t *, table2, ut_ad(table2->cached), table2 == table);
    ut_ad(table2 == nullptr);
#endif /* UNIV_DEBUG */
  }

  /* Look for a table with the same id: error if such exists */
  {
    dict_table_t *table2;
    HASH_SEARCH(id_hash, dict_sys->table_id_hash, id_fold, dict_table_t *, table2, ut_ad(table2->cached), table2->id == table->id);
    ut_a(table2 == nullptr);

#ifdef UNIV_DEBUG
    /* Look for the same table pointer with a different id */
    HASH_SEARCH_ALL(id_hash, dict_sys->table_id_hash, dict_table_t *, table2, ut_ad(table2->cached), table2 == table);
    ut_ad(table2 == nullptr);
#endif /* UNIV_DEBUG */
  }

  /* Add table to hash table of tables */
  HASH_INSERT(dict_table_t, name_hash, dict_sys->table_hash, fold, table);

  /* Add table to hash table of tables based on table id */
  HASH_INSERT(dict_table_t, id_hash, dict_sys->table_id_hash, id_fold, table);
  /* Add table to LRU list of tables */
  UT_LIST_ADD_FIRST(dict_sys->table_LRU, table);

  dict_sys->size += mem_heap_get_size(table->heap);
}

dict_index_t *dict_index_find_on_id_low(uint64_t id) {
  auto table = UT_LIST_GET_FIRST(dict_sys->table_LRU);

  while (table != nullptr) {
    auto dict_index = dict_table_get_first_index(table);

    while (dict_index != nullptr) {
      if (id == dict_index->id) {

        return dict_index;
      }

      dict_index = dict_table_get_next_index(dict_index);
    }

    table = UT_LIST_GET_NEXT(table_LRU, table);
  }

  return nullptr;
}

bool dict_table_rename_in_cache(dict_table_t *table, const char *new_name, bool rename_also_foreigns) {
  dict_foreign_t *foreign;
  dict_index_t *index;
  ulint fold;
  ulint old_size;
  const char *old_name;

  ut_ad(table);
  ut_ad(mutex_own(&(dict_sys->mutex)));

  old_size = mem_heap_get_size(table->heap);
  old_name = table->name;

  fold = ut_fold_string(new_name);

  /* Look for a table with the same name: error if such exists */
  {
    dict_table_t *table2;
    HASH_SEARCH(
      name_hash, dict_sys->table_hash, fold, dict_table_t *, table2, ut_ad(table2->cached), (strcmp(table2->name, new_name) == 0)
    );
    if (likely_null(table2)) {
      ut_print_timestamp(ib_stream);
      ib_logger(
        ib_stream,
        "  Error: dictionary cache"
        " already contains a table "
      );
      ut_print_name(ib_stream, nullptr, true, new_name);
      ib_logger(
        ib_stream,
        "\n"
        "cannot rename table "
      );
      ut_print_name(ib_stream, nullptr, true, old_name);
      ib_logger(ib_stream, "\n");
      return false;
    }
  }

  /* If the table is stored in a single-table tablespace, rename the
  .ibd file */

  if (table->space != 0) {
    if (table->dir_path_of_temp_table != nullptr) {
      ut_print_timestamp(ib_stream);
      ib_logger(
        ib_stream,
        "  Error: trying to rename a"
        " TEMPORARY TABLE "
      );
      ut_print_name(ib_stream, nullptr, true, old_name);
      ib_logger(ib_stream, " (");
      ut_print_filename(ib_stream, table->dir_path_of_temp_table);
      ib_logger(ib_stream, " )\n");
      return false;
    } else if (!fil_rename_tablespace(old_name, table->space, new_name)) {
      return false;
    }
  }

  /* Remove table from the hash tables of tables */
  HASH_DELETE(dict_table_t, name_hash, dict_sys->table_hash, ut_fold_string(old_name), table);
  table->name = mem_heap_strdup(table->heap, new_name);

  /* Add table to hash table of tables */
  HASH_INSERT(dict_table_t, name_hash, dict_sys->table_hash, fold, table);
  dict_sys->size += (mem_heap_get_size(table->heap) - old_size);

  /* Update the table_name field in indexes */
  index = dict_table_get_first_index(table);

  while (index != nullptr) {
    index->table_name = table->name;

    index = dict_table_get_next_index(index);
  }

  if (!rename_also_foreigns) {
    /* In ALTER TABLE we think of the rename table operation
    in the direction table -> temporary table (#sql...)
    as dropping the table with the old name and creating
    a new with the new name. Thus we kind of drop the
    constraints from the dictionary cache here. The foreign key
    constraints will be inherited to the new table from the
    system tables through a call of dict_load_foreigns. */

    /* Remove the foreign constraints from the cache */
    foreign = UT_LIST_GET_LAST(table->foreign_list);

    while (foreign != nullptr) {
      dict_foreign_remove_from_cache(foreign);
      foreign = UT_LIST_GET_LAST(table->foreign_list);
    }

    /* Reset table field in referencing constraints */

    foreign = UT_LIST_GET_FIRST(table->referenced_list);

    while (foreign != nullptr) {
      foreign->referenced_table = nullptr;
      foreign->referenced_index = nullptr;

      foreign = UT_LIST_GET_NEXT(referenced_list, foreign);
    }

    /* Make the list of referencing constraints empty */

    UT_LIST_INIT(table->referenced_list);

    return true;
  }

  /* Update the table name fields in foreign constraints, and update also
  the constraint id of new format >= 4.0.18 constraints. Note that at
  this point we have already changed table->name to the new name. */

  foreign = UT_LIST_GET_FIRST(table->foreign_list);

  while (foreign != nullptr) {
    if (strlen(foreign->foreign_table_name) < strlen(table->name)) {
      /* Allocate a longer name buffer;
      TODO: store buf len to save memory */

      foreign->foreign_table_name = (char *)mem_heap_alloc(foreign->heap, strlen(table->name) + 1);
    }

    strcpy(foreign->foreign_table_name, table->name);

    if (strchr(foreign->id, '/')) {
      ulint db_len;
      char *old_id;

      /* This is a >= 4.0.18 format id */

      old_id = mem_strdup(foreign->id);

      if (strlen(foreign->id) > strlen(old_name) + ((sizeof dict_ibfk) - 1) && !memcmp(foreign->id, old_name, strlen(old_name)) &&
          !memcmp(foreign->id + strlen(old_name), dict_ibfk, (sizeof dict_ibfk) - 1)) {

        /* This is a generated >= 4.0.18 format id */

        if (strlen(table->name) > strlen(old_name)) {
          foreign->id = (char *)mem_heap_alloc(foreign->heap, strlen(table->name) + strlen(old_id) + 1);
        }

        /* Replace the prefix 'databasename/tablename'
        with the new names */
        strcpy(foreign->id, table->name);
        strcat(foreign->id, old_id + strlen(old_name));
      } else {
        /* This is a >= 4.0.18 format id where the user
        gave the id name */
        db_len = dict_get_db_name_len(table->name) + 1;

        if (dict_get_db_name_len(table->name) > dict_get_db_name_len(foreign->id)) {

          foreign->id = (char *)mem_heap_alloc(foreign->heap, db_len + strlen(old_id) + 1);
        }

        /* Replace the database prefix in id with the
        one from table->name */

        memcpy(foreign->id, table->name, db_len);

        strcpy(foreign->id + db_len, dict_remove_db_name(old_id));
      }

      mem_free(old_id);
    }

    foreign = UT_LIST_GET_NEXT(foreign_list, foreign);
  }

  foreign = UT_LIST_GET_FIRST(table->referenced_list);

  while (foreign != nullptr) {
    if (strlen(foreign->referenced_table_name) < strlen(table->name)) {
      /* Allocate a longer name buffer;
      TODO: store buf len to save memory */

      foreign->referenced_table_name = (char *)mem_heap_alloc(foreign->heap, strlen(table->name) + 1);
    }

    strcpy(foreign->referenced_table_name, table->name);

    foreign = UT_LIST_GET_NEXT(referenced_list, foreign);
  }

  return true;
}

/** Change the id of a table object in the dictionary cache. This is used in
DISCARD TABLESPACE. */

void dict_table_change_id_in_cache(
  dict_table_t *table, /*!< in/out: table object already in cache */
  uint64_t new_id
) /*!< in: new id to set */
{
  ut_ad(table);
  ut_ad(mutex_own(&(dict_sys->mutex)));
  ut_ad(table->magic_n == DICT_TABLE_MAGIC_N);

  /* Remove the table from the hash table of id's */

  HASH_DELETE(dict_table_t, id_hash, dict_sys->table_id_hash, ut_uint64_fold(table->id), table);
  table->id = new_id;

  /* Add the table back to the hash table */
  HASH_INSERT(dict_table_t, id_hash, dict_sys->table_id_hash, ut_uint64_fold(table->id), table);
}

void dict_table_remove_from_cache(dict_table_t *table) {
  dict_foreign_t *foreign;
  dict_index_t *index;
  ulint size;

  ut_ad(table);
  ut_ad(mutex_own(&(dict_sys->mutex)));
  ut_ad(table->magic_n == DICT_TABLE_MAGIC_N);

  /* Remove the foreign constraints from the cache */
  foreign = UT_LIST_GET_LAST(table->foreign_list);

  while (foreign != nullptr) {
    dict_foreign_remove_from_cache(foreign);
    foreign = UT_LIST_GET_LAST(table->foreign_list);
  }

  /* Reset table field in referencing constraints */

  foreign = UT_LIST_GET_FIRST(table->referenced_list);

  while (foreign != nullptr) {
    foreign->referenced_table = nullptr;
    foreign->referenced_index = nullptr;

    foreign = UT_LIST_GET_NEXT(referenced_list, foreign);
  }

  /* Remove the indexes from the cache */
  index = UT_LIST_GET_LAST(table->indexes);

  while (index != nullptr) {
    dict_index_remove_from_cache(table, index);
    index = UT_LIST_GET_LAST(table->indexes);
  }

  /* Remove table from the hash tables of tables */
  HASH_DELETE(dict_table_t, name_hash, dict_sys->table_hash, ut_fold_string(table->name), table);
  HASH_DELETE(dict_table_t, id_hash, dict_sys->table_id_hash, ut_uint64_fold(table->id), table);

  /* Remove table from LRU list of tables */
  UT_LIST_REMOVE(dict_sys->table_LRU, table);

  size = mem_heap_get_size(table->heap);

  ut_ad(dict_sys->size >= size);

  dict_sys->size -= size;

  dict_mem_table_free(table);
}

bool dict_col_name_is_reserved(const char *name) {
  /* This check reminds that if a new system column is added to
  the program, it should be dealt with here. */
  static_assert(DATA_N_SYS_COLS == 3, "error DATA_N_SYS_COLS != 3");

  static const char *reserved_names[] = {"DB_ROW_ID", "DB_TRX_ID", "DB_ROLL_PTR"};

  for (ulint i = 0; i < std::size(reserved_names); i++) {
    if (strcasecmp(name, reserved_names[i]) == 0) {

      return true;
    }
  }

  return false;
}

/**
 * If an undo log record for this table might not fit on a single page,
 * return true.
 *
 * @param table The table.
 * @param new_index The index.
 * @return True if the undo log record could become too big.
 */
static bool dict_index_too_big_for_undo(
  const dict_table_t *table,
  const dict_index_t *new_index
) {
  /* Make sure that all column prefixes will fit in the undo log record
  in trx_undo_page_report_modify() right after trx_undo_page_init(). */

  ulint i;
  const dict_index_t *clust_index = dict_table_get_first_index(table);
  ulint undo_page_len = TRX_UNDO_PAGE_HDR - TRX_UNDO_PAGE_HDR_SIZE + 2 /* next record pointer */
                        + 1                                            /* type_cmpl */
                        + 11 /* trx->undo_no */ + 11                   /* table->id */
                        + 1                                            /* rec_get_info_bits() */
                        + 11                                           /* DB_TRX_ID */
                        + 11                                           /* DB_ROLL_PTR */
                        + 10 + FIL_PAGE_DATA_END                       /* trx_undo_left() */
                        + 2 /* pointer to previous undo log record */;

  if (unlikely(!clust_index)) {
    ut_a(dict_index_is_clust(new_index));
    clust_index = new_index;
  }

  /* Add the size of the ordering columns in the
  clustered index. */
  for (i = 0; i < clust_index->n_uniq; i++) {
    const dict_col_t *col = dict_index_get_nth_col(clust_index, i);

    /* Use the maximum output size of
    mach_write_compressed(), although the encoded
    length should always fit in 2 bytes. */
    undo_page_len += 5 + dict_col_get_max_size(col);
  }

  /* Add the old values of the columns to be updated.
  First, the amount and the numbers of the columns.
  These are written by mach_write_compressed() whose
  maximum output length is 5 bytes.  However, given that
  the quantities are below REC_MAX_N_FIELDS (10 bits),
  the maximum length is 2 bytes per item. */
  undo_page_len += 2 * (dict_table_get_n_cols(table) + 1);

  for (i = 0; i < clust_index->n_def; i++) {
    const dict_col_t *col = dict_index_get_nth_col(clust_index, i);
    ulint max_size = dict_col_get_max_size(col);
    ulint fixed_size = dict_col_get_fixed_size(col, dict_table_is_comp(table));

    if (fixed_size) {
      /* Fixed-size columns are stored locally. */
      max_size = fixed_size;
    } else if (max_size <= BTR_EXTERN_FIELD_REF_SIZE * 2) {
      /* Short columns are stored locally. */
    } else if (!col->ord_part) {
      /* See if col->ord_part would be set
      because of new_index. */
      ulint j;

      for (j = 0; j < new_index->n_uniq; j++) {
        if (dict_index_get_nth_col(new_index, j) == col) {

          goto is_ord_part;
        }
      }

      /* This is not an ordering column in any index.
      Thus, it can be stored completely externally. */
      max_size = BTR_EXTERN_FIELD_REF_SIZE;
    } else {
    is_ord_part:
      /* This is an ordering column in some index.
      A long enough prefix must be written to the
      undo log.  See trx_undo_page_fetch_ext(). */

      if (max_size > REC_MAX_INDEX_COL_LEN) {
        max_size = REC_MAX_INDEX_COL_LEN;
      }

      max_size += BTR_EXTERN_FIELD_REF_SIZE;
    }

    undo_page_len += 5 + max_size;
  }

  return undo_page_len >= UNIV_PAGE_SIZE;
}

/**
 * @brief If a record of this index might not fit on a single B-tree page, return true.
 * @param table - in: table
 * @param new_index - in: index
 * @return true if the index record could become too big
 */
static bool dict_index_too_big_for_tree(
  const dict_table_t *table,
  const dict_index_t *new_index
)
{
  ulint comp;
  ulint i;
  /* maximum possible storage size of a record */
  ulint rec_max_size;
  /* maximum allowed size of a record on a leaf page */
  ulint page_rec_max;
  /* maximum allowed size of a node pointer record */
  ulint page_ptr_max;

  comp = dict_table_is_comp(table);

  /* The maximum allowed record size is half a B-tree
  page.  No additional sparse page directory entry will
  be generated for the first few user records. */
  page_rec_max = page_get_free_space_of_empty(comp) / 2;
  page_ptr_max = page_rec_max;
  /* Each record has a header. */
  rec_max_size = comp ? REC_N_NEW_EXTRA_BYTES : REC_N_OLD_EXTRA_BYTES;

  if (comp) {
    /* Include the "null" flags in the
    maximum possible record size. */
    rec_max_size += UT_BITS_IN_BYTES(new_index->n_nullable);
  } else {
    /* For each column, include a 2-byte offset and a
    "null" flag.  The 1-byte format is only used in short
    records that do not contain externally stored columns.
    Such records could never exceed the page limit, even
    when using the 2-byte format. */
    rec_max_size += 2 * new_index->n_fields;
  }

  /* Compute the maximum possible record size. */
  for (i = 0; i < new_index->n_fields; i++) {
    const dict_field_t *field = dict_index_get_nth_field(new_index, i);
    const dict_col_t *col = dict_field_get_col(field);
    ulint field_max_size;
    ulint field_ext_max_size;

    /* In dtuple_convert_big_rec(), variable-length columns
    that are longer than BTR_EXTERN_FIELD_REF_SIZE * 2
    may be chosen for external storage.

    Fixed-length columns, and all columns of secondary
    index records are always stored inline. */

    /* Determine the maximum length of the index field.
    The field_ext_max_size should be computed as the worst
    case in rec_get_converted_size_comp() for
    REC_STATUS_ORDINARY records. */

    field_max_size = dict_col_get_fixed_size(col, comp);
    if (field_max_size) {
      /* dict_index_add_col() should guarantee this */
      ut_ad(!field->prefix_len || field->fixed_len == field->prefix_len);
      /* Fixed lengths are not encoded
      in ROW_FORMAT=COMPACT. */
      field_ext_max_size = 0;
      goto add_field_size;
    }

    field_max_size = dict_col_get_max_size(col);
    field_ext_max_size = field_max_size < 256 ? 1 : 2;

    if (field->prefix_len) {
      if (field->prefix_len < field_max_size) {
        field_max_size = field->prefix_len;
      }
    } else if (field_max_size > BTR_EXTERN_FIELD_REF_SIZE * 2 && dict_index_is_clust(new_index)) {

      /* In the worst case, we have a locally stored
      column of BTR_EXTERN_FIELD_REF_SIZE * 2 bytes.
      The length can be stored in one byte.  If the
      column were stored externally, the lengths in
      the clustered index page would be
      BTR_EXTERN_FIELD_REF_SIZE and 2. */
      field_max_size = BTR_EXTERN_FIELD_REF_SIZE * 2;
      field_ext_max_size = 1;
    }

    if (comp) {
      /* Add the extra size for ROW_FORMAT=COMPACT.
      For ROW_FORMAT=REDUNDANT, these bytes were
      added to rec_max_size before this loop. */
      rec_max_size += field_ext_max_size;
    }
  add_field_size:
    rec_max_size += field_max_size;

    /* Check the size limit on leaf pages. */
    if (unlikely(rec_max_size >= page_rec_max)) {

      return true;
    }

    /* Check the size limit on non-leaf pages.  Records
    stored in non-leaf B-tree pages consist of the unique
    columns of the record (the key columns of the B-tree)
    and a node pointer field.  When we have processed the
    unique columns, rec_max_size equals the size of the
    node pointer record minus the node pointer column. */
    if (i + 1 == dict_index_get_n_unique_in_tree(new_index) && rec_max_size + REC_NODE_PTR_SIZE >= page_ptr_max) {

      return true;
    }
  }

  return false;
}

db_err dict_index_add_to_cache(dict_table_t *table, dict_index_t *index, ulint page_no, bool strict) {
  dict_index_t *new_index;
  ulint n_ord;
  ulint i;

  ut_ad(index);
  ut_ad(mutex_own(&(dict_sys->mutex)));
  ut_ad(index->n_def == index->n_fields);
  ut_ad(index->magic_n == DICT_INDEX_MAGIC_N);

  ut_ad(mem_heap_validate(index->heap));
  ut_a(!dict_index_is_clust(index) || UT_LIST_GET_LEN(table->indexes) == 0);

  if (!dict_index_find_cols(table, index)) {

    dict_mem_index_free(index);
    return DB_CORRUPTION;
  }

  /* Build the cache internal representation of the index,
  containing also the added system fields */

  if (dict_index_is_clust(index)) {
    new_index = dict_index_build_internal_clust(table, index);
  } else {
    new_index = dict_index_build_internal_non_clust(table, index);
  }

  /* Set the n_fields value in new_index to the actual defined
  number of fields in the cache internal representation */

  new_index->n_fields = new_index->n_def;

  if (strict && dict_index_too_big_for_tree(table, new_index)) {
  too_big:
    dict_mem_index_free(new_index);
    dict_mem_index_free(index);
    return DB_TOO_BIG_RECORD;
  }

  n_ord = new_index->n_uniq;

  switch (dict_table_get_format(table)) {
    case DICT_TF_FORMAT_51:
      /* ROW_FORMAT=REDUNDANT and ROW_FORMAT=COMPACT store
    prefixes of externally stored columns locally within
    the record.  There are no special considerations for
    the undo log record size. */
      goto undo_size_ok;

    case DICT_TF_FORMAT_V1:
      /* In ROW_FORMAT=DYNAMIC and ROW_FORMAT=COMPRESSED,
    column prefix indexes require that prefixes of
    externally stored columns are written to the undo log.
    This may make the undo log record bigger than the
    record on the B-tree page.  The maximum size of an
    undo log record is the page size.  That must be
    checked for below. */
      break;

      static_assert(DICT_TF_FORMAT_V1 == DICT_TF_FORMAT_MAX, "error DICT_TF_FORMAT_V1 != DICT_TF_FORMAT_MAX");
  }

  for (i = 0; i < n_ord; i++) {
    const dict_field_t *field = dict_index_get_nth_field(new_index, i);
    const dict_col_t *col = dict_field_get_col(field);

    /* In dtuple_convert_big_rec(), variable-length columns
    that are longer than BTR_EXTERN_FIELD_REF_SIZE * 2
    may be chosen for external storage.  If the column appears
    in an ordering column of an index, a longer prefix of
    REC_MAX_INDEX_COL_LEN will be copied to the undo log
    by trx_undo_page_report_modify() and
    trx_undo_page_fetch_ext().  It suffices to check the
    capacity of the undo log whenever new_index includes
    a column prefix on a column that may be stored externally. */

    if (field->prefix_len                      /* prefix index */
        && !col->ord_part                      /* not yet ordering column */
        && !dict_col_get_fixed_size(col, true) /* variable-length */
        && dict_col_get_max_size(col) > BTR_EXTERN_FIELD_REF_SIZE * 2 /* long enough */) {

      if (dict_index_too_big_for_undo(table, new_index)) {
        /* An undo log record might not fit in
        a single page.  Refuse to create this index. */

        goto too_big;
      }

      break;
    }
  }

undo_size_ok:
  /* Flag the ordering columns */

  for (i = 0; i < n_ord; i++) {

    dict_index_get_nth_field(new_index, i)->col->ord_part = 1;
  }

  /* Add the new index as the last index for the table */

  UT_LIST_ADD_LAST(table->indexes, new_index);
  new_index->table = table;
  new_index->table_name = table->name;

  new_index->stat_index_size = 1;
  new_index->stat_n_leaf_pages = 1;

  new_index->page = page_no;
  rw_lock_create(&new_index->lock, SYNC_INDEX_TREE);

  new_index->stat_n_diff_key_vals =
    (int64_t *)mem_heap_alloc(new_index->heap, (1 + dict_index_get_n_unique(new_index)) * sizeof(int64_t));

  /* Give some sensible values to stat_n_... in case we do
  not calculate statistics quickly enough */

  for (i = 0; i <= dict_index_get_n_unique(new_index); i++) {

    new_index->stat_n_diff_key_vals[i] = 100;
  }

  dict_sys->size += mem_heap_get_size(new_index->heap);

  dict_mem_index_free(index);

  return DB_SUCCESS;
}

void dict_index_remove_from_cache(dict_table_t *table, dict_index_t *index) {
  ut_ad(table != nullptr);
  ut_ad(index != nullptr);
  ut_ad(table->magic_n == DICT_TABLE_MAGIC_N);
  ut_ad(index->magic_n == DICT_INDEX_MAGIC_N);
  ut_ad(mutex_own(&(dict_sys->mutex)));

  rw_lock_free(&index->lock);

  /* Remove the index from the list of indexes of the table */
  UT_LIST_REMOVE(table->indexes, index);

  auto size = mem_heap_get_size(index->heap);

  ut_ad(dict_sys->size >= size);

  dict_sys->size -= size;

  dict_mem_index_free(index);
}

/**
 * Tries to find column names for the index and sets the col field of the index.
 *
 * @param table - in: table
 * @param index - in: index
 *
 * @return true if the column names were found
 */
static bool dict_index_find_cols(dict_table_t *table, dict_index_t *index) {
  ut_ad(table && index);
  ut_ad(table->magic_n == DICT_TABLE_MAGIC_N);
  ut_ad(mutex_own(&(dict_sys->mutex)));

  for (ulint i = 0; i < index->n_fields; i++) {
    auto field = dict_index_get_nth_field(index, i);

    for (ulint j = 0; j < table->n_cols; j++) {
      if (!strcmp(dict_table_get_col_name(table, j), field->name)) {
        field->col = dict_table_get_nth_col(table, j);

        goto found;
      }
    }

#ifdef UNIV_DEBUG
    /* It is an error not to find a matching column. */
    ib_logger(ib_stream, "Error: no matching column for ");
    ut_print_name(ib_stream, nullptr, false, field->name);
    ib_logger(ib_stream, " in ");
    dict_index_name_print(ib_stream, nullptr, index);
    ib_logger(ib_stream, "!\n");
#endif /* UNIV_DEBUG */
    return false;

  found:;
  }

  return true;
}

void dict_index_add_col(dict_index_t *index, const dict_table_t *table, dict_col_t *col, ulint prefix_len) {
  dict_field_t *field;
  const char *col_name;

  col_name = dict_table_get_col_name(table, dict_col_get_no(col));

  dict_mem_index_add_field(index, col_name, prefix_len);

  field = dict_index_get_nth_field(index, index->n_def - 1);

  field->col = col;
  field->fixed_len = (unsigned int)dict_col_get_fixed_size(col, dict_table_is_comp(table));

  if (prefix_len && field->fixed_len > prefix_len) {
    field->fixed_len = (unsigned int)prefix_len;
  }

  /* Long fixed-length fields that need external storage are treated as
  variable-length fields, so that the extern flag can be embedded in
  the length word. */

  if (field->fixed_len > DICT_MAX_INDEX_COL_LEN) {
    field->fixed_len = 0;
  }

  /* The comparison limit above must be constant.  If it were
  changed, the disk format of some fixed-length columns would
  change, which would be a disaster. */
  static_assert(DICT_MAX_INDEX_COL_LEN == 768, "error DICT_MAX_INDEX_COL_LEN != 768");

  if (!(col->prtype & DATA_NOT_NULL)) {
    index->n_nullable++;
  }
}

/**
 * @brief Copies fields contained in index2 to index1.
 *
 * @param index1 - in: index to copy to
 * @param index2 - in: index to copy from
 * @param table - in: table
 * @param start - in: first position to copy
 * @param end - in: last position to copy
 */
static void dict_index_copy(
  dict_index_t *index1,
  dict_index_t *index2,
  const dict_table_t *table,
  ulint start,
  ulint end
) {
  dict_field_t *field;

  /* Copy fields contained in index2 */

  for (ulint i = start; i < end; i++) {

    field = dict_index_get_nth_field(index2, i);
    dict_index_add_col(index1, table, field->col, field->prefix_len);
  }
}

void dict_index_copy_types(dtuple_t *tuple, const dict_index_t *index, ulint n_fields) {
  for (ulint i = 0; i < n_fields; i++) {
    const dict_field_t *ifield;
    dtype_t *dfield_type;

    ifield = dict_index_get_nth_field(index, i);
    dfield_type = dfield_get_type(dtuple_get_nth_field(tuple, i));
    dict_col_copy_type(dict_field_get_col(ifield), dfield_type);
  }
}

void dict_table_copy_types(dtuple_t *tuple, const dict_table_t *table) {
  for (ulint i = 0; i < dtuple_get_n_fields(tuple); i++) {

    dfield_t *dfield = dtuple_get_nth_field(tuple, i);
    dtype_t *dtype = dfield_get_type(dfield);

    dfield_set_null(dfield);
    dict_col_copy_type(dict_table_get_nth_col(table, i), dtype);
  }
}

/**
 * @brief Builds the internal dictionary cache representation for a clustered index, containing also system fields not defined by the user.
 *
 * @param table - in: table
 * @param index - in: user representation of a clustered index
 *
 * @return own: the internal representation of the clustered index
 */
static dict_index_t *dict_index_build_internal_clust(const dict_table_t *table, dict_index_t *index) {
  dict_index_t *new_index;
  dict_field_t *field;
  ulint fixed_size;
  ulint trx_id_pos;
  ulint i;
  bool *indexed;

  ut_ad(table && index);
  ut_ad(dict_index_is_clust(index));
  ut_ad(mutex_own(&(dict_sys->mutex)));
  ut_ad(table->magic_n == DICT_TABLE_MAGIC_N);

  /* Create a new index object with certainly enough fields */
  new_index = dict_mem_index_create(table->name, index->name, table->space, index->type, index->n_fields + table->n_cols);

  /* Copy other relevant data from the old index struct to the new
  struct: it inherits the values */

  new_index->n_user_defined_cols = index->n_fields;

  new_index->id = index->id;

  /* Copy the fields of index */
  dict_index_copy(new_index, index, table, 0, index->n_fields);

  if (dict_index_is_unique(index)) {
    /* Only the fields defined so far are needed to identify
    the index entry uniquely */

    new_index->n_uniq = new_index->n_def;
  } else {
    /* Also the row id is needed to identify the entry */
    new_index->n_uniq = 1 + new_index->n_def;
  }

  new_index->trx_id_offset = 0;

  /* Add system columns, trx id first */

  trx_id_pos = new_index->n_def;

  static_assert(DATA_ROW_ID == 0, "error DATA_ROW_ID != 0");
  static_assert(DATA_TRX_ID == 1, "error DATA_TRX_ID != 1");
  static_assert(DATA_ROLL_PTR == 2, "error DATA_ROLL_PTR != 2");

  if (!dict_index_is_unique(index)) {

    dict_index_add_col(new_index, table, dict_table_get_sys_col(table, DATA_ROW_ID), 0);
    ++trx_id_pos;
  }

  dict_index_add_col(new_index, table, dict_table_get_sys_col(table, DATA_TRX_ID), 0);

  dict_index_add_col(new_index, table, dict_table_get_sys_col(table, DATA_ROLL_PTR), 0);

  for (i = 0; i < trx_id_pos; i++) {

    fixed_size = dict_col_get_fixed_size(dict_index_get_nth_col(new_index, i), dict_table_is_comp(table));

    if (fixed_size == 0) {
      new_index->trx_id_offset = 0;

      break;
    }

    if (dict_index_get_nth_field(new_index, i)->prefix_len > 0) {
      new_index->trx_id_offset = 0;

      break;
    }

    new_index->trx_id_offset += (unsigned int)fixed_size;
  }

  /* Remember the table columns already contained in new_index */
  indexed = (bool *)mem_zalloc(table->n_cols * sizeof *indexed);

  /* Mark the table columns already contained in new_index */
  for (i = 0; i < new_index->n_def; i++) {

    field = dict_index_get_nth_field(new_index, i);

    /* If there is only a prefix of the column in the index
    field, do not mark the column as contained in the index */

    if (field->prefix_len == 0) {

      indexed[field->col->ind] = true;
    }
  }

  /* Add to new_index non-system columns of table not yet included there */
  for (i = 0; i + DATA_N_SYS_COLS < (ulint)table->n_cols; i++) {

    dict_col_t *col = dict_table_get_nth_col(table, i);
    ut_ad(col->mtype != DATA_SYS);

    if (!indexed[col->ind]) {
      dict_index_add_col(new_index, table, col, 0);
    }
  }

  mem_free(indexed);

  ut_ad(UT_LIST_GET_LEN(table->indexes) == 0);

  new_index->cached = true;

  return new_index;
}

/**
 * Builds the internal dictionary cache representation for a non-clustered index, containing also system fields not defined by the user.
 * @param table in: table
 * @param index in: user representation of a non-clustered index
 * @return own: the internal representation of the non-clustered index
 */
static dict_index_t *dict_index_build_internal_non_clust(const dict_table_t *table, dict_index_t *index) {
  dict_field_t *field;
  dict_index_t *new_index;
  dict_index_t *clust_index;
  ulint i;
  bool *indexed;

  ut_ad(table && index);
  ut_ad(!dict_index_is_clust(index));
  ut_ad(mutex_own(&(dict_sys->mutex)));
  ut_ad(table->magic_n == DICT_TABLE_MAGIC_N);

  /* The clustered index should be the first in the list of indexes */
  clust_index = UT_LIST_GET_FIRST(table->indexes);

  ut_ad(clust_index);
  ut_ad(dict_index_is_clust(clust_index));

  /* Create a new index */
  new_index = dict_mem_index_create(table->name, index->name, index->space, index->type, index->n_fields + 1 + clust_index->n_uniq);

  /* Copy other relevant data from the old index
  struct to the new struct: it inherits the values */

  new_index->n_user_defined_cols = index->n_fields;

  new_index->id = index->id;

  /* Copy fields from index to new_index */
  dict_index_copy(new_index, index, table, 0, index->n_fields);

  /* Remember the table columns already contained in new_index */
  indexed = (bool *)mem_zalloc(table->n_cols * sizeof *indexed);

  /* Mark the table columns already contained in new_index */
  for (i = 0; i < new_index->n_def; i++) {

    field = dict_index_get_nth_field(new_index, i);

    /* If there is only a prefix of the column in the index
    field, do not mark the column as contained in the index */

    if (field->prefix_len == 0) {

      indexed[field->col->ind] = true;
    }
  }

  /* Add to new_index the columns necessary to determine the clustered
  index entry uniquely */

  for (i = 0; i < clust_index->n_uniq; i++) {

    field = dict_index_get_nth_field(clust_index, i);

    if (!indexed[field->col->ind]) {
      dict_index_add_col(new_index, table, field->col, field->prefix_len);
    }
  }

  mem_free(indexed);

  if (dict_index_is_unique(index)) {
    new_index->n_uniq = index->n_fields;
  } else {
    new_index->n_uniq = new_index->n_def;
  }

  /* Set the n_fields value in new_index to the actual defined
  number of fields */

  new_index->n_fields = new_index->n_def;

  new_index->cached = true;

  return new_index;
}

bool dict_table_is_referenced_by_foreign_key(const dict_table_t *table) {
  return UT_LIST_GET_LEN(table->referenced_list) > 0;
}

dict_foreign_t *dict_table_get_referenced_constraint(dict_table_t *table, dict_index_t *index) {
  dict_foreign_t *foreign;

  ut_ad(index != nullptr);
  ut_ad(table != nullptr);

  for (foreign = UT_LIST_GET_FIRST(table->referenced_list); foreign; foreign = UT_LIST_GET_NEXT(referenced_list, foreign)) {

    if (foreign->referenced_index == index) {

      return foreign;
    }
  }

  return nullptr;
}

dict_foreign_t *dict_table_get_foreign_constraint(dict_table_t *table, dict_index_t *index) {
  ut_ad(index != nullptr);
  ut_ad(table != nullptr);

  for (auto foreign = UT_LIST_GET_FIRST(table->foreign_list); foreign; foreign = UT_LIST_GET_NEXT(foreign_list, foreign)) {

    if (foreign->foreign_index == index || foreign->referenced_index == index) {

      return foreign;
    }
  }

  return nullptr;
}

/**
 * @brief Frees a foreign key struct.
 *
 * @param foreign in, own: foreign key struct
 */
static void dict_foreign_free(dict_foreign_t *foreign) {
  mem_heap_free(foreign->heap);
}

/**
 * @brief Removes a foreign constraint struct from the dictionary cache.
 * @param foreign in, own: foreign constraint
 */
static void dict_foreign_remove_from_cache(dict_foreign_t *foreign) {
  ut_ad(mutex_own(&(dict_sys->mutex)));
  ut_a(foreign);

  if (foreign->referenced_table) {
    UT_LIST_REMOVE(foreign->referenced_table->referenced_list, foreign);
  }

  if (foreign->foreign_table) {
    UT_LIST_REMOVE(foreign->foreign_table->foreign_list, foreign);
  }

  dict_foreign_free(foreign);
}

/**
 * @brief Looks for the foreign constraint from the foreign and referenced lists of a table.
 * @param table in: table object
 * @param id in: foreign constraint id
 * @return foreign constraint
 */
static dict_foreign_t *dict_foreign_find(dict_table_t *table, const char *id) {
  ut_ad(mutex_own(&(dict_sys->mutex)));

  auto foreign = UT_LIST_GET_FIRST(table->foreign_list);

  while (foreign != nullptr) {
    if (strcmp(id, foreign->id) == 0) {

      return foreign;
    }

    foreign = UT_LIST_GET_NEXT(foreign_list, foreign);
  }

  foreign = UT_LIST_GET_FIRST(table->referenced_list);

  while (foreign != nullptr) {
    if (strcmp(id, foreign->id) == 0) {

      return foreign;
    }

    foreign = UT_LIST_GET_NEXT(referenced_list, foreign);
  }

  return nullptr;
}

/**
 * Tries to find an index whose first fields are the columns in the array,
 * in the same order and is not marked for deletion and is not the same
 * as types_idx.
 *
 * @param table in: table
 * @param columns in: array of column names
 * @param n_cols in: number of columns
 * @param types_idx in: nullptr or an index to whose types the column types must match
 * @param check_charsets in: whether to check charsets. Only has an effect if types_idx != nullptr
 * @param check_null in: nonzero if none of the columns must be declared NOT nullptr
 * @return matching index, nullptr if not found
 */
static dict_index_t *dict_foreign_find_index(
  dict_table_t *table,
  const char **columns,
  ulint n_cols,
  dict_index_t *types_idx,
  bool check_charsets,
  ulint check_null
) {
  auto index = dict_table_get_first_index(table);

  while (index != nullptr) {
    /* Ignore matches that refer to the same instance
    or the index is to be dropped */
    if (index->to_be_dropped || types_idx == index) {

      goto next_rec;

    } else if (dict_index_get_n_fields(index) >= n_cols) {
      ulint i;

      for (i = 0; i < n_cols; i++) {
        auto field = dict_index_get_nth_field(index, i);
        auto col_name = dict_table_get_col_name(table, dict_col_get_no(field->col));

        if (field->prefix_len != 0) {
          /* We do not accept column prefix
          indexes here */

          break;
        }

        if (ib_utf8_strcasecmp(columns[i], col_name) != 0) {
          break;
        }

        if (check_null && (field->col->prtype & DATA_NOT_NULL)) {

          return nullptr;
        }

        if (types_idx &&
            !cmp_cols_are_equal(dict_index_get_nth_col(index, i), dict_index_get_nth_col(types_idx, i), check_charsets)) {

          break;
        }
      }

      if (i == n_cols) {
        /* We found a matching index */

        return index;
      }
    }

  next_rec:
    index = dict_table_get_next_index(index);
  }

  return nullptr;
}

dict_index_t *dict_foreign_find_equiv_index(dict_foreign_t *foreign) {
  ut_a(foreign != nullptr);

  /* Try to find an index which contains the columns as the
  first fields and in the right order, and the types are the
  same as in foreign->foreign_index */

  return dict_foreign_find_index(
    foreign->foreign_table,
    foreign->foreign_col_names,
    foreign->n_fields,
    foreign->foreign_index,
    true, /* check types */
    false /* allow columns to be nullptr */
  );
}

dict_index_t *dict_table_get_index_by_max_id(dict_table_t *table, const char *name, const char **columns, ulint n_cols) {
  dict_index_t *found{};
  auto index = dict_table_get_first_index(table);

  while (index != nullptr) {
    if (strcmp(index->name, name) == 0 && dict_index_get_n_ordering_defined_by_user(index) == n_cols) {

      ulint i;

      for (i = 0; i < n_cols; i++) {
        auto field = dict_index_get_nth_field(index, i);
        auto col_name = dict_table_get_col_name(table, dict_col_get_no(field->col));

        if (0 != ib_utf8_strcasecmp(columns[i], col_name)) {

          break;
        }
      }

      if (i == n_cols) {
        /* We found a matching index, select the index with the higher id*/

        if (!found || index->id > found->id) {

          found = index;
        }
      }
    }

    index = dict_table_get_next_index(index);
  }

  return found;
}

/**
 * Report an error in a foreign key definition.
 *
 * @param ib_stream   [in] output stream
 * @param name        [in] table name
 */
static void dict_foreign_error_report_low(
  ib_stream_t ib_stream,
  const char *name
) {
  ut_print_timestamp(ib_stream);
  ib_logger(ib_stream, " Error in foreign key constraint of table %s:\n", name);
}

/**
 * Report an error in a foreign key definition.
 *
 * @param stderr [in] output stream
 * @param fk [in] foreign key constraint
 * @param msg [in] the error message
 */
static void dict_foreign_error_report(ib_stream_t stderr, dict_foreign_t *fk, const char *msg) {
  mutex_enter(&dict_foreign_err_mutex);
  dict_foreign_error_report_low(ib_stream, fk->foreign_table_name);
  ib_logger(ib_stream, "%s", msg);
  ib_logger(ib_stream, "%s Constraint:\n", msg);

  dict_print_info_on_foreign_key_in_create_format(ib_stream, nullptr, fk, true);

  ib_logger(ib_stream, "\n");
  if (fk->foreign_index) {
    ib_logger(ib_stream, "The index in the foreign key in table is ");
    ut_print_name(ib_stream, nullptr, false, fk->foreign_index->name);
    ib_logger(
      ib_stream,
      "\n"
      "See InnoDB website for details\n"
      "for correct foreign key definition.\n"
    );
  }
  mutex_exit(&dict_foreign_err_mutex);
}

db_err dict_foreign_add_to_cache(dict_foreign_t *foreign, bool check_charsets) {
  dict_table_t *for_table;
  dict_table_t *ref_table;
  dict_foreign_t *for_in_cache = nullptr;
  dict_index_t *index;
  bool added_to_referenced_list = false;

  ut_ad(mutex_own(&(dict_sys->mutex)));

  for_table = dict_table_check_if_in_cache_low(foreign->foreign_table_name);

  ref_table = dict_table_check_if_in_cache_low(foreign->referenced_table_name);
  ut_a(for_table || ref_table);

  if (for_table) {
    for_in_cache = dict_foreign_find(for_table, foreign->id);
  }

  if (!for_in_cache && ref_table) {
    for_in_cache = dict_foreign_find(ref_table, foreign->id);
  }

  if (for_in_cache) {
    /* Free the foreign object */
    mem_heap_free(foreign->heap);
  } else {
    for_in_cache = foreign;
  }

  if (for_in_cache->referenced_table == nullptr && ref_table) {
    index = dict_foreign_find_index(
      ref_table, for_in_cache->referenced_col_names, for_in_cache->n_fields, for_in_cache->foreign_index, check_charsets, false
    );

    if (index == nullptr) {
      dict_foreign_error_report(
        ib_stream,
        for_in_cache,
        "there is no index in referenced table"
        " which would contain\n"
        "the columns as the first columns,"
        " or the data types in the\n"
        "referenced table do not match"
        " the ones in table."
      );

      if (for_in_cache == foreign) {
        mem_heap_free(foreign->heap);
      }

      return DB_CANNOT_ADD_CONSTRAINT;
    }

    for_in_cache->referenced_table = ref_table;
    for_in_cache->referenced_index = index;
    UT_LIST_ADD_LAST(ref_table->referenced_list, for_in_cache);
    added_to_referenced_list = true;
  }

  if (for_in_cache->foreign_table == nullptr && for_table) {
    index = dict_foreign_find_index(
      for_table,
      for_in_cache->foreign_col_names,
      for_in_cache->n_fields,
      for_in_cache->referenced_index,
      check_charsets,
      for_in_cache->type & (DICT_FOREIGN_ON_DELETE_SET_NULL | DICT_FOREIGN_ON_UPDATE_SET_NULL)
    );

    if (index == nullptr) {
      dict_foreign_error_report(
        ib_stream,
        for_in_cache,
        "there is no index in the table"
        " which would contain\n"
        "the columns as the first columns,"
        " or the data types in the\n"
        "table do not match"
        " the ones in the referenced table\n"
        "or one of the ON ... SET nullptr columns"
        " is declared NOT nullptr."
      );

      if (for_in_cache == foreign) {
        if (added_to_referenced_list) {
          UT_LIST_REMOVE(ref_table->referenced_list, for_in_cache);
        }

        mem_heap_free(foreign->heap);
      }

      return DB_CANNOT_ADD_CONSTRAINT;
    }

    for_in_cache->foreign_table = for_table;
    for_in_cache->foreign_index = index;
    UT_LIST_ADD_LAST(for_table->foreign_list, for_in_cache);
  }

  return DB_SUCCESS;
}

/**
 * Scans from pointer onwards. Stops if is at the start of a copy of
 * 'string' where characters are compared without case sensitivity, and
 * only outside `` or "" quotes. Stops also at NUL.
 *
 * @param ptr [in] scan from
 * @param string [in] look for this
 *
 * @return scanned up to this
 */
static const char *dict_scan_to(const char *ptr, const char *string) {
  char quote = '\0';

  for (; *ptr; ptr++) {
    if (*ptr == quote) {
      /* Closing quote character: do not look for
      starting quote or the keyword. */
      quote = '\0';
    } else if (quote) {
      /* Within quotes: do nothing. */
    } else if (*ptr == '`' || *ptr == '"') {
      /* Starting quote: remember the quote character. */
      quote = *ptr;
    } else {
      /* Outside quotes: look for the keyword. */
      ulint i;
      for (i = 0; string[i]; i++) {
        if (toupper((int)(unsigned char)(ptr[i])) != toupper((int)(unsigned char)(string[i]))) {
          goto nomatch;
        }
      }
      break;
    nomatch:;
    }
  }

  return ptr;
}

/**
 * Accepts a specified string. Comparisons are case-insensitive.
 *
 * @param cs       [in] the character set of ptr
 * @param ptr      [in] scan from this
 * @param string   [in] accept only this string as the next non-whitespace string
 * @param success  [out] true if accepted
 *
 * @return if string was accepted, the pointer is moved after that, else ptr is returned
 */
static const char *dict_accept(
  const charset_t *cs,
  const char *ptr,
  const char *string,
  bool *success
) {
  const char *old_ptr = ptr;
  const char *old_ptr2;

  *success = false;

  while (ib_utf8_isspace(cs, *ptr)) {
    ptr++;
  }

  old_ptr2 = ptr;

  ptr = dict_scan_to(ptr, string);

  if (*ptr == '\0' || old_ptr2 != ptr) {
    return old_ptr;
  }

  *success = true;

  return ptr + strlen(string);
}

/**
 * Scans an id. For the lexical definition of an 'id', see the code below.
 * Strips backquotes or double quotes from around the id.
 *
 * @param cs [in] the character set of ptr
 * @param ptr [in] scanned to
 * @param heap [in] heap where to allocate the id (nullptr=id will not be allocated, 
 *   but it will point to string near ptr)
 * @param id [out,own] the id; nullptr if no id was scannable
 * @param table_id [in] true=convert the allocated id as a table name; false=convert to UTF-8
 * @param accept_also_dot [in] true if also a dot can appear in a non-quoted id; in a quoted id
 *   it can appear always
 *
 * @return scanned to
 */
static const char *dict_scan_id(
  const charset_t *cs,
  const char *ptr,
  mem_heap_t *heap,
  const char **id,
  bool table_id,
  bool accept_also_dot
) {
  char quote = '\0';
  ulint len = 0;
  const char *s;
  char *str;
  char *dst;

  *id = nullptr;

  while (ib_utf8_isspace(cs, *ptr)) {
    ptr++;
  }

  if (*ptr == '\0') {

    return ptr;
  }

  if (*ptr == '`' || *ptr == '"') {
    quote = *ptr++;
  }

  s = ptr;

  if (quote) {
    for (;;) {
      if (!*ptr) {
        /* Syntax error */
        return ptr;
      }
      if (*ptr == quote) {
        ptr++;
        if (*ptr != quote) {
          break;
        }
      }
      ptr++;
      len++;
    }
  } else {
    while (!ib_utf8_isspace(cs, *ptr) && *ptr != '(' && *ptr != ')' && (accept_also_dot || *ptr != '.') && *ptr != ',' &&
           *ptr != '\0') {

      ptr++;
    }

    len = ptr - s;
  }

  if (unlikely(!heap)) {
    /* no heap given: id will point to source string */
    *id = s;
    return ptr;
  }

  if (quote) {
    char *d;
    str = d = (char *)mem_heap_alloc(heap, len + 1);
    while (len--) {
      if ((*d++ = *s++) == quote) {
        s++;
      }
    }
    *d++ = 0;
    len = d - str;
    ut_ad(*s == quote);
    ut_ad(s + 1 == ptr);
  } else {
    str = mem_heap_strdupl(heap, s, len);
  }

  if (!table_id) {
    /* Convert the identifier from connection character set
    to UTF-8. */
    len = 3 * len + 1;
    *id = dst = (char *)mem_heap_alloc(heap, len);

    ib_utf8_convert_from_id(cs, dst, str, len);
  } else {
    /* Encode using filename-safe characters. */
    len = 5 * len + 1;
    *id = dst = (char *)mem_heap_alloc(heap, len);

    ib_utf8_convert_from_table_id(cs, dst, str, len);
  }

  return ptr;
}

/**
 * Tries to scan a column name.
 *
 * @param cs The character set of ptr.
 * @param ptr Scanned to.
 * @param success True if success.
 * @param table Table in which the column is.
 * @param column Pointer to column if success.
 * @param heap Heap where to allocate.
 * @param name The column name; nullptr if no name was scannable.
 * @return Scanned to.
 */
static const char *dict_scan_col(
  const charset_t *cs,
  const char *ptr,
  bool *success,
  dict_table_t *table,
  const dict_col_t **column,
  mem_heap_t *heap,
  const char **name
) {
  ulint i;

  *success = false;

  ptr = dict_scan_id(cs, ptr, heap, name, false, true);

  if (*name == nullptr) {

    return ptr; /* Syntax error */
  }

  if (table == nullptr) {
    *success = true;
    *column = nullptr;
  } else {
    for (i = 0; i < dict_table_get_n_cols(table); i++) {

      const char *col_name = dict_table_get_col_name(table, i);

      if (0 == ib_utf8_strcasecmp(col_name, *name)) {
        /* Found */

        *success = true;
        *column = dict_table_get_nth_col(table, i);
        strcpy((char *)*name, col_name);

        break;
      }
    }
  }

  return ptr;
}

/**
 * Scans a table name from an SQL string.
 *
 * @param cs The character set of ptr.
 * @param ptr Scanned to.
 * @param table Table object or nullptr.
 * @param name Foreign key table name.
 * @param success True if ok name found.
 * @param heap Heap where to allocate the id.
 * @param ref_name The table name; nullptr if no name was scannable.
 * @return Scanned to.
 */
static const char *dict_scan_table_name(
  const charset_t *cs,
  const char *ptr,
  dict_table_t **table,
  const char *name,
  bool *success,
  mem_heap_t *heap,
  const char **ref_name
) {
  const char *database_name = nullptr;
  ulint database_name_len = 0;
  const char *table_name = nullptr;
  ulint table_name_len;
  const char *scan_name;
  char *ref;

  *success = false;
  *table = nullptr;

  ptr = dict_scan_id(cs, ptr, heap, &scan_name, true, false);

  if (scan_name == nullptr) {

    return ptr; /* Syntax error */
  }

  if (*ptr == '.') {
    /* We scanned the database name; scan also the table name */

    ptr++;

    database_name = scan_name;
    database_name_len = strlen(database_name);

    ptr = dict_scan_id(cs, ptr, heap, &table_name, true, false);

    if (table_name == nullptr) {

      return ptr; /* Syntax error */
    }
  } else {
    /* To be able to read table dumps made with InnoDB-4.0.17 or
    earlier, we must allow the dot separator between the database
    name and the table name also to appear within a quoted
    identifier! InnoDB used to print a constraint as:
    ... REFERENCES `databasename.tablename` ...
    starting from 4.0.18 it is
    ... REFERENCES `databasename`.`tablename` ... */
    const char *s;

    for (s = scan_name; *s; s++) {
      if (*s == '.') {
        database_name = scan_name;
        database_name_len = s - scan_name;
        scan_name = ++s;
        break; /* to do: multiple dots? */
      }
    }

    table_name = scan_name;
  }

  if (database_name == nullptr) {
    /* Use the database name of the foreign key table */

    database_name = name;
    database_name_len = dict_get_db_name_len(name);
  }

  table_name_len = strlen(table_name);

  /* Copy database_name, '/', table_name, '\0' */
  ref = (char *)mem_heap_alloc(heap, database_name_len + table_name_len + 2);
  memcpy(ref, database_name, database_name_len);
  ref[database_name_len] = '/';
  memcpy(ref + database_name_len + 1, table_name, table_name_len + 1);
  if (srv_lower_case_table_names) {
    /* The table name is always put to lower case on Windows. */
    ib_utf8_casedown(ref);
  }

  *success = true;
  *ref_name = ref;
  *table = dict_table_get_low(ref);

  return ptr;
}

/**
 * Skips one id. The id is allowed to contain also '.'.
 *
 * @param cs The character set of ptr.
 * @param ptr Scanned to.
 * @param success True if success, false if just spaces left in string or a syntax error.
 *
 * @return Scanned to.
 */
static const char *dict_skip_word(const charset_t *cs, const char *ptr, bool *success) {
  const char *start;

  *success = false;

  ptr = dict_scan_id(cs, ptr, nullptr, &start, false, true);

  if (start) {
    *success = true;
  }

  return ptr;
}

/** Removes comments from an SQL string. A comment is either
(a) '#' to the end of the line,
(b) '--[space]' to the end of the line, or
(c) '[slash][asterisk]' till the next '[asterisk][slash]' (like the familiar
C comment syntax).
@return own: SQL string stripped from comments; the caller must free
this with mem_free()! */
static char *dict_strip_comments(const char *sql_string) /*!< in: SQL string */
{
  char *ptr;
  /* unclosed quote character (0 if none) */
  char quote = 0;
  const char *sptr;

  auto str = (char *)mem_alloc(strlen(sql_string) + 1);

  sptr = sql_string;
  ptr = str;

  for (;;) {
  scan_more:
    if (*sptr == '\0') {
      *ptr = '\0';

      ut_a(ptr <= str + strlen(sql_string));

      return str;
    }

    if (*sptr == quote) {
      /* Closing quote character: do not look for
      starting quote or comments. */
      quote = 0;
    } else if (quote) {
      /* Within quotes: do not look for
      starting quotes or comments. */
    } else if (*sptr == '"' || *sptr == '`' || *sptr == '\'') {
      /* Starting quote: remember the quote character. */
      quote = *sptr;
    } else if (*sptr == '#' || (sptr[0] == '-' && sptr[1] == '-' && sptr[2] == ' ')) {
      for (;;) {
        /* In Unix a newline is 0x0A while in Windows
        it is 0x0D followed by 0x0A */

        if (*sptr == (char)0x0A || *sptr == (char)0x0D || *sptr == '\0') {

          goto scan_more;
        }

        sptr++;
      }
    } else if (!quote && *sptr == '/' && *(sptr + 1) == '*') {
      for (;;) {
        if (*sptr == '*' && *(sptr + 1) == '/') {

          sptr += 2;

          goto scan_more;
        }

        if (*sptr == '\0') {

          goto scan_more;
        }

        sptr++;
      }
    }

    *ptr = *sptr;

    ptr++;
    sptr++;
  }
}

/**
 * @brief Finds the highest [number] for foreign key constraints of the table. Looks
 * only at the >= 4.0.18-format id's, which are of the form
 * databasename/tablename_ibfk_[number].
 * 
 * @param table in: table in the dictionary memory cache
 * @return highest number, 0 if table has no new format foreign key constraints
 */
static ulint dict_table_get_highest_foreign_id(dict_table_t *table) {
  dict_foreign_t *foreign;
  char *endp;
  ulint biggest_id = 0;
  ulint id;
  ulint len;

  ut_a(table);

  len = strlen(table->name);
  foreign = UT_LIST_GET_FIRST(table->foreign_list);

  while (foreign) {
    if (strlen(foreign->id) > ((sizeof dict_ibfk) - 1) + len && 0 == memcmp(foreign->id, table->name, len) &&
        0 == memcmp(foreign->id + len, dict_ibfk, (sizeof dict_ibfk) - 1) && foreign->id[len + ((sizeof dict_ibfk) - 1)] != '0') {
      /* It is of the >= 4.0.18 format */

      id = strtoul(foreign->id + len + ((sizeof dict_ibfk) - 1), &endp, 10);
      if (*endp == '\0') {
        ut_a(id != biggest_id);

        if (id > biggest_id) {
          biggest_id = id;
        }
      }
    }

    foreign = UT_LIST_GET_NEXT(foreign_list, foreign);
  }

  return biggest_id;
}

/**
 * @brief Reports a simple foreign key create clause syntax error.
 *
 * @param name in: table name
 * @param start_of_latest_foreign in: start of the foreign key clause in the SQL string
 * @param ptr in: place of the syntax error
 */
static void dict_foreign_report_syntax_err(
  const char *name,
  const char *start_of_latest_foreign,
  const char *ptr
) {

  mutex_enter(&dict_foreign_err_mutex);
  dict_foreign_error_report_low(ib_stream, name);
  ib_logger(ib_stream, "%s:\nSyntax error close to:\n%s\n", start_of_latest_foreign, ptr);
  mutex_exit(&dict_foreign_err_mutex);
}

/**
 * Scans a table create SQL string and adds to the data dictionary the foreign
 * key constraints declared in the string. This function should be called after
 * the indexes for a table have been created. Each foreign key constraint must
 * be accompanied with indexes in both participating tables. The indexes are
 * allowed to contain more fields than mentioned in the constraint.
 *
 * @param trx in: transaction
 * @param heap in: memory heap
 * @param cs in: the character set of sql_string
 * @param sql_string in: CREATE TABLE or ALTER TABLE statement
 *                      where foreign keys are declared like:
 *                      FOREIGN KEY (a, b) REFERENCES table2(c, d),
 *                      table2 can be written also with the database
 *                      name before it: test.table2; the default
 *                      database is the database of parameter name
 * @param name in: table full name in the normalized form
 *                 database_name/table_name
 * @param reject_fks in: if true, fail with error code
 *                       DB_CANNOT_ADD_CONSTRAINT if any foreign
 *                       keys are found.
 *
 * @return error code or DB_SUCCESS
 */
static db_err dict_create_foreign_constraints_low(
  trx_t *trx,
  mem_heap_t *heap,
  const charset_t *cs,
  const char *sql_string,
  const char *name,
  bool reject_fks
) {
  dict_table_t *table;
  dict_table_t *referenced_table;
  dict_table_t *table_to_alter;
  ulint highest_id_so_far = 0;
  dict_index_t *index;
  dict_foreign_t *foreign;
  const char *ptr = sql_string;
  const char *start_of_latest_foreign = sql_string;
  const char *constraint_name;
  bool success;
  db_err error;
  const char *ptr1;
  const char *ptr2;
  ulint i;
  ulint j;
  bool is_on_delete;
  ulint n_on_deletes;
  ulint n_on_updates;
  const dict_col_t *columns[500];
  const char *column_names[500];
  const char *referenced_table_name;

  ut_ad(mutex_own(&(dict_sys->mutex)));

  table = dict_table_get_low(name);

  if (table == nullptr) {
    mutex_enter(&dict_foreign_err_mutex);
    dict_foreign_error_report_low(ib_stream, name);
    ib_logger(
      ib_stream,
      "Cannot find the table in the internal"
      " data dictionary of InnoDB.\n"
      "Create table statement:\n%s\n",
      sql_string
    );
    mutex_exit(&dict_foreign_err_mutex);

    return DB_ERROR;
  }

  /* First check if we are actually doing an ALTER TABLE, and in that
  case look for the table being altered */

  ptr = dict_accept(cs, ptr, "ALTER", &success);

  if (!success) {

    goto loop;
  }

  ptr = dict_accept(cs, ptr, "TABLE", &success);

  if (!success) {

    goto loop;
  }

  /* We are doing an ALTER TABLE: scan the table name we are altering */

  ptr = dict_scan_table_name(cs, ptr, &table_to_alter, name, &success, heap, &referenced_table_name);
  if (!success) {
    ib_logger(
      ib_stream,
      "Error: could not find"
      " the table being ALTERED in:\n%s\n",
      sql_string
    );

    return DB_ERROR;
  }

  /* Starting from 4.0.18 and 4.1.2, we generate foreign key id's in the
  format databasename/tablename_ibfk_[number], where [number] is local
  to the table; look for the highest [number] for table_to_alter, so
  that we can assign to new constraints higher numbers. */

  /* If we are altering a temporary table, the table name after ALTER
  TABLE does not correspond to the internal table name, and
  table_to_alter is nullptr. TODO: should we fix this somehow? */

  if (table_to_alter == nullptr) {
    highest_id_so_far = 0;
  } else {
    highest_id_so_far = dict_table_get_highest_foreign_id(table_to_alter);
  }

  /* Scan for foreign key declarations in a loop */
loop:
  /* Scan either to "CONSTRAINT" or "FOREIGN", whichever is closer */

  ptr1 = dict_scan_to(ptr, "CONSTRAINT");
  ptr2 = dict_scan_to(ptr, "FOREIGN");

  constraint_name = nullptr;

  if (ptr1 < ptr2) {
    /* The user may have specified a constraint name. Pick it so
    that we can store 'databasename/constraintname' as the id of
    of the constraint to system tables. */
    ptr = ptr1;

    ptr = dict_accept(cs, ptr, "CONSTRAINT", &success);

    ut_a(success);

    if (!ib_utf8_isspace(cs, *ptr) && *ptr != '"' && *ptr != '`') {

      goto loop;
    }

    while (ib_utf8_isspace(cs, *ptr)) {
      ptr++;
    }

    /* read constraint name unless got "CONSTRAINT FOREIGN" */
    if (ptr != ptr2) {
      ptr = dict_scan_id(cs, ptr, heap, &constraint_name, false, false);
    }
  } else {
    ptr = ptr2;
  }

  if (*ptr == '\0') {
    if (reject_fks && (UT_LIST_GET_LEN(table->foreign_list) > 0)) {

      return DB_CANNOT_ADD_CONSTRAINT;
    }

    /* The following call adds the foreign key constraints
    to the data dictionary system tables on disk */

    error = dict_create_add_foreigns_to_dictionary(highest_id_so_far, table, trx);
    return error;
  }

  start_of_latest_foreign = ptr;

  ptr = dict_accept(cs, ptr, "FOREIGN", &success);

  if (!success) {
    goto loop;
  }

  if (!ib_utf8_isspace(cs, *ptr)) {
    goto loop;
  }

  ptr = dict_accept(cs, ptr, "KEY", &success);

  if (!success) {
    goto loop;
  }

  ptr = dict_accept(cs, ptr, "(", &success);

  if (!success) {
    /* Skip index id before the '('. */
    ptr = dict_skip_word(cs, ptr, &success);

    if (!success) {
      dict_foreign_report_syntax_err(name, start_of_latest_foreign, ptr);

      return DB_CANNOT_ADD_CONSTRAINT;
    }

    ptr = dict_accept(cs, ptr, "(", &success);

    if (!success) {
      /* We do not flag a syntax error here because in an
      ALTER TABLE we may also have DROP FOREIGN KEY abc */

      goto loop;
    }
  }

  i = 0;

  /* Scan the columns in the first list */
col_loop1:
  ut_a(i < (sizeof column_names) / sizeof *column_names);
  ptr = dict_scan_col(cs, ptr, &success, table, columns + i, heap, column_names + i);
  if (!success) {
    mutex_enter(&dict_foreign_err_mutex);
    dict_foreign_error_report_low(ib_stream, name);
    ib_logger(ib_stream, "%s:\nCannot resolve column name close to:\n%s\n", start_of_latest_foreign, ptr);
    mutex_exit(&dict_foreign_err_mutex);

    return DB_CANNOT_ADD_CONSTRAINT;
  }

  i++;

  ptr = dict_accept(cs, ptr, ",", &success);

  if (success) {
    goto col_loop1;
  }

  ptr = dict_accept(cs, ptr, ")", &success);

  if (!success) {
    dict_foreign_report_syntax_err(name, start_of_latest_foreign, ptr);
    return DB_CANNOT_ADD_CONSTRAINT;
  }

  /* Try to find an index which contains the columns
  as the first fields and in the right order */

  index = dict_foreign_find_index(table, column_names, i, nullptr, true, false);

  if (!index) {
    mutex_enter(&dict_foreign_err_mutex);
    dict_foreign_error_report_low(ib_stream, name);
    ib_logger(ib_stream, "There is no index in table ");
    ut_print_name(ib_stream, nullptr, true, name);
    ib_logger(
      ib_stream,
      " where the columns appear\n"
      "as the first columns. Constraint:\n%s\n"
      "See InnoDB website for details"
      "for correct foreign key definition.\n",
      start_of_latest_foreign
    );
    mutex_exit(&dict_foreign_err_mutex);

    return DB_CANNOT_ADD_CONSTRAINT;
  }
  ptr = dict_accept(cs, ptr, "REFERENCES", &success);

  if (!success || !ib_utf8_isspace(cs, *ptr)) {
    dict_foreign_report_syntax_err(name, start_of_latest_foreign, ptr);
    return DB_CANNOT_ADD_CONSTRAINT;
  }

  /* Let us create a constraint struct */

  foreign = dict_mem_foreign_create();

  if (constraint_name) {
    ulint db_len;

    /* Catenate 'databasename/' to the constraint name specified
    by the user: we conceive the constraint as belonging to the
    same client 'database' as the table itself. We store the name
    to foreign->id. */

    db_len = dict_get_db_name_len(table->name);

    foreign->id = (char *)mem_heap_alloc(foreign->heap, db_len + strlen(constraint_name) + 2);

    memcpy(foreign->id, table->name, db_len);
    foreign->id[db_len] = '/';
    strcpy(foreign->id + db_len + 1, constraint_name);
  }

  foreign->foreign_table = table;
  foreign->foreign_table_name = mem_heap_strdup(foreign->heap, table->name);
  foreign->foreign_index = index;
  foreign->n_fields = (unsigned int)i;
  foreign->foreign_col_names = (const char **)mem_heap_alloc(foreign->heap, i * sizeof(void *));
  for (i = 0; i < foreign->n_fields; i++) {
    foreign->foreign_col_names[i] = mem_heap_strdup(foreign->heap, dict_table_get_col_name(table, dict_col_get_no(columns[i])));
  }

  ptr = dict_scan_table_name(cs, ptr, &referenced_table, name, &success, heap, &referenced_table_name);

  /* Note that referenced_table can be nullptr if the user has suppressed
  checking of foreign key constraints! */

  if (!success || (!referenced_table && trx->m_check_foreigns)) {
    dict_foreign_free(foreign);

    mutex_enter(&dict_foreign_err_mutex);
    dict_foreign_error_report_low(ib_stream, name);
    ib_logger(
      ib_stream,
      "%s:\nCannot resolve table name close to:\n"
      "%s\n",
      start_of_latest_foreign,
      ptr
    );
    mutex_exit(&dict_foreign_err_mutex);

    return DB_CANNOT_ADD_CONSTRAINT;
  }

  ptr = dict_accept(cs, ptr, "(", &success);

  if (!success) {
    dict_foreign_free(foreign);
    dict_foreign_report_syntax_err(name, start_of_latest_foreign, ptr);
    return DB_CANNOT_ADD_CONSTRAINT;
  }

  /* Scan the columns in the second list */
  i = 0;

col_loop2:
  ptr = dict_scan_col(cs, ptr, &success, referenced_table, columns + i, heap, column_names + i);
  i++;

  if (!success) {
    dict_foreign_free(foreign);

    mutex_enter(&dict_foreign_err_mutex);
    dict_foreign_error_report_low(ib_stream, name);
    ib_logger(
      ib_stream,
      "%s:\nCannot resolve column name close to:\n"
      "%s\n",
      start_of_latest_foreign,
      ptr
    );
    mutex_exit(&dict_foreign_err_mutex);

    return DB_CANNOT_ADD_CONSTRAINT;
  }

  ptr = dict_accept(cs, ptr, ",", &success);

  if (success) {
    goto col_loop2;
  }

  ptr = dict_accept(cs, ptr, ")", &success);

  if (!success || foreign->n_fields != i) {
    dict_foreign_free(foreign);

    dict_foreign_report_syntax_err(name, start_of_latest_foreign, ptr);
    return DB_CANNOT_ADD_CONSTRAINT;
  }

  n_on_deletes = 0;
  n_on_updates = 0;

scan_on_conditions:
  /* Loop here as long as we can find ON ... conditions */

  ptr = dict_accept(cs, ptr, "ON", &success);

  if (!success) {

    goto try_find_index;
  }

  ptr = dict_accept(cs, ptr, "DELETE", &success);

  if (!success) {
    ptr = dict_accept(cs, ptr, "UPDATE", &success);

    if (!success) {
      dict_foreign_free(foreign);

      dict_foreign_report_syntax_err(name, start_of_latest_foreign, ptr);
      return DB_CANNOT_ADD_CONSTRAINT;
    }

    is_on_delete = false;
    n_on_updates++;
  } else {
    is_on_delete = true;
    n_on_deletes++;
  }

  ptr = dict_accept(cs, ptr, "RESTRICT", &success);

  if (success) {
    goto scan_on_conditions;
  }

  ptr = dict_accept(cs, ptr, "CASCADE", &success);

  if (success) {
    if (is_on_delete) {
      foreign->type |= DICT_FOREIGN_ON_DELETE_CASCADE;
    } else {
      foreign->type |= DICT_FOREIGN_ON_UPDATE_CASCADE;
    }

    goto scan_on_conditions;
  }

  ptr = dict_accept(cs, ptr, "NO", &success);

  if (success) {
    ptr = dict_accept(cs, ptr, "ACTION", &success);

    if (!success) {
      dict_foreign_free(foreign);
      dict_foreign_report_syntax_err(name, start_of_latest_foreign, ptr);

      return DB_CANNOT_ADD_CONSTRAINT;
    }

    if (is_on_delete) {
      foreign->type |= DICT_FOREIGN_ON_DELETE_NO_ACTION;
    } else {
      foreign->type |= DICT_FOREIGN_ON_UPDATE_NO_ACTION;
    }

    goto scan_on_conditions;
  }

  ptr = dict_accept(cs, ptr, "SET", &success);

  if (!success) {
    dict_foreign_free(foreign);
    dict_foreign_report_syntax_err(name, start_of_latest_foreign, ptr);
    return DB_CANNOT_ADD_CONSTRAINT;
  }

  ptr = dict_accept(cs, ptr, "nullptr", &success);

  if (!success) {
    dict_foreign_free(foreign);
    dict_foreign_report_syntax_err(name, start_of_latest_foreign, ptr);
    return DB_CANNOT_ADD_CONSTRAINT;
  }

  for (j = 0; j < foreign->n_fields; j++) {
    if ((dict_index_get_nth_col(foreign->foreign_index, j)->prtype) & DATA_NOT_NULL) {

      /* It is not sensible to define SET nullptr
      if the column is not allowed to be nullptr! */

      dict_foreign_free(foreign);

      mutex_enter(&dict_foreign_err_mutex);
      dict_foreign_error_report_low(ib_stream, name);
      ib_logger(
        ib_stream,
        "%s:\n"
        "You have defined a SET nullptr condition"
        " though some of the\n"
        "columns are defined as NOT nullptr.\n",
        start_of_latest_foreign
      );
      mutex_exit(&dict_foreign_err_mutex);

      return DB_CANNOT_ADD_CONSTRAINT;
    }
  }

  if (is_on_delete) {
    foreign->type |= DICT_FOREIGN_ON_DELETE_SET_NULL;
  } else {
    foreign->type |= DICT_FOREIGN_ON_UPDATE_SET_NULL;
  }

  goto scan_on_conditions;

try_find_index:
  if (n_on_deletes > 1 || n_on_updates > 1) {
    /* It is an error to define more than 1 action */

    dict_foreign_free(foreign);

    mutex_enter(&dict_foreign_err_mutex);
    dict_foreign_error_report_low(ib_stream, name);
    ib_logger(
      ib_stream,
      "%s:\n"
      "You have twice an ON DELETE clause"
      " or twice an ON UPDATE clause.\n",
      start_of_latest_foreign
    );
    mutex_exit(&dict_foreign_err_mutex);

    return DB_CANNOT_ADD_CONSTRAINT;
  }

  /* Try to find an index which contains the columns as the first fields
  and in the right order, and the types are the same as in
  foreign->foreign_index */

  if (referenced_table) {
    index = dict_foreign_find_index(referenced_table, column_names, i, foreign->foreign_index, true, false);
    if (!index) {
      dict_foreign_free(foreign);
      mutex_enter(&dict_foreign_err_mutex);
      dict_foreign_error_report_low(ib_stream, name);
      ib_logger(
        ib_stream,
        "%s:\n"
        "Cannot find an index in the"
        " referenced table where the\n"
        "referenced columns appear as the"
        " first columns, or column types\n"
        "in the table and the referenced table"
        " do not match for constraint.\n"
        "Note that the internal storage type of"
        " ENUM and SET changed in\n"
        "tables created with >= InnoDB-4.1.12,"
        " and such columns in old tables\n"
        "cannot be referenced by such columns"
        " in new tables.\n"
        "See InnoDB website for details"
        "for correct foreign key definition.\n",
        start_of_latest_foreign
      );
      mutex_exit(&dict_foreign_err_mutex);

      return DB_CANNOT_ADD_CONSTRAINT;
    }
  } else {
    ut_a(trx->m_check_foreigns == false);
    index = nullptr;
  }

  foreign->referenced_index = index;
  foreign->referenced_table = referenced_table;

  foreign->referenced_table_name = mem_heap_strdup(foreign->heap, referenced_table_name);

  foreign->referenced_col_names = (const char **)mem_heap_alloc(foreign->heap, i * sizeof(void *));
  for (i = 0; i < foreign->n_fields; i++) {
    foreign->referenced_col_names[i] = mem_heap_strdup(foreign->heap, column_names[i]);
  }

  /* We found an ok constraint definition: add to the lists */

  UT_LIST_ADD_LAST(table->foreign_list, foreign);

  if (referenced_table) {
    UT_LIST_ADD_LAST(referenced_table->referenced_list, foreign);
  }

  goto loop;
}

db_err dict_create_foreign_constraints(trx_t *trx, const char *sql_string, const char *name, bool reject_fks) {
  ut_a(trx);

  auto str = dict_strip_comments(sql_string);
  auto heap = mem_heap_create(10000);

  auto cs = ib_ucode_get_connection_charset();

  auto err = dict_create_foreign_constraints_low(trx, heap, cs, str, name, reject_fks);

  mem_heap_free(heap);
  mem_free(str);

  return err;
}

db_err dict_foreign_parse_drop_constraints(mem_heap_t *heap, trx_t *trx, dict_table_t *table, ulint *n, const char ***constraints_to_drop) {
  dict_foreign_t *foreign;
  bool success;
  char *str;
  const char *ptr;
  const char *id;
  const charset_t *cs;

  ut_a(trx);

  cs = ib_ucode_get_connection_charset();

  *n = 0;

  *constraints_to_drop = (const char **)mem_heap_alloc(heap, 1000 * sizeof(char *));

  str = dict_strip_comments(*trx->client_query_str);
  ptr = str;

  ut_ad(mutex_own(&(dict_sys->mutex)));
loop:
  ptr = dict_scan_to(ptr, "DROP");

  if (*ptr == '\0') {
    mem_free(str);

    return DB_SUCCESS;
  }

  ptr = dict_accept(cs, ptr, "DROP", &success);

  if (!ib_utf8_isspace(cs, *ptr)) {

    goto loop;
  }

  ptr = dict_accept(cs, ptr, "FOREIGN", &success);

  if (!success || !ib_utf8_isspace(cs, *ptr)) {

    goto loop;
  }

  ptr = dict_accept(cs, ptr, "KEY", &success);

  if (!success) {

    goto syntax_error;
  }

  ptr = dict_scan_id(cs, ptr, heap, &id, false, true);

  if (id == nullptr) {

    goto syntax_error;
  }

  ut_a(*n < 1000);
  (*constraints_to_drop)[*n] = id;
  (*n)++;

  /* Look for the given constraint id */

  foreign = UT_LIST_GET_FIRST(table->foreign_list);

  while (foreign != nullptr) {
    if (0 == strcmp(foreign->id, id) || (strchr(foreign->id, '/') && 0 == strcmp(id, dict_remove_db_name(foreign->id)))) {
      /* Found */
      break;
    }

    foreign = UT_LIST_GET_NEXT(foreign_list, foreign);
  }

  if (foreign == nullptr) {
    mutex_enter(&dict_foreign_err_mutex);
    ut_print_timestamp(ib_stream);
    ib_logger(
      ib_stream,
      " Error in dropping of a foreign key constraint"
      " of table "
    );
    ut_print_name(ib_stream, nullptr, true, table->name);
    ib_logger(ib_stream, ",\nin SQL command\n%s", str);
    ib_logger(ib_stream, "\nCannot find a constraint with the given id ");
    ut_print_name(ib_stream, nullptr, false, id);
    ib_logger(ib_stream, ".\n");
    mutex_exit(&dict_foreign_err_mutex);

    mem_free(str);

    return DB_CANNOT_DROP_CONSTRAINT;
  }

  goto loop;

syntax_error:
  mutex_enter(&dict_foreign_err_mutex);
  ut_print_timestamp(ib_stream);
  ib_logger(
    ib_stream,
    " Syntax error in dropping of a"
    " foreign key constraint of table "
  );
  ut_print_name(ib_stream, nullptr, true, table->name);
  ib_logger(
    ib_stream,
    ",\n"
    "close to:\n%s\n in SQL command\n%s\n",
    ptr,
    str
  );
  mutex_exit(&dict_foreign_err_mutex);

  mem_free(str);

  return DB_CANNOT_DROP_CONSTRAINT;
}

dict_index_t *dict_index_get_if_in_cache_low(uint64_t index_id) {
  ut_ad(mutex_own(&(dict_sys->mutex)));

  return dict_index_find_on_id_low(index_id);
}

#if defined UNIV_DEBUG || defined UNIV_BUF_DEBUG
dict_index_t *dict_index_get_if_in_cache(uint64_t index_id) {
  dict_index_t *index;

  if (dict_sys == nullptr) {
    return nullptr;
  }

  mutex_enter(&(dict_sys->mutex));

  index = dict_index_get_if_in_cache_low(index_id);

  mutex_exit(&(dict_sys->mutex));

  return index;
}
#endif /* UNIV_DEBUG || UNIV_BUF_DEBUG */

#ifdef UNIV_DEBUG
bool dict_index_check_search_tuple(const dict_index_t *index, const dtuple_t *tuple) {
  ut_a(index);
  ut_a(dtuple_get_n_fields_cmp(tuple) <= dict_index_get_n_unique_in_tree(index));
  return true;
}
#endif /* UNIV_DEBUG */

dtuple_t *dict_index_build_node_ptr(const dict_index_t *index, const rec_t *rec, ulint page_no, mem_heap_t *heap, ulint level) {
  dtuple_t *tuple;
  dfield_t *field;

  auto n_unique = dict_index_get_n_unique_in_tree(index);

  tuple = dtuple_create(heap, n_unique + 1);

  /* When searching in the tree for the node pointer, we must not do
  comparison on the last field, the page number field, as on upper
  levels in the tree there may be identical node pointers with a
  different page number; therefore, we set the n_fields_cmp to one
  less: */

  dtuple_set_n_fields_cmp(tuple, n_unique);

  dict_index_copy_types(tuple, index, n_unique);

  auto buf = (byte *)mem_heap_alloc(heap, 4);

  mach_write_to_4(buf, page_no);

  field = dtuple_get_nth_field(tuple, n_unique);
  dfield_set_data(field, buf, 4);

  dtype_set(dfield_get_type(field), DATA_SYS_CHILD, DATA_NOT_NULL, 4);

  rec_copy_prefix_to_dtuple(tuple, rec, index, n_unique, heap);

  dtuple_set_info_bits(tuple, dtuple_get_info_bits(tuple) | REC_STATUS_NODE_PTR);

  ut_ad(dtuple_check_typed(tuple));

  return tuple;
}

rec_t *dict_index_copy_rec_order_prefix(const dict_index_t *index, const rec_t *rec, ulint *n_fields, byte *&buf, ulint &buf_size) {
  prefetch_r(rec);

  auto n = dict_index_get_n_unique_in_tree(index);

  *n_fields = n;
  return rec_copy_prefix_to_buf(rec, index, n, buf, buf_size);
}

dtuple_t *dict_index_build_data_tuple(dict_index_t *index, rec_t *rec, ulint n_fields, mem_heap_t *heap) {
  dtuple_t *tuple;

  ut_ad(dict_table_is_comp(index->table) || n_fields <= rec_get_n_fields_old(rec));

  tuple = dtuple_create(heap, n_fields);

  dict_index_copy_types(tuple, index, n_fields);

  rec_copy_prefix_to_dtuple(tuple, rec, index, n_fields, heap);

  ut_ad(dtuple_check_typed(tuple));

  return tuple;
}

ulint dict_index_calc_min_rec_len(const dict_index_t *index) {
  ulint i;
  ulint sum = 0;
  ulint comp = dict_table_is_comp(index->table);

  if (comp) {
    ulint nullable = 0;
    sum = REC_N_NEW_EXTRA_BYTES;
    for (i = 0; i < dict_index_get_n_fields(index); i++) {
      const dict_col_t *col = dict_index_get_nth_col(index, i);
      ulint size = dict_col_get_fixed_size(col, comp);
      sum += size;
      if (!size) {
        size = col->len;
        sum += size < 128 ? 1 : 2;
      }
      if (!(col->prtype & DATA_NOT_NULL)) {
        nullable++;
      }
    }

    /* round the nullptr flags up to full bytes */
    sum += UT_BITS_IN_BYTES(nullable);

    return sum;
  }

  for (i = 0; i < dict_index_get_n_fields(index); i++) {
    sum += dict_col_get_fixed_size(dict_index_get_nth_col(index, i), comp);
  }

  if (sum > 127) {
    sum += 2 * dict_index_get_n_fields(index);
  } else {
    sum += dict_index_get_n_fields(index);
  }

  sum += REC_N_OLD_EXTRA_BYTES;

  return sum;
}

void dict_update_statistics_low(dict_table_t *table, bool has_dict_mutex __attribute__((unused))) {
  dict_index_t *index;
  ulint size;
  ulint sum_of_index_sizes = 0;

  if (table->ibd_file_missing) {
    ut_print_timestamp(ib_stream);
    ib_logger(
      ib_stream,
      "  cannot calculate statistics for table %s\n"
      "because the .ibd file is missing.  For help,"
      " please refer to\n"
      "InnoDB website for details\n",
      table->name
    );

    return;
  }

  /* If we have set a high innodb_force_recovery level, do not calculate
  statistics, as a badly corrupted index can cause a crash in it. */

  if (srv_force_recovery > IB_RECOVERY_NO_TRX_UNDO) {

    return;
  }

  /* Find out the sizes of the indexes and how many different values
  for the key they approximately have */

  index = dict_table_get_first_index(table);

  if (index == nullptr) {
    /* Table definition is corrupt */

    return;
  }

  while (index) {
    size = btr_get_size(index, BTR_TOTAL_SIZE);

    index->stat_index_size = size;

    sum_of_index_sizes += size;

    size = btr_get_size(index, BTR_N_LEAF_PAGES);

    if (size == 0) {
      /* The root node of the tree is a leaf */
      size = 1;
    }

    index->stat_n_leaf_pages = size;

    btr_estimate_number_of_different_key_vals(index);

    index = dict_table_get_next_index(index);
  }

  index = dict_table_get_first_index(table);

  dict_index_stat_mutex_enter(index);

  table->stat_n_rows = index->stat_n_diff_key_vals[dict_index_get_n_unique(index)];

  dict_index_stat_mutex_exit(index);

  table->stat_clustered_index_size = index->stat_index_size;

  table->stat_sum_of_other_index_sizes = sum_of_index_sizes - index->stat_index_size;

  table->stat_initialized = true;

  table->stat_modified_counter = 0;
}

void dict_update_statistics(dict_table_t *table) {
  dict_update_statistics_low(table, false);
}

/**
 * @brief Prints info of a foreign key constraint.
 *
 * @param foreign in: foreign key constraint
 */
static void dict_foreign_print_low(dict_foreign_t *foreign) {
  ut_ad(mutex_own(&(dict_sys->mutex)));

  ib_logger(ib_stream, "  FOREIGN KEY CONSTRAINT %s: %s (", foreign->id, foreign->foreign_table_name);

  for (ulint i = 0; i < foreign->n_fields; i++) {
    ib_logger(ib_stream, " %s", foreign->foreign_col_names[i]);
  }

  ib_logger(
    ib_stream,
    " )\n"
    "             REFERENCES %s (",
    foreign->referenced_table_name
  );

  for (ulint i = 0; i < foreign->n_fields; i++) {
    ib_logger(ib_stream, " %s", foreign->referenced_col_names[i]);
  }

  ib_logger(ib_stream, " )\n");
}

void dict_table_print(dict_table_t *table) {
  mutex_enter(&(dict_sys->mutex));
  dict_table_print_low(table);
  mutex_exit(&(dict_sys->mutex));
}

void dict_table_print_by_name(const char *name) {
  dict_table_t *table;

  mutex_enter(&(dict_sys->mutex));

  table = dict_table_get_low(name);

  ut_a(table);

  dict_table_print_low(table);
  mutex_exit(&(dict_sys->mutex));
}

void dict_table_print_low(dict_table_t *table) {
  dict_index_t *index;
  dict_foreign_t *foreign;
  ulint i;

  ut_ad(mutex_own(&(dict_sys->mutex)));

  dict_update_statistics_low(table, true);

  ib_logger(
    ib_stream,
    "--------------------------------------\n"
    "TABLE: name %s, id %lu %lu, flags %lx, columns %lu,"
    " indexes %lu, appr.rows %lu\n"
    "  COLUMNS: ",
    table->name,
    (ulong)table->id,
    (ulong)table->id,
    (ulong)table->flags,
    (ulong)table->n_cols,
    (ulong)UT_LIST_GET_LEN(table->indexes),
    (ulong)table->stat_n_rows
  );

  for (i = 0; i < (ulint)table->n_cols; i++) {
    dict_col_print_low(table, dict_table_get_nth_col(table, i));
    ib_logger(ib_stream, "; ");
  }

  ib_logger(ib_stream, "\n");

  index = UT_LIST_GET_FIRST(table->indexes);

  while (index != nullptr) {
    dict_index_print_low(index);
    index = UT_LIST_GET_NEXT(indexes, index);
  }

  foreign = UT_LIST_GET_FIRST(table->foreign_list);

  while (foreign != nullptr) {
    dict_foreign_print_low(foreign);
    foreign = UT_LIST_GET_NEXT(foreign_list, foreign);
  }

  foreign = UT_LIST_GET_FIRST(table->referenced_list);

  while (foreign != nullptr) {
    dict_foreign_print_low(foreign);
    foreign = UT_LIST_GET_NEXT(referenced_list, foreign);
  }
}

/** Prints a column data. */
static void dict_col_print_low(
  const dict_table_t *table, /*!< in: table */
  const dict_col_t *col
) /*!< in: column */
{
  dtype_t type;

  ut_ad(mutex_own(&(dict_sys->mutex)));

  dict_col_copy_type(col, &type);

  ib_logger(ib_stream, "%s: ", dict_table_get_col_name(table, dict_col_get_no(col)));

  dtype_print(&type);
}

/**
 * @brief Prints an index data.
 *
 * @param index in: index
 */
static void dict_index_print_low(dict_index_t *index) {
  int64_t n_vals;
  ulint i;

  ut_ad(mutex_own(&(dict_sys->mutex)));

  dict_index_stat_mutex_enter(index);

  if (index->n_user_defined_cols > 0) {
    n_vals = index->stat_n_diff_key_vals[index->n_user_defined_cols];
  } else {
    n_vals = index->stat_n_diff_key_vals[1];
  }

  dict_index_stat_mutex_exit(index);

  ib_logger(
    ib_stream,
    "  INDEX: name %s, id %lu %lu, fields %lu/%lu,"
    " uniq %lu, type %lu\n"
    "   root page %lu, appr.key vals %lu,"
    " leaf pages %lu, size pages %lu\n"
    "   FIELDS: ",
    index->name,
    (ulong)index->id,
    (ulong)index->id,
    (ulong)index->n_user_defined_cols,
    (ulong)index->n_fields,
    (ulong)index->n_uniq,
    (ulong)index->type,
    (ulong)index->page,
    (ulong)n_vals,
    (ulong)index->stat_n_leaf_pages,
    (ulong)index->stat_index_size
  );

  for (i = 0; i < index->n_fields; i++) {
    dict_field_print_low(dict_index_get_nth_field(index, i));
  }

  ib_logger(ib_stream, "\n");

#ifdef UNIV_BTR_PRINT
  btr_print_size(index);

  btr_print_index(index, 7);
#endif /* UNIV_BTR_PRINT */
}

/** Prints a field data. */
static void dict_field_print_low(const dict_field_t *field) /*!< in: field */
{
  ut_ad(mutex_own(&(dict_sys->mutex)));

  ib_logger(ib_stream, " %s", field->name);

  if (field->prefix_len != 0) {
    ib_logger(ib_stream, "(%lu)", (ulong)field->prefix_len);
  }
}

void dict_print_info_on_foreign_key_in_create_format(
  ib_stream_t ib_stream, 
  trx_t *trx,
  dict_foreign_t *foreign,
  bool add_newline
) {
  const char *stripped_id;
  ulint i;

  if (strchr(foreign->id, '/')) {
    /* Strip the preceding database name from the constraint id */
    stripped_id = foreign->id + 1 + dict_get_db_name_len(foreign->id);
  } else {
    stripped_id = foreign->id;
  }

  ib_logger(ib_stream, ",");

  if (add_newline) {
    /* SHOW CREATE TABLE wants constraints each printed nicely
    on its own line, while error messages want no newlines
    inserted. */
    ib_logger(ib_stream, "\n ");
  }

  ib_logger(ib_stream, " CONSTRAINT ");
  ut_print_name(ib_stream, trx, false, stripped_id);
  ib_logger(ib_stream, " FOREIGN KEY (");

  for (i = 0;;) {
    ut_print_name(ib_stream, trx, false, foreign->foreign_col_names[i]);
    if (++i < foreign->n_fields) {
      ib_logger(ib_stream, ", ");
    } else {
      break;
    }
  }

  ib_logger(ib_stream, ") REFERENCES ");

  if (dict_tables_have_same_db(foreign->foreign_table_name, foreign->referenced_table_name)) {
    /* Do not print the database name of the referenced table */
    ut_print_name(ib_stream, trx, true, dict_remove_db_name(foreign->referenced_table_name));
  } else {
    ut_print_name(ib_stream, trx, true, foreign->referenced_table_name);
  }

  ib_logger(ib_stream, " (");

  for (i = 0;;) {
    ut_print_name(ib_stream, trx, false, foreign->referenced_col_names[i]);
    if (++i < foreign->n_fields) {
      ib_logger(ib_stream, ", ");
    } else {
      break;
    }
  }

  ib_logger(ib_stream, ")");

  if (foreign->type & DICT_FOREIGN_ON_DELETE_CASCADE) {
    ib_logger(ib_stream, " ON DELETE CASCADE");
  }

  if (foreign->type & DICT_FOREIGN_ON_DELETE_SET_NULL) {
    ib_logger(ib_stream, " ON DELETE SET nullptr");
  }

  if (foreign->type & DICT_FOREIGN_ON_DELETE_NO_ACTION) {
    ib_logger(ib_stream, " ON DELETE NO ACTION");
  }

  if (foreign->type & DICT_FOREIGN_ON_UPDATE_CASCADE) {
    ib_logger(ib_stream, " ON UPDATE CASCADE");
  }

  if (foreign->type & DICT_FOREIGN_ON_UPDATE_SET_NULL) {
    ib_logger(ib_stream, " ON UPDATE SET nullptr");
  }

  if (foreign->type & DICT_FOREIGN_ON_UPDATE_NO_ACTION) {
    ib_logger(ib_stream, " ON UPDATE NO ACTION");
  }
}

void dict_print_info_on_foreign_keys(bool create_table_format, ib_stream_t ib_stream, trx_t *trx, dict_table_t *table) {
  dict_foreign_t *foreign;

  mutex_enter(&(dict_sys->mutex));

  foreign = UT_LIST_GET_FIRST(table->foreign_list);

  if (foreign == nullptr) {
    mutex_exit(&(dict_sys->mutex));

    return;
  }

  while (foreign != nullptr) {
    if (create_table_format) {
      dict_print_info_on_foreign_key_in_create_format(ib_stream, trx, foreign, true);
    } else {
      ulint i;
      ib_logger(ib_stream, "; (");

      for (i = 0; i < foreign->n_fields; i++) {
        if (i) {
          ib_logger(ib_stream, " ");
        }

        ut_print_name(ib_stream, trx, false, foreign->foreign_col_names[i]);
      }

      ib_logger(ib_stream, ") REFER ");
      ut_print_name(ib_stream, trx, true, foreign->referenced_table_name);
      ib_logger(ib_stream, "(");

      for (i = 0; i < foreign->n_fields; i++) {
        if (i) {
          ib_logger(ib_stream, " ");
        }
        ut_print_name(ib_stream, trx, false, foreign->referenced_col_names[i]);
      }

      ib_logger(ib_stream, ")");

      if (foreign->type == DICT_FOREIGN_ON_DELETE_CASCADE) {
        ib_logger(ib_stream, " ON DELETE CASCADE");
      }

      if (foreign->type == DICT_FOREIGN_ON_DELETE_SET_NULL) {
        ib_logger(ib_stream, " ON DELETE SET nullptr");
      }

      if (foreign->type & DICT_FOREIGN_ON_DELETE_NO_ACTION) {
        ib_logger(ib_stream, " ON DELETE NO ACTION");
      }

      if (foreign->type & DICT_FOREIGN_ON_UPDATE_CASCADE) {
        ib_logger(ib_stream, " ON UPDATE CASCADE");
      }

      if (foreign->type & DICT_FOREIGN_ON_UPDATE_SET_NULL) {
        ib_logger(ib_stream, " ON UPDATE SET nullptr");
      }

      if (foreign->type & DICT_FOREIGN_ON_UPDATE_NO_ACTION) {
        ib_logger(ib_stream, " ON UPDATE NO ACTION");
      }
    }

    foreign = UT_LIST_GET_NEXT(foreign_list, foreign);
  }

  mutex_exit(&(dict_sys->mutex));
}

void dict_index_name_print(ib_stream_t ib_stream, trx_t *trx, const dict_index_t *index) {
  ib_logger(ib_stream, "index ");
  ut_print_name(ib_stream, trx, false, index->name);
  ib_logger(ib_stream, " of table ");
  ut_print_name(ib_stream, trx, true, index->table_name);
}

void dict_ind_init() {
  dict_table_t *table;

  /* create dummy table and index for REDUNDANT infimum and supremum */
  table = dict_mem_table_create("SYS_DUMMY1", DICT_HDR_SPACE, 1, 0);
  dict_mem_table_add_col(table, nullptr, nullptr, DATA_CHAR, DATA_ENGLISH | DATA_NOT_NULL, 8);

  dict_ind_redundant = dict_mem_index_create("SYS_DUMMY1", "SYS_DUMMY1", DICT_HDR_SPACE, 0, 1);
  dict_index_add_col(dict_ind_redundant, table, dict_table_get_nth_col(table, 0), 0);
  dict_ind_redundant->table = table;
  /* create dummy table and index for COMPACT infimum and supremum */
  table = dict_mem_table_create("SYS_DUMMY2", DICT_HDR_SPACE, 1, DICT_TF_COMPACT);
  dict_mem_table_add_col(table, nullptr, nullptr, DATA_CHAR, DATA_ENGLISH | DATA_NOT_NULL, 8);
  dict_ind_compact = dict_mem_index_create("SYS_DUMMY2", "SYS_DUMMY2", DICT_HDR_SPACE, 0, 1);
  dict_index_add_col(dict_ind_compact, table, dict_table_get_nth_col(table, 0), 0);
  dict_ind_compact->table = table;

  /* avoid ut_ad(index->cached) in dict_index_get_n_unique_in_tree */
  dict_ind_redundant->cached = dict_ind_compact->cached = true;
}

/** Frees dict_ind_redundant and dict_ind_compact. */
static void dict_ind_free(void) {
  dict_table_t *table;

  table = dict_ind_compact->table;
  dict_mem_index_free(dict_ind_compact);
  dict_ind_compact = nullptr;
  dict_mem_table_free(table);

  table = dict_ind_redundant->table;
  dict_mem_index_free(dict_ind_redundant);
  dict_ind_redundant = nullptr;
  dict_mem_table_free(table);
}

dict_index_t *dict_table_get_index_on_name(dict_table_t *table, const char *name) {
  dict_index_t *index;

  index = dict_table_get_first_index(table);

  while (index != nullptr) {
    if (strcmp(index->name, name) == 0) {

      return index;
    }

    index = dict_table_get_next_index(index);
  }

  return nullptr;
}

void dict_table_replace_index_in_foreign_list(dict_table_t *table, dict_index_t *index) {
  dict_foreign_t *foreign;

  for (foreign = UT_LIST_GET_FIRST(table->foreign_list); foreign; foreign = UT_LIST_GET_NEXT(foreign_list, foreign)) {

    if (foreign->foreign_index == index) {
      dict_index_t *new_index = dict_foreign_find_equiv_index(foreign);
      ut_a(new_index);

      foreign->foreign_index = new_index;
    }
  }
}

dict_index_t *dict_table_get_index_on_name_and_min_id(dict_table_t *table, const char *name) {
  /* Index with matching name and min(id) */
  dict_index_t *min_index = nullptr;
  auto index = dict_table_get_first_index(table);

  while (index != nullptr) {
    if (strcmp(index->name, name) == 0) {
      if (min_index == nullptr || index->id < min_index->id) {

        min_index = index;
      }
    }

    index = dict_table_get_next_index(index);
  }

  return min_index;
}

void dict_freeze_data_dictionary(trx_t *trx) {
  ut_a(trx->m_dict_operation_lock_mode == 0);

  rw_lock_s_lock(&dict_operation_lock);

  trx->m_dict_operation_lock_mode = RW_S_LATCH;
}

void dict_unfreeze_data_dictionary(trx_t *trx) {
  ut_a(trx->m_dict_operation_lock_mode == RW_S_LATCH);

  rw_lock_s_unlock(&dict_operation_lock);

  trx->m_dict_operation_lock_mode = 0;
}

void dict_lock_data_dictionary(trx_t *trx) {
  ut_a(trx->m_dict_operation_lock_mode == 0 || trx->m_dict_operation_lock_mode == RW_X_LATCH);

  /* Serialize data dictionary operations with dictionary mutex:
  no deadlocks or lock waits can occur then in these operations */

  rw_lock_x_lock(&dict_operation_lock);
  trx->m_dict_operation_lock_mode = RW_X_LATCH;

  mutex_enter(&(dict_sys->mutex));
}

void dict_unlock_data_dictionary(trx_t *trx) {
  ut_a(trx->m_dict_operation_lock_mode == RW_X_LATCH);

  /* Serialize data dictionary operations with dictionary mutex:
  no deadlocks can occur then in these operations */

  mutex_exit(&(dict_sys->mutex));
  rw_lock_x_unlock(&dict_operation_lock);

  trx->m_dict_operation_lock_mode = 0;
}

void dict_close() {
  /* Free the hash elements. We don't remove them from the table
  because we are going to destroy the table anyway. */
  for (ulint i = 0; i < hash_get_n_cells(dict_sys->table_hash); i++) {
    auto table = (dict_table_t *)HASH_GET_FIRST(dict_sys->table_hash, i);

    while (table) {
      dict_table_t *prev_table = table;

      table = (dict_table_t *)HASH_GET_NEXT(name_hash, prev_table);
      ut_ad(prev_table->magic_n == DICT_TABLE_MAGIC_N);
      /* Acquire only because it's a pre-condition. */
      mutex_enter(&dict_sys->mutex);

      dict_table_remove_from_cache(prev_table);

      mutex_exit(&dict_sys->mutex);
    }
  }

  hash_table_free(dict_sys->table_hash);

  /* The elements are the same instance as in dict_sys->table_hash,
  therefore we don't delete the individual elements. */
  hash_table_free(dict_sys->table_id_hash);

  /* Acquire only because it's a pre-condition. */
  mutex_enter(&dict_sys->mutex);

  dict_ind_free();

  mutex_exit(&dict_sys->mutex);

  mutex_free(&dict_sys->mutex);

  rw_lock_free(&dict_operation_lock);

  mutex_free(&dict_foreign_err_mutex);

  mem_free(dict_sys);
  dict_sys = nullptr;

  for (ulint i = 0; i < DICT_INDEX_STAT_MUTEX_SIZE; i++) {
    mutex_free(&dict_index_stat_mutex[i]);
  }
}
