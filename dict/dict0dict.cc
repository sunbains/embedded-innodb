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

/** @file dict/dict0dict.c
Data dictionary system

Created 1/8/1996 Heikki Tuuri
***********************************************************************/

#include "dict0dict.h"
#include "page0page.h"
#include "trx0undo.h"

#include <ctype.h>

/** The dictionary system */
Dict *srv_dict_sys{};

Dict::Dict(Btree *btree) noexcept : m_stats_mutex(), m_tables(), m_table_ids(), m_store(this, btree), m_loader(this), m_ddl{this} {
  mutex_create(&m_mutex, IF_DEBUG("dict_mutex", ) IF_SYNC_DEBUG(SYNC_DICT, ) Current_location());

  m_size = 0;

  UT_LIST_INIT(m_table_LRU);

  rw_lock_create(&m_lock, SYNC_DICT_OPERATION);

  mutex_create(&m_foreign_err_mutex, IF_DEBUG("dict_foreign_mutex", ) IF_SYNC_DEBUG(SYNC_ANY_LATCH, ) Current_location());
}

Dict::~Dict() noexcept {
  /* Free the hash elements. We don't remove them from the table
  because we are going to destroy the table anyway. */
  std::vector<Table *> tables;

  mutex_acquire();

  for (auto &it : m_tables) {
    tables.emplace_back(it.second);
  }

  for (auto table : tables) {
    table_remove_from_cache(table);
  }

  if (m_dummy_index != nullptr) {
    Index::destroy(m_dummy_index, Current_location());
  }

  mutex_release();

  mutex_free(&m_mutex);

  rw_lock_free(&m_lock);

  mutex_free(&m_foreign_err_mutex);
}

Dict *Dict::create(Btree *btree) noexcept {
  auto ptr = ut_new(sizeof(Dict));

  return new (ptr) Dict(btree);
}

void Dict::casedn_str(char *a) noexcept {
  ib_utf8_casedown(a);
}

bool Dict::tables_have_same_db(const char *name1, const char *name2) noexcept {
  for (; *name1 == *name2; name1++, name2++) {
    if (*name1 == '/') {
      return true;
    }
    ut_a(*name1); /* the names must contain '/' */
  }
  return false;
}

const char *Dict::remove_db_name(const char *name) noexcept {
  const char *s = strchr(name, '/');
  ut_a(s != nullptr);

  return s + 1;
}

ulint Dict::get_db_name_len(const char *name) noexcept {
  auto s = ::strchr(name, '/');
  ut_a(s != nullptr);

  return s - name;
}

void Dict::mutex_acquire() noexcept {
  mutex_enter(&m_mutex);
}

void Dict::mutex_release() noexcept {
  mutex_exit(&m_mutex);
}

void Dict::index_stat_mutex_enter(const Index *index) noexcept {
  ut_ad(index->m_cached);
  ut_ad(!index->m_to_be_dropped);
  ut_ad(index->m_magic_n == DICT_INDEX_MAGIC_N);

  m_stats_mutex.enter(index->m_id);
}

void Dict::index_stat_mutex_exit(const Index *index) noexcept {
  ut_ad(index != nullptr);
  ut_ad(index->m_magic_n == DICT_INDEX_MAGIC_N);
  ut_ad(index->m_cached);
  ut_ad(!index->m_to_be_dropped);

  m_stats_mutex.exit(index->m_id);
}

void Dict::table_decrement_handle_count(Table *table, bool dict_locked) noexcept {
  if (!dict_locked) {
    mutex_acquire();
  }

  ut_ad(mutex_own(&m_mutex));
  ut_a(table->m_n_handles_opened > 0);

  --table->m_n_handles_opened;

  if (!dict_locked) {
    mutex_release();
  }
}

void Dict::table_increment_handle_count(Table *table, bool dict_locked) noexcept {
  if (!dict_locked) {
    mutex_acquire();
  }

  ut_ad(mutex_own(&m_mutex));

  ++table->m_n_handles_opened;

  if (!dict_locked) {
    mutex_release();
  }
}

Index *Dict::index_find_on_id(Dict_id id) noexcept {
  ut_ad(mutex_own(&m_mutex));

  for (auto table : m_table_LRU) {
    for (auto index : table->m_indexes) {
      if (id == index->m_id) {
        return index;
      }
    }
  }

  return nullptr;
}

Index *Dict::index_get_if_in_cache(Dict_id index_id) noexcept {
  ut_ad(mutex_own(&m_mutex));

  return index_find_on_id(index_id);
}

ulint Index::get_nth_field_pos(ulint col_no) const noexcept {
  ut_ad(m_magic_n == DICT_INDEX_MAGIC_N);

  const auto col = m_table->get_nth_col(col_no);

  if (is_clustered()) {
    return get_clustered_field_pos(col);
  }

  const auto n_fields = get_n_fields();

  for (ulint i{}; i < n_fields; ++i) {
    const auto field = get_nth_field(i);

    if (col == field->m_col && field->m_prefix_len == 0) {

      return i;
    }
  }

  return ULINT_UNDEFINED;
}

bool Index::contains_col_or_prefix(ulint col_no) const noexcept {
  ut_ad(m_magic_n == DICT_INDEX_MAGIC_N);

  if (is_clustered()) {
    return true;
  }

  const auto col = m_table->get_nth_col(col_no);
  const auto n_fields = get_n_fields();

  for (ulint i{}; i < n_fields; ++i) {
    const auto field = get_nth_field(i);

    if (col == field->m_col) {

      return true;
    }
  }

  return false;
}

ulint Index::get_nth_field_pos(const Index *index, ulint col_no) const noexcept {
  ut_ad(m_magic_n == DICT_INDEX_MAGIC_N);

  const auto n_fields = get_n_fields();
  const auto check_field = index->get_nth_field(col_no);

  for (ulint i{}; i < n_fields; ++i) {
    const auto this_field = get_nth_field(i);

    if (this_field->m_col == check_field->m_col &&
        (this_field->m_prefix_len == 0 || (this_field->m_prefix_len >= check_field->m_prefix_len && check_field->m_prefix_len != 0)
        )) {

      return i;
    }
  }

  return ULINT_UNDEFINED;
}

Table *Dict::table_get_on_id(ib_recovery_t recovery, Dict_id table_id, Trx *trx) noexcept {
  if (table_id <= DICT_FIELDS_ID || trx->m_dict_operation_lock_mode == RW_X_LATCH) {
    /* It is a system table which will always exist in the table
    cache: we avoid acquiring the dictionary mutex, because
    if we are doing a rollback to handle an error in TABLE
    CREATE, for example, we already have the mutex! */

    ut_ad(mutex_own(&m_mutex) || trx->m_dict_operation_lock_mode == RW_X_LATCH);

    return table_get_on_id(recovery, table_id);
  }

  mutex_acquire();

  auto table = table_get_on_id(recovery, table_id);

  mutex_release();

  return table;
}

bool Dict::table_col_in_clustered_key(const Table *table, ulint n) noexcept {
  const auto col = table->get_nth_col(n);
  const auto index = table->get_first_index();
  const auto n_fields = index->get_n_unique();

  for (ulint pos{}; pos < n_fields; ++pos) {
    const auto field = index->get_nth_field(pos);

    if (col == field->m_col) {

      return true;
    }
  }

  return false;
}

Table *Dict::table_get(const char *table_name, bool ref_count) noexcept {
  mutex_acquire();

  auto table = table_get(table_name);

  if (ref_count && table != nullptr) {
    table_increment_handle_count(table, true);
  }

  mutex_release();

  if (table != nullptr && !table->m_stats.m_initialized) {
    /* If table->m_ibd_file_missing == true, this will
    print an error message and return without doing
    anything. */
    update_statistics(table);
  }

  return table;
}

Table *Dict::table_get_using_id(ib_recovery_t recovery, Dict_id table_id, bool ref_count) noexcept {
  ut_ad(mutex_own(&m_mutex));

  auto table = table_get_on_id(recovery, table_id);

  if (ref_count && table != nullptr) {
    table_increment_handle_count(table, true);
  }

  return table;
}

void Table::add_col(const char *name, ulint mtype, ulint prtype, ulint len) noexcept {
  ut_ad(name != nullptr);
  ut_ad(m_magic_n == DICT_TABLE_MAGIC_N);

  const auto i = m_n_fields++;
  auto col = get_nth_col(i);

  col->m_ind = uint(i);
  col->m_ord_part = 0;

  col->len = uint(len);
  col->mtype = uint(mtype);
  col->prtype = uint(prtype);
  col->m_name = mem_heap_strdup(m_heap, name);

  ulint mbminlen;
  ulint mbmaxlen;

  dtype_get_mblen(mtype, prtype, &mbminlen, &mbmaxlen);

  col->mbminlen = uint(mbminlen);
  col->mbmaxlen = uint(mbmaxlen);
}

void Table::add_system_columns() noexcept {
  ut_ad(!m_cached);
  ut_ad(m_n_fields == m_n_cols - DATA_N_SYS_COLS);
  ut_ad(m_magic_n == DICT_TABLE_MAGIC_N);

  /* This check reminds that if a new system column is added to
  the program, it should be dealt with here */
  static_assert(DATA_ROW_ID == 0, "error DATA_ROW_ID != 0");
  static_assert(DATA_TRX_ID == 1, "error DATA_TRX_ID != 1");
  static_assert(DATA_ROLL_PTR == 2, "error DATA_ROLL_PTR != 2");
  static_assert(DATA_N_SYS_COLS == 3, "error DATA_N_SYS_COLS != 3");

  /* NOTE: the system columns MUST be added in the following order
  (so that they can be indexed by the numerical value of DATA_ROW_ID,
  etc.) and as the last columns of the table memory object.
  The clustered index will not always physically contain all
  system columns. */

  add_col("DB_ROW_ID", DATA_SYS, DATA_ROW_ID | DATA_NOT_NULL, DATA_ROW_ID_LEN);
  add_col("DB_TRX_ID", DATA_SYS, DATA_TRX_ID | DATA_NOT_NULL, DATA_TRX_ID_LEN);
  add_col("DB_ROLL_PTR", DATA_SYS, DATA_ROLL_PTR | DATA_NOT_NULL, DATA_ROLL_PTR_LEN);
}

void Dict::table_add_to_cache(Table *table) noexcept {
  /* The lower limit for what we consider a "big" row */
  constexpr ulint BIG_ROW_SIZE = 1024;

  ut_ad(mutex_own(&m_mutex));

  table->add_system_columns();

  table->m_cached = true;

  ulint row_len{};

  for (ulint i{}; i < table->m_n_fields; ++i) {
    const auto col_len = table->get_nth_col(i)->get_max_size();

    row_len += col_len;

    /* If we have a single unbounded field, or several gigantic
    fields, mark the maximum row size as BIG_ROW_SIZE. */
    if (row_len >= BIG_ROW_SIZE || col_len >= BIG_ROW_SIZE) {
      row_len = BIG_ROW_SIZE;

      break;
    }
  }

  table->m_big_rows = row_len >= BIG_ROW_SIZE;

  /* Look for a table with the same name: error if such exists */
  if (const auto it = m_tables.find(table->m_name); it != m_tables.end()) {
    ut_a(it->second == nullptr);
  }

  /* Look for a table with the same id: error if such exists */
  if (const auto it = m_table_ids.find(table->m_id); it != m_table_ids.end()) {
    ut_a(it->second == nullptr);
  }

  /* Add table to hash table of tables */
  {
    auto res = m_tables.emplace(std::string_view{table->m_name}, table);
    ut_a(res.second);
  }

  /* Add table to hash table of tables based on table id */
  {
    auto res = m_table_ids.emplace(table->m_id, table);
    ut_a(res.second);
  }

  /* Add table to LRU list of tables */
  UT_LIST_ADD_FIRST(m_table_LRU, table);

  m_size += mem_heap_get_size(table->m_heap);
}

bool Dict::table_rename_in_cache(Table *table, const char *new_name, bool rename_also_foreigns) noexcept {
  ut_ad(mutex_own(&m_mutex));

  const auto old_size = mem_heap_get_size(table->m_heap);
  const auto old_name = table->m_name;

  /* Look for a table with the same name: error if such exists */
  {
    Table *table2{nullptr};

    if (const auto it = m_tables.find(new_name); it != m_tables.end()) {
      table2 = it->second;
    }

    if (likely_null(table2)) {
      log_err("Dictionary cache already contains a table ", new_name, " cannot rename table ", old_name);
      return false;
    }
  }

  /* If the table is stored in the system tablespace, rename the .ibd file */
  if (table->m_space_id != DICT_HDR_SPACE) {
    auto fil = m_store.m_fsp->m_fil;

    if (table->m_dir_path_of_temp_table != nullptr) {
      log_err("Trying to rename a TEMPORARY TABLE ", old_name, " (", table->m_dir_path_of_temp_table, ")");
      return false;
    } else if (!fil->rename_tablespace(old_name, table->m_space_id, new_name)) {
      return false;
    }
  }

  /* Remove table from the hash tables of tables */
  m_tables.erase(table->m_name);
  table->m_name = mem_heap_strdup(table->m_heap, new_name);

  /* Add table to hash table of tables */
  m_tables.emplace(table->m_name, table);
  m_size += mem_heap_get_size(table->m_heap) - old_size;

  if (!rename_also_foreigns) {
    /* In ALTER TABLE we think of the rename table operation
    in the direction table -> temporary table (#sql...)
    as dropping the table with the old name and creating
    a new with the new name. Thus we kind of drop the
    constraints from the dictionary cache here. The foreign key
    constraints will be inherited to the new table from the
    system tables through a call of dict_load_foreigns. */

    /* Remove the foreign constraints from the cache */
    for (auto foreign : table->m_foreign_list) {
      foreign_remove_from_cache(foreign);
    }

    /* Reset table field in referencing constraints */

    for (auto foreign : table->m_referenced_list) {
      foreign->m_referenced_table = nullptr;
      foreign->m_referenced_index = nullptr;
    }

    /* Make the list of referencing constraints empty */

    UT_LIST_INIT(table->m_referenced_list);

    return true;
  }

  /* Update the table name fields in foreign constraints, and update also
  the constraint id of new format >= 4.0.18 constraints. Note that at
  this point we have already changed table->m_name to the new name. */

  for (auto foreign : table->m_foreign_list) {

    if (strlen(foreign->m_foreign_table_name) < strlen(table->m_name)) {
      /* Allocate a longer name buffer;
      TODO: store buf len to save memory */

      foreign->m_foreign_table_name = reinterpret_cast<char *>(mem_heap_alloc(foreign->m_heap, strlen(table->m_name) + 1));
    }

    strcpy(foreign->m_foreign_table_name, table->m_name);

    if (strchr(foreign->m_id, '/')) {
      /* This is a >= 4.0.18 format id */

      auto old_id = mem_strdup(foreign->m_id);

      if (strlen(foreign->m_id) > strlen(old_name) + (sizeof(dict_ibfk) - 1) &&
          memcmp(foreign->m_id, old_name, strlen(old_name) == 0) &&
          !memcmp(foreign->m_id + strlen(old_name), dict_ibfk, (sizeof dict_ibfk) - 1)) {

        /* This is a generated >= 4.0.18 format id */

        if (strlen(table->m_name) > strlen(old_name)) {
          foreign->m_id = reinterpret_cast<char *>(mem_heap_alloc(foreign->m_heap, strlen(table->m_name) + strlen(old_id) + 1));
        }

        /* Replace the prefix 'databasename/tablename' with the new names */
        strcpy(foreign->m_id, table->m_name);
        strcat(foreign->m_id, old_id + strlen(old_name));

      } else {
        /* This is a >= 4.0.18 format id where the user gave the id name */
        const auto db_len = get_db_name_len(table->m_name) + 1;

        if (get_db_name_len(table->m_name) > get_db_name_len(foreign->m_id)) {

          foreign->m_id = reinterpret_cast<char *>(mem_heap_alloc(foreign->m_heap, db_len + strlen(old_id) + 1));
        }

        /* Replace the database prefix in id with the one from table->m_name */

        memcpy(foreign->m_id, table->m_name, db_len);

        strcpy(foreign->m_id + db_len, remove_db_name(old_id));
      }

      mem_free(old_id);
    }
  }

  for (auto foreign : table->m_referenced_list) {
    if (strlen(foreign->m_referenced_table_name) < strlen(table->m_name)) {
      /* Allocate a longer name buffer; TODO: store buf len to save memory */

      foreign->m_referenced_table_name = reinterpret_cast<char *>(mem_heap_alloc(foreign->m_heap, strlen(table->m_name) + 1));
    }

    strcpy(foreign->m_referenced_table_name, table->m_name);
  }

  return true;
}

void Dict::table_change_id_in_cache(Table *table, uint64_t new_id) noexcept {
  ut_ad(mutex_own(&m_mutex));
  ut_ad(table->m_magic_n == DICT_TABLE_MAGIC_N);

  /* Remove the table from the hash table of id's */
  m_table_ids.erase(table->m_id);
  table->m_id = new_id;

  /* Add the table back to the hash table */
  auto ret = m_table_ids.emplace(table->m_id, table);
  ut_ad(ret.second);
}

void Dict::table_remove_from_cache(Table *table) noexcept {
  ut_ad(mutex_own(&m_mutex));
  ut_ad(table->m_magic_n == DICT_TABLE_MAGIC_N);

  /* Remove the foreign constraints from the cache */
  for (auto foreign : table->m_foreign_list) {
    foreign_remove_from_cache(foreign);
  }

  /* Reset table field in referencing constraints */

  for (auto foreign : table->m_referenced_list) {
    foreign->m_referenced_table = nullptr;
    foreign->m_referenced_index = nullptr;
  }

  /* Remove the indexes from the cache */
  for (auto index : table->m_indexes) {
    index_remove_from_cache(table, index);
  }

  /* Remove table from the hash tables of tables */
  m_tables.erase(table->m_name);
  m_table_ids.erase(table->m_id);

  /* Remove table from LRU list of tables */
  UT_LIST_REMOVE(m_table_LRU, table);

  auto size = mem_heap_get_size(table->m_heap);

  ut_ad(m_size >= size);

  m_size -= size;

  Table::destroy(table, Current_location());
}

bool Dict::col_name_is_reserved(const char *name) noexcept {
  /* This check reminds that if a new system column is added to
  the program, it should be dealt with here. */
  static_assert(DATA_N_SYS_COLS == 3, "error DATA_N_SYS_COLS != 3");

  static const char *reserved_names[] = {"DB_ROW_ID", "DB_TRX_ID", "DB_ROLL_PTR"};

  for (ulint i{}; i < std::size(reserved_names); ++i) {
    if (strcasecmp(name, reserved_names[i]) == 0) {

      return true;
    }
  }

  return false;
}

bool Dict::index_too_big_for_undo(const Table *table, const Index *new_index) noexcept {
  /* Make sure that all column prefixes will fit in the undo log record
  in trx_undo_page_report_modify() right after trx_undo_page_init(). */

  const Index *clust_index = table->get_first_index();

  ulint undo_page_len = TRX_UNDO_PAGE_HDR - TRX_UNDO_PAGE_HDR_SIZE + 2 /* next record pointer */
                        + 1                                            /* type_cmpl */
                        + 11 /* trx->undo_no */ + 11                   /* table->m_id */
                        + 1                                            /* rec_get_info_bits() */
                        + 11                                           /* DB_TRX_ID */
                        + 11                                           /* DB_ROLL_PTR */
                        + 10 + FIL_PAGE_DATA_END                       /* trx_undo_left() */
                        + 2 /* pointer to previous undo log record */;

  if (unlikely(clust_index == nullptr)) {
    ut_a(new_index->is_clustered());
    clust_index = new_index;
  }

  /* Add the size of the ordering columns in the clustered index. */
  for (ulint i{}; i < clust_index->m_n_uniq; ++i) {
    const auto col = clust_index->get_nth_col(i);

    /* Use the maximum output size of
    mach_write_compressed(), although the encoded
    length should always fit in 2 bytes. */
    undo_page_len += 5 + col->get_max_size();
  }

  /* Add the old values of the columns to be updated.
  First, the amount and the numbers of the columns.
  These are written by mach_write_compressed() whose
  maximum output length is 5 bytes.  However, given that
  the quantities are below REC_MAX_N_FIELDS (10 bits),
  the maximum length is 2 bytes per item. */
  undo_page_len += 2 * (table->get_n_cols() + 1);

  auto is_ord_part = [](ulint &max_size) noexcept {
    /* This is an ordering column in some index.
    A long enough prefix must be written to the
    undo log.  See trx_undo_page_fetch_ext(). */

    if (max_size > REC_MAX_INDEX_COL_LEN) {
      max_size = REC_MAX_INDEX_COL_LEN;
    }

    max_size += BTR_EXTERN_FIELD_REF_SIZE;
  };

  for (ulint i{}; i < clust_index->m_n_defined; ++i) {
    const auto col = clust_index->get_nth_col(i);
    auto max_size = col->get_max_size();
    const auto fixed_size = col->get_fixed_size();

    if (fixed_size > 0) {
      /* Fixed-size columns are stored locally. */
      max_size = fixed_size;
    } else if (max_size <= BTR_EXTERN_FIELD_REF_SIZE * 2) {
      /* Short columns are stored locally. */
    } else if (!col->m_ord_part) {
      bool is_ord_part_col{};

      /* See if col->m_ord_part would be set because of new_index. */
      for (ulint j{}; j < new_index->m_n_uniq; ++j) {

        if (new_index->get_nth_col(j) == col) {
          is_ord_part_col = true;
          break;
        }
      }

      /* This is not an ordering column in any index.
      Thus, it can be stored completely externally. */
      if (!is_ord_part_col) {
        max_size = BTR_EXTERN_FIELD_REF_SIZE;
      } else {
        is_ord_part(max_size);
      }
    } else {
      is_ord_part(max_size);
    }

    undo_page_len += 5 + max_size;
  }

  return undo_page_len >= UNIV_PAGE_SIZE;
}

bool Dict::index_too_big_for_tree(const Table *table, const Index *new_index) noexcept {
  /* The maximum allowed record size is half a B-tree
  page.  No additional sparse page directory entry will
  be generated for the first few user records. */

  /* Maximum allowed size of a record on a leaf page */
  auto page_rec_max = page_get_free_space_of_empty() / 2;

  /* Maximum allowed size of a node pointer record */
  auto page_ptr_max = page_rec_max;

  /* Maximum possible storage size of a record */
  auto rec_max_size = REC_N_EXTRA_BYTES;

  /* For each column, include a 2-byte offset and a
  "null" flag.  The 1-byte format is only used in short
  records that do not contain externally stored columns.
  Such records could never exceed the page limit, even
  when using the 2-byte format. */
  rec_max_size += 2 * new_index->m_n_fields;

  /* Compute the maximum possible record size. */
  for (ulint i{}; i < new_index->m_n_fields; ++i) {
    const auto field = new_index->get_nth_field(i);
    const auto col = field->get_col();

    /* In dtuple_convert_big_rec(), variable-length columns
    that are longer than BTR_EXTERN_FIELD_REF_SIZE * 2
    may be chosen for external storage.

    Fixed-length columns, and all columns of secondary
    index records are always stored inline. */

    /* Determine the maximum length of the index field.
    The field_ext_max_size should be computed as the worst
    case in rec_get_converted_size_comp() for
    REC_STATUS_ORDINARY records. */

    auto field_max_size = col->get_fixed_size();

    if (field_max_size > 0) {
      /* index_add_col() should guarantee this */
      ut_ad(!field->m_prefix_len || field->m_fixed_len == field->m_prefix_len);
      goto add_field_size;
    }

    field_max_size = col->get_max_size();

    if (field->m_prefix_len > 0) {
      if (field->m_prefix_len < field_max_size) {
        field_max_size = field->m_prefix_len;
      }
    } else if (field_max_size > BTR_EXTERN_FIELD_REF_SIZE * 2 && new_index->is_clustered()) {

      /* In the worst case, we have a locally stored
      column of BTR_EXTERN_FIELD_REF_SIZE * 2 bytes.
      The length can be stored in one byte.  If the
      column were stored externally, the lengths in
      the clustered index page would be
      BTR_EXTERN_FIELD_REF_SIZE and 2. */
      field_max_size = BTR_EXTERN_FIELD_REF_SIZE * 2;
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
    if (i + 1 == new_index->get_n_unique_in_tree() && rec_max_size + REC_NODE_PTR_SIZE >= page_ptr_max) {

      return true;
    }
  }

  return false;
}

db_err Dict::index_add_to_cache(Index *index, ulint page_no, bool strict) noexcept {
  ut_ad(mutex_own(&m_mutex));
  ut_ad(index->m_n_defined == index->m_n_fields);
  ut_ad(index->m_magic_n == DICT_INDEX_MAGIC_N);

  ut_ad(mem_heap_validate(index->m_heap));
  ut_a(!index->is_clustered() || index->m_table->m_indexes.empty());

  auto table = index->m_table;

  if (!table->link_cols(index)) {
    Index::destroy(index, Current_location());
    return DB_CORRUPTION;
  }

  auto clean_up_on_error = [](Index *index, Index *new_index) noexcept {
    Index::destroy(index, Current_location());
    Index::destroy(new_index, Current_location());
  };

  /* Build the cache internal representation of the index, containing also the added system fields */

  Index *new_index;

  ut_a(index->m_table == table);

  if (index->is_clustered()) {
    new_index = build_cluster_index(index);
  } else {
    new_index = build_secondary_index(index);
  }

  /* Set the n_fields value in new_index to the actual defined
  number of fields in the cache internal representation */

  new_index->m_n_fields = new_index->m_n_defined;

  if (strict && index_too_big_for_tree(table, new_index)) {
    clean_up_on_error(index, new_index);
    return DB_TOO_BIG_RECORD;
  }

  auto n_ord = new_index->m_n_uniq;

  static_assert(DICT_TF_FORMAT_V1 == DICT_TF_FORMAT_MAX, "error DICT_TF_FORMAT_V1 != DICT_TF_FORMAT_MAX");

  if (table->get_format() == DICT_TF_FORMAT_V1) {
    /* Store prefixes of externally stored columns locally within
    the record. There are no special considerations for the undo
    log record size. */
  } else {
    for (ulint i{}; i < n_ord; ++i) {
      const auto field = new_index->get_nth_field(i);
      const Column *col = field->get_col();

      /* In dtuple_convert_big_rec(), variable-length columns
      that are longer than BTR_EXTERN_FIELD_REF_SIZE * 2
      may be chosen for external storage.  If the column appears
      in an ordering column of an index, a longer prefix of
      REC_MAX_INDEX_COL_LEN will be copied to the undo log
      by trx_undo_page_report_modify() and
      trx_undo_page_fetch_ext().  It suffices to check the
      capacity of the undo log whenever new_index includes
      a co  lumn prefix on a column that may be stored externally. */

      if (field->m_prefix_len > 0       /* Prefix index */
          && col->m_ord_part == 0       /* Not yet ordering column */
          && col->get_fixed_size() == 0 /* Variable-length */
          && col->get_max_size() > BTR_EXTERN_FIELD_REF_SIZE * 2 /* Long enough */) {

        if (index_too_big_for_undo(table, new_index)) {
          /* An undo log record might not fit in a single page.  Refuse to create this index. */

          clean_up_on_error(index, new_index);
          return DB_TOO_BIG_RECORD;
        }
      }
    }
  }

  /* Flag the ordering columns */
  for (ulint i{}; i < n_ord; ++i) {
    new_index->get_nth_field(i)->m_col->m_ord_part = 1;
  }

  /* Add the new index as the last index for the table */

  table->m_indexes.push_back(new_index);
  new_index->m_table = table;

  new_index->m_stats.m_index_size = 1;
  new_index->m_stats.m_n_leaf_pages = 1;

  new_index->m_page_id.m_page_no = page_no;

  rw_lock_create(&new_index->m_lock, SYNC_INDEX_TREE);

  {
    const auto sz = (1 + new_index->get_n_unique()) * sizeof(int64_t);
    new_index->m_stats.m_n_diff_key_vals = reinterpret_cast<int64_t *>(mem_heap_alloc(new_index->m_heap, sz));
  }

  /* Give some sensible values to stat_n_... in case we do
  not calculate statistics quickly enough */

  for (ulint i{}; i <= new_index->get_n_unique(); ++i) {

    new_index->m_stats.m_n_diff_key_vals[i] = 100;
  }

  m_size += mem_heap_get_size(new_index->m_heap);

  Index::destroy(index, Current_location());

  return DB_SUCCESS;
}

void Dict::index_remove_from_cache(Table *table, Index *index) noexcept {
  ut_ad(table != nullptr);
  ut_ad(index != nullptr);
  ut_ad(table->m_magic_n == DICT_TABLE_MAGIC_N);
  ut_ad(index->m_magic_n == DICT_INDEX_MAGIC_N);
  ut_ad(mutex_own(&m_mutex));

  rw_lock_free(&index->m_lock);

  /* Remove the index from the list of indexes of the table */
  table->m_indexes.remove(index);

  const auto size = mem_heap_get_size(index->m_heap);

  ut_ad(m_size >= size);

  m_size -= size;

  Index::destroy(index, Current_location());
}

bool Table::link_cols(Index *index) noexcept {
  ulint n_found{};
  auto begin = &m_cols[0];
  auto end = &m_cols[get_n_cols() - 1];

  for (ulint i{}; i < index->get_n_fields(); ++i) {
    auto field = index->get_nth_field(i);

    auto col = std::find_if(begin, end, [field](const auto col) -> auto { return strcmp(col.m_name, field->m_name) == 0; });

    if (col == end) {
      log_err("Column ", field->m_name, " in index ", index->m_name, " not found in table ", m_name);
    } else {
      ++n_found;
      field->m_col = const_cast<Column *>(col);
    }
  }

  return n_found == index->get_n_fields();
}

DTuple *Index::build_node_ptr(const rec_t *rec, ulint page_no, mem_heap_t *heap, ulint level) const noexcept {
  const auto n_unique = get_n_unique_in_tree();
  auto tuple = dtuple_create(heap, n_unique + 1);

  /* When searching in the tree for the node pointer, we must not do
  comparison on the last field, the page number field, as on upper
  levels in the tree there may be identical node pointers with a
  different page number; therefore, we set the n_fields_cmp to one
  less: */

  dtuple_set_n_fields_cmp(tuple, n_unique);

  copy_types(tuple, n_unique);

  auto buf = reinterpret_cast<byte *>(mem_heap_alloc(heap, 4));

  mach_write_to_4(buf, page_no);

  auto field = dtuple_get_nth_field(tuple, n_unique);

  dfield_set_data(field, buf, 4);

  dtype_set(dfield_get_type(field), DATA_SYS_CHILD, DATA_NOT_NULL, 4);

  rec_copy_prefix_to_dtuple(tuple, rec, this, n_unique, heap);

  dtuple_set_info_bits(tuple, dtuple_get_info_bits(tuple) | REC_STATUS_NODE_PTR);

  ut_ad(dtuple_check_typed(tuple));

  return tuple;
}

void Index::add_col(const Table *table, Column *col, ulint prefix_len) noexcept {
  const auto col_name = table->get_col_name(col->get_no());

  auto field = add_field(col_name, prefix_len);

  field->m_col = col;
  field->m_fixed_len = col->get_fixed_size();

  if (prefix_len > 0 && field->m_fixed_len > prefix_len) {
    field->m_fixed_len = prefix_len;
  }

  /* Long fixed-length fields that need external storage are treated as
  variable-length fields, so that the extern flag can be embedded in
  the length word. */

  if (field->m_fixed_len > DICT_MAX_INDEX_COL_LEN) {
    field->m_fixed_len = 0;
  }

  /* The comparison limit above must be constant.  If it were
  changed, the disk format of some fixed-length columns would
  change, which would be a disaster. */
  static_assert(DICT_MAX_INDEX_COL_LEN == 768, "error DICT_MAX_INDEX_COL_LEN != 768");

  if (!(col->prtype & DATA_NOT_NULL)) {
    ++m_n_nullable;
  }
}

rec_t *Index::copy_rec_order_prefix(const rec_t *rec, ulint *n_fields, byte *&buf, ulint &buf_size) const noexcept {
  prefetch_r(rec);

  auto n = get_n_unique_in_tree();

  *n_fields = n;

  return rec_copy_prefix_to_buf(rec, this, n, buf, buf_size);
}

DTuple *Index::build_data_tuple(rec_t *rec, ulint n_fields, mem_heap_t *heap) const noexcept {
  ut_ad(n_fields <= rec_get_n_fields(rec));

  auto tuple = dtuple_create(heap, n_fields);

  copy_types(tuple, n_fields);

  rec_copy_prefix_to_dtuple(tuple, rec, this, n_fields, heap);

  ut_ad(dtuple_check_typed(tuple));

  return tuple;
}

void Index::copy_from(Index *index) noexcept {
  m_id = index->m_id;
  m_n_user_defined_cols = index->m_n_fields;

  const auto n_fields = index->m_n_fields;

  for (ulint i{}; i < n_fields; ++i) {
    const auto field = index->get_nth_field(i);

    add_col(m_table, field->m_col, field->m_prefix_len);
  }
}

void Index::copy_types(DTuple *tuple, ulint n_fields) const noexcept {
  for (ulint i{}; i < n_fields; ++i) {
    const auto field = get_nth_field(i);
    auto dfield_type = dfield_get_type(dtuple_get_nth_field(tuple, i));

    field->m_col->copy_type(dfield_type);
  }
}

void Table::copy_types(DTuple *tuple) const noexcept {
  for (ulint i{}; i < dtuple_get_n_fields(tuple); ++i) {
    const auto dfield = dtuple_get_nth_field(tuple, i);
    const auto dtype = dfield_get_type(dfield);

    dfield_set_null(dfield);
    get_nth_col(i)->copy_type(dtype);
  }
}

Index::Index(mem_heap_t *heap, Table *table, const char *index_name, Page_id page_id, ulint type, ulint n_fields) noexcept {
  m_heap = heap;
  m_type = type;
  m_table = table;
  m_page_id = page_id;
  m_name = mem_heap_strdup(m_heap, index_name);
  ut_a(m_name != nullptr);
  m_n_fields = static_cast<unsigned int>(n_fields);

  /* The '1 +' prevents allocation of an empty mem block */
  m_fields = reinterpret_cast<Field *>(mem_heap_zalloc(m_heap, 1 + n_fields * sizeof(Field)));

  for (ulint i{}; i < n_fields; ++i) {
    new (&m_fields[i]) Field();
  }
  ut_a(m_fields != nullptr);

  ut_d(m_magic_n = DICT_INDEX_MAGIC_N);
}

Index *Index::create(Table *table, const char *index_name, Page_id page_id, ulint type, ulint n_fields) noexcept {
  ut_ad(table->m_name != nullptr);
  ut_ad(index_name != nullptr);

  auto heap = mem_heap_create(DICT_HEAP_SIZE);

  if (heap == nullptr) {
    return nullptr;
  }

  auto ptr = mem_heap_zalloc(heap, sizeof(Index));

  if (ptr == nullptr) {
    mem_heap_free(heap);
    return nullptr;
  }

  auto index = new (ptr) Index(heap, table, index_name, page_id, type, n_fields);

  return index;
}

Index *Index::create(const char *table_name, const char *index_name, Page_id page_id, ulint type, ulint n_fields) noexcept {
  ut_ad(table_name != nullptr);
  ut_ad(index_name != nullptr);

  auto heap = mem_heap_create(DICT_HEAP_SIZE);

  if (heap == nullptr) {
    return nullptr;
  }

  auto ptr = mem_heap_zalloc(heap, sizeof(Index));

  if (ptr == nullptr) {
    mem_heap_free(heap);
    return nullptr;
  }

  auto table = Table::create(table_name, NULL_SPACE_ID, 0, 0, true, Current_location());

  if (table == nullptr) {
    mem_heap_free(heap);
    return nullptr;
  }

  auto index = new (ptr) Index(heap, table, index_name, page_id, type, n_fields);

  return index;
}

ulint Index::calc_min_rec_len() const noexcept {
  ulint sum{};

  for (ulint i{}; i < get_n_fields(); ++i) {
    sum += get_nth_col(i)->get_fixed_size();
  }

  if (sum > 127) {
    sum += 2 * get_n_fields();
  } else {
    sum += get_n_fields();
  }

  sum += REC_N_EXTRA_BYTES;

  return sum;
}

Index *Dict::build_cluster_index(Index *index) noexcept {
  ut_ad(mutex_own(&m_mutex));
  ut_ad(index->is_clustered());
  ut_ad(index->m_table->m_magic_n == DICT_TABLE_MAGIC_N);

  /* Create a new index object with certainly enough fields */
  Index *new_index;
  auto table = index->m_table;

  {
    const auto n_fields = index->m_n_fields + table->m_n_cols;
    const auto page_id = Page_id(table->m_space_id, NULL_PAGE_NO);

    new_index = Index::create(table, index->m_name, page_id, index->m_type, n_fields);

    if (new_index == nullptr) {
      return nullptr;
    }
  }

  new_index->copy_from(index);

  if (index->is_unique()) {
    /* Only the fields defined so far are needed to identify the index entry uniquely */
    new_index->m_n_uniq = new_index->m_n_defined;
  } else {
    /* We have to add the hidden DB_ROW_ID to the index to make it unique */
    new_index->m_n_uniq = new_index->m_n_defined + 1;
  }

  /* Add system columns, trx id first */

  auto trx_id_pos = new_index->m_n_defined;

  static_assert(DATA_ROW_ID == 0, "error DATA_ROW_ID != 0");
  static_assert(DATA_TRX_ID == 1, "error DATA_TRX_ID != 1");
  static_assert(DATA_ROLL_PTR == 2, "error DATA_ROLL_PTR != 2");

  if (!index->is_unique()) {
    new_index->add_col(table, const_cast<Column *>(table->get_sys_col(DATA_ROW_ID)), 0);
    ++trx_id_pos;
  }

  new_index->add_col(table, const_cast<Column *>(table->get_sys_col(DATA_TRX_ID)), 0);
  new_index->add_col(table, const_cast<Column *>(table->get_sys_col(DATA_ROLL_PTR)), 0);

  new_index->m_trx_id_offset = 0;

  for (ulint i{}; i < trx_id_pos; ++i) {

    const auto fixed_size = new_index->get_nth_col(i)->get_fixed_size();

    if (fixed_size == 0) {
      new_index->m_trx_id_offset = 0;
      break;
    }

    if (new_index->get_nth_field(i)->m_prefix_len > 0) {
      new_index->m_trx_id_offset = 0;
      break;
    }

    ut_a(new_index->m_trx_id_offset + uint(fixed_size) < 1024 - DATA_TRX_ID_LEN);

    new_index->m_trx_id_offset += uint(fixed_size);
  }

  std::vector<bool> indexed(table->m_n_cols);

  /* Mark the table columns already contained in new_index */
  for (ulint i{}; i < new_index->m_n_defined; ++i) {

    auto field = new_index->get_nth_field(i);

    /* If there is only a prefix of the column in the index
    field, do not mark the column as contained in the index */

    if (field->m_prefix_len == 0) {

      indexed[field->m_col->m_ind] = true;
    }
  }

  /* Add to new_index non-system columns of table not yet included there */
  for (ulint i{}; i + DATA_N_SYS_COLS < ulint(table->m_n_cols); ++i) {

    auto col = const_cast<Column *>(table->get_nth_col(i));
    ut_ad(col->mtype != DATA_SYS);

    if (!indexed[col->m_ind]) {
      new_index->add_col(table, col, 0);
    }
  }

  ut_ad(table->m_indexes.empty());

  new_index->m_cached = true;

  return new_index;
}

Index *Dict::build_secondary_index(Index *index) noexcept {
  ut_ad(mutex_own(&m_mutex));
  ut_ad(!index->is_clustered());
  ut_ad(index->m_table->m_magic_n == DICT_TABLE_MAGIC_N);

  Index *new_index;
  auto table = index->m_table;

  /* The clustered index should be the first in the list of indexes */
  auto clust_index = table->get_first_index();
  ut_ad(clust_index->is_clustered());

  {
    const auto n_fields = index->m_n_fields + 1 + clust_index->m_n_uniq;

    new_index = Index::create(table, index->m_name, index->m_page_id, index->m_type, n_fields);

    if (new_index == nullptr) {
      return nullptr;
    }
  }

  new_index->copy_from(index);

  /* Remember the table columns already contained in new_index */
  std::vector<bool> indexed(table->m_n_cols);

  /* Mark the table columns already contained in new_index */
  for (ulint i{}; i < new_index->m_n_defined; ++i) {

    auto field = new_index->get_nth_field(i);

    /* If there is only a prefix of the column in the index
    field, do not mark the column as contained in the index */

    if (field->m_prefix_len == 0) {

      indexed[field->m_col->m_ind] = true;
    }
  }

  /* Add to new_index the columns necessary to determine the clustered index entry uniquely */

  for (ulint i{}; i < clust_index->m_n_uniq; ++i) {

    auto field = clust_index->get_nth_field(i);

    if (!indexed[field->m_col->m_ind]) {
      new_index->add_col(table, field->m_col, field->m_prefix_len);
    }
  }

  if (index->is_unique()) {
    new_index->m_n_uniq = index->m_n_fields;
  } else {
    new_index->m_n_uniq = new_index->m_n_defined;
  }

  /* Set the n_fields value in new_index to the actual defined number of fields */

  new_index->m_n_defined = new_index->m_n_defined;

  new_index->m_cached = true;

  return new_index;
}

Table::Table(mem_heap_t *heap, const char *name, space_id_t space_id, ulint n_cols, ulint flags, bool ibd_file_missing) noexcept {
  m_heap = heap;
  m_space_id = space_id;
  m_flags = uint(flags);
  m_name = mem_heap_strdup(m_heap, name);
  m_n_cols = uint(n_cols + DATA_N_SYS_COLS);
  m_ibd_file_missing = uint(ibd_file_missing);

  UT_LIST_INIT(m_locks);
  UT_LIST_INIT(m_indexes);
  UT_LIST_INIT(m_foreign_list);
  UT_LIST_INIT(m_referenced_list);
  UT_LIST_INIT(m_table_LRU);

  m_cols = reinterpret_cast<Column *>(mem_heap_alloc(m_heap, (n_cols + DATA_N_SYS_COLS) * sizeof(Column)));

  ut_d(m_magic_n = DICT_TABLE_MAGIC_N);
}

Table::~Table() noexcept {
  ut_d(m_cached = false);
  ut_ad(m_magic_n == DICT_TABLE_MAGIC_N);
}

Table *Table::create(
  const char *name, space_id_t space, ulint n_cols, ulint flags, bool ibd_file_missing, Source_location loc
) noexcept {
  ut_a(!(flags & (~0UL << DICT_TF2_BITS)));

  auto heap = mem_heap_create_func(DICT_HEAP_SIZE, MEM_HEAP_DYNAMIC, std::move(loc));
  auto ptr = mem_heap_zalloc(heap, sizeof(Table));

  new (ptr) Table(heap, name, space, n_cols, flags, ibd_file_missing);

  return reinterpret_cast<Table *>(ptr);
}

void Table::destroy(Table *&table, Source_location loc) noexcept {
  auto heap = table->m_heap;
  call_destructor(table);
  mem_heap_free_func(heap, loc);
  table = nullptr;
}

Foreign *Foreign::create() noexcept {
  auto heap = mem_heap_create(100);
  auto ptr = mem_heap_zalloc(heap, sizeof(Foreign));

  return new (ptr) Foreign(heap);
}

void Foreign::destroy(Foreign *&foreign) noexcept {
  auto heap = foreign->m_heap;
  call_destructor(foreign);
  mem_heap_free(heap);
  heap = nullptr;
}

void Dict::index_name_print(Trx *trx, const Index *index) noexcept {
  std::ostringstream os{};

  os << "index " << index->m_name << " of table " << index->m_table->m_name;

  log_err(os.str());
}

Index *Dict::create_dummy_index() noexcept {
  Index *index{};

  /* Create dummy table and index for REDUNDANT infimum and supremum */
  auto table = Table::create("SYS_DUMMY", DICT_HDR_SPACE, 1, 0, false, Current_location());

  table->add_col("DUMMY", DATA_CHAR, DATA_ENGLISH | DATA_NOT_NULL, 8);

  index = Index::create(table, "SYS_DUMMY", DICT_HDR_SPACE, 0, 1);
  index->add_col(table, table->get_nth_col(0), 0);
  index->m_table = table;

  /* Avoid ut_ad(Index::m_cached) in Index::get_n_unique_in_tree */
  index->m_cached = true;

  return index;
}

void Dict::destroy_dummy_index(Index *&index) noexcept {
  auto table = index->m_table;

  Index::destroy(index, Current_location());
  Table::destroy(table, Current_location());

  index = nullptr;
}

void Dict::freeze_data_dictionary(Trx *trx) noexcept {
  ut_a(trx->m_dict_operation_lock_mode == 0);

  rw_lock_s_lock(&m_lock);

  trx->m_dict_operation_lock_mode = RW_S_LATCH;
}

void Dict::unfreeze_data_dictionary(Trx *trx) noexcept {
  ut_a(trx->m_dict_operation_lock_mode == RW_S_LATCH);

  rw_lock_s_unlock(&m_lock);

  trx->m_dict_operation_lock_mode = 0;
}

void Dict::lock_data_dictionary(Trx *trx) noexcept {
  ut_a(trx->m_dict_operation_lock_mode == 0 || trx->m_dict_operation_lock_mode == RW_X_LATCH);

  /* Serialize data dictionary operations with dictionary mutex:
  no deadlocks or lock waits can occur then in these operations */

  rw_lock_x_lock(&m_lock);

  trx->m_dict_operation_lock_mode = RW_X_LATCH;

  mutex_acquire();
}

void Dict::unlock_data_dictionary(Trx *trx) noexcept {
  ut_a(trx->m_dict_operation_lock_mode == RW_X_LATCH);

  /* Serialize data dictionary operations with dictionary mutex:
  no deadlocks can occur then in these operations */

  mutex_release();

  rw_lock_x_unlock(&m_lock);

  trx->m_dict_operation_lock_mode = 0;
}

void Dict::destroy(Dict *&dict) noexcept {
  call_destructor(dict);
  dict = nullptr;
}

std::string Dict::to_string() noexcept {
  std::ostringstream os{};

  os << "Dict{" << " TBD " << "}";

  return os.str();
}
