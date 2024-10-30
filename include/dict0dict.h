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

/** @file include/dict0dict.h
Data dictionary system

Created 1/8/1996 Heikki Tuuri
*******************************************************/

#pragma once

#include "data0data.h"
#include "ddl0ddl.h"
#include "dict0load.h"
#include "dict0store.h"
#include "mem0mem.h"
#include "rem0types.h"
#include "srv0srv.h"
#include "sync0rw.h"
#include "sync0sync.h"
#include "trx0types.h"
#include "ut0byte.h"
#include "ut0lst.h"
#include "ut0mem.h"
#include "ut0rnd.h"

#include <string_view>
#include <unordered_map>

struct FSP;
struct mtr_t;
struct Table;
struct Index;
struct Buf_pool;
struct Btree;

/** Data dictionary system. */
struct Dict {
  /** Responsible for data dictionary persistence. */
  struct Store;

  /**
   * Constructor.
   * 
   * @param[in] btree B-tree system.
   */
  explicit Dict(Btree *btree) noexcept;

  /** Destructor. */
  ~Dict() noexcept;

  /**
   * Creates a data dictionary.
   * 
   * @param[in] fsp File space manager.
   * @param[in] btree B-tree system.
   * 
   * @return Data dictionary.
   */
  [[nodiscard]] static Dict *create(Btree *btree) noexcept;

  /**
   * Closes the data dictionary module.
   * 
   * @param[in,out] dict Data dictionary.
   */
  static void destroy(Dict *&dict) noexcept;

  /**
   * Makes all characters in a NUL-terminated UTF-8 string lower case.
   * 
   * @param[in,out] a string to put in lower case 
   */
  static void casedn_str(char *a) noexcept;

  /**
   * Creates an instance of the data dictionary.
   * 
   * @return DB_SUCCESS or error code.
   */
  db_err create_instance() noexcept {
    return m_store.create_instance();
  }

  /**
   * Get the database name length in a table name.
   * 
   * @param[in] name table name in the form dbname '/' tablename
   * 
   * @return database name length
   */
  [[nodiscard]] static ulint get_db_name_len(const char *name) noexcept;

  /**
   *  Return the end of table name where we have removed dbname and '/'.
   * 
   * @param name table name in the form dbname '/' tablename
   * 
   * @return table name
   */
  [[nodiscard]] static const char *remove_db_name(const char *name) noexcept;

  /**
   * Opens the data dictionary.
   * 
   * @param[in] in_crash_recovery Whether to open in crash recovery mode.
   * 
   * @return DB_SUCCESS or error code.
   */
  [[nodiscard]] db_err open(bool in_crash_recovery) noexcept {
    return m_loader.open(in_crash_recovery);
  }

  /**
   * Inits Dict::m_dummy_index, dummy index template required for recovery.
   */
  [[nodiscard]]static Index *create_dummy_index() noexcept;

  /**
   * Destroys Dict::m_dummy_index, dummy index template required for recovery.
   */
  static void destroy_dummy_index(Index *&index) noexcept;

  /**
   * Returns a table object based on table id.
   * 
   * @param[in] recovery recovery flag
   * @param[in] table_id table id
   * @param[in] trx transaction handle
   * 
   * @return table, NULL if does not exist
   */
  [[nodiscard]] Table *table_get_on_id(ib_recovery_t recovery, Dict_id table_id, Trx *trx) noexcept;

  /**
   * Decrements the count of open handles to a table.
   * 
   * @param[in] table table
   * @param[in] dict_locked true if data dictionary locked
   */
  void table_decrement_handle_count(Table *table, bool dict_locked) noexcept;

  /**
   * Increments the count of open client handles to a table.
   * 
   * @param[in] table table
   * @param[in] dict_locked true if data dictionary locked
   */
  void table_increment_handle_count(Table *table, bool dict_locked) noexcept;

  /**
   * @brief Returns a table object and optionally increments its open handle count.
   * 
   * @param[in] table_name The table name.
   * @param[in] inc_count Whether to increment the open handle count on the table.
   * 
   * @return The table object, or NULL if it does not exist.
   */
  [[nodiscard]] Table *table_get(const char *table_name, bool inc_count) noexcept;

  /**
   * Returns a table instance based on table id.
   * 
   * @param[in] recovery The recovery flag.
   * @param[in] table_id The table id.
   * @param[in] ref_count Whether to increment open handle count if true.
   * 
   * @return The table object, or NULL if it does not exist.
   */
  [[nodiscard]] Table *table_get_using_id(ib_recovery_t recovery, Dict_id table_id, bool ref_count) noexcept;

  /**
   * @brief Checks if the given column name is reserved for InnoDB system columns.
   * 
   * @param[in] name The column name.
   * 
   * @return true if the name is reserved, false otherwise.
   */
  [[nodiscard]] static bool col_name_is_reserved(const char *name) noexcept;

  /**
   * @brief Adds a table object to the dictionary cache.
   * 
   * @param[in] table The table object.
   */
  void table_add_to_cache(Table *table) noexcept;

  /**
   * @brief Removes a table object from the dictionary cache.
   * 
   * @param table The table object.
   */
  void table_remove_from_cache(Table *table) noexcept;

  /**
   * @brief Renames a table object.
   * 
   * @param[in] table The table object.
   * @param[in] new_name The new name.
   * @param[in] rename_also_foreigns Whether to preserve the original table name in constraints which reference it.
   * @return true if success, false otherwise.
   */
  [[nodiscard]] bool table_rename_in_cache(Table *table, const char *new_name, bool rename_also_foreigns) noexcept;

  /**
   * @brief Removes an index from the dictionary cache.
   * 
   * @param[in] table The table object.
   * @param[in] index The index object.
   */
  void index_remove_from_cache(Table *table, Index *index) noexcept;

  /**
   * @brief Changes the id of a table object in the dictionary cache.
   * 
   * @param[in] table The table object.
   * @param[in] new_id The new id to set.
   */
  void table_change_id_in_cache(Table *table, Dict_id new_id) noexcept;

  /**
   * @brief Adds a foreign key constraint object to the dictionary cache.
   * 
   * @param[in] foreign The foreign key constraint object.
   * @param[in] check_charsets Whether to check charset compatibility.
   * 
   * @return DB_SUCCESS or error code.
   */
  [[nodiscard]] db_err foreign_add_to_cache(Foreign *foreign, bool check_charsets) noexcept;

  /**
   * @brief Scans a table create SQL string and adds the foreign key constraints declared in the string to the data dictionary.
   * 
   * @param[in] trx The transaction.
   * @param[in] sql_string The table create statement where foreign keys are declared.
   * @param[in] name The table full name in the normalized form.
   * @param[in] reject_fks Whether to fail with error code DB_CANNOT_ADD_CONSTRAINT if any foreign keys are found.
   * 
   * @return Error code or DB_SUCCESS.
   */
  [[nodiscard]] db_err create_foreign_constraints(Trx *trx, const char *sql_string, const char *name, bool reject_fks) noexcept;

  /**
   * @brief Parses the CONSTRAINT id's to be dropped in an ALTER TABLE statement.
   * 
   * @param[in] heap The heap from which memory can be allocated.
   * @param[in] trx The transaction.
   * @param[in] table The table.
   * @param[in] n The number of constraints to drop.
   * @param[in] constraints_to_drop The id's of the constraints to drop.
   * 
   * @return DB_SUCCESS or DB_CANNOT_DROP_CONSTRAINT if syntax error or the constraint id does not match.
   */
  [[nodiscard]] db_err foreign_parse_drop_constraints(mem_heap_t *heap, Trx *trx, Table *table, ulint *n, const char ***constraints_to_drop) noexcept;

  /**
   * @brief Finds an index that is equivalent to the one passed in and is not marked for deletion.
   * 
   * @param[in] foreign The foreign key.
   * 
   * @return The index equivalent to foreign->foreign_index, or NULL.
   */
  [[nodiscard]] Index *foreign_find_equiv_index(Foreign *foreign) noexcept;

  /**
   * @brief Prints a table data.
   * 
   * @param[in] table The table.
   */
  void table_print(Table *table) noexcept;

  /**
   * @brief Prints a table data when we know the table name.
   * 
   * @param[in] name The table name.
   */
  void table_print_by_name(const char *name) noexcept;

  /**
   * @brief Outputs info on foreign keys of a table.
   * 
   * @param[in] create_table_format Whether to print in a format suitable to be inserted into a CREATE TABLE.
   * @param[in] trx The transaction.
   * @param[in] table The table.
   */
  void print_info_on_foreign_keys(bool create_table_format, Trx *trx, Table *table) noexcept;

  /**
   * @brief Outputs info on a foreign key of a table in a format suitable for CREATE TABLE.
   * 
   * @param[in] trx The transaction.
   * @param[in] foreign The foreign key constraint.
   * @param[in] add_newline Whether to add a newline.
   */
  void print_info_on_foreign_key_in_create_format(Trx *trx, const Foreign *foreign, bool add_newline) noexcept;

  /**
   * @brief Displays the names of the index and the table.
   * 
   * @param[in] stream The output stream.
   * @param[in] trx The transaction.
   * @param[in] index The index to print.
   */
  void index_name_print(Trx *trx, const Index *index) noexcept;

  /**
   * @brief Returns a string representation of the dictionary.
   * 
   * @return The string representation.
   */
  std::string to_string() noexcept;

  /**
   * Checks if a column is in the ordering columns of the clustered index of a table.
   * Column prefixes are treated like whole columns.
   * 
   * @param[in] table The table.
   * @param[in] n The column number.
   * 
   * @return True if the column, or its prefix, is in the clustered key.
   */
  [[nodiscard]] bool table_col_in_clustered_key(const Table *table, ulint n) noexcept;

  /**
   * Looks for an index with the given id. NOTE that we do not reserve the
   * dictionary mutex: this function is for emergency purposes like printing
   * info of a corrupt database page!
   * 
   * @param[in] id The index id.
   * 
   * @return The index or NULL if not found from cache.
   */
  [[nodiscard]] Index *index_find_on_id(Dict_id id) noexcept;

  /**
   * Adds an index to the dictionary cache.
   * 
   * @param[in] index The index.
   * @param[in] page_no The root page number of the index.
   * @param[in] strict True to refuse to create the index if records could be too big to fit in a B-tree page.
   * 
   * @return DB_SUCCESS, DB_TOO_BIG_RECORD, or DB_CORRUPTION.
   */
  [[nodiscard]] db_err index_add_to_cache(Index *index, ulint page_no, bool strict) noexcept;

  /**
   * Returns an index instance if it is found in the dictionary cache.
   * 
   * @param[in] index_id index id
   * 
   * @return index, NULL if not found
   */
  [[nodiscard]] Index *index_get_if_in_cache(Dict_id index_id) noexcept;

  /**
   * Builds a node pointer out of a physical record and a page number.
   * 
   * @param[in] index index
   * @param[in] rec record for which to build node pointer
   * @param[in] page_no page number to put in node pointer
   * @param[in] heap memory heap where pointer created
   * @param[in] level level of rec in tree: 0 means leaf level
   * 
   * @return own: node pointer
   */
  [[nodiscard]] DTuple *index_build_node_ptr(const Index *index, const rec_t *rec, ulint page_no, mem_heap_t *heap, ulint level) noexcept;

  /**
   * Copies an initial segment of a physical record, long enough to specify an index entry uniquely.
   * 
   * @param[in] index index
   * @param[in] rec record for which to copy prefix
   * @param[in,out] n_fields number of fields copied
   * @param[in,out] buf memory buffer for the copied prefix, or NULL.
   * @param[in,out] buf_size buffer size
   * 
   * @return pointer to the prefix record
   */
  [[nodiscard]] rec_t *index_copy_rec_order_prefix(const Index *index, const rec_t *rec, ulint *n_fields, byte *&buf, ulint &buf_size) noexcept;

  /**
   * Builds a typed data tuple out of a physical record.
   * 
   * @param[in] index index
   * @param[in] rec record for which to build data tuple
   * @param[in] n_fields number of data fields
   * @param[in] heap memory heap where tuple created
   * 
   * @return own: data tuple
   */
  [[nodiscard]] DTuple *index_build_data_tuple(Index *index, rec_t *rec, ulint n_fields, mem_heap_t *heap) noexcept;

  /**
   * Calculates the minimum record length in an index.
   * 
   * @param[in] index index
   * 
   * @return minimum record length
   */
  [[nodiscard]] ulint index_calc_min_rec_len(const Index *index) noexcept;

  /**
   * Calculates new estimates for table and index statistics. The statistics are used in query optimization.
   * 
   * @param[in] table table
   * @param[in,out] stats Store the latatest statistics here.
   */
  void update_statistics(Table *table) noexcept;

  /**
   * Reserves the dictionary system mutex.
   */
  void mutex_acquire() noexcept;

  /**
   * Releases the dictionary system mutex.
   */
  void mutex_release() noexcept;

  /**
   * Lock the appropriate mutex to protect index->stat_n_diff_key_vals[].
   * index->id is used to pick the right mutex and it should not change before
   * index_stat_mutex_exit() is called on this index.
   * 
   * @param[in] index index
   */
  void index_stat_mutex_enter(const Index *index) noexcept;

  /**
   * Unlock the appropriate mutex that protects index->stat_n_diff_key_vals[].
   * 
   * @param[in] index index
   */
  void index_stat_mutex_exit(const Index *index) noexcept;

  /**
   * Checks if the database name in two table names is the same.
   * 
   * @param[in] name1 table name in the form dbname '/' tablename
   * @param[in] name2 table name in the form dbname '/' tablename
   * 
   * @return true if same db name
   */
  [[nodiscard]] bool tables_have_same_db(const char *name1, const char *name2) noexcept;

  /**
   * Get index by name
   * 
   * @param[in] table table
   * @param[in] name name of the index to find
   * 
   * @return index, NULL if does not exist
   */
  [[nodiscard]] Index *table_get_index_on_name(Table *table, const char *name) noexcept;

  /**
   * In case there is more than one index with the same name return the index with the min(id).
   * 
   * @param[in] table table
   * @param[in] name name of the index to find
   * 
   * @return index, NULL if does not exist
   */
  [[nodiscard]] Index *table_get_index_on_name_and_min_id(Table *table, const char *name) noexcept;

  /**
   * Locks the data dictionary exclusively for performing a table create or other data dictionary modification operation.
   * 
   * @param[in] trx transaction
   */
  void lock_data_dictionary(Trx *trx) noexcept;

  /**
   * Unlocks the data dictionary exclusive lock.
   * 
   * @param[in] trx transaction
   */
  void unlock_data_dictionary(Trx *trx) noexcept;

  /**
   * Locks the data dictionary in shared mode from modifications,
   * for performing foreign key check, rollback, or other operation
   * invisible to users.
   * 
   * @param[in] trx transaction
   */
  void freeze_data_dictionary(Trx *trx) noexcept;

  /**
   * Unlocks the data dictionary shared lock.
   * 
   * @param[in] trx transaction
   */
  void unfreeze_data_dictionary(Trx *trx) noexcept;

  /** Reset dict variables. */
  void var_init() noexcept;

  /**
   * Returns free space reserved for future updates of records. This is
   * relevant only in the case of many consecutive inserts, as updates
   * which make the records bigger might fragment the index.
   * 
   * @return	number of free bytes on page, reserved for updates
   */
  [[nodiscard]] constexpr static inline ulint index_get_space_reserve() noexcept {
    return UNIV_PAGE_SIZE / 16;
  }

  /**
   * @brief Checks if a table is in the dictionary cache.
   * 
   * @param[in] table_name table name
   * 
   * @return table, NULL if not found
   */
  [[nodiscard]] inline Table *table_check_if_in_cache(const char *table_name) noexcept {
    ut_ad(mutex_own(&m_mutex));

    if (const auto it = m_tables.find(table_name); it != m_tables.end()) {
      return  it->second;
    } else {
      return nullptr;
    }
  }

  /**
   * @brief Gets a table; loads it to the dictionary cache if necessary. A low-level function.
   * 
   * @param[in] table_name table name
   * 
   * @return table, NULL if not found
   */
  inline Table *table_get(const char *table_name) noexcept {
    ut_ad(mutex_own(&m_mutex));

    auto table = table_check_if_in_cache(table_name);

    if (table == nullptr) {
      table = m_loader.load_table(srv_config.m_force_recovery, table_name);
    }

    ut_ad(table == nullptr || table->m_cached);

    return table;
  }

  /**
   * @brief Returns a table object based on table id.
   * 
   * @param[in] table_id table id
   * 
   * @return table, nullptr if does not exist
   */
  inline Table *table_get_on_id(Dict_id table_id) noexcept {
    ut_ad(mutex_own(&m_mutex));

    if (const auto it = m_table_ids.find(table_id); it != m_table_ids.end()) {
      return it->second;
    } else {
      return nullptr;
    }
  }

  /**
   * @brief Returns a table object based on table id.
   * 
   * @param[in] recovery recovery flag
   * @param[in] table_id table id
   * 
   * @return table, NULL if does not exist
   */
  inline Table *table_get_on_id(ib_recovery_t recovery, Dict_id table_id) noexcept {
    ut_ad(mutex_own(&m_mutex));

    /* Look for the table name in the hash table */
    if (auto table = table_get_on_id(table_id); table != nullptr) {
      return table;
    } else {
      table = m_loader.load_table_on_id(recovery, table_id);
      ut_ad(table == nullptr || table->m_cached);
      return table;
    }
  }

#ifdef UNIT_TEST
private:
#endif /* UNIT_TEST */

  
  /**
   * @brief Builds the dictionary cache representation for a clustered index.
   *
   * @param[in] index User representation of a clustered index
   * 
   * @return own: the internal representation of the clustered index
   */
  [[nodiscard]] Index *build_cluster_index(Index *index) noexcept;

  /**
   * @brief Builds the dictionary cache representation for a non-clustered index, containing also system fields not defined by the user.
   *
   * @param[in] index User representation of a non-clustered index
   * 
   * @return own: the internal representation of the non-clustered index
   */
  [[nodiscard]] Index *build_secondary_index(Index *index) noexcept;

  /**
   * @brief  Removes a foreign constraint struct from the dictionary cache.
   * 
   * @param[in] foreign foreign constraint
   */
  static void foreign_remove_from_cache(Foreign *&foreign) noexcept;

  /**
   * @brief Prints a column data.
   *
   * @param[in] table Table
   * @param[in] col Column
   */
  [[nodiscard]] static std::string col_to_string(const Table *table, const Column *col) noexcept;

  /**
   * @brief Prints an index data.
   *
   * @param[in] index Index
   */
  [[nodiscard]] static std::string index_to_string(Index *index) noexcept;

  /**
   * @brief Prints a field data.
   *
   * @param[in] field field
   */
  [[nodiscard]] static std::string field_to_string(const Field *field) noexcept;

  /**
   * @brief Creates the file page for the dictionary header. This function is called only at the database creation.
   * 
   * @param[in] mtr mtr
   * 
   * @return true if succeed
   */
  [[nodiscard]] bool hdr_create(mtr_t *mtr) noexcept;

  /**
   * @brief If an undo log record for this table might not fit on a single page, return true.
   * 
   * @param[in] table The table.
   * @param[in] new_index The index.
   * 
   * @return true if the undo log record could become too big.
   */
  [[nodiscard]] static bool index_too_big_for_undo(const Table *table, const Index *new_index) noexcept;

  /**
   * @brief If a record of this index might not fit on a single B-tree page, return true.
   * 
   * @param[in] table The table.
   * @param[in] new_index The index.
   * 
   * @return true if the index record could become too big
   */
  [[nodiscard]] static bool index_too_big_for_tree(const Table *table, const Index *new_index) noexcept;

  /**
   * @brief Prints a column data.
   *
   * @param[in] table The table containing the column.
   * @param[in] col The column to print.
   */
  void col_print(const Table *table, const Column *col) noexcept;

  /**
   * @brief Prints an index data.
   * 
   * @param[in] index in: index
   */
  void index_print(Index *index) noexcept;

  /**
   * @brief Prints a field data.
   *
   * @param[in] field Pointer to the field to be printed.
   */
  void field_print(const Field *field) noexcept;

  /**
   * @brief Report an error in a foreign key definition.
   * 
   * @param[in] name the table name
   */
  void foreign_error_report(const char *name) noexcept {
    log_err(" Foreign key constraint of table ", name);
  }

  /**
   * @brief Report an error in a foreign key definition.
   * 
   * @param[in] fk foreign key constraint
   * @param[in] msg the error message
   */
  void foreign_error_report(Foreign *fk, const char *msg) noexcept;

  /**
   * @brief Scans from pointer onwards. Stops if is at the start of a copy of 'string'
   * where characters are compared without case sensitivity, and only outside `` or "" quotes. Stops also at NUL.
   * 
   * @param[in] ptr scan from
   * @param[in] string look for this
   * 
   * @return scanned up to this
   */
  const char *scan_to(const char *ptr, const char *string) noexcept;

  /**
   * @brief Accepts a specified string. Comparisons are case-insensitive.
   * 
   * @param[in] cs character set of ptr
   * @param[in] ptr scan from this
   * @param[in] string accept only this string as the next non-whitespace string
   * @param[out] success true if accepted
   * 
   * @return if string was accepted, the pointer is moved after that, else ptr is returned
   */
  const char *accept(const charset_t *cs, const char *ptr, const char *string, bool *success) noexcept;

  /**
   * @brief Scans an id. For the lexical definition of an 'id', see the code below.
   * Strips backquotes or double quotes from around the id.
   * 
   * @param[in] cs the character set of ptr
   * @param[in] ptr scanned to
   * @param[in] heap heap where to allocate the id (nullptr=id will not be allocated,
   *   but it will point to string near ptr)
   * @param[out] id the id; nullptr if no id was scannable
   * @param[in] table_id true=convert the allocated id as a table name; false=convert to UTF-8
   * @param[in] accept_also_dot true if also a dot can appear in a non-quoted id; in a quoted id
   *   it can appear always
   * 
   * @return scanned to
   */
  const char *scan_id(const charset_t *cs, const char *ptr, mem_heap_t *heap, const char **id, bool table_id, bool accept_also_dot) noexcept;

  /**
   * @brief Tries to scan a column name.
   * 
   * @param[in] cs The character set of ptr.
   * @param[in] ptr Scanned to.
   * @param[out] success True if success.
   * @param[in] table Table in which the column is.
   * @param[out] column Pointer to column if success.
   * @param[in] heap Heap where to allocate.
   * @param[out] name The column name; nullptr if no name was scannable.
   * 
   * @return Scanned to.
   */
  const char *scan_col(const charset_t *cs, const char *ptr, bool *success, Table *table, const Column **column, mem_heap_t *heap, const char **name) noexcept;

  /**
   * @brief Scans a table name from an SQL string.
   * 
   * @param[in] cs The character set of ptr.
   * @param[in] ptr Scanned to.
   * @param[out] table Table object or nullptr.
   * @param[in] name Foreign key table name.
   * @param[out] success True if ok name found.
   * @param[in] heap Heap where to allocate the id.
   * @param[out] ref_name The table name; nullptr if no name was scannable.
   * 
   * @return Scanned to.
   */
  const char *scan_table_name(const charset_t *cs, const char *ptr, Table **table, const char *name, bool *success, mem_heap_t *heap, const char **ref_name) noexcept;

  /**
   * @brief Skips one id. The id is allowed to contain also '.'.
   * 
   * @param[in] cs The character set of ptr.
   * @param[in] ptr Scanned to.
   * @param[out] success True if success, false if just spaces left in string or a syntax error.
   * 
   * @return Scanned to.
   */
  const char *skip_word(const charset_t *cs, const char *ptr, bool *success) noexcept;

  /**
   * @brief Removes comments from an SQL string.
   * 
   * A comment is either:
   * (a) '#' to the end of the line,
   * (b) '--[space]' to the end of the line, or
   * (c) '[slash][asterisk]' till the next '[asterisk][slash]' (like the familiar
   * C comment syntax).
   * 
   * @param[in] sql_string The input SQL string.
   * 
   * @return A new SQL string stripped of comments. The caller must free this with mem_free().
    */
  char *strip_comments(const char *sql_string) noexcept;

  /**
   * @brief Finds the highest [number] for foreign key constraints of the table. Looks
   * only at the >= 4.0.18-format id's, which are of the form
   * databasename/tablename_ibfk_[number].
   *
   * @param[in] table Table in the dictionary memory cache
   * 
   * @return highest number, 0 if table has no new format foreign key constraints
   */
  ulint table_get_highest_foreign_id(Table *table) noexcept;

  /**
   * @brief Reports a simple foreign key create clause syntax error.
   *
   * @param[in] name Table name
   * @param[in] start_of_latest_foreign Start of the foreign key clause in the SQL string
   * @param[in] ptr Place of the syntax error
   */
  void foreign_report_syntax_err(const char *name, const char *start_of_latest_foreign, const char *ptr) noexcept;

  /**
   * Scans a table create SQL string and adds to the data dictionary the foreign
   * key constraints declared in the string. This function should be called after
   * the indexes for a table have been created. Each foreign key constraint must
   * be accompanied with indexes in both participating tables. The indexes are
   * allowed to contain more fields than mentioned in the constraint.
   *
   * @param[in] trx Transaction
   * @param[in] heap Memory heap
   * @param[in] cs The character set of sql_string
   * @param[in] sql_string CREATE TABLE or ALTER TABLE statement where foreign keys are declared like:
   *   FOREIGN KEY (a, b) REFERENCES table2(c, d), table2 can be written also with the database
   *   name before it: test.table2; the default database is the database of parameter name
   * @param[in] name Table full name in the normalized form database_name/table_name
   * @param[in] reject_fks If true, fail with error code DB_CANNOT_ADD_CONSTRAINT if any foreign keys are found.
   *
   * @return error code or DB_SUCCESS
   */
  db_err create_foreign_constraints(Trx *trx, mem_heap_t *heap, const charset_t *cs, const char *sql_string, const char *name, bool reject_fks) noexcept;

  /**
   * @brief Prints info of a foreign key constraint.
   * 
   * @param[in] foreign in: foreign key constraint
   */
  void foreign_print(Foreign *foreign) noexcept;

public:
  /**
   * mutex protecting the data dictionary; protects also the disk-based
   * dictionary system tables; this mutex serializes CREATE TABLE and
   * DROP TABLE, as well as reading the dictionary data for a table from 
   * system tables.
   */
  mutable mutex_t m_mutex{};

  struct Stats_mutex {
    /** Constructor */
    Stats_mutex() noexcept {
      for (auto &mutex : m_mutexes) {
        mutex_create(&mutex, IF_DEBUG("Dict::Stats_mutex",) IF_SYNC_DEBUG(SYNC_INDEX_TREE,) Current_location());
      }
    }

    /** Destructor */
    ~Stats_mutex() {
      for (auto &mutex : m_mutexes) {
        mutex_free(&mutex);
      }
    }

    /**
     * @brief Get the mutex that protects index->stat_n_diff_key_vals[]
     * 
     * @param[in] index Index ID.
     * 
     * @return mutex for the index stat slot.
     */
    [[nodiscard]] inline auto get_mutex(Dict_id id) const noexcept {
      return &m_mutexes[ut_uint64_fold(id) % DICT_INDEX_STAT_MUTEX_SIZE];
    }

    void enter(Dict_id id) noexcept {
      mutex_enter(get_mutex(id));
    }

    void exit(Dict_id id) noexcept {
      mutex_exit(get_mutex(id));
    }

    /** Mutex protecting Index::Stats::m_n_diff_key_vals[] */
    mutable std::array<mutex_t, DICT_INDEX_STAT_MUTEX_SIZE> m_mutexes;
  };

  /** Mutex protecting Index::Stats::m_n_diff_key_vals */
  Stats_mutex m_stats_mutex{};

  /**
   * @brief the data dictionary rw-latch protecting dict_sys
   * 
   * table create, drop, etc. reserve this in X-mode; implicit or
   * backround operations purge, rollback, foreign key checks reserve this
   * in S-mode; we cannot trust that the client protects implicit or background
   * operations a table drop since the client does not know about them; therefore
   * we need this; NOTE: a transaction which reserves this must keep book
   * on the mode in trx_t::m_dict_operation_lock_mode */
  mutable rw_lock_t m_lock{};

  /** Mutex protecting the foreign and unique error buffers */
  mutable mutex_t m_foreign_err_mutex{};

  /** Hash table of the tables, based on name */
  std::unordered_map<std::string_view, Table *> m_tables{};

  /** Hash table of the tables, based on id */
  std::unordered_map<Dict_id, Table *> m_table_ids{};

  /** LRU list of tables */
  UT_LIST_BASE_NODE_T(Table, m_table_LRU) m_table_LRU{};

  /** Varying space in bytes occupied by the data dictionary table and index objects */
  ulint m_size{};

  /** SYS_TABLES table */
  Table *m_sys_tables{};

  /** SYS_COLUMNS table */
  Table *m_sys_columns{};

  /** SYS_INDEXES table */
  Table *m_sys_indexes{};

  /** SYS_FIELDS table */
  Table *m_sys_fields{};

  /** Dummy index for ROW_FORMAT=REDUNDANT supremum and infimum records */
  Index *m_dummy_index{};

  /** Data dictionary booting/creation. */
  Dict_store m_store;

  /** Data dictionary loading. */
  Dict_load m_loader;

  /** Data dictionary DDL operations. */
  DDL m_ddl;
};
