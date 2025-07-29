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

/** @file include/dict0load.h
Loads to the memory cache database object definitions
from dictionary tables

Created 4/24/1996 Heikki Tuuri
*******************************************************/

#pragma once

#include "dict0types.h"
#include "srv0srv.h"
#include "ut0byte.h"

struct Dict_load {
  explicit Dict_load(Dict *dict) noexcept : m_dict{dict} {}

  /**
   * In a crash recovery we already have all the tablespace objects created.
   * This function compares the space id information in the InnoDB data dictionary
   * to what we already read with Fil::load_single_table_tablespaces().
   *
   * In a normal startup, we create the tablespace objects for every table in
   * InnoDB's data dictionary, if the corresponding .ibd file exists.
   * We also scan the biggest space id, and store it to Fil.
   *
   * @param[in] in_crash_recovery Are we doing a crash recovery
   * 
   * @return DB_SUCCESS or error code.
   */
  [[nodiscard]] db_err open(bool in_crash_recovery) noexcept;

  /**
   * Finds the first table name in the given database.
   *
   * @param[in] name Database name which ends to '/'
   * @return Table name, nullptr if does not exist; the caller must free
   *         the memory in the string.
   */
  [[nodiscard]] char *get_first_table_name_in_db(const char *name) noexcept;

  /**
   * Loads a table definition and also all its index definitions, and also
   * the cluster definition if the table is a member in a cluster. Also loads
   * all foreign key constraints where the foreign key is in the table or where
   * a foreign key references columns in this table.
   *
   * @param[in] recovery Recovery flag
   * @param[in] name Table name in the databasename/tablename format
   * 
   * @return Table object, nullptr if does not exist; if the table is stored in an
   * .ibd file, but the file does not exist, then we set the
   * ibd_file_missing flag true in the table object we return
   */
  [[nodiscard]] Table *load_table(ib_recovery_t recovery, const char *name) noexcept;

  /**
   * Loads a table object based on the table id.
   *
   * @param[in] recovery Recovery flag
   * @param[in] table_id Table id
   * 
   * @return Table object, nullptr if table does not exist
   */
  [[nodiscard]] Table *load_table_on_id(ib_recovery_t recovery, Dict_id table_id) noexcept;

  /**
   * @brief Loads system table index definitions.
   *
   * This function is called when the database is booted. It loads system table
   * index definitions except for the clustered index, which is added to the
   * dictionary cache at booting before calling this function.
   *
   * @param[in] table System table to load index definitions for.
   */
  [[nodiscard]] db_err load_sys_table(Table *table) noexcept;

  /**
   * @brief Loads foreign key constraints where the table is either the foreign key
   * holder or where the table is referenced by a foreign key. Adds these
   * constraints to the data dictionary. Note that we know that the dictionary
   * cache already contains all constraints where the other relevant table is
   * already in the dictionary cache.
   *
   * @param[in] table_name Table name
   * @param[in] check_charsets True to check charsets compatibility
   * 
   * @return DB_SUCCESS or error code
   */
  [[nodiscard]] db_err load_foreigns(const char *table_name, bool check_charsets) noexcept;

  /**
   * @brief Prints to the standard output information on all tables found in the data
   * dictionary system table.
   */
  [[nodiscard]] std::string to_string() noexcept;

  /**
   * @brief Initializes the data dictionary memory structures when the database is started.
   * 
   * This function is also called when the data dictionary is created.
   * 
   * @return DB_SUCCESS if successful, error code otherwise.
   */
  db_err load_system_tables() noexcept;

#ifndef UNIT_TEST
 private:
#endif /* !UNIT_TEST */

  /**
   * @brief Loads a system table.
   * 
   * @param[in] table The table.
   * 
   * @return DB_SUCCESS or error code.
   */
  [[nodiscard]] db_err load_system_table(Table *table) noexcept;

  /**
   * @brief Compare the name of an index column.
   *
   * @param[in] table The table.
   * @param[in] index The index.
   * @param[in] i The index field offset.
   * @param[in] name The name to compare to.
   *
   * @return True if the i'th column of index is 'name'.
   */
  [[nodiscard]] bool name_of_col_is(const Table *table, const Index *index, ulint i, const char *name) const noexcept;

  /**
   * @brief Determine the flags of a table described in SYS_TABLES.
   *
   * @param[in] rec A record of SYS_TABLES.
   *
   * @return Compressed page size in kilobytes; or 0 if the tablespace is uncompressed, ULINT_UNDEFINED on error.
   */
  [[nodiscard]] ulint sys_tables_get_flags(const rec_t *rec) const noexcept;

  /**
   * @brief Loads definitions for table columns.
   *
   * @param[in] table The table.
   * @param[in] heap The memory heap for temporary storage.
   */
  [[nodiscard]] db_err load_columns(Table *table, mem_heap_t *heap) noexcept;

  /**
   * @brief Loads definitions for table foreign keys.
   *
   * @param[in] table The table.
   * @param[in] heap The memory heap for temporary storage.
   */
  [[nodiscard]] db_err load_foreigns(Table *table, mem_heap_t *heap) noexcept;

  /**
   * @brief Loads definitions for index fields.
   *
   * @param[in] index The index whose fields to load.
   * @param[in] heap The memory heap for temporary storage.
   */
  [[nodiscard]] db_err load_fields(Index *index, mem_heap_t *heap) noexcept;

  /**
   * @brief Loads definitions for table indexes. Adds them to the data dictionary cache.
   * 
   * @param[in] table The table.
   * @param[in] heap The memory heap for temporary storage.
   * 
   * @return DB_SUCCESS if ok, DB_CORRUPTION if corruption of dictionary table or
   *   DB_UNSUPPORTED if table has unknown index type
   */
  [[nodiscard]] db_err load_indexes(Table *table, mem_heap_t *heap) noexcept;

  /**
   * @brief Loads foreign key constraint col names (also for the referenced table).
   *
   * @param[in] id foreign constraint id as a null-terminated string
   * @param[in,out] foreign foreign constraint object
   */
  [[nodiscard]] db_err load_foreign_cols(const char *id, Foreign *foreign) noexcept;

  /**
   * @brief Loads a foreign key constraint to the dictionary cache.
   *
   * @param[in] id The foreign constraint id as a null-terminated string.
   * @param[in] check_charsets True to check charset compatibility.
   *
   * @return DB_SUCCESS or error code.
   */
  [[nodiscard]] db_err load_foreign(const char *id, bool check_charsets) noexcept;

 private:
#ifndef UNIT_TEST
 private:
#endif /* !UNIT_TEST */

  /**
   * @brief The dictionary.
   */
  Dict *m_dict{};
};
