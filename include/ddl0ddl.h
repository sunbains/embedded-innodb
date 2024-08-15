/** Contains InnoDB DDL operations.

(c) 2008 Oracle Corpn/Innobase Oy
Copyright (c) 2024 Sunny Bains. All rights reserved.

Created 12 Oct 2008
*******************************************************/

#pragma once

#include "innodb0types.h"

#include "dict0types.h"

struct trx_t;

/** Get the background drop list length. NOTE: the caller must own the kernel
mutex!
@return	how many tables in list */
ulint ddl_get_background_drop_list_len_low();

/**
 * @brief Creates a table.
 *
 * If the name of the table ends in one of "innodb_monitor",
 * "innodb_lock_monitor", "innodb_tablespace_monitor", "innodb_table_monitor",
 * then this will also start the printing of monitor output by the master
 * thread. If the table name ends in "innodb_mem_validate", InnoDB will
 * try to invoke mem_validate().
 *
 * @param table - Table definition.
 * @param trx - Transaction handle.
 *
 * @return Error code or DB_SUCCESS.
 */
db_err ddl_create_table(dict_table_t *table, trx_t *trx);

/**
 * @brief Does an index creation operation.
 * TODO: currently failure to create an index results in dropping the whole table! This is no problem currently as all indexes must be created at the same time as the table.
 * @param index - Index definition.
 * @param trx - Transaction handle.
 * @return Error number or DB_SUCCESS.
 */
db_err ddl_create_index(dict_index_t *index, trx_t *trx);

/**
 * @brief Drops a table but does not commit the transaction. If the
 * name of the dropped table ends in one of "innodb_monitor",
 * "innodb_lock_monitor", "innodb_tablespace_monitor",
 * "innodb_table_monitor", then this will also stop the printing of
 * monitor output by the master thread.
 *
 * @param name - Table name.
 * @param trx - Transaction handle.
 * @param drop_db - True if dropping the whole database.
 *
 * @return Error code or DB_SUCCESS.
 */
db_err ddl_drop_table(const char *name, trx_t *trx, bool drop_db);

/**
 * @brief Drops an index.
 *
 * @param table - Table instance.
 * @param index - Id of index to drop.
 * @param trx - Transaction handle.
 *
 * @return Error code or DB_SUCCESS.
 */
db_err ddl_drop_index(dict_table_t *table, dict_index_t *index, trx_t *trx);

/**
 * @brief The master thread in srv0srv.c calls this regularly to drop tables which
 * we must drop in background after queries to them have ended. Such lazy
 * dropping of tables is needed in ALTER TABLE on Unix.
 *
 * @return how many tables dropped + remaining tables in list
 */
ulint ddl_drop_tables_in_background();

/**
 * @brief Truncates a table.
 *
 * @param table - Table handle.
 * @param trx - Transaction handle.
 *
 * @return Error code or DB_SUCCESS.
 */
db_err ddl_truncate_table(dict_table_t *table, trx_t *trx);

/**
 * @brief Renames a table.
 *
 * @param old_name - Old table name.
 * @param new_name - New table name.
 * @param trx - Transaction handle.
 *
 * @return Error code or DB_SUCCESS.
 */
db_err ddl_rename_table(const char *old_name, const char *new_name, trx_t *trx);

/**
 * @brief Renames an index.
 *
 * @param table_name - Table that owns the index.
 * @param old_name - Old table name.
 * @param new_name - New table name.
 * @param trx - Transaction handle.
 *
 * @return Error code or DB_SUCCESS.
 */
db_err ddl_rename_index(const char *table_name, const char *old_name, const char *new_name, trx_t *trx);

/**
 * Drops a database.
 *
 * @param name - Database name which ends in '/'.
 * @param trx - Transaction handle.
 *
 * @return Error code or DB_SUCCESS.
 */
db_err ddl_drop_database(const char *name, trx_t *trx);

/**
 * @brief Drop all partially created indexes.
 *
 * @param recovery - Recovery level setting.
 */
void ddl_drop_all_temp_indexes(ib_recovery_t recovery);

/**
 * @brief Drop all temporary tables.
 *
 * @param recovery - Recovery level setting.
 */
void ddl_drop_all_temp_tables(ib_recovery_t recovery);

inline DDL_index_status dict_index_t::get_online_ddl_status() const {
auto ddl_status = static_cast<DDL_index_status>(m_ddl_status);

#ifdef UNIV_DEBUG
  switch (ddl_status) {
    case DDL_index_status::READY:
    case DDL_index_status::IN_PROGRESS:
    case DDL_index_status::FAILED:
    case DDL_index_status::DROPPED:
      return ddl_status;
  }
  ut_error;
#endif /* UNIV_DEBUG */

  return ddl_status;
}

inline bool dict_index_t::is_online_ddl_in_progress() const {
#ifdef UNIV_DEBUG
  if (is_clustered()) {
    switch (get_online_ddl_status()) {
      case DDL_index_status::READY:
        return true;
      case DDL_index_status::IN_PROGRESS:
        return false;
      case DDL_index_status::FAILED:
      case DDL_index_status::DROPPED:
        break;
    }
    ut_error;
  }
#endif /* UNIV_DEBUG */

  return get_online_ddl_status() != DDL_index_status::READY;
}
