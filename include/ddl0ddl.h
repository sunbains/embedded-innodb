/** Contains InnoDB Ddl operations.

(c)opyright 2008 Oracle Corpn/Innobase Oy
Copyright (c) 2024 Sunny Bains. All rights reserved.

Created 12 Oct 2008
*******************************************************/

#pragma once

#include "srv0sys.h"

#include "dict0types.h"
#include "srv0srv.h"

struct Log;
struct Trx;
struct Dict;

/** Data Dictionary Layer Ddl operations. */
struct Ddl {
  /** The instance this sub-system belongs to. */
  System &m_sys;

  /**
   * @brief Constructor.
   *
   * @param[in] dict Data dictionary.
   */
  explicit Ddl(System &sys) noexcept : m_sys(sys) {}

  /** Get the background drop list length. NOTE: the caller must own the kernel mutex!
   * @return	how many tables in list
   */
  [[nodiscard]] ulint get_background_drop_list_len() noexcept;

  /**
   * @brief Creates a table.
   *
   * If the name of the table ends in one of "innodb_monitor",
   * "innodb_lock_monitor", "innodb_tablespace_monitor", "innodb_table_monitor",
   * then this will also start the printing of monitor output by the master
   * thread. If the table name ends in "innodb_mem_validate", InnoDB will
   * try to invoke mem_validate().
   *
   * @param[in] table - Table definition.
   * @param[in] trx - Transaction handle.
   *
   * @return Error code or DB_SUCCESS.
   */
  [[nodiscard]] DbErr create_table(Table *table, Trx *trx) noexcept;

  /**
   * @brief Does an index creation operation.
   * TODO: currently failure to create an index results in dropping the whole table!
   * This is no problem currently as all indexes must be created at the same time as the table.
   *
   * @param[in] index - Index definition.
   * @param[in] trx - Transaction handle.
   *
   * @return Error number or DB_SUCCESS.
   */
  [[nodiscard]] DbErr create_index(Index *index, Trx *trx) noexcept;

  /**
   * @brief Drops a table but does not commit the transaction. If the
   * name of the dropped table ends in one of "innodb_monitor",
   * "innodb_lock_monitor", "innodb_tablespace_monitor",
   * "innodb_table_monitor", then this will also stop the printing of
   * monitor output by the master thread.
   *
   * @param[in] name - Table name.
   * @param[in] trx - Transaction handle.
   * @param[in] drop_db - True if dropping the whole database.
   *
   * @return Error code or DB_SUCCESS.
   */
  [[nodiscard]] DbErr drop_table(const char *name, Trx *trx, bool drop_db) noexcept;

  /**
   * @brief Drops an index.
   *
   * @param[in] table - Table instance.
   * @param[in] index - Id of index to drop.
   * @param[in] trx - Transaction handle.
   *
   * @return Error code or DB_SUCCESS.
   */
  [[nodiscard]] DbErr drop_index(Table *table, Index *index, Trx *trx) noexcept;

  /**
   * @brief The master thread in srv0srv.c calls this regularly to drop tables which
   * we must drop in background after queries to them have ended. Such lazy
   * dropping of tables is needed in ALTER TABLE on Unix.
   *
   * @return how many tables dropped + remaining tables in list
   */
  [[nodiscard]] ulint drop_tables_in_background() noexcept;

  /**
   * @brief Truncates a table.
   *
   * @param[in] table - Table handle.
   * @param[in] trx - Transaction handle.
   *
   * @return Error code or DB_SUCCESS.
   */
  [[nodiscard]] DbErr truncate_table(Table *table, Trx *trx) noexcept;

  /**
   * @brief Renames a table.
   *
   * @param[in] old_name - Old table name.
   * @param[in] new_name - New table name.
   * @param[in] trx - Transaction handle.
   *
   * @return Error code or DB_SUCCESS.
   */
  [[nodiscard]] DbErr rename_table(const char *old_name, const char *new_name, Trx *trx) noexcept;

  /**
   * @brief Renames an index.
   *
   * @param[in] table_name - Table that owns the index.
   * @param[in] old_name - Old table name.
   * @param[in] new_name - New table name.
   * @param[in] trx - Transaction handle.
   *
   * @return Error code or DB_SUCCESS.
   */
  [[nodiscard]] DbErr rename_index(const char *table_name, const char *old_name, const char *new_name, Trx *trx) noexcept;

  /**
   * @brief Drops a database.
   *
   * @param[in] name - Database name which ends in '/'.
   * @param[in] trx - Transaction handle.
   *
   * @return Error code or DB_SUCCESS.
   */
  [[nodiscard]] DbErr drop_database(const char *name, Trx *trx) noexcept;

  /**
   * @brief Drop all partially created indexes.
   *
   * @param[in] recovery - Recovery level setting.
   */
  void drop_all_temp_indexes(IbRecovery recovery) noexcept;

  /**
   * @brief Drop all temporary tables.
   *
   * @param[in] recovery - Recovery level setting.
   */
  void drop_all_temp_tables(IbRecovery recovery) noexcept;

#ifndef UNIT_TEST
 private:
#endif /* UNIT_TEST */

  /**
 * @brief Drops a table as a background operation.
   * On Unix in ALTER TABLE the table handler does not remove the table before all
   * handles to it has been removed. Furhermore, the call to the drop table must be
   * non-blocking. Therefore we do the drop table as a background operation, which is
   * taken care of by the master thread in srv0srv.cc.
   *
   * @param[in] name Table name.
   *
   * @return Error code or DB_SUCCESS
   */
  [[nodiscard]] DbErr drop_table_in_background(const char *name) noexcept;

  /**
   * @brief If a table is not yet in the drop list, adds the table to the list of
   * tables which the master thread drops in background. We need this on Unix
   * because in ALTER TABLE may call drop table even if the table has running
   * queries on it. Also, if there are running foreign key checks on the table,
   * we drop the table lazily.
   *
   * @param[in] name Table name.
   *
 * @return true if the table was not yet in the drop list, and was added there
 */
  [[nodiscard]] bool add_table_to_background_drop_list(const char *name) noexcept;

  /**
   * @brief Delete a single constraint.
   * 
   * @param[in] id Constraint id
   * @param[in] trx Transaction handle
   * 
   * @return Error code or DB_SUCCESS
   */
  [[nodiscard]] DbErr delete_foreign_constraint(const char *id, Trx *trx) noexcept;

  /**
   * @brief Delete a single constraint.
   *
   * @param[in] id constraint id
   * @param[in] database_name database name, with the trailing '/'
   * @param[in] heap memory heap
   * @param[in] trx transaction handle
   *
   * @return error code or DB_SUCCESS
   */
  [[nodiscard]] DbErr delete_constraint(const char *id, const char *database_name, MemHeap *heap, Trx *trx) noexcept;

  /**
   * @brief Drop all foreign keys in a database, see Bug#18942.
   *
   * @param[in] name database name which ends to '/'
   * @param[in] trx transaction handle
   *
   * @return error code or DB_SUCCESS
   */
  [[nodiscard]] DbErr drop_all_foreign_keys_in_db(const char *name, Trx *trx) noexcept;

#ifndef UNIT_TEST
 private:
#endif /* UNIT_TEST */

  /**
   * @brief List of tables we should drop in background. ALTER TABLE requires
   * that the table handler can drop the table in background when there are no
   * queries to it any more. Protected by the kernel mutex.
   */
  struct DropList {
    /** Table name to drop.. */
    char *m_table_name{};

    /** Next node in the list. */
    UT_LIST_NODE_T(DropList) m_node{};
  };


  /** Log. */

  /** List of tables we should drop in background. */
  UT_LIST_BASE_NODE_T(DropList, m_node) m_drop_list;
};

[[nodiscard]] inline DdlIndexStatus Index::get_online_ddl_status() const noexcept {
  const auto ddl_status = static_cast<DdlIndexStatus>(m_ddl_status);

#ifdef UNIV_DEBUG
  switch (ddl_status) {
    case DdlIndexStatus::READY:
    case DdlIndexStatus::IN_PROGRESS:
    case DdlIndexStatus::FAILED:
    case DdlIndexStatus::DROPPED:
      return ddl_status;
  }
  ut_error;
#endif /* UNIV_DEBUG */

  return ddl_status;
}

[[nodiscard]] inline bool Index::is_online_ddl_in_progress() const noexcept {
#ifdef UNIV_DEBUG
  if (is_clustered()) {
    switch (get_online_ddl_status()) {
      case DdlIndexStatus::READY:
        return true;
      case DdlIndexStatus::IN_PROGRESS:
        return false;
      case DdlIndexStatus::FAILED:
      case DdlIndexStatus::DROPPED:
        break;
    }
    ut_error;
  }
#endif /* UNIV_DEBUG */

  return get_online_ddl_status() != DdlIndexStatus::READY;
}
