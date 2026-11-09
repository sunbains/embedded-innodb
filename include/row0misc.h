/******************************************************
Interface between Innobase and client. This file contains
the functions that don't have a proper home yet.

(c) 2008 Oracle Corpn./Innobase Oy
Copyright (c) 2024 Sunny Bains. All rights reserved.

*******************************************************/

#include "innodb0types.h"
#include "os0file.h"
#include "que0que.h"
#include "trx0trx.h"

#include <expected>
#include <filesystem>
#include <system_error>

/** @return the directory of the temporary files of sys: FilConfig::m_tmp_dir, else the data home. */
[[nodiscard]] std::filesystem::path row_tmp_dir(const System &sys) noexcept;

/**
 * Create a temporary file: without a name (O_TMPFILE), or with one unlinked at once where the file
 * system can't. Gone when it's closed.
 *
 * @param[in] dir               The directory.
 * @param[in] prefix            Of the name, if it has one; nullptr for "ib".
 *
 * @return the descriptor, or an errno code: ENOENT for a directory that doesn't exist.
 */
[[nodiscard]] std::expected<int, std::error_code> row_create_tempfile(const std::filesystem::path &dir, const char *prefix) noexcept;

/** Handles user errors and lock waits detected by the database engine.
@return	true if it was a lock wait and we should continue running the query
thread */

bool row_handle_errors(
  System &sys, enum DbErr *new_err, /*!< out: possible new error
                                              encountered in lock wait, or if
                                              no new error, the value of
                                              trx->error_state at the entry of
                                              this  function */
  Trx *trx,             /*!< in: transaction */
  QueThr *thr,       /*!< in: query thread */
  TrxSavept *savept
); /*!< in: savepoint or nullptr */

/** Sets a lock on a table.
@return	error code or DB_SUCCESS */
enum DbErr row_lock_table_with_retry(System &sys, Trx *trx,     /*!< in/out: transaction */
  Table *table, /*!< in: table to lock */
  enum LockMode mode
); /*!< in: lock mode */

/** Updates the table modification counter and calculates new estimates
for table and index statistics if necessary. */
void row_update_statistics_if_needed(System &sys, Table *table); /*!< in/out: table */
