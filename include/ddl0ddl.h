/** Contains InnoDB DDL operations.

(c) 2008 Oracle Corpn/Innobase Oy

Created 12 Oct 2008
*******************************************************/

#pragma once

#include "innodb0types.h"

#include "dict0types.h"
#include "trx0types.h"

/** Get the background drop list length. NOTE: the caller must own the kernel
mutex!
@return	how many tables in list */
ulint ddl_get_background_drop_list_len_low();

/** Creates a table, if the name of the table ends in one of "innodb_monitor",
"innodb_lock_monitor", "innodb_tablespace_monitor", "innodb_table_monitor",
then this will also start the printing of monitor output by the master
thread. If the table name ends in "innodb_mem_validate", InnoDB will
try to invoke mem_validate().
@return	error code or DB_SUCCESS */
db_err ddl_create_table(dict_table_t *table, /*!< in: table definition */
                        trx_t *trx);         /*!< in: transaction handle */

/** Does an index creation operation. TODO: currently failure to create an
index results in dropping the whole table! This is no problem currently
as all indexes must be created at the same time as the table.
@return	error number or DB_SUCCESS */
db_err ddl_create_index(dict_index_t *index, /*!< in: index definition */
                        trx_t *trx);         /*!< in: transaction handle */

/** Drops a table but does not commit the transaction.  If the
name of the dropped table ends in one of "innodb_monitor",
"innodb_lock_monitor", "innodb_tablespace_monitor",
"innodb_table_monitor", then this will also stop the printing of
monitor output by the master thread.
@return	error code or DB_SUCCESS */

db_err ddl_drop_table(const char *name, /*!< in: table name */
                      trx_t *trx,       /*!< in: transaction handle */
                      bool drop_db);    /*!< in: true=dropping whole database */

/** Drops an index.
@return	error code or DB_SUCCESS */
db_err ddl_drop_index(dict_table_t *table, /*!< in: table instance */
                      dict_index_t *index, /*!< in: id of index to drop */
                      trx_t *trx);         /*!< in: transaction handle */

/** The master thread in srv0srv.c calls this regularly to drop tables which
we must drop in background after queries to them have ended. Such lazy
dropping of tables is needed in ALTER TABLE on Unix.
@return	how many tables dropped + remaining tables in list */
ulint ddl_drop_tables_in_background(void);

/** Truncates a table
@return	error code or DB_SUCCESS */
db_err ddl_truncate_table(dict_table_t *table, /*!< in: table handle */
                          trx_t *trx);         /*!< in: transaction handle */
/** Renames a table.
@return	error code or DB_SUCCESS */
db_err ddl_rename_table(const char *old_name, /*!< in: old table name */
                        const char *new_name, /*!< in: new table name */
                        trx_t *trx);          /*!< in: transaction handle */

/** Renames an index.
@return	error code or DB_SUCCESS */
db_err
ddl_rename_index(const char *table_name, /*!< in: table that owns the index */
                 const char *old_name,   /*!< in: old table name */
                 const char *new_name,   /*!< in: new table name */
                 trx_t *trx);            /*!< in: transaction handle */

/** Drops a database.
@return	error code or DB_SUCCESS */
db_err ddl_drop_database(const char *name, /*!< in: database name which
                                                ends in '/' */
                         trx_t *trx);      /*!< in: transaction handle */

/** Drop all partially created indexes. */
void ddl_drop_all_temp_indexes(
    ib_recovery_t recovery); /*!< in: recovery level setting */

/** Drop all temporary tables. */
void ddl_drop_all_temp_tables(
    ib_recovery_t recovery); /*!< in: recovery level setting */
