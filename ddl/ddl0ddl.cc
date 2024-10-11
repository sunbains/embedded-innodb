/*********************************************************
(c) 2008 Oracle Corpn/Innobase Oy
Copyright (c) 2024 Sunny Bains. All rights reserved.


Created 12 Oct 2008.
*******************************************************/

#include "srv0srv.h"

#include "api0misc.h"
#include "btr0pcur.h"
#include "ddl0ddl.h"
#include "dict0store.h"
#include "dict0load.h"
#include "lock0lock.h"
#include "log0log.h"
#include "pars0pars.h"
#include "trx0roll.h"
#include "ut0lst.h"

/* List of tables we should drop in background. ALTER TABLE requires
that the table handler can drop the table in background when there are no
queries to it any more. Protected by the kernel mutex. */
typedef struct ddl_drop_struct ddl_drop_t;

struct ddl_drop_struct {
  char *table_name;
  UT_LIST_NODE_T(ddl_drop_t) ddl_drop_list;
};

static UT_LIST_BASE_NODE_T(ddl_drop_t, ddl_drop_list) ddl_drop_list;
static bool ddl_drop_list_inited = false;

/* Magic table names for invoking various monitor threads */
static const char S_innodb_monitor[] = "innodb_monitor";
static const char S_innodb_lock_monitor[] = "innodb_lock_monitor";
static const char S_innodb_tablespace_monitor[] = "innodb_tablespace_monitor";
static const char S_innodb_table_monitor[] = "innodb_table_monitor";
static const char S_innodb_mem_validate[] = "innodb_mem_validate";

/**
 * Drops a table as a background operation.
 * On Unix in ALTER TABLE the table handler does not remove the table before all handles to it has been removed.
 * Furhermore, the call to the drop table must be non-blocking. Therefore we do the drop table as a background operation, which is taken care of by the master thread in srv0srv.c.
 * @param name table name
 * @return error code or DB_SUCCESS
 */
static db_err ddl_drop_table_in_background(const char *name) {
  bool started;

  auto trx = trx_allocate_for_background();

  started = trx_start(trx, ULINT_UNDEFINED);
  ut_a(started);

  /* If the original transaction was dropping a table referenced by
  foreign keys, we must set the following to be able to drop the
  table: */

  trx->m_check_foreigns = false;

  /* Try to drop the table in InnoDB */

  srv_dict_sys->lock_data_dictionary(trx);

  auto err = ddl_drop_table(name, trx, false);

  auto err_commit = trx_commit(trx);
  ut_a(err_commit == DB_SUCCESS);

  srv_dict_sys->unlock_data_dictionary(trx);

  log_sys->buffer_flush_to_disk();

  trx_free_for_background(trx);

  return err;
}

ulint ddl_drop_tables_in_background() {
  ulint n_tables_dropped{};

  for (;;) {
    mutex_enter(&kernel_mutex);

    if (!ddl_drop_list_inited) {
      UT_LIST_INIT(ddl_drop_list);
      ddl_drop_list_inited = true;
    }

    auto drop = UT_LIST_GET_FIRST(ddl_drop_list);
    const auto n_tables = UT_LIST_GET_LEN(ddl_drop_list);

    mutex_exit(&kernel_mutex);

    if (drop == nullptr) {
      /* All tables dropped */
      return n_tables + n_tables_dropped;
    }

    srv_dict_sys->mutex_acquire();

    auto table = srv_dict_sys->table_get(drop->table_name);

    srv_dict_sys->mutex_release();

    if (table != nullptr) {
      if (DB_SUCCESS != ddl_drop_table_in_background(drop->table_name)) {
        /* If the DROP fails for some table, we return, and let the
        main thread retry later */

        return n_tables + n_tables_dropped;
      }

      ++n_tables_dropped;
    }

    mutex_enter(&kernel_mutex);

    UT_LIST_REMOVE(ddl_drop_list, drop);

    log_info(std::format("Dropped table {} in background drop queue.", drop->table_name));

    mem_free(drop->table_name);

    mem_free(drop);

    mutex_exit(&kernel_mutex);
  }
}

ulint ddl_get_background_drop_list_len_low() {
  ut_ad(mutex_own(&kernel_mutex));

  if (!ddl_drop_list_inited) {

    UT_LIST_INIT(ddl_drop_list);
    ddl_drop_list_inited = true;
  }

  return UT_LIST_GET_LEN(ddl_drop_list);
}

/**
 * If a table is not yet in the drop list, adds the table to the list of tables
 * which the master thread drops in background. We need this on Unix because in
 * ALTER TABLE may call drop table even if the table has running queries on
 * it. Also, if there are running foreign key checks on the table, we drop the
 * table lazily.
 * @param name table name
 * @return true if the table was not yet in the drop list, and was added there
 */
static bool ddl_add_table_to_background_drop_list(const char *name) {
  mutex_enter(&kernel_mutex);

  if (!ddl_drop_list_inited) {

    UT_LIST_INIT(ddl_drop_list);
    ddl_drop_list_inited = true;
  }

  /* Look if the table already is in the drop list */
  auto drop = UT_LIST_GET_FIRST(ddl_drop_list);

  while (drop != nullptr) {
    if (strcmp(drop->table_name, name) == 0) {
      /* Already in the list */

      mutex_exit(&kernel_mutex);

      return false;
    }

    drop = UT_LIST_GET_NEXT(ddl_drop_list, drop);
  }

  drop = static_cast<ddl_drop_t *>(mem_alloc(sizeof(ddl_drop_t)));

  drop->table_name = mem_strdup(name);

  UT_LIST_ADD_LAST(ddl_drop_list, drop);

  mutex_exit(&kernel_mutex);

  return true;
}

db_err ddl_drop_table(const char *name, trx_t *trx, bool drop_db) {
  Foreign *foreign;
  Table *table;
  ulint space_id;
  enum db_err err;
  const char *table_name;
  ulint namelen;
  pars_info_t *info = nullptr;

  ut_a(name != nullptr);

  if (srv_config.m_created_new_raw) {
    ib_logger(
      ib_stream,
      "A new raw disk partition was initialized:\n"
      "we do not allow database modifications"
      " by the user.\n"
      "Shut down the server and edit your config file "
      "so that newraw is replaced with raw.\n"
    );

    return DB_ERROR;
  }

  trx->m_op_info = "dropping table";

  /* The table name is prefixed with the database name and a '/'.
  Certain table names starting with 'innodb_' have their special
  meaning regardless of the database name.  Thus, we need to
  ignore the database name prefix in the comparisons. */
  table_name = strchr(name, '/');
  ut_a(table_name);
  table_name++;
  namelen = strlen(table_name) + 1;

  if (namelen == sizeof S_innodb_monitor && !memcmp(table_name, S_innodb_monitor, sizeof S_innodb_monitor)) {

    /* Table name equals "innodb_monitor":
    stop monitor prints */

    srv_print_innodb_monitor = false;
    srv_lock_sys->unset_print_lock_monitor();
  } else if (namelen == sizeof S_innodb_lock_monitor && !memcmp(table_name, S_innodb_lock_monitor, sizeof S_innodb_lock_monitor)) {
    srv_print_innodb_monitor = false;
    srv_lock_sys->unset_print_lock_monitor();
  } else if (namelen == sizeof S_innodb_tablespace_monitor && !memcmp(table_name, S_innodb_tablespace_monitor, sizeof S_innodb_tablespace_monitor)) {

    srv_print_innodb_tablespace_monitor = false;
  } else if (namelen == sizeof S_innodb_table_monitor && !memcmp(table_name, S_innodb_table_monitor, sizeof S_innodb_table_monitor)) {

    srv_print_innodb_table_monitor = false;
  }

  /* Serialize data dictionary operations with dictionary mutex:
  no deadlocks can occur then in these operations */

  if (trx->m_dict_operation_lock_mode != RW_X_LATCH) {
    return DB_SCHEMA_NOT_LOCKED;
  }

  ut_ad(mutex_own(&srv_dict_sys->m_mutex));

#ifdef UNIV_SYNC_DEBUG
  ut_ad(rw_lock_own(&srv_dict_sys->m_lock, RW_LOCK_EX));
#endif /* UNIV_SYNC_DEBUG */

  table = srv_dict_sys->table_get(name);

  if (table == nullptr) {
    err = DB_TABLE_NOT_FOUND;
    log_err(
      "Table ", name, " does not exist in the InnoDB internal data dictionary though the client is trying to drop it."
      " You can look for further help on the Embedded InnoDB website. Check the site for details"
    );
    goto func_exit;
  }

  /* Check if the table is referenced by foreign key constraints from
  some other table (not the table itself) */

  foreign = UT_LIST_GET_FIRST(table->m_referenced_list);

  while (foreign && foreign->m_foreign_table == table) {
  check_next_foreign:
    foreign = UT_LIST_GET_NEXT(m_referenced_list, foreign);
  }

  if (foreign && trx->m_check_foreigns && !(drop_db && srv_dict_sys->tables_have_same_db(name, foreign->m_foreign_table_name))) {
    /* We only allow dropping a referenced table if
    FOREIGN_KEY_CHECKS is set to 0 */

    err = DB_CANNOT_DROP_CONSTRAINT;

    mutex_enter(&srv_dict_sys->m_foreign_err_mutex);
    log_err(std::format("Cannot drop table {} because it is referenced by {}", name, foreign->m_foreign_table_name));
    mutex_exit(&srv_dict_sys->m_foreign_err_mutex);

    goto func_exit;
  }

  if (foreign && trx->m_check_foreigns) {
    goto check_next_foreign;
  }

  if (table->m_n_handles_opened > 0) {
    auto added = ddl_add_table_to_background_drop_list(table->m_name);

    if (added) {
      log_warn(std::format(
        "Client is trying to drop table {} : {} though there are still open handles to it."
        " Adding the table to the background drop queue.",
        table->m_id, table->m_name
      ));

      /* We return DB_SUCCESS though the drop will
      happen lazily later */
      err = DB_SUCCESS;
    } else {
      /* The table is already in the background drop list */
      err = DB_TABLESPACE_DELETED;
    }

    goto func_exit;
  }

  /* TODO: could we replace the counter n_foreign_key_checks_running
  with lock checks on the table? Acquire here an exclusive lock on the
  table, and rewrite lock0lock.c and the lock wait in srv0srv.c so that
  they can cope with the table having been dropped here? Foreign key
  checks take an IS or IX lock on the table. */

  if (table->m_n_foreign_key_checks_running > 0) {

    const char *table_name = table->m_name;
    auto added = ddl_add_table_to_background_drop_list(table_name);

    if (added) {
      log_warn(std::format(
        "You are trying to drop table {} : {} though there is a foreign key check running on it."
        " Adding the table to the background drop queue.",
        table->m_id, table->m_name
      ));

      /* We return DB_SUCCESS though the drop will happen lazily later */

      err = DB_SUCCESS;
    } else {
      /* The table is already in the background drop list */
      err = DB_TABLESPACE_DELETED;
    }

    goto func_exit;
  }

  /* Remove any locks there are on the table or its records */

  srv_lock_sys->remove_all_on_table(table, true);

  trx_set_dict_operation(trx, TRX_DICT_OP_TABLE);
  trx->table_id = table->m_id;

  /* We use the private SQL parser of Innobase to generate the
  query graphs needed in deleting the dictionary data from system
  tables in Innobase. Deleting a row from SYS_INDEXES table also
  frees the file segments of the B-tree associated with the index. */

  info = pars_info_create();

  pars_info_add_str_literal(info, "table_name", name);

  err = que_eval_sql(
    info,
    "PROCEDURE DROP_TABLE_PROC () IS\n"
    "sys_foreign_id CHAR;\n"
    "table_id CHAR;\n"
    "index_id CHAR;\n"
    "foreign_id CHAR;\n"
    "found INT;\n"
    "BEGIN\n"
    "SELECT ID INTO table_id\n"
    "FROM SYS_TABLES\n"
    "WHERE NAME = :table_name\n"
    "LOCK IN SHARE MODE;\n"
    "IF (SQL % NOTFOUND) THEN\n"
    "       RETURN;\n"
    "END IF;\n"
    "found := 1;\n"
    "SELECT ID INTO sys_foreign_id\n"
    "FROM SYS_TABLES\n"
    "WHERE NAME = 'SYS_FOREIGN'\n"
    "LOCK IN SHARE MODE;\n"
    "IF (SQL % NOTFOUND) THEN\n"
    "       found := 0;\n"
    "END IF;\n"
    "IF (:table_name = 'SYS_FOREIGN') THEN\n"
    "       found := 0;\n"
    "END IF;\n"
    "IF (:table_name = 'SYS_FOREIGN_COLS') THEN\n"
    "       found := 0;\n"
    "END IF;\n"
    "WHILE found = 1 LOOP\n"
    "       SELECT ID INTO foreign_id\n"
    "       FROM SYS_FOREIGN\n"
    "       WHERE FOR_NAME = :table_name\n"
    "               AND TO_BINARY(FOR_NAME)\n"
    "                 = TO_BINARY(:table_name)\n"
    "               LOCK IN SHARE MODE;\n"
    "       IF (SQL % NOTFOUND) THEN\n"
    "               found := 0;\n"
    "       ELSE\n"
    "               DELETE FROM SYS_FOREIGN_COLS\n"
    "               WHERE ID = foreign_id;\n"
    "               DELETE FROM SYS_FOREIGN\n"
    "               WHERE ID = foreign_id;\n"
    "       END IF;\n"
    "END LOOP;\n"
    "found := 1;\n"
    "WHILE found = 1 LOOP\n"
    "       SELECT ID INTO index_id\n"
    "       FROM SYS_INDEXES\n"
    "       WHERE TABLE_ID = table_id\n"
    "       LOCK IN SHARE MODE;\n"
    "       IF (SQL % NOTFOUND) THEN\n"
    "               found := 0;\n"
    "       ELSE\n"
    "               DELETE FROM SYS_FIELDS\n"
    "               WHERE INDEX_ID = index_id;\n"
    "               DELETE FROM SYS_INDEXES\n"
    "               WHERE ID = index_id\n"
    "               AND TABLE_ID = table_id;\n"
    "       END IF;\n"
    "END LOOP;\n"
    "DELETE FROM SYS_COLUMNS\n"
    "WHERE TABLE_ID = table_id;\n"
    "DELETE FROM SYS_TABLES\n"
    "WHERE ID = table_id;\n"
    "END;\n",
    false,
    trx
  );

  if (err != DB_SUCCESS) {

    if (err != DB_OUT_OF_FILE_SPACE) {
      log_err("Unexpected err: {}", (int) err);
      ut_error;
    }

    err = DB_MUST_GET_MORE_FILE_SPACE;

    ib_handle_errors(&err, trx, nullptr, nullptr);

    ut_error;
  } else {
    bool is_path;
    const char *name_or_path;

    auto heap = mem_heap_create(200);

    /* Clone the name, in case it has been allocated
    from table->heap, which will be freed by
    dict_table_remove_from_cache(table) below. */
    name = mem_heap_strdup(heap, name);
    space_id = table->m_space_id;

    if (table->m_dir_path_of_temp_table != nullptr) {
      is_path = true;
      name_or_path = mem_heap_strdup(heap, table->m_dir_path_of_temp_table);
    } else {
      is_path = false;
      name_or_path = name;
    }

    srv_dict_sys->table_remove_from_cache(table);

    // FIXME: srv_force_recovery should be passed in as an arg
    if (srv_dict_sys->m_loader.load_table(srv_config.m_force_recovery, name) != nullptr) {
      log_err(std::format("Not able to remove table {} from the dictionary cache", name));
      err = DB_ERROR;
    }

    /* Do not drop possible .ibd tablespace if something went
    wrong: we do not want to delete valuable data of the user */

    if (err == DB_SUCCESS && space_id != SYS_TABLESPACE) {
      if (!srv_fil->space_for_table_exists_in_mem(space_id, name_or_path, is_path, false, true)) {
        err = DB_SUCCESS;
        log_info(std::format("Removed {} from the internal data dictionary", name));
      } else if (!srv_fil->delete_tablespace(space_id)) {
        log_err(std::format("Unable to delete tablespace {} of table ", space_id, name));
        err = DB_ERROR;
      }
    }

    mem_heap_free(heap);
  }

func_exit:

  trx->m_op_info = "";

  InnoDB::wake_master_thread();

  return err;
}

/* Evaluates to true if str1 equals str2_onstack, used for comparing
the above strings. */
#define STR_EQ(str1, str1_len, str2_onstack) \
  ((str1_len) == sizeof(str2_onstack) && memcmp(str1, str2_onstack, sizeof(str2_onstack)) == 0)

db_err ddl_create_table(Table *table, trx_t *trx) {
  Table_node *node;
  mem_heap_t *heap;
  que_thr_t *thr;
  const char *table_name;
  ulint table_name_len;
  db_err err;
  ulint i;

  IF_SYNC_DEBUG(ut_ad(rw_lock_own(&srv_dict_sys->m_lock, RW_LOCK_EX));)

  ut_ad(mutex_own(&srv_dict_sys->m_mutex));
  ut_ad(trx->m_dict_operation_lock_mode == RW_X_LATCH);

  if (srv_config.m_created_new_raw) {
    log_err(
      "A new raw disk partition was initialized: we do not allow database modifications"
      " by the user. Shut down the database and edit your config file so that newraw is replaced with raw."
    );

    Table::destroy(table, Current_location());

    return DB_ERROR;

    /* The table name is prefixed with the database name and a '/'.
    Certain table names starting with 'innodb_' have their special
    meaning regardless of the database name.  Thus, we need to
    ignore the database name prefix in the comparisons. */
  } else if (strchr(table->m_name, '/') == nullptr) {
    log_err("Table {} not prefixed with a database name and '/'", table->m_name);
    Table::destroy(table, Current_location());

    return DB_ERROR;
  }

  trx->m_op_info = "creating table";

  /* Check that no reserved column names are used. */
  for (i = 0; i < table->get_n_user_cols(); i++) {
    if (Dict::col_name_is_reserved(table->get_col_name(i))) {
      Table::destroy(table, Current_location());
      return DB_ERROR;
    }
  }

  table_name = strchr(table->m_name, '/');
  ++table_name;
  table_name_len = strlen(table_name) + 1;

  if (STR_EQ(table_name, table_name_len, S_innodb_monitor)) {

    /* Table equals "innodb_monitor":
    start monitor prints */

    srv_print_innodb_monitor = true;

    /* The lock timeout monitor thread also takes care
    of InnoDB monitor prints */

    os_event_set(srv_lock_timeout_thread_event);
  } else if (STR_EQ(table_name, table_name_len, S_innodb_lock_monitor)) {

    srv_print_innodb_monitor = true;
    srv_lock_sys->set_print_lock_monitor();
    os_event_set(srv_lock_timeout_thread_event);
  } else if (STR_EQ(table_name, table_name_len, S_innodb_tablespace_monitor)) {

    srv_print_innodb_tablespace_monitor = true;
    os_event_set(srv_lock_timeout_thread_event);
  } else if (STR_EQ(table_name, table_name_len, S_innodb_table_monitor)) {

    srv_print_innodb_table_monitor = true;
    os_event_set(srv_lock_timeout_thread_event);
  } else if (STR_EQ(table_name, table_name_len, S_innodb_mem_validate)) {
    /* We define here a debugging feature intended for
    developers */

    log_info(
      "Validating InnoDB memory: to use this feature you must compile InnoDB with UNIV_MEM_DEBUG defined in innodb0types.h and"
      " the server must be quiet because allocation from a mem heap is not protected by any semaphore.");

#ifdef UNIV_MEM_DEBUG
    ut_a(mem_validate());
    log_info("Memory validated");
#else  /* UNIV_MEM_DEBUG */
    log_info("Memory NOT validated (recompile with UNIV_MEM_DEBUG)n");
#endif /* UNIV_MEM_DEBUG */
  }

  heap = mem_heap_create(512);

  trx_set_dict_operation(trx, TRX_DICT_OP_TABLE);

  node = Table_node::create(srv_dict_sys, table, heap, false);

  thr = pars_complete_graph_for_exec(node, trx, heap);

  auto tmp = que_fork_start_command(static_cast<que_fork_t *>(que_node_get_parent(thr)));
  ut_a(tmp == thr);

  que_run_threads(thr);

  err = trx->error_state;

  if (unlikely(err != DB_SUCCESS)) {
    trx->error_state = DB_SUCCESS;
  }

  switch (err) {
    case DB_OUT_OF_FILE_SPACE:
      ut_print_timestamp(ib_stream);
      ib_logger(ib_stream, "  Warning: cannot create table ");
      ut_print_name(table->m_name);
      ib_logger(ib_stream, " because tablespace full\n");

      if (srv_dict_sys->table_get(table->m_name)) {

        ddl_drop_table(table->m_name, trx, false);
      }
      break;

    case DB_DUPLICATE_KEY:
      log_err(std::format(
        "Table {} already exists in InnoDB internal data dictionary. You can look for further help on"
        " the Embedded InnoDB website",
        table->m_name
      ));

      /* We may also get err == DB_ERROR if the .ibd file for the table already exists */

      break;

    default:
      break;
  }

  que_graph_free((que_t *)que_node_get_parent(thr));

  trx->m_op_info = "";

  return err;
}

db_err ddl_create_index(Index *index, trx_t *trx) {
  db_err err;
  que_thr_t *thr;
  Index_node *node;
  mem_heap_t *heap;

#ifdef UNIV_SYNC_DEBUG
  ut_ad(rw_lock_own(&srv_dict_sys->m_lock, RW_LOCK_EX));
#endif /* UNIV_SYNC_DEBUG */
  ut_ad(mutex_own(&srv_dict_sys->m_mutex));

  /* This heap is destroyed when the query graph is freed. */
  heap = mem_heap_create(512);

  node = Index_node::create(srv_dict_sys, index, heap, false);
  thr = pars_complete_graph_for_exec(node, trx, heap);

  auto tmp = que_fork_start_command(static_cast<que_fork_t *>(que_node_get_parent(thr)));

  ut_a(thr == tmp);

  que_run_threads(thr);

  err = trx->error_state;

  que_graph_free((que_t *)que_node_get_parent(thr));

  return err;
}

db_err ddl_truncate_table(Table *table, trx_t *trx) {
  Foreign *foreign;
  enum db_err err;
  mem_heap_t *heap;
  byte *buf;
  DTuple *tuple;
  dfield_t *dfield;
  Index *sys_index;
  mtr_t mtr;
  uint64_t new_id;
  ulint recreate_space = 0;
  pars_info_t *info = nullptr;
  page_no_t root_page_no;
  Btree_pcursor pcur(srv_fsp, srv_btree_sys);

  /* How do we prevent crashes caused by ongoing operations on
  the table? Old operations could try to access non-existent
  pages.

  1) SQL queries, INSERT, SELECT, ...: we must get an exclusive
  table lock on the table before we can do TRUNCATE TABLE. Ensure
  there are no running queries on the table. This guarantee has
  to be provided by the SQL layer.

  2) Purge and rollback: we assign a new table id for the
  table. Since purge and rollback look for the table based on
  the table id, they see the table as 'dropped' and discard
  their operations.

  3) Insert buffer: TRUNCATE TABLE is analogous to DROP TABLE,
  so we do not have to remove insert buffer records, as the
  insert buffer works at a low level. If a freed page is later
  reallocated, the allocator will remove the ibuf entries for
  it.

  When we truncate *.ibd files by recreating them (analogous to
  DISCARD TABLESPACE), we remove all entries for the table in the
  insert buffer tree.  This is not strictly necessary, because
  in 6) we will assign a new tablespace identifier, but we can
  free up some space in the system tablespace.

  4) Linear readahead and random readahead: we use the same
  method as in 3) to discard ongoing operations. (This is only
  relevant for TRUNCATE TABLE by DISCARD TABLESPACE.)

  5) FOREIGN KEY operations: if
  table->n_foreign_key_checks_running > 0, we do not allow the
  TRUNCATE. We also reserve the data dictionary latch.

  6) Crash recovery: To prevent the application of pre-truncation
  redo log records on the truncated tablespace, we will assign
  a new tablespace identifier to the truncated tablespace. */

  if (srv_config.m_created_new_raw) {
    log_info(
      "A new raw disk partition was initialized: we do not allow database modifications"
      " by the user. Shut down server and edit config file so that newraw is replaced with raw."
    );

    return DB_ERROR;
  }

  trx->m_op_info = "truncating table";

  /* Serialize data dictionary operations with dictionary mutex:
  no deadlocks can occur then in these operations */
  ut_a(trx->m_dict_operation_lock_mode != 0);

  /* Prevent foreign key checks etc. while we are truncating the
  table */
  ut_ad(mutex_own(&srv_dict_sys->m_mutex));

#ifdef UNIV_SYNC_DEBUG
  ut_ad(rw_lock_own(&dict_operation_lock, RW_LOCK_EX));
#endif /* UNIV_SYNC_DEBUG */

  /* Check if the table is referenced by foreign key constraints from
  some other table (not the table itself) */

  foreign = UT_LIST_GET_FIRST(table->m_referenced_list);

  while (foreign && foreign->m_foreign_table == table) {
    foreign = UT_LIST_GET_NEXT(m_referenced_list, foreign);
  }

  if (foreign != nullptr && trx->m_check_foreigns) {
    /* We only allow truncating a referenced table if FOREIGN_KEY_CHECKS is set to 0 */

    mutex_enter(&srv_dict_sys->m_foreign_err_mutex);

    log_err(std::format(
      "Cannot truncate table {} by DROP+CREATE because it is referenced by {}",
      table->m_name,
      foreign->m_foreign_table_name
    ));

    mutex_exit(&srv_dict_sys->m_foreign_err_mutex);

    err = DB_ERROR;
    goto func_exit;
  }

  /* TODO: could we replace the counter n_foreign_key_checks_running
  with lock checks on the table? Acquire here an exclusive lock on the
  table, and rewrite lock0lock.c and the lock wait in srv0srv.c so that
  they can cope with the table having been truncated here? Foreign key
  checks take an IS or IX lock on the table. */

  if (table->m_n_foreign_key_checks_running > 0) {
    log_err(std::format(
      "Cannot truncate table {} by DROP+CREATE because there is a foreign key check running on it.",
      table->m_name
    ));

    err = DB_ERROR;

    goto func_exit;
  }

  /* Remove all locks except the table-level S and X locks. */
  srv_lock_sys->remove_all_on_table(table, false);

  trx->table_id = table->m_id;

  if (table->m_space_id && !table->m_dir_path_of_temp_table) {
    /* Discard and create the single-table tablespace. */
    auto space_id = table->m_space_id;
    ulint flags = srv_fil->space_get_flags(space_id);

    if (flags != ULINT_UNDEFINED && srv_fil->discard_tablespace(space_id)) {
      space_id = SYS_TABLESPACE;

      if (srv_fil->create_new_single_table_tablespace(&space_id, table->m_name, false, flags, FIL_IBD_FILE_INITIAL_SIZE) != DB_SUCCESS) {
        log_err(std::format("TRUNCATE TABLE {} failed to create a new tablespace", table->m_name));
        table->m_ibd_file_missing = true;
        err = DB_ERROR;
        goto func_exit;
      }

      recreate_space = space_id;

      /* Replace the space_id in the data dictionary cache.
      The persisent data dictionary (SYS_TABLES.SPACE
      and SYS_INDEXES.SPACE) are updated later in this
      function. */
      table->m_space_id = space_id;

      for (auto index : table->m_indexes) {
        index->m_page_id.m_space_id = space_id;
      }

      mtr.start();

      srv_fsp->header_init(space_id, FIL_IBD_FILE_INITIAL_SIZE, &mtr);

      mtr.commit();
    }
  }

  /* scan SYS_INDEXES for all indexes of the table */
  heap = mem_heap_create(800);

  tuple = dtuple_create(heap, 1);
  dfield = dtuple_get_nth_field(tuple, 0);

  buf = mem_heap_alloc(heap, 8);
  mach_write_to_8(buf, table->m_id);

  dfield_set_data(dfield, buf, 8);
  sys_index = srv_dict_sys->m_sys_indexes->get_first_index();
  sys_index->copy_types(tuple, 1);

  mtr.start();

  pcur.open_on_user_rec(sys_index, tuple, PAGE_CUR_GE, BTR_MODIFY_LEAF, &mtr, Current_location());

  for (;;) {
    rec_t *rec;
    const byte *field;
    ulint len;

    if (!pcur.is_on_user_rec()) {
      /* The end of SYS_INDEXES has been reached. */
      break;
    }

    rec = pcur.get_rec();

    field = rec_get_nth_field(rec, 0, &len);
    ut_ad(len == 8);

    if (memcmp(buf, field, len) != 0) {
      /* End of indexes for the table (TABLE_ID mismatch). */
      break;
    }

    if (rec_get_deleted_flag(rec)) {
      /* The index has been dropped. */
      goto next_rec;
    }

    /* This call may commit and restart mtr and reposition pcur. */
    root_page_no = srv_dict_sys->m_store.truncate_index_tree(table, recreate_space, &pcur, &mtr);

    rec = pcur.get_rec();

    if (root_page_no != FIL_NULL) {
      page_rec_write_index_page_no(rec, DICT_SYS_INDEXES_PAGE_NO_FIELD, root_page_no, &mtr);
      /* We will need to commit and restart the mini-transaction in order to avoid deadlocks.
      The dict_truncate_index_tree() call has allocated a page in this mini-transaction, and the rest of
      this loop could latch another index page. */
      mtr.commit();

      mtr.start();

      (void) pcur.restore_position(BTR_MODIFY_LEAF, &mtr, Current_location());
    }

  next_rec:
    (void) pcur.move_to_next_user_rec(&mtr);
  }

  pcur.close();
  mtr.commit();

  mem_heap_free(heap);

  new_id = srv_dict_sys->m_store.hdr_get_new_id(Dict_id_type::TABLE_ID);

  info = pars_info_create();

  pars_info_add_int4_literal(info, "space", (lint)table->m_space_id);
  pars_info_add_uint64_literal(info, "old_id", table->m_id);
  pars_info_add_uint64_literal(info, "new_id", new_id);

  err = que_eval_sql(
    info,
    "PROCEDURE RENUMBER_TABLESPACE_PROC () IS\n"
    "BEGIN\n"
    "UPDATE SYS_TABLES"
    " SET ID = :new_id, SPACE = :space\n"
    " WHERE ID = :old_id;\n"
    "UPDATE SYS_COLUMNS SET TABLE_ID = :new_id\n"
    " WHERE TABLE_ID = :old_id;\n"
    "UPDATE SYS_INDEXES"
    " SET TABLE_ID = :new_id, SPACE = :space\n"
    " WHERE TABLE_ID = :old_id;\n"
    "COMMIT WORK;\n"
    "END;\n",
    false,
    trx
  );

  if (err != DB_SUCCESS) {
    trx->error_state = DB_SUCCESS;
    trx_general_rollback(trx, false, nullptr);
    trx->error_state = DB_SUCCESS;
    ut_print_timestamp(ib_stream);
    ib_logger(ib_stream, "  Unable to assign a new identifier to table ");
    ut_print_name(table->m_name);
    ib_logger(
      ib_stream,
      "\n"
      "after truncating it.  Background processes"
      " may corrupt the table!\n"
    );
    err = DB_ERROR;
  } else {
    srv_dict_sys->table_change_id_in_cache(table, new_id);
  }

  srv_dict_sys->update_statistics(table);

func_exit:

  trx->m_op_info = "";

  InnoDB::wake_master_thread();

  return err;
}

db_err ddl_drop_index(Table *table, Index *index, trx_t *trx) {
  db_err err = DB_SUCCESS;
  pars_info_t *info = pars_info_create();

  /* We use the private SQL parser of Innobase to generate the
  query graphs needed in deleting the dictionary data from system
  tables in Innobase. Deleting a row from SYS_INDEXES table also
  frees the file segments of the B-tree associated with the index. */

  static const char str1[] =
    "PROCEDURE DROP_INDEX_PROC () IS\n"
    "BEGIN\n"
    /* Rename the index, so that it will be dropped by
      row_merge_drop_temp_indexes() at crash recovery
      if the server crashes before this trx is committed. */
    "UPDATE SYS_INDEXES SET NAME=CONCAT('" TEMP_INDEX_PREFIX_STR
    "', NAME) WHERE ID = :indexid;\n"
    "COMMIT WORK;\n"
    /* Drop the field definitions of the index. */
    "DELETE FROM SYS_FIELDS WHERE INDEX_ID = :indexid;\n"
    /* Drop the index definition and the B-tree. */
    "DELETE FROM SYS_INDEXES WHERE ID = :indexid;\n"
    "END;\n";

  ut_ad(index && table && trx);

  pars_info_add_uint64_literal(info, "indexid", index->m_id);

  (void) trx_start_if_not_started(trx);
  trx->m_op_info = "dropping index";

  ut_a(trx->m_dict_operation_lock_mode == RW_X_LATCH);

  err = que_eval_sql(info, str1, false, trx);

  ut_a(err == DB_SUCCESS);

  /* Replace this index with another equivalent index for all
  foreign key constraints on this table where this index is used */

  table->replace_index_in_foreign_list(index);
  srv_dict_sys->index_remove_from_cache(table, index);

  trx->m_op_info = "";
  return err;
}

/**
 * @brief Delete a single constraint.
 * 
 * @param[in] id Constraint id
 * @param[in] trx Transaction handle
 * 
 * @return Error code or DB_SUCCESS
 */
static db_err ddl_delete_constraint_low(const char *id, trx_t *trx) noexcept {
  pars_info_t *info = pars_info_create();

  pars_info_add_str_literal(info, "id", id);

  return que_eval_sql(
    info,
    "PROCEDURE DELETE_CONSTRAINT () IS\n"
    "BEGIN\n"
    "DELETE FROM SYS_FOREIGN_COLS WHERE ID = :id;\n"
    "DELETE FROM SYS_FOREIGN WHERE ID = :id;\n"
    "END;\n",
    false,
    trx
  );
}

/**
 * Delete a single constraint.
 *
 * @param[in] id constraint id
 * @param[in] database_name database name, with the trailing '/'
 * @param[in] heap memory heap
 * @param[in] trx transaction handle
 * 
 * @return error code or DB_SUCCESS
 */
static db_err ddl_delete_constraint(const char *id, const char *database_name, mem_heap_t *heap, trx_t *trx) noexcept {
  /* New format constraints have ids <databasename>/<constraintname>. */
  auto err = ddl_delete_constraint_low(mem_heap_strcat(heap, database_name, id), trx);

  if (err == DB_SUCCESS && !strchr(id, '/')) {
    /* Old format < 4.0.18 constraints have constraint ids
    <number>_<number>. We only try deleting them if the
    constraint name does not contain a '/' character, otherwise
    deleting a new format constraint named 'foo/bar' from
    database 'baz' would remove constraint 'bar' from database
    'foo', if it existed. */

    err = ddl_delete_constraint_low(id, trx);
  }

  return err;
}

db_err ddl_rename_table(const char *old_name, const char *new_name, trx_t *trx) {
  Table *table;
  db_err err = DB_ERROR;
  mem_heap_t *heap = nullptr;
  const char **constraints_to_drop = nullptr;
  ulint n_constraints_to_drop = 0;
  pars_info_t *info = nullptr;

  ut_a(old_name != nullptr);
  ut_a(new_name != nullptr);

  if (srv_config.m_created_new_raw || srv_config.m_force_recovery != IB_RECOVERY_DEFAULT) {
    log_err(
      "A new raw disk partition was initialized or innodb_force_recovery is on: we do not allow"
      " database modifications by the user. Shut down the server and ensure that newraw is replaced"
      " with raw, and innodb_force_... is removed."
    );

    goto func_exit;
  }

  trx->m_op_info = "renaming table";

  table = srv_dict_sys->table_get(old_name);

  if (!table) {
    err = DB_TABLE_NOT_FOUND;
    goto func_exit;
  } else if (table->m_ibd_file_missing) {
    err = DB_TABLE_NOT_FOUND;
    goto func_exit;
  }

  /* We use the private SQL parser of Innobase to generate the query
  graphs needed in updating the dictionary data from system tables. */

  info = pars_info_create();

  pars_info_add_str_literal(info, "new_table_name", new_name);
  pars_info_add_str_literal(info, "old_table_name", old_name);

  err = que_eval_sql(
    info,
    "PROCEDURE RENAME_TABLE () IS\n"
    "BEGIN\n"
    "UPDATE SYS_TABLES SET NAME = :new_table_name\n"
    " WHERE NAME = :old_table_name;\n"
    "END;\n",
    false,
    trx
  );

  if (err == DB_SUCCESS) {
    /* Rename all constraints. */

    info = pars_info_create();

    pars_info_add_str_literal(info, "new_table_name", new_name);
    pars_info_add_str_literal(info, "old_table_name", old_name);

    err = que_eval_sql(
      info,
      "PROCEDURE RENAME_CONSTRAINT_IDS () IS\n"
      "gen_constr_prefix CHAR;\n"
      "new_db_name CHAR;\n"
      "foreign_id CHAR;\n"
      "new_foreign_id CHAR;\n"
      "old_db_name_len INT;\n"
      "old_t_name_len INT;\n"
      "new_db_name_len INT;\n"
      "id_len INT;\n"
      "found INT;\n"
      "BEGIN\n"
      "found := 1;\n"
      "old_db_name_len := INSTR(:old_table_name, '/')-1;\n"
      "new_db_name_len := INSTR(:new_table_name, '/')-1;\n"
      "new_db_name := SUBSTR(:new_table_name, 0,\n"
      "                      new_db_name_len);\n"
      "old_t_name_len := LENGTH(:old_table_name);\n"
      "gen_constr_prefix := CONCAT(:old_table_name,\n"
      "                            '_ibfk_');\n"
      "WHILE found = 1 LOOP\n"
      "       SELECT ID INTO foreign_id\n"
      "        FROM SYS_FOREIGN\n"
      "        WHERE FOR_NAME = :old_table_name\n"
      "         AND TO_BINARY(FOR_NAME)\n"
      "           = TO_BINARY(:old_table_name)\n"
      "         LOCK IN SHARE MODE;\n"
      "       IF (SQL % NOTFOUND) THEN\n"
      "        found := 0;\n"
      "       ELSE\n"
      "        UPDATE SYS_FOREIGN\n"
      "        SET FOR_NAME = :new_table_name\n"
      "         WHERE ID = foreign_id;\n"
      "        id_len := LENGTH(foreign_id);\n"
      "        IF (INSTR(foreign_id, '/') > 0) THEN\n"
      "               IF (INSTR(foreign_id,\n"
      "                         gen_constr_prefix) > 0)\n"
      "               THEN\n"
      "                new_foreign_id :=\n"
      "                CONCAT(:new_table_name,\n"
      "                SUBSTR(foreign_id, old_t_name_len,\n"
      "                       id_len - old_t_name_len));\n"
      "               ELSE\n"
      "                new_foreign_id :=\n"
      "                CONCAT(new_db_name,\n"
      "                SUBSTR(foreign_id,\n"
      "                       old_db_name_len,\n"
      "                       id_len - old_db_name_len));\n"
      "               END IF;\n"
      "               UPDATE SYS_FOREIGN\n"
      "                SET ID = new_foreign_id\n"
      "                WHERE ID = foreign_id;\n"
      "               UPDATE SYS_FOREIGN_COLS\n"
      "                SET ID = new_foreign_id\n"
      "                WHERE ID = foreign_id;\n"
      "        END IF;\n"
      "       END IF;\n"
      "END LOOP;\n"
      "UPDATE SYS_FOREIGN SET REF_NAME = :new_table_name\n"
      "WHERE REF_NAME = :old_table_name\n"
      "  AND TO_BINARY(REF_NAME)\n"
      "    = TO_BINARY(:old_table_name);\n"
      "END;\n",
      false,
      trx
    );

  } else if (n_constraints_to_drop > 0) {
    /* Drop some constraints of tmp tables. */

    auto db_name_len = Dict::get_db_name_len(old_name) + 1;
    auto db_name = mem_heap_strdupl(heap, old_name, db_name_len);

    for (ulint i = 0; i < n_constraints_to_drop; i++) {
      err = ddl_delete_constraint(constraints_to_drop[i], db_name, heap, trx);

      if (err != DB_SUCCESS) {
        break;
      }
    }
  }

  if (err != DB_SUCCESS) {
    if (err == DB_DUPLICATE_KEY) {
      log_err(
        "possible reasons: 1) Table rename would cause two FOREIGN KEY constraints"
        "to have the same internal name  in case-insensitive comparison. trying"
        " to rename table. If table ", new_name, " is a temporary table, then it can be that"
        "there are still queries running on the table, and it will be dropped automatically"
        "when the queries end."
      );
    }
    trx->error_state = DB_SUCCESS;
    trx_general_rollback(trx, false, nullptr);
    trx->error_state = DB_SUCCESS;
  } else {
    /* The following call will also rename the .ibd data file if
    the table is stored in a single-table tablespace */

    if (!srv_dict_sys->table_rename_in_cache(table, new_name, true)) {
      trx->error_state = DB_SUCCESS;
      trx_general_rollback(trx, false, nullptr);
      trx->error_state = DB_SUCCESS;
      goto func_exit;
    }

    /* We only want to switch off some of the type checking in
    an ALTER, not in a RENAME. */

    err = srv_dict_sys->m_loader.load_foreigns(new_name, trx->m_check_foreigns);

    if (err != DB_SUCCESS) {
      bool ret;

      ut_print_timestamp(ib_stream);

      log_err(
        "RENAME TABLE table ", new_name, "is referenced in foreign key constraints which are not compatible"
        " with the new table definition.\n"
      );

      ret = srv_dict_sys->table_rename_in_cache(table, old_name, false);
      ut_a(ret);

      trx->error_state = DB_SUCCESS;
      trx_general_rollback(trx, false, nullptr);
      trx->error_state = DB_SUCCESS;
    }
  }

func_exit:

  if (likely_null(heap)) {
    mem_heap_free(heap);
  }

  trx->m_op_info = "";

  return err;
}

db_err ddl_rename_index(const char *table_name, const char *old_name, const char *new_name, trx_t *trx) {
  Table *table;
  pars_info_t *info = nullptr;
  db_err err = DB_ERROR;

  ut_a(old_name != nullptr);
  ut_a(old_name != nullptr);
  ut_a(table_name != nullptr);

  if (srv_config.m_created_new_raw || srv_config.m_force_recovery != IB_RECOVERY_DEFAULT) {
    ib_logger(
      ib_stream,
      "A new raw disk partition was initialized or\n"
      "innodb_force_recovery is on: we do not allow\n"
      "database modifications by the user. Shut down\n"
      "the server and ensure that newraw is replaced\n"
      "with raw, and innodb_force_... is removed.\n"
    );

    goto func_exit;
  }

  trx->m_op_info = "renaming index";

  table = srv_dict_sys->table_get(table_name);

  if (!table || table->m_ibd_file_missing) {
    err = DB_TABLE_NOT_FOUND;
    goto func_exit;
  }

  /* We use the private SQL parser of Innobase to generate the query
  graphs needed in updating the dictionary data from system tables. */

  info = pars_info_create();

  pars_info_add_str_literal(info, "table_name", table_name);
  pars_info_add_str_literal(info, "new_index_name", new_name);
  pars_info_add_str_literal(info, "old_index_name", old_name);

  err = que_eval_sql(
    info,
    "PROCEDURE RENAME_TABLE () IS\n"
    "table_id CHAR;\n"
    "BEGIN\n"
    "SELECT ID INTO table_id\n"
    " FROM SYS_TABLES\n"
    " WHERE NAME = :table_name\n"
    "LOCK IN SHARE MODE;\n"
    "IF (SQL % NOTFOUND) THEN\n"
    " RETURN;\n"
    "END IF;\n"
    "UPDATE SYS_INDEXES SET NAME = :new_index_name\n"
    " WHERE NAME = :old_index_name\n"
    "  AND table_id = table_id;\n"
    "END;\n",
    false,
    trx
  );

  if (err == DB_SUCCESS) {
    for (auto index : table->m_indexes) {

      /* FIXME: We are leaking memory here, well sort of, since the previous name allocation will not
      be freed till the index instance is destroyed. */
      if (strcasecmp(index->m_name, old_name) == 0) {
        index->m_name = mem_heap_strdup(index->m_heap, new_name);

        break;
      }

    }

  } else {
    trx->error_state = DB_SUCCESS;
    trx_general_rollback(trx, false, nullptr);
  }

func_exit:

  trx->m_op_info = "";

  return err;
}

/**
 * Drop all foreign keys in a database, see Bug#18942.
 *
 * @param name database name which ends to '/'
 * @param trx transaction handle
 * @return error code or DB_SUCCESS
 */
static db_err ddl_drop_all_foreign_keys_in_db(const char *name, trx_t *trx) noexcept {
  db_err err;

  ut_a(name[strlen(name) - 1] == '/');

  auto pinfo = pars_info_create();

  pars_info_add_str_literal(pinfo, "dbname", name);

/* true if for_name is not prefixed with dbname */
#define TABLE_NOT_IN_THIS_DB "SUBSTR(for_name, 0, LENGTH(:dbname)) <> :dbname"

  err = que_eval_sql(
    pinfo,
    "PROCEDURE DROP_ALL_FOREIGN_KEYS_PROC () IS\n"
    "foreign_id CHAR;\n"
    "for_name CHAR;\n"
    "found INT;\n"
    "DECLARE CURSOR cur IS\n"
    "SELECT ID, FOR_NAME FROM SYS_FOREIGN\n"
    "WHERE FOR_NAME >= :dbname\n"
    "LOCK IN SHARE MODE\n"
    "ORDER BY FOR_NAME;\n"
    "BEGIN\n"
    "found := 1;\n"
    "OPEN cur;\n"
    "WHILE found = 1 LOOP\n"
    "        FETCH cur INTO foreign_id, for_name;\n"
    "        IF (SQL % NOTFOUND) THEN\n"
    "                found := 0;\n"
    "        ELSIF (" TABLE_NOT_IN_THIS_DB
    ") THEN\n"
    "                found := 0;\n"
    "        ELSIF (1=1) THEN\n"
    "                DELETE FROM SYS_FOREIGN_COLS\n"
    "                WHERE ID = foreign_id;\n"
    "                DELETE FROM SYS_FOREIGN\n"
    "                WHERE ID = foreign_id;\n"
    "        END IF;\n"
    "END LOOP;\n"
    "CLOSE cur;\n"
    "END;\n",
    false, /* do not reserve dict mutex,
                            we are already holding it */
    trx
  );

  return err;
}

db_err ddl_drop_database(const char *name, trx_t *trx) {
  char *table_name;
  enum db_err err = DB_SUCCESS;
  ulint namelen = strlen(name);

  ut_a(name[namelen - 1] == '/');

  trx->m_op_info = "dropping database";

loop:
  srv_dict_sys->lock_data_dictionary(trx);

  while ((table_name = srv_dict_sys->m_loader.get_first_table_name_in_db(name)) != nullptr) {
    Table *table;

    ut_a(memcmp(table_name, name, namelen) == 0);

    table = srv_dict_sys->table_get(table_name);

    ut_a(table);

    /* Wait until the user does not have any queries running on
    the table */

    if (table->m_n_handles_opened > 0) {
      srv_dict_sys->unlock_data_dictionary(trx);

      log_warn("The client is trying to drop database ", name , " though there are still open handles to table ", table_name);

      os_thread_sleep(1000000);

      mem_free(table_name);

      goto loop;
    }

    err = ddl_drop_table(table_name, trx, true);

    if (err != DB_SUCCESS) {
      log_err("DROP DATABASE ", name , " failed with error ", err , " for table ", table_name);
      mem_free(table_name);
      break;
    }

    mem_free(table_name);
  }

  if (err == DB_SUCCESS) {
    /* After dropping all tables try to drop all leftover foreign keys in case orphaned ones exist */
    err = ddl_drop_all_foreign_keys_in_db(name, trx);

    if (err != DB_SUCCESS) {
      log_err("DROP DATABASE ", name , " failed with error ", err , " while dropping all foreign keys");
    }
  }

  srv_dict_sys->unlock_data_dictionary(trx);

  trx->m_op_info = "";

  return err;
}

void ddl_drop_all_temp_indexes(ib_recovery_t recovery) {
  Btree_pcursor pcur(srv_fsp, srv_btree_sys);

  /* Load the table definitions that contain partially defined indexes, so that the data dictionary
  information can be checked * when accessing the tablename.ibd files. */
  auto trx = trx_allocate_for_background();
  auto started = trx_start(trx, ULINT_UNDEFINED);
  ut_a(started);

  trx->m_op_info = "dropping partially created indexes";
  srv_dict_sys->lock_data_dictionary(trx);

  mtr_t mtr;

  mtr.start();

  pcur.open_at_index_side(true, srv_dict_sys->m_sys_indexes->get_clustered_index(), BTR_SEARCH_LEAF, true, 0, &mtr);

  for (;;) {
    const rec_t *rec;
    ulint len;
    const byte *field;
    Table *table;
    uint64_t table_id;

    (void) pcur.move_to_next_user_rec(&mtr);

    if (!pcur.is_on_user_rec()) {
      break;
    }

    rec = pcur.get_rec();
    field = rec_get_nth_field(rec, DICT_SYS_INDEXES_NAME_FIELD, &len);
    if (len == UNIV_SQL_NULL || len == 0 || mach_read_from_1(field) != (ulint)TEMP_INDEX_PREFIX) {
      continue;
    }

    /* This is a temporary index. */

    field = rec_get_nth_field(rec, 0 /*TABLE_ID*/, &len);
    if (len != 8) {
      /* Corrupted TABLE_ID */
      continue;
    }

    table_id = mach_read_from_8(field);

    pcur.store_position(&mtr);

    pcur.commit_specify_mtr(&mtr);

    table = srv_dict_sys->m_loader.load_table_on_id(recovery, table_id);

    if (table != nullptr) {
      for (auto index : table->m_indexes) {
        if (*index->m_name == TEMP_INDEX_PREFIX) {
          ddl_drop_index(table, index, trx);
          auto err_commit = trx_commit(trx);
          ut_a(err_commit == DB_SUCCESS);
        }
      }
    }

    mtr.start();
    (void) pcur.restore_position(BTR_SEARCH_LEAF, &mtr, Current_location());
  }

  pcur.close();

  mtr.commit();

  srv_dict_sys->unlock_data_dictionary(trx);

  auto err_commit = trx_commit(trx);
  ut_a(err_commit == DB_SUCCESS);
  trx_free_for_background(trx);
}

void ddl_drop_all_temp_tables(ib_recovery_t recovery) {
  auto trx = trx_allocate_for_background();

  {
    auto success = trx_start(trx, ULINT_UNDEFINED);
    ut_a(success);
  }

  trx->m_op_info = "dropping temporary tables";
  srv_dict_sys->lock_data_dictionary(trx);

  auto heap = mem_heap_create(200);

  mtr_t mtr;

  mtr.start();

  Btree_pcursor pcur(srv_fsp, srv_btree_sys);

  pcur.open_at_index_side(true, srv_dict_sys->m_sys_tables->get_clustered_index(), BTR_SEARCH_LEAF, true, 0, &mtr);

  for (;;) {
    const rec_t *rec;
    ulint len;
    const byte *field;
    Table *table;
    const char *table_name;

    (void) pcur.move_to_next_user_rec(&mtr);

    if (!pcur.is_on_user_rec()) {
      break;
    }

    rec = pcur.get_rec();
    field = rec_get_nth_field(rec, 4 /*N_COLS*/, &len);
    if (len != 4 || !(mach_read_from_4(field) & 0x80000000UL)) {
      continue;
    }

    /* Because this is not a ROW_FORMAT=REDUNDANT table,
    the is_temp flag is valid.  Examine it. */

    field = rec_get_nth_field(rec, 7 /*MIX_LEN*/, &len);
    if (len != 4 || !(mach_read_from_4(field) & DICT_TF2_TEMPORARY)) {
      continue;
    }

    /* This is a temporary table. */
    field = rec_get_nth_field(rec, 0 /*NAME*/, &len);
    if (len == UNIV_SQL_NULL || len == 0) {
      /* Corrupted SYS_TABLES.NAME */
      continue;
    }

    table_name = mem_heap_strdupl(heap, (const char *)field, len);

    pcur.store_position(&mtr);
    pcur.commit_specify_mtr(&mtr);

    table = srv_dict_sys->m_loader.load_table(recovery, table_name);

    if (table != nullptr) {
      ddl_drop_table(table_name, trx, false);
      auto err_commit = trx_commit(trx);
      ut_a(err_commit == DB_SUCCESS);
    }

    mtr.start();

    (void) pcur.restore_position(BTR_SEARCH_LEAF, &mtr, Current_location());
  }

  pcur.close();

  mtr.commit();

  mem_heap_free(heap);

  srv_dict_sys->unlock_data_dictionary(trx);

  auto err_commit = trx_commit(trx);
  ut_a(err_commit == DB_SUCCESS);

  trx_free_for_background(trx);
}
