/*********************************************************
(c) 2008 Oracle Corpn/Innobase Oy
Copyright (c) 2024 Sunny Bains. All rights reserved.


Created 12 Oct 2008.
*******************************************************/

#include "api0misc.h"
#include "btr0pcur.h"
#include "ddl0ddl.h"
#include "dict0dict.h"
#include "fil0fil.h"
#include "fsp0fsp.h"
#include "lock0lock.h"
#include "log0log.h"
#include "pars0pars.h"
#include "srv0srv.h"
#include "trx0roll.h"
#include "ut0lst.h"


/* Magic table names for invoking various monitor threads */
static const char S_innodb_monitor[] = "innodb_monitor";
static const char S_innodb_lock_monitor[] = "innodb_lock_monitor";
static const char S_innodb_tablespace_monitor[] = "innodb_tablespace_monitor";
static const char S_innodb_table_monitor[] = "innodb_table_monitor";
static const char S_innodb_mem_validate[] = "innodb_mem_validate";

db_err DDL::drop_table_in_background(const char *name) noexcept {
  auto trx = trx_allocate_for_background();

  auto started = trx_start(trx, ULINT_UNDEFINED);
  ut_a(started);

  /* If the original transaction was dropping a table referenced by
  foreign keys, we must set the following to be able to drop the
  table: */

  trx->m_check_foreigns = false;

  /* Try to drop the table in InnoDB */

  m_dict->lock_data_dictionary(trx);

  auto err = drop_table(name, trx, false);

  auto err_commit = trx_commit(trx);
  ut_a(err_commit == DB_SUCCESS);

  m_dict->unlock_data_dictionary(trx);

  m_log->buffer_flush_to_disk();

  trx_free_for_background(trx);

  return err;
}

ulint DDL::drop_tables_in_background() noexcept {
  ulint n_tables_dropped{};

  for (;;) {
    mutex_enter(&kernel_mutex);

    auto drop = m_drop_list.front();
    const auto n_tables = m_drop_list.size();

    mutex_exit(&kernel_mutex);

    if (drop == nullptr) {
      /* All tables dropped */
      return n_tables + n_tables_dropped;
    }

    m_dict->mutex_acquire();

    auto table = m_dict->table_get(drop->m_table_name);

    m_dict->mutex_release();

    if (table != nullptr) {
      if (drop_table_in_background(drop->m_table_name) != DB_SUCCESS) {
        /* If the DROP fails for some table, we return, and let the
        main thread retry later */

        return n_tables + n_tables_dropped;
      }

      ++n_tables_dropped;
    }

    mutex_enter(&kernel_mutex);

    m_drop_list.remove(drop);

    log_info(std::format("Dropped table {} in background drop queue.", drop->m_table_name));

    mem_free(drop->m_table_name);

    mem_free(drop);

    mutex_exit(&kernel_mutex);
  }
}

ulint DDL::get_background_drop_list_len() noexcept {
  ut_ad(mutex_own(&kernel_mutex));

  return m_drop_list.size();
}

bool DDL::add_table_to_background_drop_list(const char *name) noexcept {
  mutex_enter(&kernel_mutex);

  for (auto drop : m_drop_list) {
    if (strcmp(drop->m_table_name, name) == 0) {
      /* Already in the list */

      mutex_exit(&kernel_mutex);

      return false;
    }
  }

  auto drop = static_cast<Drop_list *>(mem_alloc(sizeof(Drop_list)));

  drop->m_table_name = mem_strdup(name);

  m_drop_list.push_back(drop);

  mutex_exit(&kernel_mutex);

  return true;
}

db_err DDL::drop_table(const char *in_name, trx_t *trx, bool drop_db) noexcept {
  ut_a(in_name != nullptr);

  if (srv_config.m_created_new_raw) {
    log_warn("A new raw disk partition was initialized: we do not allow database modifications"
      " by the user. Shut down the server and edit your config file so that newraw is replaced"
      " with raw."
    );

    return DB_ERROR;
  }

  trx->m_op_info = "dropping table";

  /* The table name is prefixed with the database name and a '/'.
  Certain table names starting with 'innodb_' have their special
  meaning regardless of the database name.  Thus, we need to
  ignore the database name prefix in the comparisons. */
  auto table_name = strchr(in_name, '/');
  ut_a(table_name != nullptr);

  ++table_name;

  const auto namelen = strlen(table_name) + 1;

  if (namelen == sizeof(S_innodb_monitor) && memcmp(table_name, S_innodb_monitor, sizeof S_innodb_monitor) == 0) {

    /* Table name equals "innodb_monitor": stop monitor prints */
    srv_print_innodb_monitor = false;
    m_dict->m_store.m_btree->m_lock_sys->unset_print_lock_monitor();

  } else if (namelen == sizeof(S_innodb_lock_monitor) && memcmp(table_name, S_innodb_lock_monitor, sizeof(S_innodb_lock_monitor)) == 0) {

    srv_print_innodb_monitor = false;
    m_dict->m_store.m_btree->m_lock_sys->unset_print_lock_monitor();

  } else if (namelen == sizeof(S_innodb_tablespace_monitor) && memcmp(table_name, S_innodb_tablespace_monitor, sizeof(S_innodb_tablespace_monitor)) == 0) {

    srv_print_innodb_tablespace_monitor = false;

  } else if (namelen == sizeof(S_innodb_table_monitor) && memcmp(table_name, S_innodb_table_monitor, sizeof(S_innodb_table_monitor)) == 0) {

    srv_print_innodb_table_monitor = false;
  }

  /* Serialize data dictionary operations with dictionary mutex:
  no deadlocks can occur then in these operations */

  if (trx->m_dict_operation_lock_mode != RW_X_LATCH) {
    return DB_SCHEMA_NOT_LOCKED;
  }

  ut_ad(mutex_own(&m_dict->m_mutex));

#ifdef UNIV_SYNC_DEBUG
  ut_ad(rw_lock_own(&m_dict->m_lock, RW_LOCK_EX));
#endif /* UNIV_SYNC_DEBUG */

  auto func_exit = [trx](db_err err) -> auto {
    trx->m_op_info = "";

    InnoDB::wake_master_thread();

    return err;
  };

  auto table = m_dict->table_get(in_name);

  if (table == nullptr) {
    log_err(
      "Table ", in_name, " does not exist in the InnoDB internal data dictionary though the client is trying to drop it."
      " You can look for further help on the Embedded InnoDB website. Check the site for details"
    );

    return func_exit(DB_TABLE_NOT_FOUND);
  }

  /* Check if the table is referenced by foreign key constraints from
  some other table (not the table itself) */

  for (auto foreign : table->m_referenced_list) {

    if (foreign->m_foreign_table != table && !trx->m_check_foreigns) {
      break;
    }

    if (drop_db && m_dict->tables_have_same_db(in_name, foreign->m_foreign_table_name)) {
      /* We only allow dropping a referenced table if FOREIGN_KEY_CHECKS is set to 0 */

      mutex_enter(&m_dict->m_foreign_err_mutex);

      log_err(std::format("Cannot drop table {} because it is referenced by {}", in_name, foreign->m_foreign_table_name));

      mutex_exit(&m_dict->m_foreign_err_mutex);

      return func_exit(DB_CANNOT_DROP_CONSTRAINT);
    }
  }

  if (table->m_n_handles_opened > 0) {
    auto added = add_table_to_background_drop_list(table->m_name);

    if (added) {
      log_warn(std::format(
        "Client is trying to drop table id: {} name: {} though there are still open handles to it."
        " Adding the table to the background drop queue.",
        table->m_id, table->m_name
      ));

      /* We return DB_SUCCESS though the drop will
      happen lazily later */
      return func_exit(DB_SUCCESS);
    } else {
      /* The table is already in the background drop list */
      return func_exit(DB_TABLESPACE_DELETED);
    }
  }

  /* TODO: could we replace the counter n_foreign_key_checks_running
  with lock checks on the table? Acquire here an exclusive lock on the
  table, and rewrite lock0lock.c and the lock wait in srv0srv.c so that
  they can cope with the table having been dropped here? Foreign key
  checks take an IS or IX lock on the table. */

  if (table->m_n_foreign_key_checks_running > 0) {

    auto table_name = table->m_name;
    auto added = add_table_to_background_drop_list(table_name);

    if (added) {
      log_warn(std::format(
        "You are trying to drop table id: {} name: {} though there is a foreign key check running on it."
        " Adding the table to the background drop queue.",
        table->m_id, table->m_name
      ));

      /* We return DB_SUCCESS though the drop will happen lazily later */

      return func_exit(DB_SUCCESS);
    } else {
      /* The table is already in the background drop list */
      return func_exit(DB_TABLESPACE_DELETED);
    }
  }

  /* Remove any locks there are on the table or its records */

  m_dict->m_store.m_btree->m_lock_sys->remove_all_on_table(table, true);

  trx_set_dict_operation(trx, TRX_DICT_OP_TABLE);
  trx->table_id = table->m_id;

  /* We use the private SQL parser of Innobase to generate the
  query graphs needed in deleting the dictionary data from system
  tables in Innobase. Deleting a row from SYS_INDEXES table also
  frees the file segments of the B-tree associated with the index. */

  auto info = pars_info_create();

  pars_info_add_str_literal(info, "table_name", in_name);

  auto err = que_eval_sql(
    info,
    "PROCEDURE DROP_TABLE_PROC () IS sys_foreign_id CHAR;\n"
    " table_id CHAR;\n"
    " index_id CHAR;\n"
    " foreign_id CHAR;\n"
    " found INT;\n"
    " BEGIN\n"
    "  SELECT ID INTO table_id FROM SYS_TABLES WHERE NAME = :table_name LOCK IN SHARE MODE;\n"
    "  IF (SQL % NOTFOUND) THEN\n"
    "   RETURN;\n"
    "  END IF;\n"
    "  found := 1;\n"
    "  SELECT ID INTO sys_foreign_id FROM SYS_TABLES WHERE NAME = 'SYS_FOREIGN' LOCK IN SHARE MODE;\n"
    "  IF (SQL % NOTFOUND) THEN\n"
    "    found := 0;\n"
    "  END IF;\n"
    "  IF (:table_name = 'SYS_FOREIGN') THEN\n"
    "    found := 0;\n"
    "  END IF;\n"
    "  IF (:table_name = 'SYS_FOREIGN_COLS') THEN\n"
    "    found := 0;\n"
    "  END IF;\n"
    "  WHILE found = 1 LOOP\n"
    "    SELECT ID INTO foreign_id\n"
    "      FROM SYS_FOREIGN\n"
    "      WHERE FOR_NAME = :table_name AND TO_BINARY(FOR_NAME) = TO_BINARY(:table_name)\n"
    "      LOCK IN SHARE MODE;\n"
    "    IF (SQL % NOTFOUND) THEN\n"
    "      found := 0;\n"
    "    ELSE\n"
    "      DELETE FROM SYS_FOREIGN_COLS WHERE ID = foreign_id;\n"
    "      DELETE FROM SYS_FOREIGN WHERE ID = foreign_id;\n"
    "    END IF;\n"
    "  END LOOP;\n"
    "  found := 1;\n"
    "  WHILE found = 1 LOOP\n"
    "    SELECT ID INTO index_id FROM SYS_INDEXES WHERE TABLE_ID = table_id LOCK IN SHARE MODE;\n"
    "    IF (SQL % NOTFOUND) THEN\n"
    "      found := 0;\n"
    "    ELSE\n"
    "      DELETE FROM SYS_FIELDS WHERE INDEX_ID = index_id;\n"
    "      DELETE FROM SYS_INDEXES WHERE ID = index_id AND TABLE_ID = table_id;\n"
    "    END IF;\n"
    "  END LOOP;\n"
    "  DELETE FROM SYS_COLUMNS WHERE TABLE_ID = table_id;\n"
    "  DELETE FROM SYS_TABLES WHERE ID = table_id;\n"
    "END;\n",
    false,
    trx
  );

  if (err != DB_SUCCESS) {

    if (err != DB_OUT_OF_FILE_SPACE) {
      log_fatal("Unexpected err: {}", (int) err);
    }

    err = DB_MUST_GET_MORE_FILE_SPACE;

    ib_handle_errors(&err, trx, nullptr, nullptr);

    ut_error;

  } else {
    const char *name_or_path;
    auto heap = mem_heap_create(200);

    /* Clone the name, in case it has been allocated from table->heap, which will be freed by
    dict_table_remove_from_cache(table) below. */
    auto name_copy = mem_heap_strdup(heap, in_name);
    const auto space_id = table->m_space_id;

    bool is_path;

    if (table->m_dir_path_of_temp_table != nullptr) {
      is_path = true;
      name_or_path = mem_heap_strdup(heap, table->m_dir_path_of_temp_table);
    } else {
      is_path = false;
      name_or_path = name_copy;
    }

    m_dict->table_remove_from_cache(table);

    if (m_dict->m_loader.load_table(srv_config.m_force_recovery, name_copy) != nullptr) {
      log_err(std::format("Not able to remove table id: {} name: {} from the dictionary cache", table->m_id, name_copy));
      err = DB_ERROR;
    }

    /* Do not drop possible .ibd tablespace if something went
    wrong: we do not want to delete valuable data of the user */

    if (err == DB_SUCCESS && space_id != SYS_TABLESPACE) {
      auto fil = m_dict->m_store.m_fsp->m_fil;

      if (!fil->space_for_table_exists_in_mem(space_id, name_or_path, is_path, false, true)) {
        err = DB_SUCCESS;
        log_info(std::format("Removed {} from the internal data dictionary", name_copy));
      } else if (!fil->delete_tablespace(space_id)) {
        log_err(std::format("Unable to delete tablespace {} of table ", space_id, name_copy));
        err = DB_ERROR;
      }
    }

    mem_heap_free(heap);
  }

  return func_exit(err);
}

/** Evaluates to true if str1 equals str2_onstack, used for comparing
 * the above strings. */
#define STR_EQ(str1, str1_len, str2_onstack) \
  ((str1_len) == sizeof(str2_onstack) && memcmp(str1, str2_onstack, sizeof(str2_onstack)) == 0)

db_err DDL::create_table(Table *table, trx_t *trx) noexcept {
  IF_SYNC_DEBUG(ut_ad(rw_lock_own(&m_dict->m_lock, RW_LOCK_EX));)

  ut_ad(mutex_own(&m_dict->m_mutex));
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
    log_err(std::format("Table id: {} name: {} not prefixed with a database name and '/'", table->m_id, table->m_name));
    Table::destroy(table, Current_location());

    return DB_ERROR;
  }

  trx->m_op_info = "creating table";

  /* Check that no reserved column names are used. */
  for (ulint i {}; i < table->get_n_user_cols(); ++i) {
    if (Dict::col_name_is_reserved(table->get_col_name(i))) {
      Table::destroy(table, Current_location());
      return DB_ERROR;
    }
  }

  auto table_name = strchr(table->m_name, '/');

  ++table_name;

  const auto table_name_len = strlen(table_name) + 1;

  if (STR_EQ(table_name, table_name_len, S_innodb_monitor)) {

    /* Table equals "innodb_monitor": start monitor prints */

    srv_print_innodb_monitor = true;

    /* The lock timeout monitor thread also takes care
    of InnoDB monitor prints */

    os_event_set(srv_lock_timeout_thread_event);

  } else if (STR_EQ(table_name, table_name_len, S_innodb_lock_monitor)) {

    srv_print_innodb_monitor = true;

    m_dict->m_store.m_btree->m_lock_sys->set_print_lock_monitor();

    os_event_set(srv_lock_timeout_thread_event);

  } else if (STR_EQ(table_name, table_name_len, S_innodb_tablespace_monitor)) {

    srv_print_innodb_tablespace_monitor = true;

    os_event_set(srv_lock_timeout_thread_event);

  } else if (STR_EQ(table_name, table_name_len, S_innodb_table_monitor)) {

    srv_print_innodb_table_monitor = true;

    os_event_set(srv_lock_timeout_thread_event);

  } else if (STR_EQ(table_name, table_name_len, S_innodb_mem_validate)) {
    /* We define here a debugging feature intended for developers */

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

  auto heap = mem_heap_create(512);

  trx_set_dict_operation(trx, TRX_DICT_OP_TABLE);

  auto node = Table_node::create(m_dict, table, heap, false);

  auto thr = pars_complete_graph_for_exec(node, trx, heap);

  auto tmp = que_fork_start_command(static_cast<que_fork_t *>(que_node_get_parent(thr)));
  ut_a(tmp == thr);

  que_run_threads(thr);

  auto err = trx->error_state;

  if (unlikely(err != DB_SUCCESS)) {
    trx->error_state = DB_SUCCESS;
  }

  switch (err) {
    case DB_OUT_OF_FILE_SPACE:
      log_warn("Cannot create table ", table->m_name);

      if (m_dict->table_get(table->m_name)) {

        auto err = drop_table(table->m_name, trx, false);

        if (err != DB_SUCCESS) {
          log_fatal(std::format("Failed to drop table {}", table->m_name));
        }
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

  que_graph_free(static_cast<que_t *>(que_node_get_parent(thr)));

  trx->m_op_info = "";

  return err;
}

db_err DDL::create_index(Index *index, trx_t *trx) noexcept{
#ifdef UNIV_SYNC_DEBUG
  ut_ad(rw_lock_own(&m_dict->m_lock, RW_LOCK_EX));
#endif /* UNIV_SYNC_DEBUG */
  ut_ad(mutex_own(&m_dict->m_mutex));

  /* This heap is destroyed when the query graph is freed. */
  auto heap = mem_heap_create(512);
  auto node = Index_node::create(m_dict, index, heap, false);
  auto thr = pars_complete_graph_for_exec(node, trx, heap);
  auto tmp = que_fork_start_command(static_cast<que_fork_t *>(que_node_get_parent(thr)));

  ut_a(thr == tmp);

  que_run_threads(thr);

  const auto err = trx->error_state;

  que_graph_free(static_cast<que_t *>(que_node_get_parent(thr)));

  return err;
}

db_err DDL::truncate_table(Table *table, trx_t *trx) noexcept{
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

  /* Prevent foreign key checks etc. while we are truncating the table */
  ut_ad(mutex_own(&m_dict->m_mutex));

#ifdef UNIV_SYNC_DEBUG
  ut_ad(rw_lock_own(&m_dict->m_lock, RW_LOCK_EX));
#endif /* UNIV_SYNC_DEBUG */

  auto func_exit = [trx](db_err err) -> auto {
    trx->m_op_info = "";
    InnoDB::wake_master_thread();

    return err;
  };

  /* Check if the table is referenced by foreign key constraints from
  some other table (not the table itself) */

  auto it = std::find_if(table->m_referenced_list.begin(), table->m_referenced_list.end(), [table](const Foreign *foreign) {
    return foreign->m_foreign_table == table;
  });

  if (it != table->m_referenced_list.end() && trx->m_check_foreigns) {
    auto foreign = *it;

    /* We only allow truncating a referenced table if FOREIGN_KEY_CHECKS is set to 0 */
    mutex_enter(&m_dict->m_foreign_err_mutex);

    log_err(std::format(
      "Cannot truncate table {} by DROP+CREATE because it is referenced by {}",
      table->m_name,
      foreign->m_foreign_table_name
    ));

    mutex_exit(&m_dict->m_foreign_err_mutex);

    return func_exit(DB_ERROR);
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

    return func_exit(DB_ERROR);
  }

  auto fsp = m_dict->m_store.m_fsp;
  space_id_t recreate_space{NULL_SPACE_ID};
  auto lock_sys = m_dict->m_store.m_btree->m_lock_sys;

  /* Remove all locks except the table-level S and X locks. */
  lock_sys->remove_all_on_table(table, false);

  trx->table_id = table->m_id;

  if (table->m_space_id > 0 && !table->m_dir_path_of_temp_table) {
    /* Discard and create the single-table tablespace. */
    auto space_id = table->m_space_id;
    const auto flags = fsp->m_fil->space_get_flags(space_id);

    if (flags != ULINT_UNDEFINED && fsp->m_fil->discard_tablespace(space_id)) {
      space_id = SYS_TABLESPACE;

      if (fsp->m_fil->create_new_single_table_tablespace(&space_id, table->m_name, false, flags, FIL_IBD_FILE_INITIAL_SIZE) != DB_SUCCESS) {
        log_err(std::format("TRUNCATE TABLE {} failed to create a new tablespace", table->m_name));
        table->m_ibd_file_missing = true;
        return func_exit(DB_ERROR);
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

      mtr_t mtr;

      mtr.start();

      fsp->header_init(space_id, FIL_IBD_FILE_INITIAL_SIZE, &mtr);

      mtr.commit();
    }
  }

  auto heap = mem_heap_create(800);
  auto buf = mem_heap_alloc(heap, 8);
  auto tuple = dtuple_create(heap, 1);
  auto dfield = dtuple_get_nth_field(tuple, 0);

  mach_write_to_8(buf, table->m_id);

  dfield_set_data(dfield, buf, 8);
  auto sys_index = m_dict->m_sys_indexes->get_first_index();
  sys_index->copy_types(tuple, 1);

  mtr_t mtr;

  mtr.start();

  Btree_pcursor pcur(fsp, m_dict->m_store.m_btree);

  /* Scan SYS_INDEXES for all indexes of the table */
  pcur.open_on_user_rec(sys_index, tuple, PAGE_CUR_GE, BTR_MODIFY_LEAF, &mtr, Current_location());

  while (pcur.is_on_user_rec()) {

    ulint len{ULINT_UNDEFINED};
    auto rec = pcur.get_rec();
    const auto field = rec_get_nth_field(rec, 0, &len);
    ut_ad(len == 8);

    if (memcmp(buf, field, len) != 0) {
      /* End of indexes for the table (TABLE_ID mismatch). */
      break;

    } else if (!rec_get_deleted_flag(rec)) {

      /* This call may commit and restart mtr and reposition pcur. */
      const auto root_page_no = m_dict->m_store.truncate_index_tree(table, recreate_space, &pcur, &mtr);

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
    }

    (void) pcur.move_to_next_user_rec(&mtr);
  }

  pcur.close();

  mtr.commit();

  mem_heap_free(heap);

  const auto new_id = m_dict->m_store.hdr_get_new_id(Dict_id_type::TABLE_ID);

  auto info = pars_info_create();

  pars_info_add_uint64_literal(info, "new_id", new_id);
  pars_info_add_uint64_literal(info, "old_id", table->m_id);
  pars_info_add_int4_literal(info, "space", (lint)table->m_space_id);

  auto err = que_eval_sql(
    info,
    "PROCEDURE RENUMBER_TABLESPACE_PROC () IS\n"
    "  BEGIN\n"
    "    UPDATE SYS_TABLES SET ID = :new_id, SPACE = :space WHERE ID = :old_id;\n"
    "    UPDATE SYS_COLUMNS SET TABLE_ID = :new_id WHERE TABLE_ID = :old_id;\n"
    "    UPDATE SYS_INDEXES SET TABLE_ID = :new_id, SPACE = :space WHERE TABLE_ID = :old_id;\n"
    "  COMMIT WORK;\n"
    "END;\n",
    false,
    trx
  );

  if (err != DB_SUCCESS) {

    trx->error_state = DB_SUCCESS;

    trx_general_rollback(trx, false, nullptr);

    trx->error_state = DB_SUCCESS;

    log_err("Unable to assign a new identifier to table ", table->m_name, "after truncating it. Background processes may corrupt the table!");

    err = DB_ERROR;

  } else {
    m_dict->table_change_id_in_cache(table, new_id);
  }

  m_dict->update_statistics(table);

  return func_exit(err);
}

db_err DDL::drop_index(Table *table, Index *index, trx_t *trx) noexcept {
  /* We use the private SQL parser of Innobase to generate the
  query graphs needed in deleting the dictionary data from system
  tables in Innobase. Deleting a row from SYS_INDEXES table also
  frees the file segments of the B-tree associated with the index. */

  static const char str1[] =
    "PROCEDURE DROP_INDEX_PROC () IS\n"
    "BEGIN\n"
    /* Rename the index, so that it will be dropped by row_merge_drop_temp_indexes() at crash recovery if the server crashes before this trx is committed. */
    "UPDATE SYS_INDEXES SET NAME=CONCAT('" TEMP_INDEX_PREFIX_STR "', NAME) WHERE ID = :indexid;\n"
    "COMMIT WORK;\n"
    /* Drop the field definitions of the index. */
    "DELETE FROM SYS_FIELDS WHERE INDEX_ID = :indexid;\n"
    /* Drop the index definition and the B-tree. */
    "DELETE FROM SYS_INDEXES WHERE ID = :indexid;\n"
    "END;\n";


  auto info = pars_info_create();

  pars_info_add_uint64_literal(info, "indexid", index->m_id);

  (void) trx_start_if_not_started(trx);
  trx->m_op_info = "dropping index";

  ut_a(trx->m_dict_operation_lock_mode == RW_X_LATCH);

  auto err = que_eval_sql(info, str1, false, trx);

  ut_a(err == DB_SUCCESS);

  /* Replace this index with another equivalent index for all
  foreign key constraints on this table where this index is used */

  table->replace_index_in_foreign_list(index);
  m_dict->index_remove_from_cache(table, index);

  trx->m_op_info = "";
  return err;
}

db_err DDL::delete_foreign_constraint(const char *id, trx_t *trx) noexcept {
  auto info = pars_info_create();

  pars_info_add_str_literal(info, "id", id);

  return que_eval_sql(
    info,
    "PROCEDURE DELETE_CONSTRAINT () IS\n"
    "BEGIN\n"
    "  DELETE FROM SYS_FOREIGN_COLS WHERE ID = :id;\n"
    "  DELETE FROM SYS_FOREIGN WHERE ID = :id;\n"
    "END;\n",
    false,
    trx
  );
}

db_err DDL::delete_constraint(const char *id, const char *database_name, mem_heap_t *heap, trx_t *trx) noexcept {
  /* New format constraints have ids <databasename>/<constraintname>. */
  auto err = delete_foreign_constraint(mem_heap_strcat(heap, database_name, id), trx);

  if (err == DB_SUCCESS && strchr(id, '/') == nullptr) {
    /* Old format < 4.0.18 constraints have constraint ids
    <number>_<number>. We only try deleting them if the
    constraint name does not contain a '/' character, otherwise
    deleting a new format constraint named 'foo/bar' from
    database 'baz' would remove constraint 'bar' from database
    'foo', if it existed. */

    err = delete_foreign_constraint(id, trx);
  }

  return err;
}

db_err DDL::rename_table(const char *old_name, const char *new_name, trx_t *trx) noexcept {
  ut_a(old_name != nullptr);
  ut_a(new_name != nullptr);

  auto func_exit = [&](db_err err) -> auto {
    trx->m_op_info = "";

    return err;
  };

  if (srv_config.m_created_new_raw || srv_config.m_force_recovery != IB_RECOVERY_DEFAULT) {
    log_err(
      "A new raw disk partition was initialized or innodb_force_recovery is on: we do not allow"
      " database modifications by the user. Shut down the server and ensure that newraw is replaced"
      " with raw, and innodb_force_... is removed."
    );

    return func_exit(DB_ERROR);
  }

  trx->m_op_info = "renaming table";

  auto table = m_dict->table_get(old_name);

  if (table == nullptr) {
    return func_exit(DB_TABLE_NOT_FOUND);
  } else if (table->m_ibd_file_missing) {
    return func_exit(DB_TABLE_NOT_FOUND);
  }

  /* We use the private SQL parser of Innobase to generate the query
  graphs needed in updating the dictionary data from system tables. */

  auto info = pars_info_create();

  pars_info_add_str_literal(info, "new_table_name", new_name);
  pars_info_add_str_literal(info, "old_table_name", old_name);

  auto err = que_eval_sql(
    info,
    "PROCEDURE RENAME_TABLE () IS\n"
    "BEGIN\n"
    "  UPDATE SYS_TABLES SET NAME = :new_table_name WHERE NAME = :old_table_name;\n"
    "END;\n",
    false,
    trx
  );

  if (err == DB_SUCCESS) {
    /* Rename all constraints. */

    auto info = pars_info_create();

    pars_info_add_str_literal(info, "new_table_name", new_name);
    pars_info_add_str_literal(info, "old_table_name", old_name);

    err = que_eval_sql(
      info,
      "PROCEDURE RENAME_CONSTRAINT_IDS () IS\n"
      "  gen_constr_prefix CHAR;\n"
      "  new_db_name CHAR;\n"
      "  foreign_id CHAR;\n"
      "  new_foreign_id CHAR;\n"
      "  old_db_name_len INT;\n"
      "  old_t_name_len INT;\n"
      "  new_db_name_len INT;\n"
      "  id_len INT;\n"
      "  found INT;\n"
      "BEGIN\n"
      "  found := 1;\n"
      "  old_db_name_len := INSTR(:old_table_name, '/')-1;\n"
      "  new_db_name_len := INSTR(:new_table_name, '/')-1;\n"
      "  new_db_name := SUBSTR(:new_table_name, 0, new_db_name_len);\n"
      "  old_t_name_len := LENGTH(:old_table_name);\n"
      "  gen_constr_prefix := CONCAT(:old_table_name, '_ibfk_');\n"
      "  WHILE found = 1 LOOP\n"
      "    SELECT ID INTO foreign_id\n"
      "      FROM SYS_FOREIGN\n"
      "      WHERE FOR_NAME = :old_table_name AND TO_BINARY(FOR_NAME) = TO_BINARY(:old_table_name)\n"
      "      LOCK IN SHARE MODE;\n"
      "    IF (SQL % NOTFOUND) THEN\n"
      "      found := 0;\n"
      "    ELSE\n"
      "      UPDATE SYS_FOREIGN SET FOR_NAME = :new_table_name WHERE ID = foreign_id;\n"
      "      id_len := LENGTH(foreign_id);\n"
      "      IF (INSTR(foreign_id, '/') > 0) THEN\n"
      "        IF (INSTR(foreign_id, gen_constr_prefix) > 0) THEN\n"
      "          new_foreign_id :=\n"
      "          CONCAT(:new_table_name, SUBSTR(foreign_id, old_t_name_len, id_len - old_t_name_len));\n"
      "        ELSE\n"
      "          new_foreign_id :=\n"
      "          CONCAT(new_db_name, SUBSTR(foreign_id, old_db_name_len, id_len - old_db_name_len));\n"
      "        END IF;\n"
      "        UPDATE SYS_FOREIGN SET ID = new_foreign_id WHERE ID = foreign_id;\n"
      "        UPDATE SYS_FOREIGN_COLS SET ID = new_foreign_id WHERE ID = foreign_id;\n"
      "          SET ID = new_foreign_id\n"
      "          WHERE ID = foreign_id;\n"
      "      END IF;\n"
      "    END IF;\n"
      "  END LOOP;\n"
      "  UPDATE SYS_FOREIGN SET REF_NAME = :new_table_name "
      "    WHERE REF_NAME = :old_table_name AND TO_BINARY(REF_NAME) = TO_BINARY(:old_table_name);\n"
      "END;\n",
      false,
      trx
    );

  }

  if (err != DB_SUCCESS) {
    if (err == DB_DUPLICATE_KEY) {
      log_err(
        "Duplicate key error while renaming table. Possible reasons:\n"
        "Table rename would cause two FOREIGN KEY constraints to have the"
        " same internal name in case-insensitive comparison. trying to"
        " rename table. If table ", new_name, " is a temporary table,"
        " then it can be that there are still queries running on the table,"
        " and it will be dropped automatically  when the queries end."
      );
    }

    trx->error_state = DB_SUCCESS;

    trx_general_rollback(trx, false, nullptr);

    trx->error_state = DB_SUCCESS;

  } else {
    /* The following call will also rename the .ibd data file if
    the table is stored in a single-table tablespace */

    if (!m_dict->table_rename_in_cache(table, new_name, true)) {
      trx->error_state = DB_SUCCESS;

      trx_general_rollback(trx, false, nullptr);

      trx->error_state = DB_SUCCESS;

      return func_exit(DB_ERROR);
    }

    /* We only want to switch off some of the type checking in
    an ALTER, not in a RENAME. */

    err = m_dict->m_loader.load_foreigns(new_name, trx->m_check_foreigns);

    if (err != DB_SUCCESS) {
      log_err(
        "RENAME TABLE table ", new_name, "is referenced in foreign key constraints which are not compatible"
        " with the new table definition.\n"
      );

      auto success = m_dict->table_rename_in_cache(table, old_name, false);
      ut_a(success);

      trx->error_state = DB_SUCCESS;

      trx_general_rollback(trx, false, nullptr);

      trx->error_state = DB_SUCCESS;
    }
  }

  return func_exit(err);
}

db_err DDL::rename_index(const char *table_name, const char *old_name, const char *new_name, trx_t *trx) noexcept {
  ut_a(old_name != nullptr);
  ut_a(old_name != nullptr);
  ut_a(table_name != nullptr);

  if (srv_config.m_created_new_raw || srv_config.m_force_recovery != IB_RECOVERY_DEFAULT) {
    log_err(
      "A new raw disk partition was initialized or innodb_force_recovery is on: we do not allow"
      " database modifications by the user. Shut down the server and ensure that newraw is"
      "replaced with raw, and innodb_force_... is removed."
    );

    trx->m_op_info = "";

    return DB_ERROR;
  }

  trx->m_op_info = "renaming index";

  auto table = m_dict->table_get(table_name);

  if (table == nullptr || table->m_ibd_file_missing) {
    trx->m_op_info = "";
    return DB_TABLE_NOT_FOUND;
  }

  /* We use the private SQL parser of Innobase to generate the query
  graphs needed in updating the dictionary data from system tables. */

  auto info = pars_info_create();

  pars_info_add_str_literal(info, "table_name", table_name);
  pars_info_add_str_literal(info, "new_index_name", new_name);
  pars_info_add_str_literal(info, "old_index_name", old_name);

  auto err = que_eval_sql(
    info,
    "PROCEDURE RENAME_TABLE () IS\n"
    "  table_id CHAR;\n"
    "BEGIN\n"
    "  SELECT ID INTO table_id\n"
    "  FROM SYS_TABLES\n"
    "  WHERE NAME = :table_name\n"
    "  LOCK IN SHARE MODE;\n"
    "  IF (SQL % NOTFOUND) THEN\n"
    "    RETURN;\n"
    "  END IF;\n"
    "  UPDATE SYS_INDEXES SET NAME = :new_index_name\n"
    "  WHERE NAME = :old_index_name AND table_id = table_id;\n"
    "END;\n",
    false,
    trx
  );

  if (err == DB_SUCCESS) {
    for (const auto index : table->m_indexes) {

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

  trx->m_op_info = "";

  return err;
}

db_err DDL::drop_all_foreign_keys_in_db(const char *name, trx_t *trx) noexcept {
  ut_a(name[strlen(name) - 1] == '/');

  auto info = pars_info_create();

  pars_info_add_str_literal(info, "dbname", name);

/* true if for_name is not prefixed with dbname */
#define TABLE_NOT_IN_THIS_DB "SUBSTR(for_name, 0, LENGTH(:dbname)) <> :dbname"

  auto err = que_eval_sql(
    info,
    "PROCEDURE DROP_ALL_FOREIGN_KEYS_PROC () IS\n"
    "  foreign_id CHAR;\n"
    "  for_name CHAR;\n"
    "  found INT;\n"
    "  DECLARE CURSOR cur IS\n"
    "    SELECT ID, FOR_NAME FROM SYS_FOREIGN\n"
    "    WHERE FOR_NAME >= :dbname\n"
    "    LOCK IN SHARE MODE\n"
    "    ORDER BY FOR_NAME;\n"
    "BEGIN\n"
    "  found := 1;\n"
    "  OPEN cur;\n"
    "  WHILE found = 1 LOOP\n"
    "    FETCH cur INTO foreign_id, for_name;\n"
    "    IF (SQL % NOTFOUND) THEN\n"
    "      found := 0;\n"
    "    ELSIF (" TABLE_NOT_IN_THIS_DB ") THEN\n"
    "      found := 0;\n"
    "    ELSIF (1=1) THEN\n"
    "      DELETE FROM SYS_FOREIGN_COLS\n"
    "      WHERE ID = foreign_id;\n"
    "      DELETE FROM SYS_FOREIGN\n"
    "      WHERE ID = foreign_id;\n"
    "    END IF;\n"
    "  END LOOP;\n"
    "  CLOSE cur;\n"
    "END;\n",
    false, /* Do not reserve dict mutex, we are already holding it */
    trx
  );

  return err;
}

db_err DDL::drop_database(const char *name, trx_t *trx) noexcept {
  const auto namelen = strlen(name);

  ut_a(name[namelen - 1] == '/');

  trx->m_op_info = "dropping database";

  db_err err{DB_SUCCESS};

  char *table_name;

  do {
    m_dict->lock_data_dictionary(trx);

    while ((table_name = m_dict->m_loader.get_first_table_name_in_db(name)) != nullptr) {
      ut_a(memcmp(table_name, name, namelen) == 0);

      auto table = m_dict->table_get(table_name);

      /* Wait until the user does not have any queries running on the table */

      if (table->m_n_handles_opened > 0) {
        m_dict->unlock_data_dictionary(trx);

        log_warn("The client is trying to drop database ", name , " though there are still open handles to table ", table_name);

        os_thread_sleep(1000000);

        mem_free(table_name);

        break;

      } else {

        auto err = drop_table(table_name, trx, true);

        if (err != DB_SUCCESS) {
          log_err("DROP DATABASE ", name , " failed with error ", err , " for table ", table_name);
          mem_free(table_name);
          table_name = nullptr;
        } else {
          mem_free(table_name);
          table_name = nullptr;

          /* After dropping all tables try to drop all leftover foreign keys in case orphaned ones exist */
          err = drop_all_foreign_keys_in_db(name, trx);

          if (err != DB_SUCCESS) {
            log_err("DROP DATABASE ", name , " failed with error ", err , " while dropping all foreign keys");
          }
        }
      }

      break;
    }

  } while (table_name != nullptr);

  m_dict->unlock_data_dictionary(trx);

  trx->m_op_info = "";
  return err;
}

void DDL::drop_all_temp_indexes(ib_recovery_t recovery) noexcept {
  Btree_pcursor pcur(m_dict->m_store.m_fsp, m_dict->m_store.m_btree);

  /* Load the table definitions that contain partially defined indexes, so that the data dictionary
  information can be checked * when accessing the tablename.ibd files. */
  auto trx = trx_allocate_for_background();
  auto started = trx_start(trx, ULINT_UNDEFINED);
  ut_a(started);

  trx->m_op_info = "dropping partially created indexes";
  m_dict->lock_data_dictionary(trx);

  mtr_t mtr;

  mtr.start();

  auto sys_clustered_index = m_dict->m_sys_tables->get_clustered_index();

  pcur.open_at_index_side(true, sys_clustered_index, BTR_SEARCH_LEAF, true, 0, &mtr);

  for (;;) {
    (void) pcur.move_to_next_user_rec(&mtr);

    if (!pcur.is_on_user_rec()) {
      break;
    }

    ulint len;
    const auto rec = pcur.get_rec();
    {
      const auto field = rec_get_nth_field(rec, DICT_SYS_INDEXES_NAME_FIELD, &len);

      if (len == UNIV_SQL_NULL || len == 0 || mach_read_from_1(field) != (ulint)TEMP_INDEX_PREFIX) {
        continue;
      }
    }

    /* This is a temporary index. */

    Dict_id table_id;

    {
      const auto field = rec_get_nth_field(rec, 0 /*TABLE_ID*/, &len);

      if (len != 8) {
        /* Corrupted TABLE_ID */
        continue;
      }

      table_id = mach_read_from_8(field);
    }

    pcur.store_position(&mtr);

    pcur.commit_specify_mtr(&mtr);

    auto table = m_dict->m_loader.load_table_on_id(recovery, table_id);

    if (table != nullptr) {

      for (auto index : table->m_indexes) {
        if (*index->m_name == TEMP_INDEX_PREFIX) {
          if (auto err = drop_index(table, index, trx); err != DB_SUCCESS) {
            log_warn("DROP DATABASE failed with error ", err , " while dropping temporaryindex ", index->m_name);
          }
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

  m_dict->unlock_data_dictionary(trx);

  auto err_commit = trx_commit(trx);
  ut_a(err_commit == DB_SUCCESS);

  trx_free_for_background(trx);
}

void DDL::drop_all_temp_tables(ib_recovery_t recovery) noexcept {
  auto trx = trx_allocate_for_background();

  {
    const auto success = trx_start(trx, ULINT_UNDEFINED);
    ut_a(success);
  }

  trx->m_op_info = "dropping temporary tables";
  m_dict->lock_data_dictionary(trx);

  auto heap = mem_heap_create(200);

  mtr_t mtr;

  mtr.start();

  Btree_pcursor pcur(m_dict->m_store.m_fsp, m_dict->m_store.m_btree);

  auto sys_clustered_index = m_dict->m_sys_tables->get_clustered_index();
  pcur.open_at_index_side(true, sys_clustered_index, BTR_SEARCH_LEAF, true, 0, &mtr);

  for (;;) {

    (void) pcur.move_to_next_user_rec(&mtr);

    if (!pcur.is_on_user_rec()) {
      break;
    }

    ulint len;
    const auto rec = pcur.get_rec();

    {
      const auto field = rec_get_nth_field(rec, 4 /*N_COLS*/, &len);

      if (len != 4 || !(mach_read_from_4(field) & 0x80000000UL)) {
        continue;
      }
    }

    {
      /* Check value of the MIX_LEN field. */
      const auto field = rec_get_nth_field(rec, 7 /*MIX_LEN*/, &len);

      if (len != 4 || !(mach_read_from_4(field) & DICT_TF2_TEMPORARY)) {
        continue;
      }
    }

    char *table_name;

    {
      /* This is a temporary table. */
      const auto field = rec_get_nth_field(rec, 0 /*NAME*/, &len);

      if (len == UNIV_SQL_NULL || len == 0) {
        /* Corrupted SYS_TABLES.NAME */
        continue;
      }

      table_name = mem_heap_strdupl(heap, reinterpret_cast<const char *>(field), len);
    }

    pcur.store_position(&mtr);
    pcur.commit_specify_mtr(&mtr);

    auto table = m_dict->m_loader.load_table(recovery, table_name);

    if (table != nullptr) {
      if (auto err = drop_table(table_name, trx, false); err != DB_SUCCESS) {
        log_warn("DROP DATABASE failed with error ", err , " while dropping table ", table_name);
      }
      const auto err_commit = trx_commit(trx);
      ut_a(err_commit == DB_SUCCESS);
    }

    mtr.start();

    (void) pcur.restore_position(BTR_SEARCH_LEAF, &mtr, Current_location());
  }

  pcur.close();

  mtr.commit();

  mem_heap_free(heap);

  m_dict->unlock_data_dictionary(trx);

  auto err_commit = trx_commit(trx);
  ut_a(err_commit == DB_SUCCESS);

  trx_free_for_background(trx);
}
