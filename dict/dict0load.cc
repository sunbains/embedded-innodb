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

/** @file dict/dict0load.c
Loads to the memory cache database object definitions
from dictionary tables

Created 4/24/1996 Heikki Tuuri
*******************************************************/

#include "btr0btr.h"
#include "btr0pcur.h"
#include "dict0dict.h"
#include "mach0data.h"
#include "page0page.h"
#include "rem0cmp.h"
#include "srv0srv.h"

bool Dict_load::name_of_col_is(const Table *table, const Index *index, ulint i, const char *name) const noexcept {
  ulint tmp = index->get_nth_field(i)->get_col()->get_no();

  return strcmp(name, table->get_col_name(tmp)) == 0;
}

char *Dict_load::get_first_table_name_in_db(const char *name) noexcept {
  ut_ad(mutex_own(&m_dict->m_mutex));
  auto fsp = m_dict->m_store.m_fsp;
  auto btree = m_dict->m_store.m_btree;
  Btree_pcursor pcur(fsp, btree);

  auto heap = mem_heap_create(1000);

  mtr_t mtr;

  mtr.start();

  auto sys_tables = m_dict->table_get("SYS_TABLES");
  auto sys_index = UT_LIST_GET_FIRST(sys_tables->m_indexes);

  auto tuple = dtuple_create(heap, 1);
  auto dfield = dtuple_get_nth_field(tuple, 0);

  dfield_set_data(dfield, name, strlen(name));
  sys_index->copy_types(tuple, 1);

  pcur.open_on_user_rec(sys_index, tuple, PAGE_CUR_GE, BTR_SEARCH_LEAF, &mtr, Current_location());

  for (;;) {
    const auto rec = pcur.get_rec();

    if (!pcur.is_on_user_rec()) {
      /* Not found */

      pcur.close();

      mtr.commit();

      mem_heap_free(heap);

      return (nullptr);
    }

    ulint len;
    const auto field = rec_get_nth_field(rec, 0, &len);

    if (len < strlen(name) || memcmp(name, field, strlen(name)) != 0) {
      /* Not found */

      pcur.close();

      mtr.commit();

      mem_heap_free(heap);

      return nullptr;
    }

    if (!rec_get_deleted_flag(rec)) {
      /* We found one */
      auto table_name = mem_strdupl(reinterpret_cast<char *>(field), len);

      pcur.close();

      mtr.commit();

      mem_heap_free(heap);

      return table_name;
    }

    (void)pcur.move_to_next_user_rec(&mtr);
  }
}

std::string Dict_load::to_string() noexcept {
  std::string str;
  auto fsp = m_dict->m_store.m_fsp;
  auto btree = m_dict->m_store.m_btree;
  Btree_pcursor pcur(fsp, btree);

  /* Enlarge the fatal semaphore wait timeout during the InnoDB table
  monitor printout */

  mutex_enter(&kernel_mutex);
  srv_fatal_semaphore_wait_threshold += 7200; /* 2 hours */
  mutex_exit(&kernel_mutex);

  m_dict->mutex_acquire();

  mtr_t mtr;

  mtr.start();

  auto sys_tables = m_dict->table_get("SYS_TABLES");
  auto sys_index = sys_tables->m_indexes.front();

  pcur.open_at_index_side(true, sys_index, BTR_SEARCH_LEAF, true, 0, &mtr);

  for (;;) {
    (void)pcur.move_to_next_user_rec(&mtr);

    const auto rec = pcur.get_rec();

    if (!pcur.is_on_user_rec()) {
      /* end of index */

      pcur.close();

      mtr.commit();

      m_dict->mutex_release();

      /* Restore the fatal semaphore wait timeout */

      mutex_enter(&kernel_mutex);
      srv_fatal_semaphore_wait_threshold -= 7200; /* 2 hours */
      mutex_exit(&kernel_mutex);

      return str;
    }

    ulint len;
    const auto field = rec_get_nth_field(rec, 0, &len);

    if (!rec_get_deleted_flag(rec)) {
      /* We found one */
      auto table_name = mem_strdupl(reinterpret_cast<char *>(field), len);

      pcur.store_position(&mtr);

      mtr.commit();

      auto table = m_dict->table_get(table_name);

      mem_free(table_name);

      if (table == nullptr) {
        std::string name{reinterpret_cast<const char *>(field), len};
        log_err("Failed to load table ", name);
      } else {
        /* The table definition was corrupt if there is no index */

        if (table->get_first_index()) {
          m_dict->update_statistics(table);
        }

        str += table->to_string(m_dict);
      }

      mtr.start();

      (void)pcur.restore_position(BTR_SEARCH_LEAF, &mtr, Current_location());
    }
  }

  ut_a(false);
}

ulint Dict_load::sys_tables_get_flags(const rec_t *rec) const noexcept {
  ulint len;

  auto field = rec_get_nth_field(rec, 5, &len);
  ut_a(len == 4);

  auto flags = mach_read_from_4(field);

  if (likely(flags == DICT_TABLE_ORDINARY)) {
    return 0;
  }

  field = rec_get_nth_field(rec, 4 /*N_COLS*/, &len);
  auto n_cols = mach_read_from_4(field);

  if (unlikely(!(n_cols & 0x80000000UL))) {
    /* New file formats require ROW_FORMAT=COMPACT. */
    return ULINT_UNDEFINED;
  }

  ut_a(flags == DICT_TF_FORMAT_V1);

  if (unlikely(flags & (~0UL << DICT_TF_BITS))) {
    /* Some unused bits are set. */
    return ULINT_UNDEFINED;
  } else {
    return flags;
  }
}

db_err Dict_load::open(bool in_crash_recovery) noexcept {
  space_id_t max_space_id{};
  auto fsp = m_dict->m_store.m_fsp;
  auto btree = m_dict->m_store.m_btree;
  Btree_pcursor pcur(fsp, btree);

  m_dict->mutex_acquire();

  load_system_tables();

  mtr_t mtr;

  mtr.start();

  auto sys_tables = m_dict->table_get("SYS_TABLES");
  auto sys_index = sys_tables->m_indexes.front();

  pcur.open_at_index_side(true, sys_index, BTR_SEARCH_LEAF, true, 0, &mtr);

  auto fil = fsp->m_fil;

  for (;;) {
    (void)pcur.move_to_next_user_rec(&mtr);

    const auto rec = pcur.get_rec();

    if (!pcur.is_on_user_rec()) {
      /* End of index */
      pcur.close();

      mtr.commit();

      /* We must make the tablespace cache aware of the biggest known space id */
      fil->set_max_space_id_if_bigger(max_space_id);

      m_dict->mutex_release();

      return DB_SUCCESS;

    } else if (!rec_get_deleted_flag(rec)) {
      /* We found one */
      ulint len;
      auto field = rec_get_nth_field(rec, 0, &len);
      auto name = mem_strdupl(reinterpret_cast<char *>(field), len);
      auto flags = sys_tables_get_flags(rec);

      if (flags == ULINT_UNDEFINED) {

        field = rec_get_nth_field(rec, 5, &len);
        flags = mach_read_from_4(field);

        std::string str{};

        str += std::format("Table {} in InnoDB data dictionary has unknown type {:x}.", name, flags);

        log_err(str);

      } else {

        field = rec_get_nth_field(rec, 9, &len);
        ut_a(len == 4);

        const auto space_id = mach_read_from_4(field);

        pcur.store_position(&mtr);

        mtr.commit();

        if (space_id == DICT_HDR_SPACE) {
          /* The system tablespace always exists. */
        } else if (in_crash_recovery) {
          /* Check that the tablespace (the .ibd file) really exists; print a warning
           if not. Do not print warnings for temporary tables. */
          field = rec_get_nth_field(rec, 4, &len);

          /* Don't support any other format than V1 */
          ut_a((mach_read_from_4(field) & 0x80000000UL) != 0);

          field = rec_get_nth_field(rec, 7, &len);
          auto is_temp = (mach_read_from_4(field) & DICT_TF2_TEMPORARY) != 0;

          fil->space_for_table_exists_in_mem(space_id, name, is_temp, true, !is_temp);

        } else {
          /* It is a normal database startup: create the space object and
          check that the .ibd file exists. */

          fil->open_single_table_tablespace(false, space_id, flags, name);
        }

        mem_free(name);

        if (space_id > max_space_id) {
          max_space_id = space_id;
        }

        mtr.start();

        (void)pcur.restore_position(BTR_SEARCH_LEAF, &mtr, Current_location());
      }
    }
  }
}

db_err Dict_load::load_columns(Table *table, mem_heap_t *heap) noexcept {
  auto fsp = m_dict->m_store.m_fsp;
  auto btree = m_dict->m_store.m_btree;
  Btree_pcursor pcur(fsp, btree);

  ut_ad(mutex_own(&m_dict->m_mutex));

  mtr_t mtr;

  mtr.start();

  auto sys_columns = m_dict->table_get("SYS_COLUMNS");
  auto sys_index = sys_columns->m_indexes.front();

  auto tuple = dtuple_create(heap, 1);
  auto dfield = dtuple_get_nth_field(tuple, 0);
  auto buf = static_cast<byte *>(mem_heap_alloc(heap, 8));

  mach_write_to_8(buf, table->m_id);

  dfield_set_data(dfield, buf, 8);

  sys_index->copy_types(tuple, 1);

  pcur.open_on_user_rec(sys_index, tuple, PAGE_CUR_GE, BTR_SEARCH_LEAF, &mtr, Current_location());

  for (ulint i{}; i + DATA_N_SYS_COLS < ulint(table->m_n_cols); i++) {

    const auto rec = pcur.get_rec();

    ut_a(pcur.is_on_user_rec());

    ut_a(!rec_get_deleted_flag(rec));

    ulint len;
    auto field = rec_get_nth_field(rec, 0, &len);
    ut_ad(len == 8);
    ut_a(table->m_id == mach_read_from_8(field));

    field = rec_get_nth_field(rec, 1, &len);
    ut_ad(len == 4);
    ut_a(i == mach_read_from_4(field));

    ut_a(name_of_col_is(sys_columns, sys_index, 4, "NAME"));

    field = rec_get_nth_field(rec, 4, &len);
    auto name = mem_heap_strdupl(heap, (char *)field, len);

    field = rec_get_nth_field(rec, 5, &len);
    auto mtype = mach_read_from_4(field);

    field = rec_get_nth_field(rec, 6, &len);
    auto prtype = mach_read_from_4(field);

    if (dtype_get_charset_coll(prtype) == 0 && dtype_is_string_type(mtype)) {
      /* The table was created with < 4.1.2. */

      if (dtype_is_binary_string_type(mtype, prtype)) {
        /* Use the binary collation for
        string columns of binary type. */

        prtype = dtype_form_prtype(prtype, DATA_CLIENT_BINARY_CHARSET_COLL);
      } else {
        /* Use the default charset for
        other than binary columns. */

        prtype = dtype_form_prtype(prtype, data_client_default_charset_coll);
      }
    }

    field = rec_get_nth_field(rec, 7, &len);
    auto col_len = mach_read_from_4(field);

    ut_a(name_of_col_is(sys_columns, sys_index, 8, "PREC"));

    table->add_col(name, mtype, prtype, col_len);

    (void)pcur.move_to_next_user_rec(&mtr);
  }

  pcur.close();

  mtr.commit();

  return DB_SUCCESS;
}

db_err Dict_load::load_fields(Index *index, mem_heap_t *heap) noexcept {
  ulint len;
  auto fsp = m_dict->m_store.m_fsp;
  auto btree = m_dict->m_store.m_btree;
  Btree_pcursor pcur(fsp, btree);

  ut_ad(mutex_own(&m_dict->m_mutex));

  mtr_t mtr;

  mtr.start();

  auto sys_fields = m_dict->table_get("SYS_FIELDS");
  auto sys_index = sys_fields->m_indexes.front();

  auto tuple = dtuple_create(heap, 1);
  auto dfield = dtuple_get_nth_field(tuple, 0);
  auto buf = static_cast<byte *>(mem_heap_alloc(heap, 8));

  mach_write_to_8(buf, index->m_id);

  dfield_set_data(dfield, buf, 8);
  sys_index->copy_types(tuple, 1);

  pcur.open_on_user_rec(sys_index, tuple, PAGE_CUR_GE, BTR_SEARCH_LEAF, &mtr, Current_location());

  for (ulint i{}; i < index->m_n_fields; ++i) {

    const auto rec = pcur.get_rec();

    ut_a(pcur.is_on_user_rec());

    /* There could be delete marked records in SYS_FIELDS
    because SYS_FIELDS.INDEX_ID can be updated
    by ALTER TABLE ADD INDEX. */

    if (rec_get_deleted_flag(rec)) {
      (void)pcur.move_to_next_user_rec(&mtr);
      continue;
    }

    auto field = rec_get_nth_field(rec, 0, &len);
    ut_ad(len == 8);

    field = rec_get_nth_field(rec, 1, &len);
    ut_a(len == 4);

    /* The next field stores the field position in the index
    and a possible column prefix length if the index field
    does not contain the whole column. The storage format is
    like this: if there is at least one prefix field in the index,
    then the HIGH 2 bytes contain the field number (== i) and the
    low 2 bytes the prefix length for the field. Otherwise the
    field number (== i) is contained in the 2 LOW bytes. */

    auto pos_and_prefix_len = mach_read_from_4(field);

    ut_a((pos_and_prefix_len & 0xFFFFUL) == i || (pos_and_prefix_len & 0xFFFF0000UL) == (i << 16));

    ulint prefix_len;

    if ((i == 0 && pos_and_prefix_len > 0) || (pos_and_prefix_len & 0xFFFF0000UL) > 0) {
      prefix_len = pos_and_prefix_len & 0xFFFFUL;
    } else {
      prefix_len = 0;
    }

    ut_a(name_of_col_is(sys_fields, sys_index, 4, "COL_NAME"));

    field = rec_get_nth_field(rec, 4, &len);

    (void)index->add_field(mem_heap_strdupl(heap, reinterpret_cast<char *>(field), len), prefix_len);

    (void)pcur.move_to_next_user_rec(&mtr);
  }

  pcur.close();

  mtr.commit();

  return DB_SUCCESS;
}

db_err Dict_load::load_indexes(Table *table, mem_heap_t *heap) noexcept {
  auto err{DB_SUCCESS};
  auto fsp = m_dict->m_store.m_fsp;
  auto btree = m_dict->m_store.m_btree;
  Btree_pcursor pcur(fsp, btree);

  ut_ad(mutex_own(&m_dict->m_mutex));

  const auto is_sys_table = table->m_id < DICT_HDR_FIRST_ID;

  mtr_t mtr;

  mtr.start();

  auto sys_indexes = m_dict->table_get("SYS_INDEXES");
  auto sys_index = sys_indexes->m_indexes.front();

  auto tuple = dtuple_create(heap, 1);
  auto dfield = dtuple_get_nth_field(tuple, 0);

  auto buf = static_cast<byte *>(mem_heap_alloc(heap, 8));
  mach_write_to_8(buf, table->m_id);

  dfield_set_data(dfield, buf, 8);
  sys_index->copy_types(tuple, 1);

  pcur.open_on_user_rec(sys_index, tuple, PAGE_CUR_GE, BTR_SEARCH_LEAF, &mtr, Current_location());

  auto cleanup = [&]() noexcept {
    pcur.close();
    mtr.commit();
  };

  for (;;) {
    if (!pcur.is_on_user_rec()) {
      break;
    }

    const auto rec = pcur.get_rec();

    ulint len;
    auto field = rec_get_nth_field(rec, 0, &len);
    ut_ad(len == 8);

    if (memcmp(buf, field, len) != 0) {
      break;
    } else if (rec_get_deleted_flag(rec)) {
      /* Skip delete marked records */
      (void)pcur.move_to_next_user_rec(&mtr);
      continue;
    }

    field = rec_get_nth_field(rec, 1, &len);
    ut_ad(len == 8);
    auto id = mach_read_from_8(field);

    ut_a(name_of_col_is(sys_indexes, sys_index, 4, "NAME"));

    ulint name_len;

    field = rec_get_nth_field(rec, 4, &name_len);
    const auto name_buf = mem_heap_strdupl(heap, reinterpret_cast<char *>(field), name_len);

    field = rec_get_nth_field(rec, 5, &len);
    const auto n_fields = mach_read_from_4(field);

    field = rec_get_nth_field(rec, 6, &len);
    const auto type = mach_read_from_4(field);

    field = rec_get_nth_field(rec, 7, &len);
    const space_id_t space = mach_read_from_4(field);

    ut_a(name_of_col_is(sys_indexes, sys_index, 8, "PAGE_NO"));

    field = rec_get_nth_field(rec, 8, &len);
    const page_no_t page_no = mach_read_from_4(field);

    /* We check for unsupported types first, so that the
    subsequent checks are relevant for the supported types. */
    if (type & ~(DICT_CLUSTERED | DICT_UNIQUE)) {

      log_err(std::format("Unknown type {} of index %s of table {}", type, name_buf, table->m_name));

      cleanup();

      return DB_UNSUPPORTED;

    } else if (page_no == NULL_PAGE_NO) {

      log_err(std::format("Trying to load index {} for table {} but the index tree has been freed!", name_buf, table->m_name));

      cleanup();

      return DB_CORRUPTION;

    } else if ((type & DICT_CLUSTERED) == 0 && table->get_first_index() == nullptr) {

      log_err(std::format("Trying to load index {} for table {} but the first index is not clustered!", name_buf, table->m_name));

      cleanup();

      return DB_CORRUPTION;

    } else if (is_sys_table && ((type & DICT_CLUSTERED) || (table == m_dict->m_sys_tables && name_len == sizeof("ID_IND") - 1 &&
                                                            memcmp(name_buf, "ID_IND", name_len) == 0))) {

      /* The index was created in memory already at booting of the database server */

    } else {
      auto index = Index::create(table, name_buf, Page_id{space, NULL_PAGE_NO}, type, n_fields);

      index->m_id = id;

      auto err = load_fields(index, heap);

      if (err != DB_SUCCESS) {
        cleanup();
        return err;
      }

      err = m_dict->index_add_to_cache(index, page_no, false);

      /* The data dictionary tables should never contain invalid index definitions.  If we ignored this error and
      simply did not load this index definition, the .frm file would disagree with the index definitions inside InnoDB. */
      if (unlikely(err != DB_SUCCESS)) {

        cleanup();

        return err;
      }
    }

    (void)pcur.move_to_next_user_rec(&mtr);
  }

  cleanup();

  return err;
}

Table *Dict_load::load_table(ib_recovery_t recovery, const char *name) noexcept {
  ut_ad(mutex_own(&m_dict->m_mutex));

  auto heap = mem_heap_create(32000);

  mtr_t mtr;

  mtr.start();

  auto sys_tables = m_dict->table_get("SYS_TABLES");
  auto sys_index = sys_tables->m_indexes.front();

  auto tuple = dtuple_create(heap, 1);
  auto dfield = dtuple_get_nth_field(tuple, 0);

  dfield_set_data(dfield, name, strlen(name));
  sys_index->copy_types(tuple, 1);

  Btree_pcursor pcur(m_dict->m_store.m_fsp, m_dict->m_store.m_btree);

  pcur.open_on_user_rec(sys_index, tuple, PAGE_CUR_GE, BTR_SEARCH_LEAF, &mtr, Current_location());

  const auto rec = pcur.get_rec();

  auto cleanup = [&]() noexcept {
    pcur.close();
    mtr.commit();
    mem_heap_free(heap);
  };

  if (!pcur.is_on_user_rec() || rec_get_deleted_flag(rec)) {
    /* Not found */
    cleanup();
    return nullptr;
  }

  ulint len;
  auto field = rec_get_nth_field(rec, 0, &len);

  /* Check if the table name in record is the searched one */
  if (len != strlen(name) || memcmp(name, field, len) != 0) {
    cleanup();
    return nullptr;
  }

  ut_a(name_of_col_is(sys_tables, sys_index, 9, "SPACE"));

  field = rec_get_nth_field(rec, 9, &len);
  space_id_t space = mach_read_from_4(field);

  ulint flags;

  /* Check if the tablespace exists and has the right name */
  if (space != DICT_HDR_SPACE) {
    flags = sys_tables_get_flags(rec);

    if (unlikely(flags == ULINT_UNDEFINED)) {
      const auto field = rec_get_nth_field(rec, 5, &len);

      flags = mach_read_from_4(field);
      log_err(std::format("Table {} in InnoDB data dictionary has unknown type {}", name, flags));
      cleanup();

      return nullptr;
    }

  } else {
    flags = 0;
  }

  ut_a(name_of_col_is(sys_tables, sys_index, 4, "N_COLS"));

  field = rec_get_nth_field(rec, 4, &len);

  bool ibd_file_missing{};
  auto fil = m_dict->m_store.m_fsp->m_fil;
  auto n_cols = mach_read_from_4(field);

  /* See if the tablespace is available. */
  if (space == DICT_HDR_SPACE) {
    /* The system tablespace is always available. */
  } else if (!fil->space_for_table_exists_in_mem(space, name, (flags >> DICT_TF2_SHIFT) & DICT_TF2_TEMPORARY, false, false)) {

    if ((flags >> DICT_TF2_SHIFT) & DICT_TF2_TEMPORARY) {
      /* Do not bother to retry opening temporary tables. */
      ibd_file_missing = true;
    } else {
      log_err(std::format("Table space of table {}, space id {} did not exist in memory. Retrying an open.", name, space));
      /* Try to open the tablespace */
      if (!fil->open_single_table_tablespace(true, space, flags & ~(~0UL << DICT_TF_BITS), name)) {
        /* We failed to find a sensible tablespace file */

        ibd_file_missing = true;
      }
    }
  }

  auto table = Table::create(name, space, n_cols & ~0x80000000UL, flags, ibd_file_missing, Current_location());

  ut_a(name_of_col_is(sys_tables, sys_index, 3, "ID"));

  field = rec_get_nth_field(rec, 3, &len);

  table->m_id = mach_read_from_8(field);

  pcur.close();

  mtr.commit();

  auto err = load_columns(table, heap);

  if (err != DB_SUCCESS) {
    return nullptr;
  }

  m_dict->table_add_to_cache(table);

  mem_heap_empty(heap);

  err = load_indexes(table, heap);

  /* If the force recovery flag is set, we open the table irrespective
  of the error condition, since the user may want to dump data from the
  clustered index. However we load the foreign key information only if
  all indexes were loaded. */
  if (err == DB_SUCCESS) {
    err = load_foreigns(table->m_name, true);
  } else if (recovery == IB_RECOVERY_DEFAULT) {
    m_dict->table_remove_from_cache(table);
    table = nullptr;
  }

#if 0
	if (err != DB_SUCCESS && table != nullptr) {

		mutex_enter(&m_dict->m_foreign_err_mutex);

		log_err(std::format(
      "Could not make a foreign key definition to match the foreign key table"
      " or the referenced table! The data dictionary of InnoDB is corrupt. You"
      " may need to drop and recreate the foreign key table or the referenced"
      " table. Latest foreign key error printout:\n{}", m_dict->m_foreign_err_buf));

		mutex_exit(&m_dict->m_foreign_err_mutex);
	}
#endif /* 0 */

  mem_heap_free(heap);

  return table;
}

Table *Dict_load::load_table_on_id(ib_recovery_t recovery, Dict_id table_id) noexcept {
  ut_ad(mutex_own(&m_dict->m_mutex));

  auto fsp = m_dict->m_store.m_fsp;
  auto btree = m_dict->m_store.m_btree;
  Btree_pcursor pcur(fsp, btree);

  /* NOTE that the operation of this function is protected by
  the dictionary mutex, and therefore no deadlocks can occur
  with other dictionary operations. */

  mtr_t mtr;

  mtr.start();

  /* Get the secondary index based on ID for table SYS_TABLES */
  auto sys_tables = m_dict->m_sys_tables;
  auto sys_table_ids = sys_tables->m_indexes.front()->get_next();
  auto heap = mem_heap_create(256);

  auto tuple = dtuple_create(heap, 1);
  auto dfield = dtuple_get_nth_field(tuple, 0);

  byte id_buf[sizeof(table_id)];

  /* Write the table id in byte format to id_buf */
  mach_write_to_8(id_buf, table_id);

  dfield_set_data(dfield, id_buf, 8);
  sys_table_ids->copy_types(tuple, 1);

  pcur.open_on_user_rec(sys_table_ids, tuple, PAGE_CUR_GE, BTR_SEARCH_LEAF, &mtr, Current_location());

  auto rec = pcur.get_rec();

  auto cleanup = [&]() noexcept {
    pcur.close();
    mtr.commit();
    mem_heap_free(heap);
  };

  if (!pcur.is_on_user_rec() || rec_get_deleted_flag(rec)) {
    /* Not found */
    cleanup();
    return nullptr;
  }

  /* Now we have the record in the secondary index containing the
  table ID and NAME */

  rec = pcur.get_rec();

  ulint len;
  auto field = rec_get_nth_field(rec, 0, &len);
  ut_ad(len == 8);

  /* Check if the table id in record is the one searched for */
  if (table_id != mach_read_from_8(field)) {
    cleanup();
    return nullptr;
  }

  /* Now we get the table name from the record */
  field = rec_get_nth_field(rec, 1, &len);
  /* Load the table definition to memory */
  auto table = load_table(recovery, mem_heap_strdupl(heap, reinterpret_cast<char *>(field), len));

  cleanup();

  return table;
}

db_err Dict_load::load_system_table(Table *table) noexcept {
  ut_ad(mutex_own(&m_dict->m_mutex));

  auto heap = mem_heap_create(1000);
  auto err = load_indexes(table, heap);

  if (err == DB_SUCCESS) {
    err = load_foreigns(table->m_name, true);

    if (err == DB_NOT_FOUND) {
      err = DB_SUCCESS;
    }
  }

  mem_heap_free(heap);

  return err;
}

db_err Dict_load::load_foreign_cols(const char *id, Foreign *foreign) noexcept {
  ut_ad(mutex_own(&m_dict->m_mutex));

  auto fsp = m_dict->m_store.m_fsp;
  auto btree = m_dict->m_store.m_btree;
  Btree_pcursor pcur(fsp, btree);

  foreign->m_foreign_col_names =
    reinterpret_cast<const char **>(mem_heap_alloc(foreign->m_heap, foreign->m_n_fields * sizeof(void *)));
  foreign->m_referenced_col_names =
    reinterpret_cast<const char **>(mem_heap_alloc(foreign->m_heap, foreign->m_n_fields * sizeof(void *)));

  mtr_t mtr;

  mtr.start();

  auto sys_foreign_cols = m_dict->table_get("SYS_FOREIGN_COLS");
  auto sys_index = sys_foreign_cols->m_indexes.front();

  auto tuple = dtuple_create(foreign->m_heap, 1);
  auto dfield = dtuple_get_nth_field(tuple, 0);

  dfield_set_data(dfield, id, strlen(id));
  sys_index->copy_types(tuple, 1);

  pcur.open_on_user_rec(sys_index, tuple, PAGE_CUR_GE, BTR_SEARCH_LEAF, &mtr, Current_location());

  for (ulint i{}; i < foreign->m_n_fields; ++i) {
    auto rec = pcur.get_rec();

    ut_a(pcur.is_on_user_rec());
    ut_a(!rec_get_deleted_flag(rec));

    ulint len;

    auto field = rec_get_nth_field(rec, 0, &len);
    ut_a(len == strlen(id));
    ut_a(memcmp(id, field, len) == 0);

    field = rec_get_nth_field(rec, 1, &len);
    ut_a(len == 4);
    ut_a(i == mach_read_from_4(field));

    field = rec_get_nth_field(rec, 4, &len);
    foreign->m_foreign_col_names[i] = mem_heap_strdupl(foreign->m_heap, (char *)field, len);

    field = rec_get_nth_field(rec, 5, &len);
    foreign->m_referenced_col_names[i] = mem_heap_strdupl(foreign->m_heap, (char *)field, len);

    (void)pcur.move_to_next_user_rec(&mtr);
  }

  pcur.close();

  mtr.commit();

  return DB_SUCCESS;
}

db_err Dict_load::load_foreign(const char *id, bool check_charsets) noexcept {
  ut_ad(mutex_own(&m_dict->m_mutex));

  auto heap2 = mem_heap_create(1000);
  auto fsp = m_dict->m_store.m_fsp;
  auto btree = m_dict->m_store.m_btree;
  Btree_pcursor pcur(fsp, btree);

  mtr_t mtr;

  mtr.start();

  auto sys_foreign = m_dict->table_get("SYS_FOREIGN");
  auto sys_index = sys_foreign->m_indexes.front();

  auto tuple = dtuple_create(heap2, 1);
  auto dfield = dtuple_get_nth_field(tuple, 0);

  dfield_set_data(dfield, id, strlen(id));
  sys_index->copy_types(tuple, 1);

  pcur.open_on_user_rec(sys_index, tuple, PAGE_CUR_GE, BTR_SEARCH_LEAF, &mtr, Current_location());
  auto rec = pcur.get_rec();

  auto cleanup = [&]() noexcept {
    pcur.close();
    mtr.commit();
    mem_heap_free(heap2);
  };

  if (!pcur.is_on_user_rec() || rec_get_deleted_flag(rec)) {
    /* Not found */

    log_err("A: cannot load foreign constraint ", id);

    cleanup();

    return DB_ERROR;
  }

  ulint len;
  auto field = rec_get_nth_field(rec, 0, &len);

  /* Check if the id in record is the searched one */
  if (len != strlen(id) || memcmp(id, field, len) != 0) {

    log_err("B: cannot load foreign constraint ", id);

    cleanup();

    return DB_ERROR;
  }

  /* Read the table names and the number of columns associated
  with the constraint */

  mem_heap_free(heap2);

  auto foreign = Foreign::create();
  auto n_fields_and_type = mach_read_from_4(rec_get_nth_field(rec, 5, &len));

  ut_a(len == 4);

  /* We store the type in the bits 24..29 of n_fields_and_type. */

  foreign->m_type = (unsigned int)(n_fields_and_type >> 24);
  foreign->m_n_fields = (unsigned int)(n_fields_and_type & 0x3FFUL);

  foreign->m_id = mem_heap_strdup(foreign->m_heap, id);

  field = rec_get_nth_field(rec, 3, &len);
  foreign->m_foreign_table_name = mem_heap_strdupl(foreign->m_heap, (char *)field, len);

  field = rec_get_nth_field(rec, 4, &len);
  foreign->m_referenced_table_name = mem_heap_strdupl(foreign->m_heap, (char *)field, len);

  pcur.close();

  mtr.commit();

  auto err = load_foreign_cols(id, foreign);

  if (err != DB_SUCCESS) {
    return err;
  }

  /* If the foreign table is not yet in the dictionary cache, we
  have to load it so that we are able to make type comparisons
  in the next function call. */

  (void)m_dict->table_get(foreign->m_foreign_table_name);

  /* Note that there may already be a foreign constraint object in
  the dictionary cache for this constraint: then the following
  call only sets the pointers in it to point to the appropriate table
  and index objects and frees the newly created object foreign.
  Adding to the cache should always succeed since we are not creating
  a new foreign key constraint but loading one from the data
  dictionary. */

  return m_dict->foreign_add_to_cache(foreign, check_charsets);
}

db_err Dict_load::load_foreigns(const char *table_name, bool check_charsets) noexcept {
  ut_ad(mutex_own(&m_dict->m_mutex));

  Btree_pcursor pcur(m_dict->m_store.m_fsp, m_dict->m_store.m_btree);

  auto sys_foreign = m_dict->table_get("SYS_FOREIGN");

  if (sys_foreign == nullptr) {
    /* No foreign keys defined yet in this database */
    log_info("No foreign key system table in the database");
    return DB_NOT_FOUND;
  }

  mtr_t mtr;

  /* Get the secondary index based on FOR_NAME from table SYS_FOREIGN */

  for (auto sec_index : sys_foreign->m_indexes) {
    auto heap = mem_heap_create(256);
    auto tuple = dtuple_create(heap, 1);
    auto dfield = dtuple_get_nth_field(tuple, 0);

    dfield_set_data(dfield, table_name, strlen(table_name));

    sec_index->copy_types(tuple, 1);

    mtr.start();

    pcur.open_on_user_rec(sec_index, tuple, PAGE_CUR_GE, BTR_SEARCH_LEAF, &mtr, Current_location());

    for (auto rec = pcur.get_rec();; !pcur.is_on_user_rec(), (void)pcur.move_to_next_user_rec(&mtr)) {

      /* Now we have the record in the secondary index containing a table
      name and a foreign constraint ID */

      ulint len;
      auto field = rec_get_nth_field(rec, 0, &len);

      const auto data = reinterpret_cast<const byte *>(dfield_get_data(dfield));
      const auto data_len = dfield_get_len(dfield);
      const auto mtype = dfield_get_type(dfield)->mtype;
      const auto prtype = dfield_get_type(dfield)->prtype;

      /* Check if the table name in the record is the one searched for; the following call
      does the comparison in the latin1_swedish_ci charset-collation, in a case-insensitive way. */

      if (cmp_data_data(nullptr, mtype, prtype, data, data_len, field, len) != 0) {
        break;
      }

      /* Since table names in SYS_FOREIGN are stored in a case-insensitive
      order, we have to check that the table name matches also in a binary
      string comparison. */

      /* FIXME: On Unix, allow table names that only differ in character case. */

      if (memcmp(field, table_name, len) != 0 || rec_get_deleted_flag(rec)) {
        continue;
      }

      /* Now we get a foreign key constraint id */
      field = rec_get_nth_field(rec, 1, &len);

      auto id = mem_heap_strdupl(heap, (char *)field, len);

      pcur.store_position(&mtr);

      mtr.commit();

      /* Load the foreign constraint definition to the dictionary cache */

      const auto err = load_foreign(id, check_charsets);

      if (err != DB_SUCCESS) {
        pcur.close();
        mem_heap_free(heap);

        return err;
      }

      mtr.start();

      (void)pcur.restore_position(BTR_SEARCH_LEAF, &mtr, Current_location());
    }

    mem_heap_free(heap);

    pcur.close();

    mtr.commit();
  }

  return DB_SUCCESS;
}

db_err Dict_load::load_system_tables() noexcept {
  ut_ad(mutex_own(&m_dict->m_mutex));

  mtr_t mtr;

  mtr.start();

  auto &store = m_dict->m_store;

  /* Get the dictionary header */
  auto hdr = store.hdr_get(&mtr);

  /* Because we only write new row ids to disk-based data structure (dictionary header)
  when it is divisible by DICT_HDR_ROW_ID_WRITE_MARGIN, in recovery we will not recover
  the latest value of the row id counter. Therefore we advance the counter at the database
  startup to avoid overlapping values. Note that when a user after database startup first
  time asks for a new row id, then because the counter is now divisible by ..._MARGIN,
  it will immediately be updated to the disk-based header. */

  store.m_row_id =
    ut_uint64_align_up(mtr.read_uint64(hdr + DICT_HDR_ROW_ID), DICT_HDR_ROW_ID_WRITE_MARGIN) + DICT_HDR_ROW_ID_WRITE_MARGIN;

  /* Insert into the dictionary cache the descriptions of the basic system tables */

  auto table = Table::create("SYS_TABLES", DICT_HDR_SPACE, 8, 0, false, Current_location());

  table->add_col("NAME", DATA_BINARY, 0, 0);
  table->add_col("ID", DATA_BINARY, 0, 0);

  /* ROW_FORMAT = (N_COLS >> 31) ? COMPACT : REDUNDANT */
  table->add_col("N_COLS", DATA_INT, 0, 4);

  /* TYPE is either DICT_TABLE_ORDINARY, or (TYPE & DICT_TF_COMPACT)
  and (TYPE & DICT_TF_FORMAT_MASK) are nonzero and TYPE = table->flags */
  table->add_col("TYPE", DATA_INT, 0, 4);
  table->add_col("MIX_ID", DATA_BINARY, 0, 0);

  /* MIX_LEN may contain additional table flags when
  ROW_FORMAT!=REDUNDANT.  Currently, these flags include
  DICT_TF2_TEMPORARY. */
  table->add_col("MIX_LEN", DATA_INT, 0, 4);
  table->add_col("CLUSTER_NAME", DATA_BINARY, 0, 0);
  table->add_col("SPACE", DATA_INT, 0, 4);

  table->m_id = DICT_TABLES_ID;

  m_dict->table_add_to_cache(table);

  m_dict->m_sys_tables = table;

  auto index = Index::create(table, "CLUST_IND", Page_id{DICT_HDR_SPACE, NULL_PAGE_NO}, DICT_UNIQUE | DICT_CLUSTERED, 1);

  (void)index->add_field("NAME", 0);

  index->m_id = DICT_TABLES_ID;

  auto err = m_dict->index_add_to_cache(index, mtr.read_ulint(hdr + DICT_HDR_TABLES, MLOG_4BYTES), false);
  ut_a(err == DB_SUCCESS);

  /*-------------------------*/
  index = Index::create(table, "ID_IND", Page_id{DICT_HDR_SPACE, NULL_PAGE_NO}, DICT_UNIQUE, 1);

  (void)index->add_field("ID", 0);

  index->m_id = DICT_TABLE_IDS_ID;

  err = m_dict->index_add_to_cache(index, mtr.read_ulint(hdr + DICT_HDR_TABLE_IDS, MLOG_4BYTES), false);
  ut_a(err == DB_SUCCESS);

  /*-------------------------*/
  table = Table::create("SYS_COLUMNS", DICT_HDR_SPACE, 7, 0, false, Current_location());

  table->add_col("TABLE_ID", DATA_BINARY, 0, 0);
  table->add_col("POS", DATA_INT, 0, 4);
  table->add_col("NAME", DATA_BINARY, 0, 0);
  table->add_col("MTYPE", DATA_INT, 0, 4);
  table->add_col("PRTYPE", DATA_INT, 0, 4);
  table->add_col("LEN", DATA_INT, 0, 4);
  table->add_col("PREC", DATA_INT, 0, 4);

  table->m_id = DICT_COLUMNS_ID;

  m_dict->table_add_to_cache(table);

  m_dict->m_sys_columns = table;

  index = Index::create(table, "CLUST_IND", Page_id{DICT_HDR_SPACE, NULL_PAGE_NO}, DICT_UNIQUE | DICT_CLUSTERED, 2);

  (void)index->add_field("TABLE_ID", 0);
  (void)index->add_field("POS", 0);

  index->m_id = DICT_COLUMNS_ID;

  err = m_dict->index_add_to_cache(index, mtr.read_ulint(hdr + DICT_HDR_COLUMNS, MLOG_4BYTES), false);
  ut_a(err == DB_SUCCESS);

  /*-------------------------*/
  table = Table::create("SYS_INDEXES", DICT_HDR_SPACE, 7, 0, false, Current_location());

  table->add_col("TABLE_ID", DATA_BINARY, 0, 0);
  table->add_col("ID", DATA_BINARY, 0, 0);
  table->add_col("NAME", DATA_BINARY, 0, 0);
  table->add_col("N_FIELDS", DATA_INT, 0, 4);
  table->add_col("TYPE", DATA_INT, 0, 4);
  table->add_col("SPACE", DATA_INT, 0, 4);
  table->add_col("PAGE_NO", DATA_INT, 0, 4);

  /* The '+ 2' below comes from the fields DB_TRX_ID, DB_ROLL_PTR */

  static_assert(DICT_SYS_INDEXES_PAGE_NO_FIELD == 6 + 2, "error DICT_SYS_INDEXES_PAGE_NO_FIELD != 6 + 2");

  static_assert(DICT_SYS_INDEXES_SPACE_NO_FIELD == 5 + 2, "rror DICT_SYS_INDEXES_SPACE_NO_FIELD != 5 + 2");

  static_assert(DICT_SYS_INDEXES_TYPE_FIELD == 4 + 2, "error DICT_SYS_INDEXES_TYPE_FIELD != 4 + 2");

  static_assert(DICT_SYS_INDEXES_NAME_FIELD == 2 + 2, "error DICT_SYS_INDEXES_NAME_FIELD != 2 + 2");

  table->m_id = DICT_INDEXES_ID;

  m_dict->table_add_to_cache(table);

  m_dict->m_sys_indexes = table;

  index = Index::create(table, "CLUST_IND", Page_id{DICT_HDR_SPACE, NULL_PAGE_NO}, DICT_UNIQUE | DICT_CLUSTERED, 2);

  (void)index->add_field("TABLE_ID", 0);
  (void)index->add_field("ID", 0);

  index->m_id = DICT_INDEXES_ID;

  err = m_dict->index_add_to_cache(index, mtr.read_ulint(hdr + DICT_HDR_INDEXES, MLOG_4BYTES), false);
  ut_a(err == DB_SUCCESS);

  /*-------------------------*/
  table = Table::create("SYS_FIELDS", DICT_HDR_SPACE, 3, 0, false, Current_location());

  table->add_col("INDEX_ID", DATA_BINARY, 0, 0);
  table->add_col("POS", DATA_INT, 0, 4);
  table->add_col("COL_NAME", DATA_BINARY, 0, 0);

  table->m_id = DICT_FIELDS_ID;

  m_dict->table_add_to_cache(table);

  m_dict->m_sys_fields = table;

  index = Index::create(table, "CLUST_IND", Page_id{DICT_HDR_SPACE, NULL_PAGE_NO}, DICT_UNIQUE | DICT_CLUSTERED, 2);

  (void)index->add_field("INDEX_ID", 0);
  (void)index->add_field("POS", 0);

  index->m_id = DICT_FIELDS_ID;

  err = m_dict->index_add_to_cache(index, mtr.read_ulint(hdr + DICT_HDR_FIELDS, MLOG_4BYTES), false);
  ut_a(err == DB_SUCCESS);

  mtr.commit();

  /* Load definitions of other indexes on system tables */
  err = load_system_table(m_dict->m_sys_tables);
  ut_a(err == DB_SUCCESS);

  err = load_system_table(m_dict->m_sys_columns);
  ut_a(err == DB_SUCCESS);

  err = load_system_table(m_dict->m_sys_indexes);
  ut_a(err == DB_SUCCESS);

  err = load_system_table(m_dict->m_sys_fields);
  ut_a(err == DB_SUCCESS);

  return DB_SUCCESS;
}