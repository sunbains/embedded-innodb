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

#include "dict0load.h"

#ifdef UNIV_NONINL
#include "dict0load.ic"
#endif

#include "btr0btr.h"
#include "btr0pcur.h"
#include "dict0boot.h"
#include "dict0dict.h"
#include "mach0data.h"
#include "page0page.h"
#include "rem0cmp.h"
#include "srv0srv.h"

/**
 * Compare the name of an index column.
 *
 * @param table The table.
 * @param index The index.
 * @param i The index field offset.
 * @param name The name to compare to.
 *
 * @return True if the i'th column of index is 'name'.
 */
static bool name_of_col_is(
  const dict_table_t *table,
  const dict_index_t *index,
  ulint i,
  const char *name
) {
  ulint tmp = dict_col_get_no(dict_field_get_col(dict_index_get_nth_field(index, i)));

  return strcmp(name, dict_table_get_col_name(table, tmp)) == 0;
}

char *dict_get_first_table_name_in_db(const char *name) {
  mtr_t mtr;
  ulint len;
  dict_table_t *sys_tables;
  dict_index_t *sys_index;
  dtuple_t *tuple;
  mem_heap_t *heap;
  dfield_t *dfield;
  const rec_t *rec;
  const byte *field;
  Btree_pcursor pcur(srv_fsp, srv_btree_sys, srv_lock_sys);

  ut_ad(mutex_own(&(dict_sys->mutex)));

  heap = mem_heap_create(1000);

  mtr.start();

  sys_tables = dict_table_get_low("SYS_TABLES");
  sys_index = UT_LIST_GET_FIRST(sys_tables->indexes);

  tuple = dtuple_create(heap, 1);
  dfield = dtuple_get_nth_field(tuple, 0);

  dfield_set_data(dfield, name, strlen(name));
  dict_index_copy_types(tuple, sys_index, 1);

  pcur.open_on_user_rec(sys_index, tuple, PAGE_CUR_GE, BTR_SEARCH_LEAF, &mtr, Source_location{});
loop:
  rec = pcur.get_rec();

  if (!pcur.is_on_user_rec()) {
    /* Not found */

    pcur.close();
    mtr.commit();
    mem_heap_free(heap);

    return (nullptr);
  }

  field = rec_get_nth_field(rec, 0, &len);

  if (len < strlen(name) || memcmp(name, field, strlen(name)) != 0) {
    /* Not found */

    pcur.close();
    mtr.commit();

    mem_heap_free(heap);

    return (nullptr);
  }

  if (!rec_get_deleted_flag(rec)) {

    /* We found one */

    char *table_name = mem_strdupl((char *)field, len);

    pcur.close();

    mtr.commit();

    mem_heap_free(heap);

    return (table_name);
  }

  (void) pcur.move_to_next_user_rec(&mtr);

  goto loop;
}

void dict_print(void) {
  dict_table_t *sys_tables;
  dict_index_t *sys_index;
  dict_table_t *table;
  const rec_t *rec;
  const byte *field;
  ulint len;
  mtr_t mtr;
  Btree_pcursor pcur(srv_fsp, srv_btree_sys, srv_lock_sys);

  /* Enlarge the fatal semaphore wait timeout during the InnoDB table
  monitor printout */

  mutex_enter(&kernel_mutex);
  srv_fatal_semaphore_wait_threshold += 7200; /* 2 hours */
  mutex_exit(&kernel_mutex);

  mutex_enter(&(dict_sys->mutex));

  mtr.start();

  sys_tables = dict_table_get_low("SYS_TABLES");
  sys_index = UT_LIST_GET_FIRST(sys_tables->indexes);

  pcur.open_at_index_side(true, sys_index, BTR_SEARCH_LEAF, true, 0, &mtr);

loop:
  (void) pcur.move_to_next_user_rec(&mtr);

  rec = pcur.get_rec();

  if (!pcur.is_on_user_rec()) {
    /* end of index */

    pcur.close();

    mtr.commit();

    mutex_exit(&(dict_sys->mutex));

    /* Restore the fatal semaphore wait timeout */

    mutex_enter(&kernel_mutex);
    srv_fatal_semaphore_wait_threshold -= 7200; /* 2 hours */
    mutex_exit(&kernel_mutex);

    return;
  }

  field = rec_get_nth_field(rec, 0, &len);

  if (!rec_get_deleted_flag(rec)) {

    /* We found one */

    char *table_name = mem_strdupl((char *)field, len);

    pcur.store_position(&mtr);

    mtr.commit();

    table = dict_table_get_low(table_name);
    mem_free(table_name);

    if (table == nullptr) {
      std::string name{reinterpret_cast<const char *>(field), len};
      ib_logger(ib_stream, "Failed to load table ");
      ut_print_name(name);
      ib_logger(ib_stream, "\n");
    } else {
      /* The table definition was corrupt if there
      is no index */

      if (dict_table_get_first_index(table)) {
        dict_update_statistics_low(table, true);
      }

      dict_table_print_low(table);
    }

    mtr.start();

    (void) pcur.restore_position(BTR_SEARCH_LEAF, &mtr, Source_location{});
  }

  goto loop;
}

/**
 * @brief Determine the flags of a table described in SYS_TABLES.
 * @param rec A record of SYS_TABLES.
 * @return Compressed page size in kilobytes; or 0 if the tablespace is uncompressed, ULINT_UNDEFINED on error.
 */
static ulint dict_sys_tables_get_flags(const rec_t *rec) {
  const byte *field;
  ulint len;
  ulint n_cols;
  ulint flags;

  field = rec_get_nth_field(rec, 5, &len);
  ut_a(len == 4);

  flags = mach_read_from_4(field);

  if (likely(flags == DICT_TABLE_ORDINARY)) {
    return 0;
  }

  field = rec_get_nth_field(rec, 4 /*N_COLS*/, &len);
  n_cols = mach_read_from_4(field);

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

void dict_check_tablespaces_and_store_max_id(bool in_crash_recovery) {
  dict_table_t *sys_tables;
  dict_index_t *sys_index;
  const rec_t *rec;
  ulint max_space_id = 0;
  mtr_t mtr;
  Btree_pcursor pcur(srv_fsp, srv_btree_sys, srv_lock_sys);

  mutex_enter(&(dict_sys->mutex));

  mtr.start();

  sys_tables = dict_table_get_low("SYS_TABLES");
  sys_index = UT_LIST_GET_FIRST(sys_tables->indexes);

  pcur.open_at_index_side(true, sys_index, BTR_SEARCH_LEAF, true, 0, &mtr);

loop:
  (void) pcur.move_to_next_user_rec(&mtr);

  rec = pcur.get_rec();

  if (!pcur.is_on_user_rec()) {
    /* end of index */

    pcur.close();

    mtr.commit();

    /* We must make the tablespace cache aware of the biggest
    known space id */

    /* printf("Biggest space id in data dictionary %lu\n",
    max_space_id); */
    srv_fil->set_max_space_id_if_bigger(max_space_id);

    mutex_exit(&(dict_sys->mutex));

    return;
  }

  if (!rec_get_deleted_flag(rec)) {

    /* We found one */
    const byte *field;
    ulint len;
    ulint space_id;
    ulint flags;
    char *name;

    field = rec_get_nth_field(rec, 0, &len);
    name = mem_strdupl((char *)field, len);

    flags = dict_sys_tables_get_flags(rec);
    if (flags == ULINT_UNDEFINED) {

      field = rec_get_nth_field(rec, 5, &len);
      flags = mach_read_from_4(field);

      ut_print_timestamp(ib_stream);
      ib_logger(ib_stream, "  Error: table ");
      ut_print_filename(name);
      ib_logger(
        ib_stream,
        "\n"
        "in InnoDB data dictionary"
        " has unknown type %lx.\n",
        (ulong)flags
      );

      goto loop;
    }

    field = rec_get_nth_field(rec, 9, &len);
    ut_a(len == 4);

    space_id = mach_read_from_4(field);

    pcur.store_position(&mtr);

    mtr.commit();

    if (space_id == 0) {
      /* The system tablespace always exists. */
    } else if (in_crash_recovery) {
      /* Check that the tablespace (the .ibd file) really
      exists; print a warning to the .err log if not.
      Do not print warnings for temporary tables. */
      bool is_temp;

      field = rec_get_nth_field(rec, 4, &len);
      if (0x80000000UL & mach_read_from_4(field)) {
        /* ROW_FORMAT=COMPACT: read the is_temp
        flag from SYS_TABLES.MIX_LEN. */
        field = rec_get_nth_field(rec, 7, &len);
        is_temp = mach_read_from_4(field) & DICT_TF2_TEMPORARY;
      } else {
        /* For tables created with old versions
        of InnoDB, SYS_TABLES.MIX_LEN may contain
        garbage.  Such tables would always be
        in ROW_FORMAT=REDUNDANT.  Pretend that
        all such tables are non-temporary.  That is,
        do not suppress error printouts about
        temporary tables not being found. */
        is_temp = false;
      }

      srv_fil->space_for_table_exists_in_mem(space_id, name, is_temp, true, !is_temp);
    } else {
      /* It is a normal database startup: create the space
      object and check that the .ibd file exists. */

      srv_fil->open_single_table_tablespace(false, space_id, flags, name);
    }

    mem_free(name);

    if (space_id > max_space_id) {
      max_space_id = space_id;
    }

    mtr.start();

    (void) pcur.restore_position(BTR_SEARCH_LEAF, &mtr, Source_location{});
  }

  goto loop;
}

/**
 * @brief Loads definitions for table columns.
 * @param table The table.
 * @param heap The memory heap for temporary storage.
 */
static void dict_load_columns(dict_table_t *table, mem_heap_t *heap) {
  dict_table_t *sys_columns;
  dict_index_t *sys_index;
  dtuple_t *tuple;
  dfield_t *dfield;
  const rec_t *rec;
  const byte *field;
  ulint len;
  byte *buf;
  char *name;
  ulint mtype;
  ulint prtype;
  ulint col_len;
  ulint i;
  mtr_t mtr;
  Btree_pcursor pcur(srv_fsp, srv_btree_sys, srv_lock_sys);

  ut_ad(mutex_own(&(dict_sys->mutex)));

  mtr.start();

  sys_columns = dict_table_get_low("SYS_COLUMNS");
  sys_index = UT_LIST_GET_FIRST(sys_columns->indexes);

  tuple = dtuple_create(heap, 1);
  dfield = dtuple_get_nth_field(tuple, 0);

  buf = (byte *)mem_heap_alloc(heap, 8);
  mach_write_to_8(buf, table->id);

  dfield_set_data(dfield, buf, 8);
  dict_index_copy_types(tuple, sys_index, 1);

  pcur.open_on_user_rec(sys_index, tuple, PAGE_CUR_GE, BTR_SEARCH_LEAF, &mtr, Source_location{});

  for (i = 0; i + DATA_N_SYS_COLS < (ulint)table->n_cols; i++) {

    rec = pcur.get_rec();

    ut_a(pcur.is_on_user_rec());

    ut_a(!rec_get_deleted_flag(rec));

    field = rec_get_nth_field(rec, 0, &len);
    ut_ad(len == 8);
    ut_a(table->id == mach_read_from_8(field));

    field = rec_get_nth_field(rec, 1, &len);
    ut_ad(len == 4);
    ut_a(i == mach_read_from_4(field));

    ut_a(name_of_col_is(sys_columns, sys_index, 4, "NAME"));

    field = rec_get_nth_field(rec, 4, &len);
    name = mem_heap_strdupl(heap, (char *)field, len);

    field = rec_get_nth_field(rec, 5, &len);
    mtype = mach_read_from_4(field);

    field = rec_get_nth_field(rec, 6, &len);
    prtype = mach_read_from_4(field);

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
    col_len = mach_read_from_4(field);

    ut_a(name_of_col_is(sys_columns, sys_index, 8, "PREC"));

    dict_mem_table_add_col(table, heap, name, mtype, prtype, col_len);
    (void) pcur.move_to_next_user_rec(&mtr);
  }

  pcur.close();

  mtr.commit();
}

/**
 * @brief Loads definitions for index fields.
 * @param index The index whose fields to load.
 * @param heap The memory heap for temporary storage.
 */
static void dict_load_fields(dict_index_t *index, mem_heap_t *heap) {
  dict_table_t *sys_fields;
  dict_index_t *sys_index;
  dtuple_t *tuple;
  dfield_t *dfield;
  ulint pos_and_prefix_len;
  ulint prefix_len;
  const rec_t *rec;
  const byte *field;
  ulint len;
  byte *buf;
  ulint i;
  mtr_t mtr;
  Btree_pcursor pcur(srv_fsp, srv_btree_sys, srv_lock_sys);

  ut_ad(mutex_own(&(dict_sys->mutex)));

  mtr.start();

  sys_fields = dict_table_get_low("SYS_FIELDS");
  sys_index = UT_LIST_GET_FIRST(sys_fields->indexes);

  tuple = dtuple_create(heap, 1);
  dfield = dtuple_get_nth_field(tuple, 0);

  buf = (byte *)mem_heap_alloc(heap, 8);
  mach_write_to_8(buf, index->id);

  dfield_set_data(dfield, buf, 8);
  dict_index_copy_types(tuple, sys_index, 1);

  pcur.open_on_user_rec(sys_index, tuple, PAGE_CUR_GE, BTR_SEARCH_LEAF, &mtr, Source_location{});

  for (i = 0; i < index->n_fields; i++) {

    rec = pcur.get_rec();

    ut_a(pcur.is_on_user_rec());

    /* There could be delete marked records in SYS_FIELDS
    because SYS_FIELDS.INDEX_ID can be updated
    by ALTER TABLE ADD INDEX. */

    if (rec_get_deleted_flag(rec)) {

      goto next_rec;
    }

    field = rec_get_nth_field(rec, 0, &len);
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

    pos_and_prefix_len = mach_read_from_4(field);

    ut_a((pos_and_prefix_len & 0xFFFFUL) == i || (pos_and_prefix_len & 0xFFFF0000UL) == (i << 16));

    if ((i == 0 && pos_and_prefix_len > 0) || (pos_and_prefix_len & 0xFFFF0000UL) > 0) {

      prefix_len = pos_and_prefix_len & 0xFFFFUL;
    } else {
      prefix_len = 0;
    }

    ut_a(name_of_col_is(sys_fields, sys_index, 4, "COL_NAME"));

    field = rec_get_nth_field(rec, 4, &len);

    dict_mem_index_add_field(index, mem_heap_strdupl(heap, (char *)field, len), prefix_len);

  next_rec:
    (void) pcur.move_to_next_user_rec(&mtr);
  }

  pcur.close();

  mtr.commit();
}

/**
 * @brief Loads definitions for table indexes. Adds them to the data dictionary cache.
 * 
 * @param table - in: table
 * @param heap - in: memory heap for temporary storage
 * @return DB_SUCCESS if ok, DB_CORRUPTION if corruption of dictionary table or
 *   DB_UNSUPPORTED if table has unknown index type
 */
static ulint dict_load_indexes(dict_table_t *table, mem_heap_t *heap) {
  dict_table_t *sys_indexes;
  dict_index_t *sys_index;
  dict_index_t *index;
  dtuple_t *tuple;
  dfield_t *dfield;
  const rec_t *rec;
  const byte *field;
  ulint len;
  ulint name_len;
  char *name_buf;
  ulint type;
  ulint space;
  ulint page_no;
  ulint n_fields;
  byte *buf;
  uint64_t id;
  mtr_t mtr;
  Btree_pcursor pcur(srv_fsp, srv_btree_sys, srv_lock_sys);
  ulint error = DB_SUCCESS;

  ut_ad(mutex_own(&(dict_sys->mutex)));

  auto is_sys_table = table->id < DICT_HDR_FIRST_ID;

  mtr.start();

  sys_indexes = dict_table_get_low("SYS_INDEXES");
  sys_index = UT_LIST_GET_FIRST(sys_indexes->indexes);

  tuple = dtuple_create(heap, 1);
  dfield = dtuple_get_nth_field(tuple, 0);

  buf = (byte *)mem_heap_alloc(heap, 8);
  mach_write_to_8(buf, table->id);

  dfield_set_data(dfield, buf, 8);
  dict_index_copy_types(tuple, sys_index, 1);

  pcur.open_on_user_rec(sys_index, tuple, PAGE_CUR_GE, BTR_SEARCH_LEAF, &mtr, Source_location{});

  for (;;) {
    if (!pcur.is_on_user_rec()) {

      break;
    }

    rec = pcur.get_rec();

    field = rec_get_nth_field(rec, 0, &len);
    ut_ad(len == 8);

    if (memcmp(buf, field, len) != 0) {
      break;
    } else if (rec_get_deleted_flag(rec)) {
      /* Skip delete marked records */
      goto next_rec;
    }

    field = rec_get_nth_field(rec, 1, &len);
    ut_ad(len == 8);
    id = mach_read_from_8(field);

    ut_a(name_of_col_is(sys_indexes, sys_index, 4, "NAME"));

    field = rec_get_nth_field(rec, 4, &name_len);
    name_buf = mem_heap_strdupl(heap, (char *)field, name_len);

    field = rec_get_nth_field(rec, 5, &len);
    n_fields = mach_read_from_4(field);

    field = rec_get_nth_field(rec, 6, &len);
    type = mach_read_from_4(field);

    field = rec_get_nth_field(rec, 7, &len);
    space = mach_read_from_4(field);

    ut_a(name_of_col_is(sys_indexes, sys_index, 8, "PAGE_NO"));

    field = rec_get_nth_field(rec, 8, &len);
    page_no = mach_read_from_4(field);

    /* We check for unsupported types first, so that the
    subsequent checks are relevant for the supported types. */
    if (type & ~(DICT_CLUSTERED | DICT_UNIQUE)) {

      ib_logger(
        ib_stream,
        "Error: unknown type %lu"
        " of index %s of table %s\n",
        (ulong)type,
        name_buf,
        table->name
      );

      error = DB_UNSUPPORTED;
      goto func_exit;
    } else if (page_no == FIL_NULL) {

      ib_logger(
        ib_stream,
        "Error: trying to load index %s"
        " for table %s\n"
        "but the index tree has been freed!\n",
        name_buf,
        table->name
      );

      error = DB_CORRUPTION;
      goto func_exit;
    } else if ((type & DICT_CLUSTERED) == 0 && nullptr == dict_table_get_first_index(table)) {

      ib_logger(ib_stream, "Error: trying to load index ");
      ut_print_name(name_buf);
      ib_logger(ib_stream, " for table ");
      ut_print_name(table->name);
      ib_logger(
        ib_stream,
        "\nbut the first index"
        " is not clustered!\n"
      );

      error = DB_CORRUPTION;
      goto func_exit;
    } else if (is_sys_table &&
               ((type & DICT_CLUSTERED) || ((table == dict_sys->sys_tables) && (name_len == (sizeof "ID_IND") - 1) &&
                                            (0 == memcmp(name_buf, "ID_IND", name_len))))) {

      /* The index was created in memory already at booting
      of the database server */
    } else {
      index = dict_mem_index_create(table->name, name_buf, space, type, n_fields);
      index->id = id;

      dict_load_fields(index, heap);
      error = dict_index_add_to_cache(table, index, page_no, false);
      /* The data dictionary tables should never contain
      invalid index definitions.  If we ignored this error
      and simply did not load this index definition, the
      .frm file would disagree with the index definitions
      inside InnoDB. */
      if (unlikely(error != DB_SUCCESS)) {

        goto func_exit;
      }
    }

  next_rec:
    (void) pcur.move_to_next_user_rec(&mtr);
  }

func_exit:
  pcur.close();

  mtr.commit();

  return error;
}

dict_table_t *dict_load_table(ib_recovery_t recovery, const char *name) {
  bool ibd_file_missing = false;
  dict_table_t *table;
  dict_table_t *sys_tables;
  Btree_pcursor pcur(srv_fsp, srv_btree_sys, srv_lock_sys);
  dict_index_t *sys_index;
  dtuple_t *tuple;
  mem_heap_t *heap;
  dfield_t *dfield;
  const rec_t *rec;
  const byte *field;
  ulint len;
  ulint space;
  ulint n_cols;
  ulint flags;
  ulint err;
  mtr_t mtr;

  ut_ad(mutex_own(&(dict_sys->mutex)));

  heap = mem_heap_create(32000);

  mtr.start();

  sys_tables = dict_table_get_low("SYS_TABLES");
  sys_index = UT_LIST_GET_FIRST(sys_tables->indexes);

  tuple = dtuple_create(heap, 1);
  dfield = dtuple_get_nth_field(tuple, 0);

  dfield_set_data(dfield, name, strlen(name));
  dict_index_copy_types(tuple, sys_index, 1);

  pcur.open_on_user_rec(sys_index, tuple, PAGE_CUR_GE, BTR_SEARCH_LEAF, &mtr, Source_location{});
  rec = pcur.get_rec();

  if (!pcur.is_on_user_rec() || rec_get_deleted_flag(rec)) {
    /* Not found */
  err_exit:
    pcur.close();

    mtr.commit();

    mem_heap_free(heap);

    return (nullptr);
  }

  field = rec_get_nth_field(rec, 0, &len);

  /* Check if the table name in record is the searched one */
  if (len != strlen(name) || memcmp(name, field, len) != 0) {

    goto err_exit;
  }

  ut_a(name_of_col_is(sys_tables, sys_index, 9, "SPACE"));

  field = rec_get_nth_field(rec, 9, &len);
  space = mach_read_from_4(field);

  /* Check if the tablespace exists and has the right name */
  if (space != 0) {
    flags = dict_sys_tables_get_flags(rec);

    if (unlikely(flags == ULINT_UNDEFINED)) {
      field = rec_get_nth_field(rec, 5, &len);
      flags = mach_read_from_4(field);

      ut_print_timestamp(ib_stream);
      ib_logger(ib_stream, "  Error: table ");
      ut_print_filename(name);
      ib_logger(
        ib_stream,
        "\n"
        "in InnoDB data dictionary"
        " has unknown type %lx.\n",
        (ulong)flags
      );
      goto err_exit;
    }

  } else {
    flags = 0;
  }

  ut_a(name_of_col_is(sys_tables, sys_index, 4, "N_COLS"));

  field = rec_get_nth_field(rec, 4, &len);
  n_cols = mach_read_from_4(field);

  /* See if the tablespace is available. */
  if (space == SYS_TABLESPACE) {
    /* The system tablespace is always available. */
  } else if (!srv_fil->space_for_table_exists_in_mem(space, name, (flags >> DICT_TF2_SHIFT) & DICT_TF2_TEMPORARY, false, false)) {

    if ((flags >> DICT_TF2_SHIFT) & DICT_TF2_TEMPORARY) {
      /* Do not bother to retry opening temporary tables. */
      ibd_file_missing = true;
    } else {
      ut_print_timestamp(ib_stream);
      ib_logger(ib_stream, "  error: space object of table");
      ut_print_filename(name);
      ib_logger(
        ib_stream,
        ",\n"
        "space id %lu did not exist in memory."
        " Retrying an open.\n",
        (ulong)space
      );
      /* Try to open the tablespace */
      if (!srv_fil->open_single_table_tablespace(true, space, flags & ~(~0UL << DICT_TF_BITS), name)) {
        /* We failed to find a sensible
        tablespace file */

        ibd_file_missing = true;
      }
    }
  }

  table = dict_mem_table_create(name, space, n_cols & ~0x80000000UL, flags);

  table->ibd_file_missing = (unsigned int)ibd_file_missing;

  ut_a(name_of_col_is(sys_tables, sys_index, 3, "ID"));

  field = rec_get_nth_field(rec, 3, &len);
  table->id = mach_read_from_8(field);

  pcur.close();

  mtr.commit();

  dict_load_columns(table, heap);

  dict_table_add_to_cache(table, heap);

  mem_heap_empty(heap);

  err = dict_load_indexes(table, heap);

  /* If the force recovery flag is set, we open the table irrespective
  of the error condition, since the user may want to dump data from the
  clustered index. However we load the foreign key information only if
  all indexes were loaded. */
  if (err == DB_SUCCESS) {
    err = dict_load_foreigns(table->name, true);
  } else if (recovery == IB_RECOVERY_DEFAULT) {
    dict_table_remove_from_cache(table);
    table = nullptr;
  }
#if 0
	if (err != DB_SUCCESS && table != nullptr) {

		mutex_enter(&dict_foreign_err_mutex);

		ut_print_timestamp(ib_stream);

		ib_logger(ib_stream,
			"  Error: could not make a foreign key"
			" definition to match\n"
			"the foreign key table"
			" or the referenced table!\n"
			"The data dictionary of InnoDB is corrupt."
			" You may need to drop\n"
			"and recreate the foreign key table"
			" or the referenced table.\n"
			"Submit a detailed bug report"
			" check the InnoDB website for details"
			"Latest foreign key error printout:\n%s\n",
			dict_foreign_err_buf);

		mutex_exit(&dict_foreign_err_mutex);
	}
#endif /* 0 */
  mem_heap_free(heap);

  return (table);
}

dict_table_t *dict_load_table_on_id(ib_recovery_t recovery, uint64_t table_id) {
  byte id_buf[8];
  mem_heap_t *heap;
  dtuple_t *tuple;
  dfield_t *dfield;
  dict_index_t *sys_table_ids;
  dict_table_t *sys_tables;
  const rec_t *rec;
  const byte *field;
  ulint len;
  dict_table_t *table;
  mtr_t mtr;
  Btree_pcursor pcur(srv_fsp, srv_btree_sys, srv_lock_sys);

  ut_ad(mutex_own(&(dict_sys->mutex)));

  /* NOTE that the operation of this function is protected by
  the dictionary mutex, and therefore no deadlocks can occur
  with other dictionary operations. */

  mtr.start();
  /*---------------------------------------------------*/
  /* Get the secondary index based on ID for table SYS_TABLES */
  sys_tables = dict_sys->sys_tables;
  sys_table_ids = dict_table_get_next_index(dict_table_get_first_index(sys_tables));
  heap = mem_heap_create(256);

  tuple = dtuple_create(heap, 1);
  dfield = dtuple_get_nth_field(tuple, 0);

  /* Write the table id in byte format to id_buf */
  mach_write_to_8(id_buf, table_id);

  dfield_set_data(dfield, id_buf, 8);
  dict_index_copy_types(tuple, sys_table_ids, 1);

  pcur.open_on_user_rec(sys_table_ids, tuple, PAGE_CUR_GE, BTR_SEARCH_LEAF, &mtr, Source_location{});

  rec = pcur.get_rec();

  if (!pcur.is_on_user_rec() || rec_get_deleted_flag(rec)) {
    /* Not found */

    pcur.close();

    mtr.commit();

    mem_heap_free(heap);

    return (nullptr);
  }

  /*---------------------------------------------------*/
  /* Now we have the record in the secondary index containing the
  table ID and NAME */

  rec = pcur.get_rec();
  field = rec_get_nth_field(rec, 0, &len);
  ut_ad(len == 8);

  /* Check if the table id in record is the one searched for */
  if (table_id != mach_read_from_8(field)) {

    pcur.close();

    mtr.commit();

    mem_heap_free(heap);

    return (nullptr);
  }

  /* Now we get the table name from the record */
  field = rec_get_nth_field(rec, 1, &len);
  /* Load the table definition to memory */
  table = dict_load_table(recovery, mem_heap_strdupl(heap, (char *)field, len));

  pcur.close();

  mtr.commit();

  mem_heap_free(heap);

  return (table);
}

void dict_load_sys_table(dict_table_t *table) {
  ut_ad(mutex_own(&(dict_sys->mutex)));

  auto heap = mem_heap_create(1000);

  dict_load_indexes(table, heap);

  mem_heap_free(heap);
}

/**
 * @brief Loads foreign key constraint col names (also for the referenced table).
 *
 * @param id in: foreign constraint id as a null-terminated string
 * @param foreign in: foreign constraint object
 */
static void dict_load_foreign_cols(const char *id, dict_foreign_t *foreign) {
  dict_table_t *sys_foreign_cols;
  dict_index_t *sys_index;
  Btree_pcursor pcur(srv_fsp, srv_btree_sys, srv_lock_sys);
  dtuple_t *tuple;
  dfield_t *dfield;
  const rec_t *rec;
  const byte *field;
  ulint len;
  ulint i;
  mtr_t mtr;

  ut_ad(mutex_own(&(dict_sys->mutex)));

  foreign->foreign_col_names = (const char **)mem_heap_alloc(foreign->heap, foreign->n_fields * sizeof(void *));

  foreign->referenced_col_names = (const char **)mem_heap_alloc(foreign->heap, foreign->n_fields * sizeof(void *));
  mtr.start();

  sys_foreign_cols = dict_table_get_low("SYS_FOREIGN_COLS");
  sys_index = UT_LIST_GET_FIRST(sys_foreign_cols->indexes);

  tuple = dtuple_create(foreign->heap, 1);
  dfield = dtuple_get_nth_field(tuple, 0);

  dfield_set_data(dfield, id, strlen(id));
  dict_index_copy_types(tuple, sys_index, 1);

  pcur.open_on_user_rec(sys_index, tuple, PAGE_CUR_GE, BTR_SEARCH_LEAF, &mtr, Source_location{});

  for (i = 0; i < foreign->n_fields; i++) {

    rec = pcur.get_rec();

    ut_a(pcur.is_on_user_rec());
    ut_a(!rec_get_deleted_flag(rec));

    field = rec_get_nth_field(rec, 0, &len);
    ut_a(len == strlen(id));
    ut_a(memcmp(id, field, len) == 0);

    field = rec_get_nth_field(rec, 1, &len);
    ut_a(len == 4);
    ut_a(i == mach_read_from_4(field));

    field = rec_get_nth_field(rec, 4, &len);
    foreign->foreign_col_names[i] = mem_heap_strdupl(foreign->heap, (char *)field, len);

    field = rec_get_nth_field(rec, 5, &len);
    foreign->referenced_col_names[i] = mem_heap_strdupl(foreign->heap, (char *)field, len);

    (void)pcur.move_to_next_user_rec(&mtr);
  }

  pcur.close();

  mtr.commit();
}

/**
 * Loads a foreign key constraint to the dictionary cache.
 *
 * @param id The foreign constraint id as a null-terminated string.
 * @param check_charsets True to check charset compatibility.
 *
 * @return DB_SUCCESS or error code.
 */
static db_err dict_load_foreign(const char *id, bool check_charsets) {
  dict_foreign_t *foreign;
  dict_table_t *sys_foreign;
  Btree_pcursor pcur(srv_fsp, srv_btree_sys, srv_lock_sys);
  dict_index_t *sys_index;
  dtuple_t *tuple;
  mem_heap_t *heap2;
  dfield_t *dfield;
  const rec_t *rec;
  const byte *field;
  ulint len;
  ulint n_fields_and_type;
  mtr_t mtr;

  ut_ad(mutex_own(&(dict_sys->mutex)));

  heap2 = mem_heap_create(1000);

  mtr.start();

  sys_foreign = dict_table_get_low("SYS_FOREIGN");
  sys_index = UT_LIST_GET_FIRST(sys_foreign->indexes);

  tuple = dtuple_create(heap2, 1);
  dfield = dtuple_get_nth_field(tuple, 0);

  dfield_set_data(dfield, id, strlen(id));
  dict_index_copy_types(tuple, sys_index, 1);

  pcur.open_on_user_rec(sys_index, tuple, PAGE_CUR_GE, BTR_SEARCH_LEAF, &mtr, Source_location{});
  rec = pcur.get_rec();

  if (!pcur.is_on_user_rec() || rec_get_deleted_flag(rec)) {
    /* Not found */

    ib_logger(ib_stream, "Error A: cannot load foreign constraint %s\n", id);

    pcur.close();

    mtr.commit();

    mem_heap_free(heap2);

    return DB_ERROR;
  }

  field = rec_get_nth_field(rec, 0, &len);

  /* Check if the id in record is the searched one */
  if (len != strlen(id) || memcmp(id, field, len) != 0) {

    ib_logger(ib_stream, "Error B: cannot load foreign constraint %s\n", id);

    pcur.close();

    mtr.commit();

    mem_heap_free(heap2);

    return (DB_ERROR);
  }

  /* Read the table names and the number of columns associated
  with the constraint */

  mem_heap_free(heap2);

  foreign = dict_mem_foreign_create();

  n_fields_and_type = mach_read_from_4(rec_get_nth_field(rec, 5, &len));

  ut_a(len == 4);

  /* We store the type in the bits 24..29 of n_fields_and_type. */

  foreign->type = (unsigned int)(n_fields_and_type >> 24);
  foreign->n_fields = (unsigned int)(n_fields_and_type & 0x3FFUL);

  foreign->id = mem_heap_strdup(foreign->heap, id);

  field = rec_get_nth_field(rec, 3, &len);
  foreign->foreign_table_name = mem_heap_strdupl(foreign->heap, (char *)field, len);

  field = rec_get_nth_field(rec, 4, &len);
  foreign->referenced_table_name = mem_heap_strdupl(foreign->heap, (char *)field, len);

  pcur.close();

  mtr.commit();

  dict_load_foreign_cols(id, foreign);

  /* If the foreign table is not yet in the dictionary cache, we
  have to load it so that we are able to make type comparisons
  in the next function call. */

  dict_table_get_low(foreign->foreign_table_name);

  /* Note that there may already be a foreign constraint object in
  the dictionary cache for this constraint: then the following
  call only sets the pointers in it to point to the appropriate table
  and index objects and frees the newly created object foreign.
  Adding to the cache should always succeed since we are not creating
  a new foreign key constraint but loading one from the data
  dictionary. */

  return (dict_foreign_add_to_cache(foreign, check_charsets));
}

db_err dict_load_foreigns(const char *table_name, bool check_charsets) {
  Btree_pcursor pcur(srv_fsp, srv_btree_sys, srv_lock_sys);
  mem_heap_t *heap;
  dtuple_t *tuple;
  dfield_t *dfield;
  dict_index_t *sec_index;
  dict_table_t *sys_foreign;
  const rec_t *rec;
  const byte *field;
  ulint len;
  char *id;
  mtr_t mtr;

  ut_ad(mutex_own(&(dict_sys->mutex)));

  sys_foreign = dict_table_get_low("SYS_FOREIGN");

  if (sys_foreign == nullptr) {
    /* No foreign keys defined yet in this database */

    ib_logger(
      ib_stream,
      "Error: no foreign key system tables"
      " in the database\n"
    );

    return (DB_ERROR);
  }

  mtr.start();

  /* Get the secondary index based on FOR_NAME from table
  SYS_FOREIGN */

  sec_index = dict_table_get_next_index(dict_table_get_first_index(sys_foreign));

  db_err err;

start_load:
  heap = mem_heap_create(256);

  tuple = dtuple_create(heap, 1);
  dfield = dtuple_get_nth_field(tuple, 0);

  dfield_set_data(dfield, table_name, strlen(table_name));
  dict_index_copy_types(tuple, sec_index, 1);

  pcur.open_on_user_rec(sec_index, tuple, PAGE_CUR_GE, BTR_SEARCH_LEAF, &mtr, Source_location{});
loop:
  rec = pcur.get_rec();

  if (!pcur.is_on_user_rec()) {
    /* End of index */

    goto load_next_index;
  }

  /* Now we have the record in the secondary index containing a table
  name and a foreign constraint ID */

  rec = pcur.get_rec();
  field = rec_get_nth_field(rec, 0, &len);

  /* Check if the table name in the record is the one searched for; the
  following call does the comparison in the latin1_swedish_ci
  charset-collation, in a case-insensitive way. */

  if (0 != cmp_data_data(nullptr, dfield_get_type(dfield)->mtype, dfield_get_type(dfield)->prtype, (const byte *)dfield_get_data(dfield), dfield_get_len(dfield), field, len)) {

    goto load_next_index;
  }

  /* Since table names in SYS_FOREIGN are stored in a case-insensitive
  order, we have to check that the table name matches also in a binary
  string comparison. */

  /* FIXME: On Unix, allow table names that only differ in character
  case. */

  if (0 != memcmp(field, table_name, len)) {

    goto next_rec;
  }

  if (rec_get_deleted_flag(rec)) {

    goto next_rec;
  }

  /* Now we get a foreign key constraint id */
  field = rec_get_nth_field(rec, 1, &len);
  id = mem_heap_strdupl(heap, (char *)field, len);

  pcur.store_position(&mtr);

  mtr.commit();

  /* Load the foreign constraint definition to the dictionary cache */

  err = dict_load_foreign(id, check_charsets);

  if (err != DB_SUCCESS) {
    pcur.close();
    mem_heap_free(heap);

    return err;
  }

  mtr.start();

  (void) pcur.restore_position(BTR_SEARCH_LEAF, &mtr, Source_location{});

next_rec:
  (void) pcur.move_to_next_user_rec(&mtr);

  goto loop;

load_next_index:
  pcur.close();

  mtr.commit();

  mem_heap_free(heap);

  sec_index = dict_table_get_next_index(sec_index);

  if (sec_index != nullptr) {

    mtr.start();

    goto start_load;
  }

  return (DB_SUCCESS);
}
