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

/** @file dict/dict0fk.cc
Data dictionary foreign key system

Created 2024-Oct-15 by Sunny Bains
***********************************************************************/

#include "api0ucode.h"
#include "btr0btr.h"
#include "btr0cur.h"
#include "buf0buf.h"
#include "data0type.h"
#include "dict0dict.h"
#include "mach0data.h"
#include "page0page.h"
#include "pars0pars.h"
#include "pars0sym.h"
#include "que0que.h"
#include "rem0cmp.h"
#include "row0merge.h"
#include "trx0undo.h"

#include <ctype.h>

void Dict::foreign_remove_from_cache(Foreign *&foreign) noexcept {
  // ut_ad(mutex_own(&m_mutex));

  foreign->drop_constraint();

  Foreign::destroy(foreign);
}

Index *Table::foreign_find_index(
  const char **columns, ulint n_cols, Index *types_idx, bool check_charsets, bool check_null
) noexcept {
  for (auto index = get_first_index(); index != nullptr; index = index->get_next()) {

    /* Ignore matches that refer to the same instance or the index is to be dropped */
    if (unlikely(index->m_to_be_dropped || types_idx == index || index->get_n_fields() < n_cols)) {
      continue;
    }
    ulint i;

    for (i = 0; i < n_cols; ++i) {
      const auto field = index->get_nth_field(i);
      const auto col_name = get_col_name(field->m_col->get_no());

      if (field->m_prefix_len != 0) {
        /* We do not accept column prefix indexes here */
        break;
      }

      if (ib_utf8_strcasecmp(columns[i], col_name) != 0) {
        break;
      }

      if (check_null && (field->m_col->prtype & DATA_NOT_NULL)) {
        return nullptr;
      }

      if (types_idx != nullptr && !cmp_cols_are_equal(index->get_nth_col(i), types_idx->get_nth_col(i), check_charsets)) {

        break;
      }
    }

    if (i == n_cols) {
      /* We found a matching index */
      return index;
    }
  }

  return nullptr;
}

Index *Table::get_index_by_max_id(const char *name, const char **columns, ulint n_cols) noexcept {
  Index *found{};

  for (auto index : m_indexes) {

    if (strcmp(index->m_name, name) == 0 && index->get_n_ordering_defined_by_user() == n_cols) {

      ulint i;

      for (i = 0; i < n_cols; ++i) {
        auto field = index->get_nth_field(i);
        auto col_name = get_col_name(field->m_col->get_no());

        if (0 != ib_utf8_strcasecmp(columns[i], col_name)) {

          break;
        }
      }

      if (i == n_cols) {
        /* We found a matching index, select the index with the higher id*/

        if (found == nullptr || index->m_id > found->m_id) {

          found = index;
        }
      }
    }
  }

  return found;
}

void Dict::foreign_error_report(Foreign *fk, const char *msg) noexcept {
  mutex_enter(&m_foreign_err_mutex);

  log_err(" Foreign key constraint of table ", fk->m_foreign_table_name);
  log_err(std::format("{}", msg));

  print_info_on_foreign_key_in_create_format(nullptr, fk, true);

  if (fk->m_foreign_index) {
    log_err("The index in the foreign key in table is ", fk->m_foreign_index->m_name);
    log_err("See Embedded InnoDB website for details for correct foreign key definition.");
  }

  mutex_exit(&m_foreign_err_mutex);
}

db_err Dict::foreign_add_to_cache(Foreign *foreign, bool check_charsets) noexcept {
  Index *index;

  ut_ad(mutex_own(&m_mutex));

  auto for_table = table_check_if_in_cache(foreign->m_foreign_table_name);
  auto ref_table = table_check_if_in_cache(foreign->m_referenced_table_name);

  ut_a(for_table != nullptr || ref_table != nullptr);

  Foreign *for_in_cache{};

  if (for_table) {
    for_in_cache = for_table->find_foreign_constraint(foreign->m_id);
  }

  if (for_in_cache == nullptr && ref_table != nullptr) {
    for_in_cache = ref_table->find_foreign_constraint(foreign->m_id);
  }

  if (for_in_cache != nullptr) {
    /* Free the foreign object */
    mem_heap_free(foreign->m_heap);
  } else {
    for_in_cache = foreign;
  }

  bool added_to_referenced_list{};

  if (for_in_cache->m_referenced_table == nullptr && ref_table) {
    index = ref_table->foreign_find_index(
      for_in_cache->m_referenced_col_names, for_in_cache->m_n_fields, for_in_cache->m_foreign_index, check_charsets, false
    );

    if (index == nullptr) {
      log_err(
        "There is no index in referenced table which would contain the columns as the first columns,"
        " or the data types in the referenced table do not match the ones in table."
      );

      if (for_in_cache == foreign) {
        mem_heap_free(foreign->m_heap);
      }

      return DB_CANNOT_ADD_CONSTRAINT;
    }

    for_in_cache->m_referenced_table = ref_table;
    for_in_cache->m_referenced_index = index;
    UT_LIST_ADD_LAST(ref_table->m_referenced_list, for_in_cache);
    added_to_referenced_list = true;
  }

  if (for_in_cache->m_foreign_table == nullptr && for_table) {
    index = for_table->foreign_find_index(
      for_in_cache->m_foreign_col_names,
      for_in_cache->m_n_fields,
      for_in_cache->m_referenced_index,
      check_charsets,
      for_in_cache->m_type & (DICT_FOREIGN_ON_DELETE_SET_NULL | DICT_FOREIGN_ON_UPDATE_SET_NULL)
    );

    if (index == nullptr) {
      foreign_error_report(
        for_in_cache,
        "there is no index in the table which would contain the columns as the first columns,"
        " or the data types in the table do not match the ones in the referenced table"
        " or one of the ON ... SET nullptr columns is declared NOT nullptr."
      );

      if (for_in_cache == foreign) {
        if (added_to_referenced_list) {
          UT_LIST_REMOVE(ref_table->m_referenced_list, for_in_cache);
        }

        mem_heap_free(foreign->m_heap);
      }

      return DB_CANNOT_ADD_CONSTRAINT;
    }

    for_in_cache->m_foreign_table = for_table;
    for_in_cache->m_foreign_index = index;

    UT_LIST_ADD_LAST(for_table->m_foreign_list, for_in_cache);
  }

  return DB_SUCCESS;
}

const char *Dict::scan_to(const char *ptr, const char *string) noexcept {
  char quote = '\0';

  for (; *ptr; ptr++) {
    if (*ptr == quote) {
      /* Closing quote character: do not look for starting quote or the keyword. */
      quote = '\0';
    } else if (quote) {
      /* Within quotes: do nothing. */
    } else if (*ptr == '`' || *ptr == '"') {
      /* Starting quote: remember the quote character. */
      quote = *ptr;
    } else {
      /* Outside quotes: look for the keyword. */
      for (ulint i{}; string[i]; ++i) {
        if (toupper((int)(unsigned char)(ptr[i])) != toupper((int)(unsigned char)(string[i]))) {
          continue;
        }
      }
      break;
    }
  }

  return ptr;
}

const char *Dict::accept(const charset_t *cs, const char *ptr, const char *string, bool *success) noexcept {
  auto old_ptr = ptr;

  *success = false;

  while (ib_utf8_isspace(cs, *ptr)) {
    ptr++;
  }

  const char *old_ptr2 = ptr;

  ptr = scan_to(ptr, string);

  if (*ptr == '\0' || old_ptr2 != ptr) {
    return old_ptr;
  } else {
    *success = true;
    return ptr + strlen(string);
  }
}

const char *Dict::scan_id(
  const charset_t *cs, const char *ptr, mem_heap_t *heap, const char **id, bool table_id, bool accept_also_dot
) noexcept {
  ulint len{};
  char quote = '\0';

  *id = nullptr;

  while (ib_utf8_isspace(cs, *ptr)) {
    ptr++;
  }

  if (*ptr == '\0') {

    return ptr;
  }

  if (*ptr == '`' || *ptr == '"') {
    quote = *ptr++;
  }

  const char *s = ptr;

  if (quote != '\0') {
    for (;;) {
      if (!*ptr) {
        /* Syntax error */
        return ptr;
      }
      if (*ptr == quote) {
        ++ptr;
        if (*ptr != quote) {
          break;
        }
      }
      ptr++;
      len++;
    }
  } else {

    while (!ib_utf8_isspace(cs, *ptr) && *ptr != '(' && *ptr != ')' && (accept_also_dot || *ptr != '.') && *ptr != ',' &&
           *ptr != '\0') {
      ptr++;
    }

    len = ptr - s;
  }

  if (heap == nullptr) {
    /* no heap given: id will point to source string */
    *id = s;
    return ptr;
  }

  char *str;

  if (quote != '\0') {
    auto d = reinterpret_cast<char *>(mem_heap_alloc(heap, len + 1));
    str = d;

    s = d;

    while (len--) {
      if ((*d++ = *s++) == quote) {
        s++;
      }
    }

    *d++ = 0;
    len = d - str;
    ut_ad(*s == quote);
    ut_ad(s + 1 == ptr);
  } else {
    str = mem_heap_strdupl(heap, s, len);
  }

  if (!table_id) {
    /* Convert the identifier from connection character set to UTF-8. */
    len = 3 * len + 1;

    auto dst = reinterpret_cast<char *>(mem_heap_alloc(heap, len));

    ib_utf8_convert_from_id(cs, dst, str, len);

  } else {
    /* Encode using filename-safe characters. */
    len = 5 * len + 1;

    auto dst = reinterpret_cast<char *>(mem_heap_alloc(heap, len));

    ib_utf8_convert_from_table_id(cs, dst, str, len);
    *id = dst;
    ib_utf8_convert_from_table_id(cs, dst, str, len);
  }

  return ptr;
}

const char *Dict::scan_col(
  const charset_t *cs, const char *ptr, bool *success, Table *table, const Column **column, mem_heap_t *heap, const char **name
) noexcept {

  *success = false;

  ptr = scan_id(cs, ptr, heap, name, false, true);

  if (*name == nullptr) {

    return ptr; /* Syntax error */
  }

  if (table == nullptr) {
    *success = true;
    *column = nullptr;
  } else {
    for (ulint i = 0; i < table->get_n_cols(); i++) {

      auto col_name = table->get_col_name(i);

      if (ib_utf8_strcasecmp(col_name, *name) == 0) {
        /* Found */

        *success = true;
        *column = table->get_nth_col(i);
        strcpy((char *)*name, col_name);

        break;
      }
    }
  }

  return ptr;
}

const char *Dict::scan_table_name(
  const charset_t *cs, const char *ptr, Table **table, const char *name, bool *success, mem_heap_t *heap, const char **ref_name
) noexcept {
  const char *scan_name;
  ulint database_name_len = 0;
  const char *table_name = nullptr;
  const char *database_name = nullptr;

  *success = false;
  *table = nullptr;

  ptr = scan_id(cs, ptr, heap, &scan_name, true, false);

  if (scan_name == nullptr) {

    return ptr; /* Syntax error */
  }

  if (*ptr == '.') {
    /* We scanned the database name; scan also the table name */

    ptr++;

    database_name = scan_name;
    database_name_len = strlen(database_name);

    ptr = scan_id(cs, ptr, heap, &table_name, true, false);

    if (table_name == nullptr) {

      return ptr; /* Syntax error */
    }
  } else {
    /* To be able to read table dumps made with InnoDB-4.0.17 or
    earlier, we must allow the dot separator between the database
    name and the table name also to appear within a quoted
    identifier! InnoDB used to print a constraint as:
    ... REFERENCES `databasename.tablename` ...
    starting from 4.0.18 it is
    ... REFERENCES `databasename`.`tablename` ... */

    for (auto s = scan_name; *s; s++) {
      if (*s == '.') {
        database_name = scan_name;
        database_name_len = s - scan_name;
        scan_name = ++s;
        break; /* to do: multiple dots? */
      }
    }

    table_name = scan_name;
  }

  if (database_name == nullptr) {
    /* Use the database name of the foreign key table */

    database_name = name;
    database_name_len = get_db_name_len(name);
  }

  const auto table_name_len = strlen(table_name);

  /* Copy database_name, '/', table_name, '\0' */
  auto ref = reinterpret_cast<char *>(mem_heap_alloc(heap, database_name_len + table_name_len + 2));

  memcpy(ref, database_name, database_name_len);

  ref[database_name_len] = '/';

  memcpy(ref + database_name_len + 1, table_name, table_name_len + 1);

  if (srv_lower_case_table_names) {
    /* The table name is always put to lower case on Windows. */
    ib_utf8_casedown(ref);
  }

  *success = true;
  *ref_name = ref;
  *table = table_get(ref);

  return ptr;
}

const char *Dict::skip_word(const charset_t *cs, const char *ptr, bool *success) noexcept {
  const char *start;

  *success = false;

  ptr = scan_id(cs, ptr, nullptr, &start, false, true);

  if (start != nullptr) {
    *success = true;
  }

  return ptr;
}

char *Dict::strip_comments(const char *sql_string) noexcept {
  /* unclosed quote character (0 if none) */
  char quote = 0;

  auto str = static_cast<char *>(mem_alloc(strlen(sql_string) + 1));

  auto sptr = sql_string;
  auto ptr = str;

  for (;;) {
  scan_more:
    if (*sptr == '\0') {
      *ptr = '\0';

      ut_a(ptr <= str + strlen(sql_string));

      return str;
    }

    if (*sptr == quote) {
      /* Closing quote character: do not look for
      starting quote or comments. */
      quote = 0;
    } else if (quote) {
      /* Within quotes: do not look for
      starting quotes or comments. */
    } else if (*sptr == '"' || *sptr == '`' || *sptr == '\'') {
      /* Starting quote: remember the quote character. */
      quote = *sptr;
    } else if (*sptr == '#' || (sptr[0] == '-' && sptr[1] == '-' && sptr[2] == ' ')) {
      for (;;) {
        /* In Unix a newline is 0x0A while in Windows
        it is 0x0D followed by 0x0A */

        if (*sptr == (char)0x0A || *sptr == (char)0x0D || *sptr == '\0') {

          goto scan_more;
        }

        sptr++;
      }
    } else if (!quote && *sptr == '/' && *(sptr + 1) == '*') {
      for (;;) {
        if (*sptr == '*' && *(sptr + 1) == '/') {

          sptr += 2;

          goto scan_more;
        }

        if (*sptr == '\0') {

          goto scan_more;
        }

        sptr++;
      }
    }

    *ptr = *sptr;

    ptr++;
    sptr++;
  }
}

ulint Dict::table_get_highest_foreign_id(Table *table) noexcept {
  char *endp;
  ulint biggest_id{};

  const auto len = strlen(table->m_name);

  for (auto foreign : table->m_foreign_list) {
    if (strlen(foreign->m_id) > (sizeof(dict_ibfk) - 1) + len && memcmp(foreign->m_id, table->m_name, len) == 0 &&
        memcmp(foreign->m_id + len, dict_ibfk, (sizeof dict_ibfk) - 1) == 0 &&
        foreign->m_id[len + ((sizeof dict_ibfk) - 1)] != '0') {
      /* It is of the >= 4.0.18 format */

      auto id = strtoul(foreign->m_id + len + (sizeof(dict_ibfk) - 1), &endp, 10);

      if (*endp == '\0') {
        ut_a(id != biggest_id);

        if (id > biggest_id) {
          biggest_id = id;
        }
      }
    }
  }

  return biggest_id;
}

void Dict::foreign_report_syntax_err(const char *name, const char *start_of_latest_foreign, const char *ptr) noexcept {
  mutex_enter(&m_foreign_err_mutex);

  foreign_error_report(name);
  log_err(std::format("{}:\nSyntax error close to:\n{}\n", start_of_latest_foreign, ptr));

  mutex_exit(&m_foreign_err_mutex);
}

db_err Dict::create_foreign_constraints(
  Trx *trx, mem_heap_t *heap, const charset_t *cs, const char *sql_string, const char *name, bool reject_fks
) noexcept {
  ulint i{};
  Table *referenced_table;
  Table *table_to_alter;
  ulint highest_id_so_far{};
  Index *index;
  Foreign *foreign;
  const char *ptr = sql_string;
  const char *start_of_latest_foreign = sql_string;
  const char *constraint_name;
  bool success;
  const char *ptr2;
  bool is_on_delete;
  ulint n_on_deletes;
  ulint n_on_updates;
  const char *ptr1;
  const Column *columns[500];
  const char *column_names[500];
  const char *referenced_table_name;

  ut_ad(mutex_own(&m_mutex));

  auto table = table_get(name);

  if (table == nullptr) {
    mutex_enter(&m_foreign_err_mutex);

    foreign_error_report(name);
    log_err("Cannot find the table in the internal data dictionary of InnoDB. Create table statement: {}", sql_string);

    mutex_exit(&m_foreign_err_mutex);

    return DB_ERROR;
  }

  /* First check if we are actually doing an ALTER TABLE, and in that
  case look for the table being altered */

  accept(cs, ptr, "ALTER", &success);

  if (!success) {

    goto loop;
  }

  ptr = accept(cs, ptr, "TABLE", &success);

  if (!success) {

    goto loop;
  }

  /* We are doing an ALTER TABLE: scan the table name we are altering */

  ptr = scan_table_name(cs, ptr, &table_to_alter, name, &success, heap, &referenced_table_name);

  if (!success) {
    log_err("Could not find the table being ALTERED in: {}", sql_string);
    return DB_ERROR;
  }

  /* Starting from 4.0.18 and 4.1.2, we generate foreign key id's in the
  format databasename/tablename_ibfk_[number], where [number] is local
  to the table; look for the highest [number] for table_to_alter, so
  that we can assign to new constraints higher numbers. */

  /* If we are altering a temporary table, the table name after ALTER
  TABLE does not correspond to the internal table name, and
  table_to_alter is nullptr. TODO: should we fix this somehow? */

  if (table_to_alter == nullptr) {
    highest_id_so_far = 0;
  } else {
    highest_id_so_far = table_get_highest_foreign_id(table_to_alter);
  }

  /* Scan for foreign key declarations in a loop */
loop:
  /* Scan either to "CONSTRAINT" or "FOREIGN", whichever is closer */

  ptr1 = scan_to(ptr, "CONSTRAINT");
  ptr2 = scan_to(ptr, "FOREIGN");

  constraint_name = nullptr;

  if (ptr1 < ptr2) {
    /* The user may have specified a constraint name. Pick it so
    that we can store 'databasename/constraintname' as the id of
    of the constraint to system tables. */
    ptr = ptr1;

    ptr = accept(cs, ptr, "CONSTRAINT", &success);

    ut_a(success);

    if (!ib_utf8_isspace(cs, *ptr) && *ptr != '"' && *ptr != '`') {

      goto loop;
    }

    while (ib_utf8_isspace(cs, *ptr)) {
      ptr++;
    }

    /* read constraint name unless got "CONSTRAINT FOREIGN" */
    if (ptr != ptr2) {
      ptr = scan_id(cs, ptr, heap, &constraint_name, false, false);
    }
  } else {
    ptr = ptr2;
  }

  if (*ptr == '\0') {
    if (reject_fks && (UT_LIST_GET_LEN(table->m_foreign_list) > 0)) {

      return DB_CANNOT_ADD_CONSTRAINT;
    }

    /* The following call adds the foreign key constraints
    to the data dictionary system tables on disk */

    return m_store.add_foreigns_to_dictionary(highest_id_so_far, table, trx);
  }

  start_of_latest_foreign = ptr;

  ptr = accept(cs, ptr, "FOREIGN", &success);

  if (!success) {
    goto loop;
  }

  if (!ib_utf8_isspace(cs, *ptr)) {
    goto loop;
  }

  ptr = accept(cs, ptr, "KEY", &success);

  if (!success) {
    goto loop;
  }

  ptr = accept(cs, ptr, "(", &success);

  if (!success) {
    /* Skip index id before the '('. */
    ptr = skip_word(cs, ptr, &success);

    if (!success) {
      foreign_report_syntax_err(name, start_of_latest_foreign, ptr);

      return DB_CANNOT_ADD_CONSTRAINT;
    }

    ptr = accept(cs, ptr, "(", &success);

    if (!success) {
      /* We do not flag a syntax error here because in an
      ALTER TABLE we may also have DROP FOREIGN KEY abc */

      goto loop;
    }
  }

  i = 0;

  /* Scan the columns in the first list */
col_loop1:
  ut_a(i < (sizeof column_names) / sizeof *column_names);

  ptr = scan_col(cs, ptr, &success, table, columns + i, heap, column_names + i);

  if (!success) {
    mutex_enter(&m_foreign_err_mutex);

    foreign_error_report(name);
    log_err(start_of_latest_foreign, "\nCannot resolve column name close to: ", ptr);

    mutex_exit(&m_foreign_err_mutex);

    return DB_CANNOT_ADD_CONSTRAINT;
  }

  i++;

  ptr = accept(cs, ptr, ",", &success);

  if (success) {
    goto col_loop1;
  }

  ptr = accept(cs, ptr, ")", &success);

  if (!success) {
    foreign_report_syntax_err(name, start_of_latest_foreign, ptr);
    return DB_CANNOT_ADD_CONSTRAINT;
  }

  /* Try to find an index which contains the columns
  as the first fields and in the right order */

  index = table->foreign_find_index(column_names, i, nullptr, true, false);

  if (index == nullptr) {
    mutex_enter(&m_foreign_err_mutex);

    foreign_error_report(name);

    log_err(
      "There is no index in table ",
      name,
      " where the columns appears as"
      " the first columns. Constraint:\n",
      start_of_latest_foreign,
      ". See"
      " Embedded InnoDB website for details for correct foreign key definition."
    );

    mutex_exit(&m_foreign_err_mutex);

    return DB_CANNOT_ADD_CONSTRAINT;
  }
  ptr = accept(cs, ptr, "REFERENCES", &success);

  if (!success || !ib_utf8_isspace(cs, *ptr)) {
    foreign_report_syntax_err(name, start_of_latest_foreign, ptr);
    return DB_CANNOT_ADD_CONSTRAINT;
  }

  /* Let us create a constraint struct */

  foreign = Foreign::create();

  if (constraint_name) {
    /* Catenate 'databasename/' to the constraint name specified
    by the user: we conceive the constraint as belonging to the
    same client 'database' as the table itself. We store the name
    to foreign->m_id. */

    const auto db_len = get_db_name_len(table->m_name);

    foreign->m_id = (char *)mem_heap_alloc(foreign->m_heap, db_len + strlen(constraint_name) + 2);

    memcpy(foreign->m_id, table->m_name, db_len);

    foreign->m_id[db_len] = '/';

    strcpy(foreign->m_id + db_len + 1, constraint_name);
  }

  foreign->m_foreign_table = table;
  foreign->m_foreign_table_name = mem_heap_strdup(foreign->m_heap, table->m_name);
  foreign->m_foreign_index = index;
  foreign->m_n_fields = uint(i);
  foreign->m_foreign_col_names = reinterpret_cast<const char **>(mem_heap_alloc(foreign->m_heap, i * sizeof(void *)));

  for (ulint i{}; i < foreign->m_n_fields; ++i) {
    foreign->m_foreign_col_names[i] = mem_heap_strdup(foreign->m_heap, table->get_col_name(columns[i]->get_no()));
  }

  ptr = scan_table_name(cs, ptr, &referenced_table, name, &success, heap, &referenced_table_name);

  /* Note that referenced_table can be nullptr if the user has suppressed
  checking of foreign key constraints! */

  if (!success || (!referenced_table && trx->m_check_foreigns)) {
    Foreign::destroy(foreign);

    mutex_enter(&m_foreign_err_mutex);

    foreign_error_report(name);

    log_err(std::format("{}:\nCannot resolve table name close to:\n{}", start_of_latest_foreign, ptr));

    mutex_exit(&m_foreign_err_mutex);

    return DB_CANNOT_ADD_CONSTRAINT;
  }

  ptr = accept(cs, ptr, "(", &success);

  if (!success) {
    Foreign::destroy(foreign);
    foreign_report_syntax_err(name, start_of_latest_foreign, ptr);
    return DB_CANNOT_ADD_CONSTRAINT;
  }

  /* Scan the columns in the second list */
  i = 0;

col_loop2:
  ptr = scan_col(cs, ptr, &success, referenced_table, columns + i, heap, column_names + i);
  i++;

  if (!success) {
    Foreign::destroy(foreign);

    mutex_enter(&m_foreign_err_mutex);

    foreign_error_report(name);
    log_err(std::format("{}:\nCannot resolve column name close to: {}", start_of_latest_foreign, ptr));

    mutex_exit(&m_foreign_err_mutex);

    return DB_CANNOT_ADD_CONSTRAINT;
  }

  ptr = accept(cs, ptr, ",", &success);

  if (success) {
    goto col_loop2;
  }

  ptr = accept(cs, ptr, ")", &success);

  if (!success || foreign->m_n_fields != i) {
    Foreign::destroy(foreign);

    foreign_report_syntax_err(name, start_of_latest_foreign, ptr);

    return DB_CANNOT_ADD_CONSTRAINT;
  }

  n_on_deletes = 0;
  n_on_updates = 0;

scan_on_conditions:
  /* Loop here as long as we can find ON ... conditions */

  ptr = accept(cs, ptr, "ON", &success);

  if (!success) {

    goto try_find_index;
  }

  ptr = accept(cs, ptr, "DELETE", &success);

  if (!success) {
    ptr = accept(cs, ptr, "UPDATE", &success);

    if (!success) {

      Foreign::destroy(foreign);
      foreign_report_syntax_err(name, start_of_latest_foreign, ptr);

      return DB_CANNOT_ADD_CONSTRAINT;
    }

    is_on_delete = false;
    n_on_updates++;
  } else {
    is_on_delete = true;
    n_on_deletes++;
  }

  ptr = accept(cs, ptr, "RESTRICT", &success);

  if (success) {
    goto scan_on_conditions;
  }

  ptr = accept(cs, ptr, "CASCADE", &success);

  if (success) {
    if (is_on_delete) {
      foreign->m_type |= DICT_FOREIGN_ON_DELETE_CASCADE;
    } else {
      foreign->m_type |= DICT_FOREIGN_ON_UPDATE_CASCADE;
    }

    goto scan_on_conditions;
  }

  ptr = accept(cs, ptr, "NO", &success);

  if (success) {
    ptr = accept(cs, ptr, "ACTION", &success);

    if (!success) {

      Foreign::destroy(foreign);
      foreign_report_syntax_err(name, start_of_latest_foreign, ptr);

      return DB_CANNOT_ADD_CONSTRAINT;
    }

    if (is_on_delete) {
      foreign->m_type |= DICT_FOREIGN_ON_DELETE_NO_ACTION;
    } else {
      foreign->m_type |= DICT_FOREIGN_ON_UPDATE_NO_ACTION;
    }

    goto scan_on_conditions;
  }

  ptr = accept(cs, ptr, "SET", &success);

  if (!success) {

    Foreign::destroy(foreign);
    foreign_report_syntax_err(name, start_of_latest_foreign, ptr);

    return DB_CANNOT_ADD_CONSTRAINT;
  }

  ptr = accept(cs, ptr, "nullptr", &success);

  if (!success) {

    Foreign::destroy(foreign);
    foreign_report_syntax_err(name, start_of_latest_foreign, ptr);

    return DB_CANNOT_ADD_CONSTRAINT;
  }

  for (ulint j{}; j < foreign->m_n_fields; ++j) {

    if ((foreign->m_foreign_index->get_nth_col(j)->prtype) & DATA_NOT_NULL) {

      /* It is not sensible to define SET nullptr
      if the column is not allowed to be nullptr! */

      Foreign::destroy(foreign);

      mutex_enter(&m_foreign_err_mutex);

      foreign_error_report(name);

      log_err(std::format(
        "{}:\nYou have defined a SET nullptr condition though some of the columns are defined as NOT nullptr.",
        start_of_latest_foreign
      ));

      mutex_exit(&m_foreign_err_mutex);

      return DB_CANNOT_ADD_CONSTRAINT;
    }
  }

  if (is_on_delete) {
    foreign->m_type |= DICT_FOREIGN_ON_DELETE_SET_NULL;
  } else {
    foreign->m_type |= DICT_FOREIGN_ON_UPDATE_SET_NULL;
  }

  goto scan_on_conditions;

try_find_index:
  if (n_on_deletes > 1 || n_on_updates > 1) {
    /* It is an error to define more than 1 action */

    Foreign::destroy(foreign);

    mutex_enter(&m_foreign_err_mutex);

    foreign_error_report(name);

    log_err(std::format("{}: You have twice an ON DELETE clause or twice an ON UPDATE clause.", start_of_latest_foreign));

    mutex_exit(&m_foreign_err_mutex);

    return DB_CANNOT_ADD_CONSTRAINT;
  }

  /* Try to find an index which contains the columns as the first fields
  and in the right order, and the types are the same as in
  foreign->m_foreign_index */

  if (referenced_table) {

    index = referenced_table->foreign_find_index(column_names, i, foreign->m_foreign_index, true, false);

    if (index == nullptr) {
      Foreign::destroy(foreign);

      mutex_enter(&m_foreign_err_mutex);

      foreign_error_report(name);

      log_err(std::format(
        "{} - Cannot find an index in the referenced table where the referenced columns appear as the"
        " first columns, or column types in the table and the referenced table do not match for constraint."
        " See Emnbedded InnoDB website for details for correct foreign key definition.",
        start_of_latest_foreign
      ));

      mutex_exit(&m_foreign_err_mutex);

      return DB_CANNOT_ADD_CONSTRAINT;
    }
  } else {
    ut_a(trx->m_check_foreigns == false);
    index = nullptr;
  }

  foreign->m_referenced_index = index;
  foreign->m_referenced_table = referenced_table;
  foreign->m_referenced_table_name = mem_heap_strdup(foreign->m_heap, referenced_table_name);
  foreign->m_referenced_col_names = reinterpret_cast<const char **>(mem_heap_alloc(foreign->m_heap, i * sizeof(void *)));

  for (ulint j{}; j < foreign->m_n_fields; ++j) {
    foreign->m_referenced_col_names[j] = mem_heap_strdup(foreign->m_heap, column_names[j]);
  }

  /* We found an ok constraint definition: add to the lists */

  UT_LIST_ADD_LAST(table->m_foreign_list, foreign);

  if (referenced_table) {
    UT_LIST_ADD_LAST(referenced_table->m_referenced_list, foreign);
  }

  goto loop;
}

db_err Dict::create_foreign_constraints(Trx *trx, const char *sql_string, const char *name, bool reject_fks) noexcept {
  auto str = strip_comments(sql_string);
  auto heap = mem_heap_create(10000);
  auto cs = ib_ucode_get_connection_charset();
  auto err = create_foreign_constraints(trx, heap, cs, str, name, reject_fks);

  mem_heap_free(heap);
  mem_free(str);

  return err;
}

db_err Dict::foreign_parse_drop_constraints(
  mem_heap_t *heap, Trx *trx, Table *table, ulint *n, const char ***constraints_to_drop
) noexcept {
  bool success;
  const char *id;

  auto cs = ib_ucode_get_connection_charset();

  *n = 0;

  *constraints_to_drop = reinterpret_cast<const char **>(mem_heap_alloc(heap, 1000 * sizeof(char *)));

  auto str = strip_comments(*trx->m_client_query_str);
  char const *ptr = str;

  ut_ad(mutex_own(&m_mutex));

  for (;;) {
    ptr = scan_to(ptr, "DROP");

    if (*ptr == '\0') {
      mem_free(str);

      return DB_SUCCESS;
    }

    ptr = accept(cs, ptr, "DROP", &success);

    if (!ib_utf8_isspace(cs, *ptr)) {
      continue;
    }

    ptr = accept(cs, ptr, "FOREIGN", &success);

    if (!success || !ib_utf8_isspace(cs, *ptr)) {
      continue;
    }

    ptr = accept(cs, ptr, "KEY", &success);

    if (!success) {
      break;
    }

    ptr = scan_id(cs, ptr, heap, &id, false, true);

    if (id == nullptr) {
      break;
    }

    ut_a(*n < 1000);
    (*constraints_to_drop)[*n] = id;
    (*n)++;

    /* Look for the given constraint id */
    auto foreign = std::find_if(table->m_foreign_list.begin(), table->m_foreign_list.end(), [this, id](auto foreign) {
      return strcmp(foreign->m_id, id) == 0 || (strchr(foreign->m_id, '/') && strcmp(id, remove_db_name(foreign->m_id)) == 0);
    });

    if (foreign == nullptr) {
      mutex_enter(&m_foreign_err_mutex);

      log_err(std::format(
        "Dropping of a foreign key constraint of table {} failed because the constraint with id {} was not found"
        " in SQL command {}. Cannot find a constraint with the given id ",
        table->m_name,
        id,
        str
      ));

      mutex_exit(&m_foreign_err_mutex);

      mem_free(str);

      return DB_CANNOT_DROP_CONSTRAINT;
    }
  }

  mutex_enter(&m_foreign_err_mutex);

  log_err(std::format(
    "Syntax error in dropping of a foreign key constraint of table {} close to: {} in SQL command: {}", table->m_name, ptr, str
  ));

  mutex_exit(&m_foreign_err_mutex);

  mem_free(str);

  return DB_CANNOT_DROP_CONSTRAINT;
}

DTuple *Dict::index_build_node_ptr(const Index *index, const rec_t *rec, ulint page_no, mem_heap_t *heap, ulint level) noexcept {
  const auto n_unique = index->get_n_unique_in_tree();
  auto tuple = dtuple_create(heap, n_unique + 1);

  /* When searching in the tree for the node pointer, we must not do
  comparison on the last field, the page number field, as on upper
  levels in the tree there may be identical node pointers with a
  different page number; therefore, we set the n_fields_cmp to one
  less: */

  dtuple_set_n_fields_cmp(tuple, n_unique);

  index->copy_types(tuple, n_unique);

  auto buf = reinterpret_cast<byte *>(mem_heap_alloc(heap, 4));

  mach_write_to_4(buf, page_no);

  auto field = dtuple_get_nth_field(tuple, n_unique);

  dfield_set_data(field, buf, 4);

  dtype_set(dfield_get_type(field), DATA_SYS_CHILD, DATA_NOT_NULL, 4);

  rec_copy_prefix_to_dtuple(tuple, rec, index, n_unique, heap);

  dtuple_set_info_bits(tuple, dtuple_get_info_bits(tuple) | REC_STATUS_NODE_PTR);

  ut_ad(dtuple_check_typed(tuple));

  return tuple;
}

void Dict::update_statistics(Table *table) noexcept {

  if (table->m_ibd_file_missing) {
    log_err(std::format(
      "Cannot calculate statistics for table {} because the .ibd file is missing. For help,"
      " please refer to the InnoDB website for details",
      table->m_name
    ));

    return;
  }

  /* If we have set a high innodb_force_recovery level, do not calculate
  statistics, as a badly corrupted index can cause a crash in it. */

  if (srv_config.m_force_recovery > IB_RECOVERY_NO_TRX_UNDO) {

    return;
  }

  /* Find out the sizes of the indexes and how many different values
  for the key they approximately have */

  if (table->m_indexes.empty()) {
    /* Table definition is corrupt */
    return;
  }

  auto fsp = m_store.m_fsp;
  auto &stats = table->m_stats;
  auto btree = m_store.m_btree;
  ulint sum_of_index_sizes{};
  Btree_cursor btr_cur(fsp, btree);

  for (auto index : table->m_indexes) {
    auto size = btree->get_size(index, BTR_TOTAL_SIZE);

    index->m_stats.m_index_size = size;

    sum_of_index_sizes += size;

    size = btree->get_size(index, BTR_N_LEAF_PAGES);

    if (size == 0) {
      /* The root node of the tree is a leaf */
      size = 1;
    }

    index->m_stats.m_n_leaf_pages = size;

    btr_cur.estimate_number_of_different_key_vals(index);
  }

  auto index = table->m_indexes.front();

  // FIXME: Use atomics and get rid of this mutex
  index_stat_mutex_enter(index);

  stats.m_n_rows = index->m_stats.m_n_diff_key_vals[index->get_n_unique()];

  index_stat_mutex_exit(index);

  stats.m_clustered_index_size = index->m_stats.m_index_size;

  stats.m_sum_of_secondary_index_sizes = sum_of_index_sizes - index->m_stats.m_index_size;

  stats.m_initialized = true;

  stats.m_modified_counter = 0;
}

void Dict::foreign_print(Foreign *foreign) noexcept {
  ut_ad(mutex_own(&m_mutex));

  log_err(std::format("  FOREIGN KEY CONSTRAINT {}: {} (", foreign->m_id, foreign->m_foreign_table_name));

  for (ulint i{}; i < foreign->m_n_fields; ++i) {
    log_err(" ", foreign->m_foreign_col_names[i], " ");
  }

  log_err(" ) REFERENCES ", foreign->m_referenced_table_name, " (");

  for (ulint i{}; i < foreign->m_n_fields; ++i) {
    log_err(" ", foreign->m_referenced_col_names[i], " ");
  }

  log_err(" )");
}

void Dict::table_print(Table *table) noexcept {
  mutex_enter(&m_mutex);

  log_err(table->to_string(this));

  mutex_exit(&m_mutex);
}

void Dict::table_print_by_name(const char *name) noexcept {
  mutex_enter(&m_mutex);

  auto table = table_get(name);

  table_print(table);

  mutex_exit(&m_mutex);
}

std::string Index::to_string() const noexcept {
  std::string str;

  return str;
}

std::string Foreign::to_string() const noexcept {
  std::string str;

  return str;
}

std::string Table::to_string(Dict *dict) noexcept {
  std::string str;
  ut_ad(mutex_own(&dict->m_mutex));

  dict->update_statistics(this);

  str = std::format(
    "--------------------------------------\n"
    "TABLE: name {}, id {} {}, flags {}, columns {}, indexes {}, appr.rows {}\n"
    "  COLUMNS: ",
    m_name,
    m_id,
    m_id,
    (ulong)m_flags,
    (ulong)m_n_cols,
    m_indexes.size(),
    m_stats.m_n_rows
  );

  for (ulint i{}; i < (ulint)m_n_cols; ++i) {
    dict->col_print(this, get_nth_col(i));
    str += "; ";
  }

  for (auto index : m_indexes) {
    str += index->to_string();
  }

  for (auto foreign : m_foreign_list) {
    str += foreign->to_string();
  }

  for (auto foreign : m_referenced_list) {
    str += foreign->to_string();
  }

  return str;
}

void Dict::col_print(const Table *table, const Column *col) noexcept {
  dtype_t type;

  ut_ad(mutex_own(&m_mutex));

  col->copy_type(&type);

  log_err(table->get_col_name(col->get_no()), ": ");

  dtype_print(&type);
}

void Dict::index_print(Index *index) noexcept {
  ut_ad(mutex_own(&m_mutex));

  index_stat_mutex_enter(index);

  int64_t n_vals;

  if (index->m_n_user_defined_cols > 0) {
    n_vals = index->m_stats.m_n_diff_key_vals[index->m_n_user_defined_cols];
  } else {
    n_vals = index->m_stats.m_n_diff_key_vals[1];
  }

  index_stat_mutex_exit(index);

  log_err(std::format(
    "  INDEX: name {}, id {},  fields {}/{}, uniq %lu, type {}\n"
    "   root page {}, appr.key vals {}, leaf pages {}, size pages {}\n"
    "   FIELDS: ",
    index->m_name,
    index->m_id,
    (int)index->m_n_user_defined_cols,
    (int)index->m_n_fields,
    (int)index->m_n_uniq,
    (int)index->m_type,
    index->m_page_id.to_string(),
    n_vals,
    index->m_stats.m_n_leaf_pages,
    index->m_stats.m_index_size
  ));

  for (ulint i{}; i < index->m_n_fields; ++i) {
    field_print(index->get_nth_field(i));
  }

  log_err("");

#ifdef UNIV_BTR_PRINT
  m_store.m_btree->print_size(index);
  m_store.m_btree->print_index(index, 7);
#endif /* UNIV_BTR_PRINT */
}

/**
 * @brief Prints a field data.
 *
 * @param[in] field Pointer to the field to be printed.
 */
void Dict::field_print(const Field *field) noexcept {
  ut_ad(mutex_own(&m_mutex));

  log_err(" ", field->m_name);

  if (field->m_prefix_len != 0) {
    log_err("( ", field->m_prefix_len, " )");
  }
}

void Dict::print_info_on_foreign_key_in_create_format(Trx *trx, const Foreign *foreign, bool add_newline) noexcept {
  const char *stripped_id;

  if (strchr(foreign->m_id, '/')) {
    /* Strip the preceding database name from the constraint id */
    stripped_id = foreign->m_id + 1 + get_db_name_len(foreign->m_id);
  } else {
    stripped_id = foreign->m_id;
  }

  log_err(",");

  if (add_newline) {
    /* SHOW CREATE TABLE wants constraints each printed nicely
    on its own line, while error messages want no newlines
    inserted. */
    log_err(" ");
  }

  std::string str{};

  str = std::format(" CONSTRAINT {} FOREIGN KEY (", stripped_id);

  for (ulint i = 0;;) {
    str += foreign->m_foreign_col_names[i];

    if (++i < foreign->m_n_fields) {
      str += ", ";
    } else {
      break;
    }
  }

  str += ") REFERENCES ";

  if (tables_have_same_db(foreign->m_foreign_table_name, foreign->m_referenced_table_name)) {
    /* Do not print the database name of the referenced table */
    str += remove_db_name(foreign->m_referenced_table_name);
  } else {
    str += foreign->m_referenced_table_name;
  }

  str += " (";

  for (ulint i = 0;;) {
    str += foreign->m_referenced_col_names[i];
    if (++i < foreign->m_n_fields) {
      str += ", ";
    } else {
      break;
    }
  }

  str += ")";

  if (foreign->m_type & DICT_FOREIGN_ON_DELETE_CASCADE) {
    str += " ON DELETE CASCADE";
  }

  if (foreign->m_type & DICT_FOREIGN_ON_DELETE_SET_NULL) {
    str += " ON DELETE SET nullptr";
  }

  if (foreign->m_type & DICT_FOREIGN_ON_DELETE_NO_ACTION) {
    str += " ON DELETE NO ACTION";
  }

  if (foreign->m_type & DICT_FOREIGN_ON_UPDATE_CASCADE) {
    str += " ON UPDATE CASCADE";
  }

  if (foreign->m_type & DICT_FOREIGN_ON_UPDATE_SET_NULL) {
    str += " ON UPDATE SET nullptr";
  }

  if (foreign->m_type & DICT_FOREIGN_ON_UPDATE_NO_ACTION) {
    str += " ON UPDATE NO ACTION";
  }

  log_err(str);
}

void Dict::print_info_on_foreign_keys(bool create_table_format, Trx *trx, Table *table) noexcept {
  std::string str{};

  mutex_enter(&m_mutex);

  for (auto foreign : table->m_foreign_list) {
    if (create_table_format) {
      print_info_on_foreign_key_in_create_format(trx, foreign, true);
    } else {
      str = "; (";

      for (ulint i{}; i < foreign->m_n_fields; ++i) {
        if (i > 0) {
          str += " ";
        }

        str += foreign->m_foreign_col_names[i];
      }

      str += std::format(") REFER {} (", foreign->m_referenced_table_name);

      for (ulint i{}; i < foreign->m_n_fields; ++i) {
        if (i > 0) {
          str += " ";
        }
        str += foreign->m_referenced_col_names[i];
      }

      str += ")";

      if (foreign->m_type == DICT_FOREIGN_ON_DELETE_CASCADE) {
        str += " ON DELETE CASCADE";
      }

      if (foreign->m_type == DICT_FOREIGN_ON_DELETE_SET_NULL) {
        str += " ON DELETE SET nullptr";
      }

      if (foreign->m_type & DICT_FOREIGN_ON_DELETE_NO_ACTION) {
        str += " ON DELETE NO ACTION";
      }

      if (foreign->m_type & DICT_FOREIGN_ON_UPDATE_CASCADE) {
        str += " ON UPDATE CASCADE";
      }

      if (foreign->m_type & DICT_FOREIGN_ON_UPDATE_SET_NULL) {
        str += " ON UPDATE SET nullptr";
      }

      if (foreign->m_type & DICT_FOREIGN_ON_UPDATE_NO_ACTION) {
        str += " ON UPDATE NO ACTION";
      }
    }
  }

  mutex_exit(&m_mutex);

  log_err(str);
}