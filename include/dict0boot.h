/****************************************************************************
Copyright (c) 1996, 2010, Innobase Oy. All Rights Reserved.

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

/** @file include/dict0boot.h
Data dictionary creation and booting

Created 4/18/1996 Heikki Tuuri
*******************************************************/

#pragma once

#include "innodb0types.h"

#include "buf0buf.h"
#include "dict0dict.h"
#include "fsp0fsp.h"
#include "mtr0log.h"
#include "mtr0mtr.h"
#include "ut0byte.h"

typedef byte dict_hdr_t;

/** Gets a pointer to the dictionary header and x-latches its page.
@return	pointer to the dictionary header, page x-latched */
dict_hdr_t *dict_hdr_get(mtr_t *mtr); /*!< in: mtr */

/** Returns a new row, table, index, or tree id.
@return	the new id */
uint64_t dict_hdr_get_new_id(ulint type); /*!< in: DICT_HDR_ROW_ID, ... */

/** Returns a new row id.
@return	the new id */
inline uint64_t dict_sys_get_new_row_id(void);

/** Reads a row id from a record or other 6-byte stored form.
@return	row id */
inline uint64_t dict_sys_read_row_id(byte *field); /*!< in: record field */

/** Writes a row id to a record or other 6-byte stored form. */
inline void dict_sys_write_row_id(
  byte *field, /*!< in: record field */
  uint64_t row_id
); /*!< in: row id */

/** Initializes the data dictionary memory structures when the database is
started. This function is also called when the data dictionary is created. */
void dict_boot();

/** Creates and initializes the data dictionary at the database creation. */
void dict_create();

/* Space id and page no where the dictionary header resides */
#define DICT_HDR_SPACE 0 /* the SYSTEM tablespace */
#define DICT_HDR_PAGE_NO FSP_DICT_HDR_PAGE_NO

/* The ids for the basic system tables and their indexes */
constexpr ulint DICT_TABLES_ID = 1;
constexpr ulint DICT_COLUMNS_ID = 2;
constexpr ulint DICT_INDEXES_ID = 3;
constexpr ulint DICT_FIELDS_ID = 4;

/** The following is a secondary index on SYS_TABLES */
constexpr ulint DICT_TABLE_IDS_ID = 5;

/** The ids for tables etc. start from this number, except for basic system tables and their above defined indexes. */
constexpr ulint DICT_HDR_FIRST_ID = 10;

/* System table IDs start from this. */
constexpr uint64_t DICT_SYS_ID_MIN = (0xFFFFFFFFUL << 32) | SYS_TABLESPACE;

/* The offset of the dictionary header on the page */
constexpr auto DICT_HDR = FSEG_PAGE_DATA;

/*-------------------------------------------------------------*/
/* Dictionary header offsets */
/** The latest assigned row id */
constexpr ulint DICT_HDR_ROW_ID = 0;

/** The latest assigned table id */
constexpr ulint DICT_HDR_TABLE_ID = 8;

/** The latest assigned index id */
constexpr ulint DICT_HDR_INDEX_ID = 16;

/** Obsolete, always 0. */
constexpr ulint DICT_HDR_MIX_ID = 24;

/** Root of the table index tree */
constexpr ulint DICT_HDR_TABLES = 32;

/** Root of the table index tree */
constexpr ulint DICT_HDR_TABLE_IDS = 36;

/** Root of the column index tree */
constexpr ulint DICT_HDR_COLUMNS = 40;

/** Root of the index index tree */
constexpr ulint DICT_HDR_INDEXES = 44;

/** Root of the index field index tree */
constexpr ulint DICT_HDR_FIELDS = 48;

/** Segment header for the tablespace segment into which the dictionary header is created */
constexpr ulint DICT_HDR_FSEG_HEADER = 56;

/*-------------------------------------------------------------*/

/** The field number of the page number field in the sys_indexes table clustered index */

constexpr ulint DICT_SYS_INDEXES_PAGE_NO_FIELD = 8;
constexpr ulint DICT_SYS_INDEXES_SPACE_NO_FIELD = 7;
constexpr ulint DICT_SYS_INDEXES_TYPE_FIELD = 6;
constexpr ulint DICT_SYS_INDEXES_NAME_FIELD = 4;

/** When a row id which is zero modulo this number (which must be a power of
two) is assigned, the field DICT_HDR_ROW_ID on the dictionary header page is
updated */
constexpr ulint DICT_HDR_ROW_ID_WRITE_MARGIN = 256;

/** Writes the current value of the row id counter to the dictionary header file
page. */
void dict_hdr_flush_row_id();

/** Returns a new row id.
@return	the new id */
inline uint64_t dict_sys_get_new_row_id(void) {
  mutex_enter(&(dict_sys->mutex));

  auto id = dict_sys->row_id;

  if (0 == (id % DICT_HDR_ROW_ID_WRITE_MARGIN)) {

    dict_hdr_flush_row_id();
  }

  ++dict_sys->row_id;

  mutex_exit(&dict_sys->mutex);

  return id;
}

/** Reads a row id from a record or other 6-byte stored form.
@return	row id */
inline uint64_t dict_sys_read_row_id(byte *field) /*!< in: record field */
{
  static_assert(DATA_ROW_ID_LEN == 6, "error DATA_ROW_ID_LEN != 6");

  return mach_read_from_6(field);
}

/** Writes a row id to a record or other 6-byte stored form. */
inline void dict_sys_write_row_id(
  byte *field, /*!< in: record field */
  uint64_t row_id
) /*!< in: row id */
{
  static_assert(DATA_ROW_ID_LEN == 6, "error DATA_ROW_ID_LEN != 6");

  mach_write_to_6(field, row_id);
}
