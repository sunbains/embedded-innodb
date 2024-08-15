/****************************************************************************
Copyright (c) 1996, 2009, Innobase Oy. All Rights Reserved.
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

/** @file include/dict0mem.h
Data dictionary memory object creation

Created 1/8/1996 Heikki Tuuri
*******************************************************/

#pragma once

#include "innodb0types.h"
#include "mem0types.h"

struct dict_index_t;
struct dict_table_t;
struct dict_foreign_t;

/**
 * @brief Creates a table memory object.
 * @param name The table name.
 * @param space The space where the clustered index of the table is placed; this parameter is ignored if the table is made a member of a cluster.
 * @param n_cols The number of columns.
 * @param flags The table flags.
 * @return The created table object.
 */
dict_table_t *dict_mem_table_create(const char *name, space_id_t space, ulint n_cols, ulint flags);

/**
 * @brief Free a table memory object.
 * @param table The table object to be freed.
 */
void dict_mem_table_free(dict_table_t *table);

/**
 * @brief Adds a column definition to a table.
 * @param table The table object.
 * @param heap The temporary memory heap, or nullptr.
 * @param name The column name, or nullptr.
 * @param mtype The main datatype.
 * @param prtype The precise type.
 * @param len The precision.
 */
void dict_mem_table_add_col(dict_table_t *table, mem_heap_t *heap, const char *name, ulint mtype, ulint prtype, ulint len);

/**
 * @brief Creates an index memory object.
 * @param table_name The table name.
 * @param index_name The index name.
 * @param space The space where the index tree is placed, ignored if the index is of the clustered type.
 * @param type The index type (DICT_UNIQUE, DICT_CLUSTERED, ... ORed).
 * @param n_fields The number of fields.
 * @return The created index object.
 */
dict_index_t *dict_mem_index_create(const char *table_name, const char *index_name, space_id_t space, ulint type, ulint n_fields);

/**
 * @brief Adds a field definition to an index.
 * @param index The index object.
 * @param name The column name.
 * @param prefix_len The column prefix length in a column prefix index like INDEX (textcol(25)).
 */
void dict_mem_index_add_field(dict_index_t *index, const char *name, ulint prefix_len);

/**
 * @brief Frees an index memory object.
 * @param index The index object to be freed.
 */
void dict_mem_index_free(dict_index_t *index);

/**
 * @brief Creates and initializes a foreign constraint memory object.
 * @return The created foreign constraint struct.
 */
dict_foreign_t *dict_mem_foreign_create();
