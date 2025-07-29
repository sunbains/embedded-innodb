/****************************************************************************
Copyright (c) 1997, 2009, Innobase Oy. All Rights Reserved.
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

/** @file include/pars0sym.h
SQL parser symbol table

Created 12/15/1997 Heikki Tuuri
*******************************************************/

#pragma once

#include "dict0types.h"
#include "innodb0types.h"
#include "pars0types.h"
#include "que0types.h"
#include "row0types.h"
#include "usr0types.h"

/**
 * @brief Creates a symbol table for a single stored procedure or query.
 * 
 * @param[in] heap Memory heap where to create the symbol table.
 * @return Pointer to the created symbol table.
 */
sym_tab_t *sym_tab_create(mem_heap_t *heap);

/** Frees the memory allocated dynamically AFTER parsing phase for variables
etc. in the symbol table. Does not free the mem heap where the table was
originally created. Frees also SQL explicit cursor definitions. */
void sym_tab_free_private(sym_tab_t *sym_tab); /** in, own: symbol table */

/**
 * @brief Adds an integer literal to a symbol table.
 * 
 * @param[in] sym_tab Symbol table to add the integer literal to.
 * @param[in] val Integer value to add.
 * 
 * @return Pointer to the symbol table node.
 */
sym_node_t *sym_tab_add_int_lit(sym_tab_t *sym_tab, ulint val);

/**
 * @brief Adds an string literal to a symbol table.
 * 
 * @param[in] sym_tab Symbol table to add the string literal to.
 * @param[in] str String to add.
 * @param[in] len Length of the string.
 * 
 * @return Pointer to the symbol table node.
 */
sym_node_t *sym_tab_add_int_lit(sym_tab_t *sym_tab, ulint val);

/**
 * @brief Adds an string literal to a symbol table.
 * 
 * @param[in] sym_tab Symbol table to add the string literal to.
 * @param[in] str String to add.
 * @param[in] len Length of the string.
 * 
 * @return Pointer to the symbol table node.
 */
sym_node_t *sym_tab_add_str_lit(sym_tab_t *sym_tab, byte *str, ulint len);

/**
 * Add a bound literal to a symbol table.
 * 
 * @param[in] sym_tab Symbol table to add the bound literal to.
 * @param[in] name Name of the bound literal.
 * @param[out] lit_type Type of the literal (PARS_*_LIT).
 * 
 * @return Pointer to the symbol table node.
 */
sym_node_t *sym_tab_add_bound_lit(sym_tab_t *sym_tab, const char *name, ulint *lit_type);

/**
 * @brief Adds an SQL null literal to a symbol table.
 * 
 * @param[in] sym_tab Symbol table to add the null literal to.
 * 
 * @return Pointer to the symbol table node.
 */
sym_node_t *sym_tab_add_null_lit(sym_tab_t *sym_tab);

/**
 * @brief Adds an identifier to a symbol table.
 * 
 * @param[in] sym_tab Symbol table to add the identifier to.
 * @param[in] name Name of the identifier.
 * @param[in] len Length of the identifier.
 * 
 * @return Pointer to the symbol table node.
 */
sym_node_t *sym_tab_add_id(sym_tab_t *sym_tab, byte *name, ulint len);

/**
 * @brief Add a bound identifier to a symbol table.
 * 
 * @param[in] sym_tab Symbol table to add the bound identifier to.
 * @param[in] name Name of the bound identifier.
 * 
 * @return Pointer to the symbol table node.
 */
sym_node_t *sym_tab_add_bound_id(sym_tab_t *sym_tab, const char *name);

/** Index of sym_node_struct::field_nos corresponding to the clustered index */
constexpr ulint SYM_CLUST_FIELD_NO = 0;

/** Index of sym_node_struct::field_nos corresponding to a secondary index */
constexpr ulint SYM_SEC_FIELD_NO = 1;

/** Types of a symbol table node */
enum sym_tab_entry {
  /**
   * @brief Declared parameter or local variable of a procedure.
   */
  SYM_VAR = 91,

  /**
   * @brief Storage for an intermediate result of a calculation.
   */
  SYM_IMPLICIT_VAR,

  /**
   * @brief Literal.
   */
  SYM_LIT,

  /**
   * @brief Database table name.
   */
  SYM_TABLE,

  /**
   * @brief Database column name.
   */
  SYM_COLUMN,

  /**
   * @brief Named cursor.
   */
  SYM_CURSOR,

  /**
   * @brief Stored procedure name.
   */
  SYM_PROCEDURE_NAME,

  /**
   * @brief Database index name.
   */
  SYM_INDEX,

  /**
   * @brief User function name.
   */
  SYM_FUNCTION
};

/** Symbol table node */
struct sym_node_struct {
  /* NOTE: if the data field in 'common.val' is not nullptr and the symbol
  table node is not for a temporary column, the memory for the value has
  been allocated from dynamic memory and it should be freed when the
  symbol table is discarded */
  /* 'alias' and 'indirection' are almost the same, but not quite.
  'alias' always points to the primary instance of the variable, while
  'indirection' does the same only if we should use the primary
  instance's values for the node's data. This is usually the case, but
  when initializing a cursor (e.g., "DECLARE CURSOR c IS SELECT * FROM
  t WHERE id = x;"), we copy the values from the primary instance to
  the cursor's instance so that they are fixed for the duration of the
  cursor, and set 'indirection' to nullptr. If we did not, the value of
  'x' could change between fetches and things would break horribly.

  TODO: It would be cleaner to make 'indirection' a boolean field and
  always use 'alias' to refer to the primary node. */

  /** Node type: QUE_NODE_SYMBOL */
  que_common_t common;

  /** Pointer to another symbol table node which contains the value for this node, nullptr otherwise */
  sym_node_t *indirection;

  /** Pointer to another symbol table node for which this node is an alias, nullptr otherwise */
  sym_node_t *alias;

  /** List of table columns or a list of input variables for an explicit cursor */
  UT_LIST_NODE_T(sym_node_t) col_var_list;

  /** True if a column and its value should be copied to dynamic memory when fetched */
  bool copy_val;

  /** If a column, in the position SYM_CLUST_FIELD_NO is the field number in the clustered
   * index; in the position SYM_SEC_FIELD_NO the field number in the non-clustered index
   * to use first; if not found from the index, then ULINT_UNDEFINED */
  ulint field_nos[2];

  /** True if the meaning of a variable or a column has been resolved; for literals this is always true */
  bool resolved;

  /** Type of the parsed token */
  sym_tab_entry token_type;

  /** Name of an id */
  const char *name;

  /** Id name length */
  ulint name_len;

  /** Table definition if a table id or a column id */
  Table *table;

  /** Column number if a column */
  ulint col_no;

  /** nullptr, or a buffer for cached column values for prefetched rows */
  sel_buf_t *prefetch_buf;

  /** Cursor definition select node if a named cursor */
  sel_node_t *cursor_def;

  /** PARS_INPUT, PARS_OUTPUT, or PARS_NOT_PARAM if not a procedure parameter */
  ulint param_type;

  /** Back pointer to the symbol table */
  sym_tab_t *sym_table;

  /** List of symbol nodes */
  UT_LIST_NODE_T(sym_node_t) sym_list;
};

UT_LIST_NODE_GETTER_DEFINITION(sym_node_t, col_var_list);
UT_LIST_NODE_GETTER_DEFINITION(sym_node_t, sym_list);

/** Symbol table */
struct sym_tab_t {
  que_t *query_graph;
  /** query graph generated by the
  parser */
  const char *sql_string;
  /** SQL string to parse */
  size_t string_len;
  /** SQL string length */
  int next_char_pos;
  /** position of the next character in
  sql_string to give to the lexical
  analyzer */
  pars_info_t *info; /** extra information, or nullptr */

  /** List of symbol nodes in the symbol table */
  UT_LIST_BASE_NODE_T_EXTERN(sym_node_t, sym_list) sym_list;

  /** List of function nodes in the parsed query graph */
  func_node_list_t func_node_list;

  /** memory heap from which we can allocate space */
  mem_heap_t *heap;
};
