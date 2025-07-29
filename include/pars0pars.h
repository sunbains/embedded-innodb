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

/** @file include/pars0pars.h
SQL parser

Created 11/19/1996 Heikki Tuuri
*******************************************************/

#pragma once

#include "pars0types.h"
#include "que0types.h"
#include "row0types.h"
#include "trx0types.h"
#include "usr0types.h"

#include <vector>

struct Index_node;
struct Table_node;

/** Type of the user functions. The first argument is always InnoDB-supplied
and varies in type, while 'user_arg' is a user-supplied argument. The
meaning of the return type also varies. See the individual use cases, e.g.
the FETCH statement, for details on them. */
typedef void *(*pars_user_func_cb_t)(void *arg, void *user_arg);

/** If the following is set true, the parser will emit debugging
information */
extern int yydebug;

#ifdef UNIV_SQL_DEBUG
/** If the following is set true, the lexer will print the SQL string
as it tokenizes it */
extern bool pars_print_lexed;
#endif /* UNIV_SQL_DEBUG */

/* Global variable used while parsing a single procedure or query : the code is
NOT re-entrant */
extern sym_tab_t *pars_sym_tab_global;

extern pars_res_word_t pars_to_char_token;
extern pars_res_word_t pars_to_number_token;
extern pars_res_word_t pars_to_binary_token;
extern pars_res_word_t pars_binary_to_number_token;
extern pars_res_word_t pars_substr_token;
extern pars_res_word_t pars_replstr_token;
extern pars_res_word_t pars_concat_token;
extern pars_res_word_t pars_length_token;
extern pars_res_word_t pars_instr_token;
extern pars_res_word_t pars_sysdate_token;
extern pars_res_word_t pars_printf_token;
extern pars_res_word_t pars_assert_token;
extern pars_res_word_t pars_rnd_token;
extern pars_res_word_t pars_rnd_str_token;
extern pars_res_word_t pars_count_token;
extern pars_res_word_t pars_sum_token;
extern pars_res_word_t pars_distinct_token;
extern pars_res_word_t pars_binary_token;
extern pars_res_word_t pars_blob_token;
extern pars_res_word_t pars_int_token;
extern pars_res_word_t pars_char_token;
extern pars_res_word_t pars_float_token;
extern pars_res_word_t pars_update_token;
extern pars_res_word_t pars_asc_token;
extern pars_res_word_t pars_desc_token;
extern pars_res_word_t pars_open_token;
extern pars_res_word_t pars_close_token;
extern pars_res_word_t pars_share_token;
extern pars_res_word_t pars_unique_token;
extern pars_res_word_t pars_clustered_token;

extern ulint pars_star_denoter;

/* Procedure parameter types */
constexpr ulint PARS_INPUT = 0;
constexpr ulint PARS_OUTPUT = 1;
constexpr ulint PARS_NOT_PARAM = 2;

/** Classes of functions */
/* @{ */

/** +, -, *, / */
constexpr ulint PARS_FUNC_ARITH = 1;

/** AND, OR, NOT */
constexpr ulint PARS_FUNC_LOGICAL = 2;

/** Comparison operators */
constexpr ulint PARS_FUNC_CMP = 3;

/** TO_NUMBER, SUBSTR, ... */
constexpr ulint PARS_FUNC_PREDEFINED = 4;

/** COUNT, DISTINCT, SUM */
constexpr ulint PARS_FUNC_AGGREGATE = 5;

/** these are not real functions, e.g., := */
constexpr ulint PARS_FUNC_OTHER = 6;

/* @} */

int yyparse();

/**
 * @brief Parses an SQL string returning the query graph.
 * 
 * @param[in] info Extra information, or nullptr.
 * @param[in] str SQL string.
 * 
 * @return The query graph.
 */
que_t *pars_sql(pars_info_t *info, const char *str);

/**
 * @brief Retrieves characters to the lexical analyzer.
 * 
 * @param[in,out] buf Buffer where to copy characters.
 * @param[in] max_size Maximum number of characters which fit in the buffer.
 * 
 * @return Number of characters copied.
 */
int pars_get_lex_chars(char *buf, int max_size);

/**
 * @brief Called by yyparse on error.
 * 
 * @param[in] s Error message string.
 */
void yyerror(const char *s);

/**
 * @brief Parses a variable declaration.
 * 
 * @param[in] node Symbol table node allocated for the id of the variable.
 * @param[in] type Pointer to a type token.
 * 
 * @return Symbol table node of type SYM_VAR.
 */
sym_node_t *pars_variable_declaration(sym_node_t *node, pars_res_word_t *type);

/**
 * @brief Parses a function expression.
 * 
 * @param[in] res_word Function name reserved word.
 * @param[in] arg First argument in the argument list.
 * 
 * @return Function node in a query tree.
 */
func_node_t *pars_func(que_node_t *res_word, que_node_t *arg);

/**
 * @brief Parses an operator expression.
 * 
 * @param[in] func Operator token code.
 * @param[in] arg1 First argument.
 * @param[in] arg2 Second argument or nullptr for a unary operator.
 * 
 * @return Function node in a query tree.
 */
func_node_t *pars_op(int func, que_node_t *arg1, que_node_t *arg2);

/**
 * @brief Parses an ORDER BY clause. Order by a single column only is supported.
 * 
 * @param[in] column Column name.
 * @param[in] asc &pars_asc_token or pars_desc_token.
 * 
 * @return Order-by node in a query tree.
 */
order_node_t *pars_order_by(sym_node_t *column, pars_res_word_t *asc);

/**
 * @brief Parses a select list and creates a query graph node for the whole SELECT statement.
 * 
 * @param[in] select_list The select list.
 * @param[in] into_list The variables list or nullptr.
 * 
 * @return sel_node_t* The select node in a query tree.
 */
sel_node_t *pars_select_list(que_node_t *select_list, sym_node_t *into_list);

/**
 * @brief Parses a cursor declaration.
 * 
 * @param[in] sym_node Cursor id node in the symbol table.
 * @param[in] select_node Select node.
 * 
 * @return que_node_t* The cursor declaration node.
 */
que_node_t *pars_cursor_declaration(sym_node_t *sym_node, sel_node_t *select_node);

/**
 * @brief Parses a function declaration.
 * 
 * @param[in] sym_node Function id node in the symbol table.
 * 
 * @return que_node_t* The function declaration node.
 */
que_node_t *pars_function_declaration(sym_node_t *sym_node);

/**
 * @brief Parses a select statement.
 * 
 * @param[in] select_node Select node already containing the select list.
 * @param[in] table_list Table list.
 * @param[in] search_cond Search condition or nullptr.
 * @param[in] for_update nullptr or &pars_update_token.
 * @param[in] consistent_read nullptr or &pars_consistent_token.
 * @param[in] order_by nullptr or an order-by node.
 * 
 * @return sel_node_t* Select node in a query tree.
 */
sel_node_t *pars_select_statement(
  sel_node_t *select_node, sym_node_t *table_list, que_node_t *search_cond, pars_res_word_t *for_update,
  pars_res_word_t *consistent_read, order_node_t *order_by
);

/**
 * @brief Parses a column assignment in an update.
 * 
 * @param[in] column Column to assign.
 * @param[in] exp Value to assign.
 * 
 * @return col_assign_node_t* Column assignment node.
 */
col_assign_node_t *pars_column_assignment(sym_node_t *column, que_node_t *exp);

/**
 * @brief Parses a delete or update statement start.
 * 
 * @param[in] is_delete true if delete.
 * @param[in] table_sym Table name node.
 * @param[in] col_assign_list Column assignment list or nullptr.
 * 
 * @return upd_node_t* Update node in a query tree.
 */
upd_node_t *pars_update_statement_start(bool is_delete, sym_node_t *table_sym, col_assign_node_t *col_assign_list);

/**
 * @brief Parses an update or delete statement.
 * 
 * @param[in] node Update node.
 * @param[in] cursor_sym Pointer to a cursor entry in the symbol table or nullptr.
 * 
 * @return upd_node_t* Update node in a query tree.
 */
upd_node_t *pars_update_statement(upd_node_t *node, sym_node_t *cursor_sym, que_node_t *search_cond);

/**
 * @brief Parses an insert statement.
 * 
 * @param[in] table_sym Table name node.
 * @param[in] values_list Value expression list or nullptr.
 * @param[in] select Select condition or nullptr.
 * 
 * @return ins_node_t* Insert node in a query tree.
 */
ins_node_t *pars_insert_statement(sym_node_t *table_sym, que_node_t *values_list, sel_node_t *select);

/**
 * @brief Parses a procedure parameter declaration.
 * 
 * @param[in] node Symbol table node allocated for the id of the parameter.
 * @param[in] param_type PARS_INPUT or PARS_OUTPUT.
 * @param[in] type Pointer to a type token.
 * 
 * @return sym_node_t* Symbol table node of type SYM_VAR.
 */
sym_node_t *pars_parameter_declaration(sym_node_t *node, ulint param_type, pars_res_word_t *type);

/**
 * @brief Parses an elsif element.
 * 
 * @param[in] cond If-condition.
 * @param[in] stat_list Statement list.
 * 
 * @return elsif_node_t* Elsif node.
 */
elsif_node_t *pars_elsif_element(que_node_t *cond, que_node_t *stat_list);

/**
 * @brief Parses an if-statement.
 * 
 * @param[in] cond If-condition.
 * @param[in] stat_list Statement list.
 * @param[in] else_part Else-part statement list.
 * 
 * @return if_node_t* If-statement node.
 */
if_node_t *pars_if_statement(que_node_t *cond, que_node_t *stat_list, que_node_t *else_part);

/**
 * @brief Parses a for-loop-statement.
 * 
 * @param[in] loop_var Loop variable.
 * @param[in] loop_start_limit Loop start expression.
 * @param[in] loop_end_limit Loop end expression.
 * @param[in] stat_list Statement list.
 * 
 * @return for_node_t* For-statement node.
 */
for_node_t *pars_for_statement(
  sym_node_t *loop_var, que_node_t *loop_start_limit, que_node_t *loop_end_limit, que_node_t *stat_list
);

/**
 * @brief Parses a while-statement.
 * 
 * @param[in] cond While-condition.
 * @param[in] stat_list Statement list.
 * 
 * @return while_node_t* While-statement node.
 */
while_node_t *pars_while_statement(que_node_t *cond, que_node_t *stat_list);

/**
 * @brief Parses an exit statement.
 * 
 * @return exit_node_t* Exit statement node.
 */
exit_node_t *pars_exit_statement();

/**
 * @brief Parses a return-statement.
 * 
 * @return return_node_t* Return statement node.
 */
return_node_t *pars_return_statement();

/**
 * @brief Parses a procedure call.
 * 
 * @param[in] res_word Procedure name reserved word.
 * @param[in] args Argument list.
 * 
 * @return func_node_t* Function node.
 */
func_node_t *pars_procedure_call(que_node_t *res_word, que_node_t *args);

/**
 * @brief Parses an assignment statement.
 * 
 * @param[in] var Variable to assign.
 * @param[in] val Value to assign.
 * 
 * @return assign_node_t* Assignment statement node.
 */
assign_node_t *pars_assignment_statement(sym_node_t *var, que_node_t *val);

/**
 * @brief Parses a fetch statement.
 * 
 * @param[in] cursor Cursor node.
 * @param[in] into_list Variables to set or nullptr.
 * @param[in] user_func User function name or nullptr.
 * 
 * @return fetch_node_t* Fetch statement node.
 */
fetch_node_t *pars_fetch_statement(sym_node_t *cursor, sym_node_t *into_list, sym_node_t *user_func);

/**
 * @brief Parses an open or close cursor statement.
 * 
 * @param[in] type ROW_SEL_OPEN_CURSOR or ROW_SEL_CLOSE_CURSOR.
 * @param[in] cursor Cursor node.
 * 
 * @return open_node_t* Open or close cursor statement node.
 */
open_node_t *pars_open_statement(ulint type, sym_node_t *cursor);

/**
 * @brief Parses a row_printf-statement.
 * 
 * @param[in] sel_node Select node.
 * 
 * @return row_printf_node_t* Row_printf-statement node.
 */
row_printf_node_t *pars_row_printf_statement(sel_node_t *sel_node);

/**
 * @brief Parses a commit statement.
 * 
 * @return commit_node_t* Commit statement node.
 */
Commit_node *pars_commit_statement();

/**
 * @brief Parses a rollback statement.
 * 
 * @return roll_node_t* Rollback statement node.
 */
roll_node_t *pars_rollback_statement();

/**
 * @brief Parses a column definition at a table creation.
 * 
 * @param[in] sym_node Column node in the symbol table.
 * @param[in] type Data type.
 * @param[in] len Length of column or nullptr.
 * @param[in] is_unsigned If not nullptr, column is of type UNSIGNED.
 * @param[in] is_not_null If not nullptr, column is of type NOT nullptr.
 * 
 * @return sym_node_t* Column symbol table node.
 */
sym_node_t *pars_column_def(sym_node_t *sym_node, pars_res_word_t *type, sym_node_t *len, void *is_unsigned, void *is_not_null);

/**
 * @brief Parses a table creation operation.
 *  
 * @param[in] table_sym Table name node in the symbol table.
 * @param[in] column_defs List of column names.
 * @param[in] not_fit_in_memory If not nullptr, then this table which is in simulations should be simulated as
 *  not fitting in memory; thread is put to sleep to simulate disk accesses.; NOTE that this flag is not stored
 *  to the data dictionary on disk, and the database will forget about non-nullptr value if it has to reload
 *  the table definition from disk.
 * 
 * @return Table_node* Table create subgraph.
 */
Table_node *pars_create_table(sym_node_t *table_sym, sym_node_t *column_defs, void *not_fit_in_memory);

/**
 * @brief Parses an index creation operation.
 * 
 * @param[in] unique_def Not nullptr if a unique index.
 * @param[in] clustered_def Not nullptr if a clustered index.
 * @param[in] index_sym Index name node in the symbol table.
 * @param[in] table_sym Table name node in the symbol table.
 * @param[in] column_list List of column names.
 * 
 * @return Index_node* Index create subgraph.
 */
Index_node *pars_create_index(
  pars_res_word_t *unique_def, pars_res_word_t *clustered_def, sym_node_t *index_sym, sym_node_t *table_sym, sym_node_t *column_list
);

/**
 * @brief Parses a procedure definition.
 * 
 * @param[in] sym_node Procedure id node in the symbol table.
 * @param[in] param_list Parameter declaration list.
 * @param[in] stat_list Statement list.
 * 
 * @return que_fork_t* Query fork node.
 */
que_fork_t *pars_procedure_definition(sym_node_t *sym_node, sym_node_t *param_list, que_node_t *stat_list);

/**
 * @brief Parses a stored procedure call, when this is not within another stored
 * procedure, that is, the client issues a procedure call directly.
 * In InnoDB, stored InnoDB procedures are invoked via the
 * parsed procedure tree, not via InnoDB SQL, so this function is not used.
 * 
 * @param[in] sym_node Stored procedure name node.
 * 
 * @return que_fork_t* Query graph.
 */
que_fork_t *pars_stored_procedure_call(sym_node_t *sym_node);

/**
 * @brief Completes a query graph by adding query thread and fork nodes
 * above it and prepares the graph for running. The fork created is of
 * type QUE_FORK_USER_INTERFACE.
 * 
 * @param[in] node Root node for an incomplete query graph.
 * @param[in] trx Transaction handle.
 * @param[in] heap Memory heap from which allocated.
 * 
 * @return que_thr_t* Query thread node to run.
 */
que_thr_t *pars_complete_graph_for_exec(que_node_t *node, Trx *trx, mem_heap_t *heap);

/**
 * @brief Create parser info struct.
 * 
 * @return pars_info_t* Info struct.
 */
pars_info_t *pars_info_create();

/**
 * @brief Free info struct and everything it contains.
 * 
 * @param[in] info Info struct.
 */
void pars_info_free(pars_info_t *info);

/**
 * @brief Add bound literal.
 * 
 * @param[in] info Info struct.
 * @param[in] name Name.
 * @param[in] address Address.
 * @param[in] length Length of data.
 * @param[in] type Type, e.g. DATA_FIXBINARY.
 * @param[in] prtype Precise type, e.g. DATA_UNSIGNED.
 */
void pars_info_add_literal(pars_info_t *info, const char *name, const void *address, ulint length, ulint type, ulint prtype);

/**
 * @brief Equivalent to pars_info_add_literal(info, name, str, strlen(str),
 * DATA_VARCHAR, DATA_ENGLISH).
 * 
 * @param[in] info Info struct.
 * @param[in] name Name.
 * @param[in] str String.
 */
void pars_info_add_str_literal(pars_info_t *info, const char *name, const char *str);

/**
 * @brief Equivalent to:
 * 
 * char buf[4];
 * mach_write_to_4(buf, val);
 * pars_info_add_literal(info, name, buf, 4, DATA_INT, 0);
 * 
 * except that the buffer is dynamically allocated from the info struct's heap.
 * 
 * @param[in] info Info struct.
 * @param[in] name Name.
 * @param[in] val Value.
 */
void pars_info_add_int4_literal(pars_info_t *info, const char *name, lint val);

/** Equivalent to:
 * 
 * char buf[8];
 * mach_write_to_8(buf, val);
 * pars_info_add_literal(info, name, buf, 8, DATA_INT, 0);
 * 
 * except that the buffer is dynamically allocated from the info struct's
 * heap.
 * 
 * @param[in] info Info struct.
 * @param[in] name Name.
 * @param[in] val Value.
 */
void pars_info_add_int8_literal(pars_info_t *info, const char *name, uint64_t val);

/** Equivalent to:
 * 
 * char buf[8];
 * mach_write_to_8(buf, val);
 * pars_info_add_literal(info, name, buf, 8, DATA_BINARY, 0);
 * 
 * except that the buffer is dynamically allocated from the info struct's
 * heap.
 * 
 * @param[in] info Info struct.
 * @param[in] name Name.
 * @param[in] val Value.
 */
void pars_info_add_uint64_literal(pars_info_t *info, const char *name, uint64_t val);

/**
 * @brief Add user function.
 * 
 * @param[in] info Info struct.
 * @param[in] name Function name.
 * @param[in] func Function address.
 * @param[in] arg User-supplied argument.
 */
void pars_info_add_function(pars_info_t *info, const char *name, pars_user_func_cb_t func, void *arg);

/**
 * @brief Add bound id.
 * 
 * @param[in] info Info struct.
 * @param[in] name Name.
 * @param[in] id Id.
 */
void pars_info_add_id(pars_info_t *info, const char *name, const char *id);

/**
 * @brief Get user function with the given name.
 * 
 * @param[in] info Info struct.
 * @param[in] name Function name.
 * 
 * @return User func, or nullptr if not found.
 */
pars_user_func_t *pars_info_get_user_func(pars_info_t *info, const char *name);

/**
 * @brief Get bound literal with the given name.
 * 
 * @param[in] info Info struct.
 * @param[in] name Name.
 * 
 * @return Bound literal, or nullptr if not found.
 */
pars_bound_lit_t *pars_info_get_bound_lit(pars_info_t *info, const char *name);

/**
 * @brief Get bound id with the given name.
 * 
 * @param[in] info Info struct.
 * @param[in] name Name.
 * 
 * @return Bound id, or nullptr if not found.
 */
pars_bound_id_t *pars_info_get_bound_id(pars_info_t *info, const char *name);

/**
 * @brief Release any resources used by the parser and lexer.
 */
void pars_close();

/**
 * @brief Reset and check parser variables.
 */
void pars_var_init(void);

/**
 * @brief Reset the lexing variables.
 */
void pars_lexer_var_init(void);

/**
 * @brief Release any resources used by the lexer.
 */
void pars_lexer_close(void);

/**
 * @brief Extra information supplied for pars_sql().
 */
struct pars_info_t {
  /** Our own memory heap */
  mem_heap_t *m_heap{};

  /** User functions, or nullptr (pars_user_func_t*) */
  std::vector<pars_user_func_t *> m_funcs{};

  /** Bound literals, or nullptr (pars_bound_lit_t*) */
  std::vector<pars_bound_lit_t *> m_bound_lits{};

  /** Bound ids, or nullptr (pars_bound_id_t*) */
  std::vector<pars_bound_id_t *> m_bound_ids{};

  /** If true (which is the default), que_graph_free() will free us */
  bool m_graph_owns_us{};
};

/** User-supplied function and argument. */
struct pars_user_func_struct {
  /** function name */
  const char *name;

  /** function address */
  pars_user_func_cb_t func;

  /** user-supplied argument */
  void *arg;
};

/** Bound literal. */
struct pars_bound_lit_struct {
  /** name */
  const char *name;

  /** address */
  const void *address;

  /** length of data */
  ulint length;

  /** type, e.g. DATA_FIXBINARY */
  ulint type;

  /** precise type, e.g. DATA_UNSIGNED */
  ulint prtype;
};

/** Bound identifier. */
struct pars_bound_id_struct {
  /** name */
  const char *name;

  /** identifier */
  const char *id;
};

/** Struct used to denote a reserved word in a parsing tree */
struct pars_res_word_struct {
  /** the token code for the reserved word from pars0grm.h */
  int code;
};

/** A predefined function or operator node in a parsing tree; this construct
is also used for some non-functions like the assignment ':=' */
struct func_node_struct {
  /** type: QUE_NODE_FUNC */
  que_common_t common;

  /** token code of the function name */
  int func;

  /** class of the function */
  ulint func_class;

  /** argument(s) of the function */
  que_node_t *args;

  /** list of comparison conditions; defined only for comparison operator
   * nodes except, presently, for OPT_SCROLL_TYPE ones */
  UT_LIST_NODE_T(func_node_t) cond_list;

  /** list of function nodes in a parsed query graph */
  UT_LIST_NODE_T(func_node_t) func_node_list;
};

UT_LIST_NODE_GETTER_DEFINITION(func_node_t, cond_list);

UT_LIST_NODE_GETTER_DEFINITION(func_node_t, func_node_list);

/** An order-by node in a select */
struct order_node_struct {
  /** type: QUE_NODE_ORDER */
  que_common_t common;

  /** order-by column */
  sym_node_t *column;

  /** true if ascending, false if descending */
  bool asc;
};

/** Procedure definition node */
struct proc_node_struct {
  /** type: QUE_NODE_PROC */
  que_common_t common;

  /** procedure name symbol in the symbol table of this same procedure */
  sym_node_t *proc_id;

  /** input and output parameters */
  sym_node_t *param_list;

  /** statement list */
  que_node_t *stat_list;

  /** symbol table of this procedure */
  sym_tab_t *sym_tab;
};

/** elsif-element node */
struct elsif_node_struct {
  /** type: QUE_NODE_ELSIF */
  que_common_t common;

  /** if condition */
  que_node_t *cond;

  /** statement list */
  que_node_t *stat_list;
};

/** if-statement node */
struct if_node_struct {
  /** type: QUE_NODE_IF */
  que_common_t common;

  /** if condition */
  que_node_t *cond;

  /** statement list */
  que_node_t *stat_list;

  /** else-part statement list */
  que_node_t *else_part;

  /** elsif element list */
  elsif_node_t *elsif_list;
};

/** while-statement node */
struct while_node_struct {
  /** type: QUE_NODE_WHILE */
  que_common_t common;

  /** while condition */
  que_node_t *cond;

  /** statement list */
  que_node_t *stat_list;
};

/** for-loop-statement node */
struct for_node_struct {
  /** type: QUE_NODE_FOR */
  que_common_t common;

  /** Loop variable: this is the dereferenced symbol from the variable declarations,
   * not the symbol occurrence in the for loop definition */
  sym_node_t *loop_var;

  /** Initial value of loop variable */
  que_node_t *loop_start_limit;

  /** end value of loop variable */
  que_node_t *loop_end_limit;

  /** Evaluated value for the end value: it is calculated only when the loop is
   * entered, and will not change within the loop */
  lint loop_end_value;

  /** statement list */
  que_node_t *stat_list;
};

/** exit statement node */
struct exit_node_struct {
  /** type: QUE_NODE_EXIT */
  que_common_t common;
};

/** return-statement node */
struct return_node_struct {
  /**
   * @brief Type: QUE_NODE_RETURN
   */
  que_common_t common;
};

/** Assignment statement node */
struct assign_node_struct {
  /**
   * @brief Type: QUE_NODE_ASSIGNMENT
   */
  que_common_t common;

  /**
   * @brief Variable to set
   */
  sym_node_t *var;

  /**
   * @brief Value to assign
   */
  que_node_t *val;
};

/** Column assignment node */
struct col_assign_node_struct {
  /**
   * @brief Type: QUE_NODE_COL_ASSIGN
   */
  que_common_t common;

  /**
   * @brief Column to set
   */
  sym_node_t *col;

  /**
   * @brief Value to assign
   */
  que_node_t *val;
};
