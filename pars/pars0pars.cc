/***************************************************************************
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

/** @file pars/pars0pars.c
SQL parser

Created 11/19/1996 Heikki Tuuri
*******************************************************/

/* Historical note: Innobase executed its first SQL string (CREATE TABLE) on 1/27/1998 */

#include "data0data.h"
#include "data0type.h"
#include "dict0dict.h"
#include "eval0eval.h"
#include "lock0lock.h"
#include "pars0grm.h"
#include "pars0opt.h"
#include "pars0pars.h"
#include "que0que.h"
#include "row0sel.h"
#include "row0ins.h"
#include "row0upd.h"
#include "trx0trx.h"
#include "trx0roll.h"

#ifdef UNIV_SQL_DEBUG
/** If the following is set true, the lexer will print the SQL string
as it tokenizes it */
bool pars_print_lexed = false;
#endif /* UNIV_SQL_DEBUG */

/* Global variable used while parsing a single procedure or query : the code is
NOT re-entrant */
sym_tab_t *pars_sym_tab_global;

/* Global variables used to denote certain reserved words, used in
constructing the parsing tree */

pars_res_word_t pars_to_char_token = {PARS_TO_CHAR_TOKEN};
pars_res_word_t pars_to_number_token = {PARS_TO_NUMBER_TOKEN};
pars_res_word_t pars_to_binary_token = {PARS_TO_BINARY_TOKEN};
pars_res_word_t pars_binary_to_number_token = {PARS_BINARY_TO_NUMBER_TOKEN};
pars_res_word_t pars_substr_token = {PARS_SUBSTR_TOKEN};
pars_res_word_t pars_replstr_token = {PARS_REPLSTR_TOKEN};
pars_res_word_t pars_concat_token = {PARS_CONCAT_TOKEN};
pars_res_word_t pars_instr_token = {PARS_INSTR_TOKEN};
pars_res_word_t pars_length_token = {PARS_LENGTH_TOKEN};
pars_res_word_t pars_sysdate_token = {PARS_SYSDATE_TOKEN};
pars_res_word_t pars_printf_token = {PARS_PRINTF_TOKEN};
pars_res_word_t pars_assert_token = {PARS_ASSERT_TOKEN};
pars_res_word_t pars_rnd_token = {PARS_RND_TOKEN};
pars_res_word_t pars_rnd_str_token = {PARS_RND_STR_TOKEN};
pars_res_word_t pars_count_token = {PARS_COUNT_TOKEN};
pars_res_word_t pars_sum_token = {PARS_SUM_TOKEN};
pars_res_word_t pars_distinct_token = {PARS_DISTINCT_TOKEN};
pars_res_word_t pars_binary_token = {PARS_BINARY_TOKEN};
pars_res_word_t pars_blob_token = {PARS_BLOB_TOKEN};
pars_res_word_t pars_int_token = {PARS_INT_TOKEN};
pars_res_word_t pars_char_token = {PARS_CHAR_TOKEN};
pars_res_word_t pars_float_token = {PARS_FLOAT_TOKEN};
pars_res_word_t pars_update_token = {PARS_UPDATE_TOKEN};
pars_res_word_t pars_asc_token = {PARS_ASC_TOKEN};
pars_res_word_t pars_desc_token = {PARS_DESC_TOKEN};
pars_res_word_t pars_open_token = {PARS_OPEN_TOKEN};
pars_res_word_t pars_close_token = {PARS_CLOSE_TOKEN};
pars_res_word_t pars_share_token = {PARS_SHARE_TOKEN};
pars_res_word_t pars_unique_token = {PARS_UNIQUE_TOKEN};
pars_res_word_t pars_clustered_token = {PARS_CLUSTERED_TOKEN};

/** Global variable used to denote the '*' in SELECT * FROM.. */
#define PARS_STAR_DENOTER 12345678
ulint pars_star_denoter = PARS_STAR_DENOTER;

/** Reset and check parser variables. */

void pars_var_init(void) {
#ifdef UNIV_SQL_DEBUG
  pars_print_lexed = false;
#endif /* UNIV_SQL_DEBUG */

  pars_lexer_var_init();

  pars_sym_tab_global = nullptr;

  /* These should really be const, so we simply check that they
  are set to the correct value. */
  ut_a(pars_to_char_token.code == PARS_TO_CHAR_TOKEN);
  ut_a(pars_to_number_token.code == PARS_TO_NUMBER_TOKEN);
  ut_a(pars_to_binary_token.code == PARS_TO_BINARY_TOKEN);
  ut_a(pars_binary_to_number_token.code == PARS_BINARY_TO_NUMBER_TOKEN);
  ut_a(pars_substr_token.code == PARS_SUBSTR_TOKEN);
  ut_a(pars_replstr_token.code == PARS_REPLSTR_TOKEN);
  ut_a(pars_concat_token.code == PARS_CONCAT_TOKEN);
  ut_a(pars_instr_token.code == PARS_INSTR_TOKEN);
  ut_a(pars_length_token.code == PARS_LENGTH_TOKEN);
  ut_a(pars_sysdate_token.code == PARS_SYSDATE_TOKEN);
  ut_a(pars_printf_token.code == PARS_PRINTF_TOKEN);
  ut_a(pars_assert_token.code == PARS_ASSERT_TOKEN);
  ut_a(pars_rnd_token.code == PARS_RND_TOKEN);
  ut_a(pars_rnd_str_token.code == PARS_RND_STR_TOKEN);
  ut_a(pars_count_token.code == PARS_COUNT_TOKEN);
  ut_a(pars_sum_token.code == PARS_SUM_TOKEN);
  ut_a(pars_distinct_token.code == PARS_DISTINCT_TOKEN);
  ut_a(pars_binary_token.code == PARS_BINARY_TOKEN);
  ut_a(pars_blob_token.code == PARS_BLOB_TOKEN);
  ut_a(pars_int_token.code == PARS_INT_TOKEN);
  ut_a(pars_char_token.code == PARS_CHAR_TOKEN);
  ut_a(pars_float_token.code == PARS_FLOAT_TOKEN);
  ut_a(pars_update_token.code == PARS_UPDATE_TOKEN);
  ut_a(pars_asc_token.code == PARS_ASC_TOKEN);
  ut_a(pars_desc_token.code == PARS_DESC_TOKEN);
  ut_a(pars_open_token.code == PARS_OPEN_TOKEN);
  ut_a(pars_close_token.code == PARS_CLOSE_TOKEN);
  ut_a(pars_share_token.code == PARS_SHARE_TOKEN);
  ut_a(pars_unique_token.code == PARS_UNIQUE_TOKEN);
  ut_a(pars_clustered_token.code == PARS_CLUSTERED_TOKEN);

  pars_star_denoter = PARS_STAR_DENOTER;
}

/**
 * @brief Determines the class of a function code.
 * 
 * @param[in] func Function code: '=', PARS_GE_TOKEN, etc.
 * 
 * @return Function class: PARS_FUNC_ARITH, PARS_FUNC_CMP, etc.
 */
static ulint pars_func_get_class(int func) noexcept {
  switch (func) {
  case '+':
  case '-':
  case '*':
  case '/':
    return (PARS_FUNC_ARITH);

  case '=':
  case '<':
  case '>':
  case PARS_GE_TOKEN:
  case PARS_LE_TOKEN:
  case PARS_NE_TOKEN:
    return (PARS_FUNC_CMP);

  case PARS_AND_TOKEN:
  case PARS_OR_TOKEN:
  case PARS_NOT_TOKEN:
    return (PARS_FUNC_LOGICAL);

  case PARS_COUNT_TOKEN:
  case PARS_SUM_TOKEN:
    return (PARS_FUNC_AGGREGATE);

  case PARS_TO_CHAR_TOKEN:
  case PARS_TO_NUMBER_TOKEN:
  case PARS_TO_BINARY_TOKEN:
  case PARS_BINARY_TO_NUMBER_TOKEN:
  case PARS_SUBSTR_TOKEN:
  case PARS_CONCAT_TOKEN:
  case PARS_LENGTH_TOKEN:
  case PARS_INSTR_TOKEN:
  case PARS_SYSDATE_TOKEN:
  case PARS_NOTFOUND_TOKEN:
  case PARS_PRINTF_TOKEN:
  case PARS_ASSERT_TOKEN:
  case PARS_RND_TOKEN:
  case PARS_RND_STR_TOKEN:
  case PARS_REPLSTR_TOKEN:
    return (PARS_FUNC_PREDEFINED);

  default:
    return (PARS_FUNC_OTHER);
  }
}

/**
 * @brief Parses an operator or predefined function expression.
 * 
 * @param[in] func Function token code.
 * @param[in] arg First argument in the argument list.
 * 
 * @return func_node_t* Function node in a query tree.
 */
static func_node_t * pars_func_low(int func, que_node_t *arg) {
  auto node = reinterpret_cast<func_node_t *>(mem_heap_alloc(pars_sym_tab_global->heap, sizeof(func_node_t)));

  node->common.type = QUE_NODE_FUNC;
  dfield_set_data(&(node->common.val), nullptr, 0);
  node->common.val_buf_size = 0;

  node->func = func;

  node->func_class = pars_func_get_class(func);

  node->args = arg;

  pars_sym_tab_global->func_node_list.push_back(node);

  return node;
}

func_node_t *pars_func(que_node_t *res_word, que_node_t *arg) {
  return pars_func_low((reinterpret_cast<pars_res_word_t *>(res_word))->code, arg);
}

func_node_t *pars_op(int func, que_node_t *arg1, que_node_t *arg2) {
  que_node_list_add_last(nullptr, arg1);

  if (arg2 != nullptr) {
    que_node_list_add_last(arg1, arg2);
  }

  return pars_func_low(func, arg1);
}

order_node_t *pars_order_by(sym_node_t *column, pars_res_word_t *asc) {
  auto node = reinterpret_cast<order_node_t *>(mem_heap_alloc(pars_sym_tab_global->heap, sizeof(order_node_t)));

  node->common.type = QUE_NODE_ORDER;

  node->column = column;

  if (asc == &pars_asc_token) {
    node->asc = true;
  } else {
    ut_a(asc == &pars_desc_token);
    node->asc = false;
  }

  return node;
}

/**
 * @brief Determine if a data type is a built-in string data type of the InnoDB SQL parser.
 * 
 * @param mtype The main data type.
 * @return true if the data type is a string data type, false otherwise.
 */
static bool pars_is_string_type(ulint mtype) {
  switch (mtype) {
  case DATA_VARCHAR:
  case DATA_CHAR:
  case DATA_FIXBINARY:
  case DATA_BINARY:
    return true;
  }

  return false;
}

/**
 * @brief Resolves the data type of a function in an expression.
 * 
 * The argument data types must already be resolved.
 * 
 * @param node The function node whose data type is to be resolved.
 */
static void pars_resolve_func_data_type(func_node_t *node) {
  ut_a(que_node_get_type(node) == QUE_NODE_FUNC);

  auto arg = node->args;

  switch (node->func) {
  case PARS_SUM_TOKEN:
  case '+':
  case '-':
  case '*':
  case '/':
    /* Inherit the data type from the first argument (which must
    not be the SQL null literal whose type is DATA_ERROR) */

    dtype_copy(que_node_get_data_type(node), que_node_get_data_type(arg));

    ut_a(dtype_get_mtype(que_node_get_data_type(node)) == DATA_INT);
    break;

  case PARS_COUNT_TOKEN:
    ut_a(arg);
    dtype_set(que_node_get_data_type(node), DATA_INT, 0, 4);
    break;

  case PARS_TO_CHAR_TOKEN:
  case PARS_RND_STR_TOKEN:
    ut_a(dtype_get_mtype(que_node_get_data_type(arg)) == DATA_INT);
    dtype_set(que_node_get_data_type(node), DATA_VARCHAR, DATA_ENGLISH, 0);
    break;

  case PARS_TO_BINARY_TOKEN:
    if (dtype_get_mtype(que_node_get_data_type(arg)) == DATA_INT) {
      dtype_set(que_node_get_data_type(node), DATA_VARCHAR, DATA_ENGLISH, 0);
    } else {
      dtype_set(que_node_get_data_type(node), DATA_BINARY, 0, 0);
    }
    break;

  case PARS_TO_NUMBER_TOKEN:
  case PARS_BINARY_TO_NUMBER_TOKEN:
  case PARS_LENGTH_TOKEN:
  case PARS_INSTR_TOKEN:
    ut_a(pars_is_string_type(que_node_get_data_type(arg)->mtype));
    dtype_set(que_node_get_data_type(node), DATA_INT, 0, 4);
    break;

  case PARS_SYSDATE_TOKEN:
    ut_a(arg == nullptr);
    dtype_set(que_node_get_data_type(node), DATA_INT, 0, 4);
    break;

  case PARS_SUBSTR_TOKEN:
  case PARS_CONCAT_TOKEN:
    ut_a(pars_is_string_type(que_node_get_data_type(arg)->mtype));
    dtype_set(que_node_get_data_type(node), DATA_VARCHAR, DATA_ENGLISH, 0);
    break;

  case '>':
  case '<':
  case '=':
  case PARS_GE_TOKEN:
  case PARS_LE_TOKEN:
  case PARS_NE_TOKEN:
  case PARS_AND_TOKEN:
  case PARS_OR_TOKEN:
  case PARS_NOT_TOKEN:
  case PARS_NOTFOUND_TOKEN:

    /* We currently have no boolean type: use integer type */
    dtype_set(que_node_get_data_type(node), DATA_INT, 0, 4);
    break;

  case PARS_RND_TOKEN:
    ut_a(dtype_get_mtype(que_node_get_data_type(arg)) == DATA_INT);
    dtype_set(que_node_get_data_type(node), DATA_INT, 0, 4);
    break;

  default:
    ut_error;
  }
}

/**
 * @brief Resolves the meaning of variables in an expression and the data types of functions.
 * 
 * It is an error if some identifier cannot be resolved here.
 * 
 * @param[in] sel_node Select node or nullptr; if this is not nullptr then the variable
 *                    sym nodes are added to the copy_variables list of sel_node.
 * @param[in] exp_node Expression node.
 */
static void pars_resolve_exp_variables_and_types(sel_node_t *sel_node, que_node_t *exp_node) {
  ut_a(exp_node != nullptr);

  if (que_node_get_type(exp_node) == QUE_NODE_FUNC) {
    auto func_node = reinterpret_cast<func_node_t *>(exp_node);
    auto arg = func_node->args;

    while (arg != nullptr) {
      pars_resolve_exp_variables_and_types(sel_node, arg);

      arg = que_node_get_next(arg);
    }

    pars_resolve_func_data_type(func_node);

    return;
  }

  ut_a(que_node_get_type(exp_node) == QUE_NODE_SYMBOL);

  auto sym_node = reinterpret_cast<sym_node_t *>(exp_node);

  if (sym_node->resolved) {

    return;
  }

  /* Not resolved yet: look in the symbol table for a variable or a cursor or a function with the same name */

  auto it = std::find_if(pars_sym_tab_global->sym_list.begin(), pars_sym_tab_global->sym_list.end(), [sym_node](const auto &node) {
    return node->resolved &&
           (node->token_type == SYM_VAR || node->token_type == SYM_CURSOR || node->token_type == SYM_FUNCTION) &&
           node->name != nullptr &&
           sym_node->name_len == node->name_len &&
           memcmp(sym_node->name, node->name, node->name_len) == 0;
  });

  if (it == pars_sym_tab_global->sym_list.end()) {
    log_err("PARSER ERROR: Unresolved identifier ", sym_node->name);
  }

  sym_node->alias = *it;
  sym_node->resolved = true;
  sym_node->indirection = *it;
  sym_node->token_type = SYM_IMPLICIT_VAR;

  if (sel_node != nullptr) {
    sel_node->m_copy_variables.push_back(sym_node);
  }

  dfield_set_type(que_node_get_val(sym_node), que_node_get_data_type(*it));
}

/**
 * @brief Resolves the meaning of variables in an expression list.
 * 
 * This function resolves the meaning of variables in an expression list. 
 * It is an error if some identifier cannot be resolved here. 
 * It also resolves the data types of functions.
 * 
 * @param[in] sel_node The select node or nullptr.
 * @param[in] exp_node The expression list first node, or nullptr.
 */
static void pars_resolve_exp_list_variables_and_types(sel_node_t *sel_node, que_node_t *exp_node) noexcept {
  while (exp_node != nullptr) {
    pars_resolve_exp_variables_and_types(sel_node, exp_node);

    exp_node = que_node_get_next(exp_node);
  }
}

/**
 * @brief Resolves the columns in an expression.
 * 
 * @param[in] table_node First node in a table list.
 * @param[in] exp_node Expression.
 */
static void pars_resolve_exp_columns(sym_node_t *table_node, que_node_t *exp_node) noexcept {
  ut_a(exp_node != nullptr);

  if (que_node_get_type(exp_node) == QUE_NODE_FUNC) {
    auto func_node = reinterpret_cast<func_node_t *>(exp_node);
    auto arg = func_node->args;

    while (arg != nullptr) {
      pars_resolve_exp_columns(table_node, arg);

      arg = que_node_get_next(arg);
    }

    return;
  }

  ut_a(que_node_get_type(exp_node) == QUE_NODE_SYMBOL);

  auto sym_node = reinterpret_cast<sym_node_t *>(exp_node);

  if (sym_node->resolved) {

    return;
  }

  /* Not resolved yet: look in the table list for a column with the same name */

  auto t_node = table_node;

  while (t_node != nullptr) {
    auto table = t_node->table;
    auto n_cols = table->get_n_cols();

    for (ulint i = 0; i < n_cols; ++i) {
      const auto col = table->get_nth_col(i);
      const auto col_name = table->get_col_name(i);

      if (sym_node->name_len == strlen(col_name) && memcmp(sym_node->name, col_name, sym_node->name_len) == 0) {
        /* Found */
        sym_node->resolved = true;
        sym_node->token_type = SYM_COLUMN;
        sym_node->table = table;
        sym_node->col_no = i;
        sym_node->prefetch_buf = nullptr;

        col->copy_type(dfield_get_type(&sym_node->common.val));

        return;
      }
    }

    t_node = reinterpret_cast<sym_node_t *>(que_node_get_next(t_node));
  }
}

/**
 * @brief Resolves the meaning of columns in an expression list.
 * 
 * @param[in] table_node First node in a table list.
 * @param[in] exp_node Expression list first node, or nullptr.
 */
static void pars_resolve_exp_list_columns(sym_node_t *table_node, que_node_t *exp_node) noexcept {

  while (exp_node != nullptr) {
    pars_resolve_exp_columns(table_node, exp_node);

    exp_node = que_node_get_next(exp_node);
  }
}

/**
 * @brief Retrieves the table definition for a table name id.
 * 
 * @param[in] sym_node Table node.
 */
static void pars_retrieve_table_def(sym_node_t *sym_node) noexcept {
  ut_a(sym_node != nullptr);
  ut_a(que_node_get_type(sym_node) == QUE_NODE_SYMBOL);

  sym_node->resolved = true;
  sym_node->token_type = SYM_TABLE;

  auto table_name = const_cast<char *>(sym_node->name);

  sym_node->table = srv_dict_sys->table_get(table_name);

  ut_a(sym_node->table != nullptr);
}

/**
 * @brief Retrieves the table definitions for a list of table name ids.
 * 
 * @param[in] sym_node First table node in the list.
 * 
 * @return Number of tables.
 */
static ulint pars_retrieve_table_list_defs(sym_node_t *sym_node) noexcept {
  if (sym_node == nullptr) {
    return 0;
  }

  ulint count{};

  while (sym_node != nullptr) {
    pars_retrieve_table_def(sym_node);

    ++count;

    sym_node = reinterpret_cast<sym_node_t *>(que_node_get_next(sym_node));
  }

  return count;
}

/**
 * @brief Adds all columns to the select list if the query is SELECT * FROM ...
 * 
 * @param[in] sel_node The select node already containing the table list.
 */
static void pars_select_all_columns(sel_node_t *sel_node) noexcept {
  sel_node->m_select_list = nullptr;

  auto table_node = sel_node->m_table_list;

  while (table_node != nullptr) {
    auto table = table_node->table;

    for (ulint i{}; i < table->get_n_user_cols(); ++i) {
      const auto col_name = table->get_col_name(i);

      auto col_node = sym_tab_add_id(pars_sym_tab_global, (byte *)col_name, strlen(col_name));

      sel_node->m_select_list = que_node_list_add_last(sel_node->m_select_list, col_node);
    }

    table_node = reinterpret_cast<sym_node_t *>(que_node_get_next(table_node));
  }
}

sel_node_t *pars_select_list(que_node_t *select_list, sym_node_t *into_list) {
  auto sel_node = sel_node_t::create(pars_sym_tab_global->heap);

  sel_node->m_select_list = select_list;
  sel_node->m_into_list = into_list;

  pars_resolve_exp_list_variables_and_types(nullptr, into_list);

  return sel_node;
}

/**
 * @brief Checks if the query is an aggregate query.
 * 
 * This function determines if the select list contains only aggregate function items.
 * 
 * @param[in] sel_node The select node already containing the select list.
 */
static void pars_check_aggregate(sel_node_t *sel_node) noexcept {
  ulint n_nodes{};
  ulint n_aggregate_nodes{};

  auto exp_node = sel_node->m_select_list;

  while (exp_node != nullptr) {

    ++n_nodes;

    if (que_node_get_type(exp_node) == QUE_NODE_FUNC) {

      auto func_node = reinterpret_cast<func_node_t *>(exp_node);

      if (func_node->func_class == PARS_FUNC_AGGREGATE) {

        ++n_aggregate_nodes;
      }
    }

    exp_node = que_node_get_next(exp_node);
  }

  if (n_aggregate_nodes > 0) {
    ut_a(n_nodes == n_aggregate_nodes);

    sel_node->m_is_aggregate = true;
  } else {
    sel_node->m_is_aggregate = false;
  }
}

sel_node_t *pars_select_statement(sel_node_t *sel_node, sym_node_t *table_list, que_node_t *search_cond, pars_res_word_t *for_update, pars_res_word_t *lock_shared, order_node_t *order_by) {
  sel_node->m_state = SEL_NODE_OPEN;

  sel_node->m_table_list = table_list;
  sel_node->m_n_tables = pars_retrieve_table_list_defs(table_list);

  if (sel_node->m_select_list == &pars_star_denoter) {

    /* SELECT * FROM ... */
    pars_select_all_columns(sel_node);
  }

  if (sel_node->m_into_list) {
    ut_a(que_node_list_get_len(sel_node->m_into_list) ==
         que_node_list_get_len(sel_node->m_select_list));
  }

  UT_LIST_INIT(sel_node->m_copy_variables);

  pars_resolve_exp_list_columns(table_list, sel_node->m_select_list);
  pars_resolve_exp_list_variables_and_types(sel_node, sel_node->m_select_list);
  pars_check_aggregate(sel_node);

  sel_node->m_search_cond = search_cond;

  if (search_cond != nullptr) {
    pars_resolve_exp_columns(table_list, search_cond);
    pars_resolve_exp_variables_and_types(sel_node, search_cond);
  }

  if (for_update != nullptr) {
    ut_a(!lock_shared);

    sel_node->m_set_x_locks = true;
    sel_node->m_row_lock_mode = LOCK_X;

    sel_node->m_consistent_read = false;
    sel_node->m_read_view = nullptr;
  } else if (lock_shared) {
    sel_node->m_set_x_locks = false;
    sel_node->m_row_lock_mode = LOCK_S;

    sel_node->m_consistent_read = false;
    sel_node->m_read_view = nullptr;
  } else {
    sel_node->m_set_x_locks = false;
    sel_node->m_row_lock_mode = LOCK_S;

    sel_node->m_consistent_read = true;
  }

  sel_node->m_order_by = order_by;

  if (order_by != nullptr) {
    pars_resolve_exp_columns(table_list, order_by->column);
  }

  /* The final value of the following fields depend on the environment
  where the select statement appears: */

  sel_node->m_can_get_updated = false;
  sel_node->m_explicit_cursor = nullptr;

  opt_search_plan(sel_node);

  return sel_node;
}

que_node_t *pars_cursor_declaration(sym_node_t *sym_node, sel_node_t *sel_node) {
  sym_node->resolved = true;
  sym_node->token_type = SYM_CURSOR;
  sym_node->cursor_def = sel_node;

  sel_node->m_state = SEL_NODE_CLOSED;
  sel_node->m_explicit_cursor = sym_node;

  return sym_node;
}

que_node_t * pars_function_declaration(sym_node_t *sym_node) {
  sym_node->resolved = true;
  sym_node->token_type = SYM_FUNCTION;

  /* Check that the function exists. */
  ut_a(pars_info_get_user_func(pars_sym_tab_global->info, sym_node->name));

  return sym_node;
}

upd_node_t *pars_update_statement_start(bool is_delete, sym_node_t *table_sym, col_assign_node_t *col_assign_list) {
  auto node = srv_row_upd->node_create(pars_sym_tab_global->heap);

  node->m_is_delete = is_delete;
  node->m_table_sym = table_sym;
  node->m_col_assign_list = col_assign_list;

  return node;
}

col_assign_node_t *pars_column_assignment(sym_node_t *column, que_node_t *exp) {
  auto node = reinterpret_cast<col_assign_node_t *>(mem_heap_alloc(pars_sym_tab_global->heap, sizeof(col_assign_node_t)));

  node->common.type = QUE_NODE_COL_ASSIGNMENT;

  node->col = column;
  node->val = exp;

  return node;
}

/**
 * @brief Processes an update node assignment list.
 * 
 * This function resolves the columns and values in the assignment list,
 * ensures the data types match, and adds the columns found in assignment
 * values to the update node.
 * 
 * @param[in] node The update node containing the assignment list.
 */
static void pars_process_assign_list(upd_node_t *node) noexcept {
  auto table_sym = node->m_table_sym;
  auto col_assign_list = reinterpret_cast<col_assign_node_t *>(node->m_col_assign_list);
  auto clust_index = node->m_table->get_first_index();

  ulint n_assigns{};
  auto assign_node = col_assign_list;

  while (assign_node != nullptr) {
    pars_resolve_exp_columns(table_sym, assign_node->col);
    pars_resolve_exp_columns(table_sym, assign_node->val);
    pars_resolve_exp_variables_and_types(nullptr, assign_node->val);

#if 0
		ut_a(dtype_get_mtype(dfield_get_type(que_node_get_val(assign_node->col))) == dtype_get_mtype( dfield_get_type(que_node_get_val(assign_node->val))));
#endif

    /* Add to the update node all the columns found in assignment
    values as columns to copy: therefore, true */

    opt_find_all_cols(true, clust_index, &node->m_columns, nullptr, assign_node->val);
    ++n_assigns;

    assign_node = reinterpret_cast<col_assign_node_t *>(que_node_get_next(assign_node));
  }

  node->m_update = Row_update::upd_create(n_assigns, pars_sym_tab_global->heap);

  assign_node = col_assign_list;

  auto changes_field_size = UPD_NODE_NO_SIZE_CHANGE;

  for (ulint i{}; i < n_assigns; ++i) {
    auto upd_field = node->m_update->get_nth_field(i);
    auto col_sym = assign_node->col;

    upd_field->set_field_no(clust_index->get_nth_field_pos(col_sym->col_no), clust_index, nullptr);
    upd_field->m_exp = assign_node->val;

    if (clust_index->get_nth_col(upd_field->m_field_no)->get_fixed_size() == 0) {
      changes_field_size = 0;
    }

    assign_node = reinterpret_cast<col_assign_node_t *>(que_node_get_next(assign_node));
  }

  /* Find out if the update can modify an ordering field in any index */

  auto changes_ord_field = UPD_NODE_NO_ORD_CHANGE;

  if (srv_row_upd->changes_some_index_ord_field_binary(node->m_table, node->m_update)) {
    changes_ord_field = 0;
  }

  node->m_cmpl_info = changes_ord_field | changes_field_size;
}

upd_node_t *pars_update_statement(upd_node_t *node, sym_node_t *cursor_sym, que_node_t *search_cond) {
  auto table_sym = node->m_table_sym;

  pars_retrieve_table_def(table_sym);
  node->m_table = table_sym->table;

  UT_LIST_INIT(node->m_columns);

  /* Make the single table node into a list of table nodes of length 1 */

  que_node_list_add_last(nullptr, table_sym);

  sel_node_t *sel_node;

  if (cursor_sym != nullptr) {
    pars_resolve_exp_variables_and_types(nullptr, cursor_sym);

    sel_node = cursor_sym->alias->cursor_def;

    node->m_searched_update = false;
  } else {
    sel_node = pars_select_list(nullptr, nullptr);

    pars_select_statement(sel_node, table_sym, search_cond, nullptr,
                          &pars_share_token, nullptr);
    node->m_searched_update = true;
    sel_node->m_common.parent = node;
  }

  node->m_select = sel_node;

  ut_a(!node->m_is_delete || (node->m_col_assign_list == nullptr));
  ut_a(node->m_is_delete || (node->m_col_assign_list != nullptr));

  if (node->m_is_delete) {
    node->m_cmpl_info = 0;
  } else {
    pars_process_assign_list(node);
  }

  if (node->m_searched_update) {
    node->m_has_clust_rec_x_lock = true;
    sel_node->m_set_x_locks = true;
    sel_node->m_row_lock_mode = LOCK_X;
  } else {
    node->m_has_clust_rec_x_lock = sel_node->m_set_x_locks;
  }

  ut_a(sel_node->m_n_tables == 1);
  ut_a(sel_node->m_consistent_read == false);
  ut_a(sel_node->m_order_by == nullptr);
  ut_a(sel_node->m_is_aggregate == false);

  sel_node->m_can_get_updated = true;

  node->m_state = UPD_NODE_UPDATE_CLUSTERED;

  auto plan = sel_node->get_nth_plan(0);

  plan->m_no_prefetch = true;

  if (!plan->m_index->is_clustered()) {
    plan->m_must_get_clust = true;
    node->m_pcur = &plan->m_clust_pcur;
  } else {
    node->m_pcur = &plan->m_pcur;
  }

  return node;
}

ins_node_t *pars_insert_statement(sym_node_t *table_sym, que_node_t *values_list, sel_node_t *sel_node) {
  ut_a(values_list != nullptr || sel_node != nullptr);
  ut_a(values_list == nullptr || sel_node == nullptr);

  auto ins_type = values_list != nullptr ? INS_VALUES : INS_SEARCHED;

  pars_retrieve_table_def(table_sym);

  auto ins_node = reinterpret_cast<ins_node_t *>(srv_row_ins->node_create(ins_type, table_sym->table, pars_sym_tab_global->heap));
  auto row = dtuple_create(pars_sym_tab_global->heap, ins_node->m_table->get_n_cols());

  table_sym->table->copy_types(row);

  srv_row_ins->set_new_row(ins_node, row);

  ins_node->m_select = sel_node;

  if (sel_node != nullptr) {
    sel_node->m_common.parent = ins_node;

    ut_a(que_node_list_get_len(sel_node->m_select_list) == table_sym->table->get_n_user_cols());
  }

  ins_node->m_values_list = values_list;

  if (values_list != nullptr) {
    pars_resolve_exp_list_variables_and_types(nullptr, values_list);

    ut_a(que_node_list_get_len(values_list) == table_sym->table->get_n_user_cols());
  }

  return ins_node;
}

/**
 * @brief Set the type of a dfield.
 * 
 * @param[in] dfield dfield
 * @param[in] type pointer to a type token
 * @param[in] len length, or 0
 * @param[in] is_unsigned if true, column is UNSIGNED
 * @param[in] is_not_null if true, column is NOT nullptr
 */
static void pars_set_dfield_type(dfield_t *dfield, pars_res_word_t *type, ulint len, bool is_unsigned, bool is_not_null) noexcept {
  ulint flags{};

  if (is_not_null) {
    flags |= DATA_NOT_NULL;
  }

  if (is_unsigned) {
    flags |= DATA_UNSIGNED;
  }

  if (type == &pars_int_token) {
    ut_a(len == 0);

    dtype_set(dfield_get_type(dfield), DATA_INT, flags, 4);

  } else if (type == &pars_char_token) {
    ut_a(len == 0);

    dtype_set(dfield_get_type(dfield), DATA_VARCHAR, DATA_ENGLISH | flags, 0);
  } else if (type == &pars_binary_token) {
    ut_a(len != 0);

    dtype_set(dfield_get_type(dfield), DATA_FIXBINARY, DATA_BINARY_TYPE | flags,
              len);
  } else if (type == &pars_blob_token) {
    ut_a(len == 0);

    dtype_set(dfield_get_type(dfield), DATA_BLOB, DATA_BINARY_TYPE | flags, 0);
  } else {
    ut_error;
  }
}

sym_node_t *pars_variable_declaration(sym_node_t *node, pars_res_word_t *type) {
  node->resolved = true;
  node->token_type = SYM_VAR;

  node->param_type = PARS_NOT_PARAM;

  pars_set_dfield_type(que_node_get_val(node), type, 0, false, false);

  return node;
}

sym_node_t *pars_parameter_declaration(sym_node_t *node, ulint param_type, pars_res_word_t *type) {
  ut_a((param_type == PARS_INPUT) || (param_type == PARS_OUTPUT));

  pars_variable_declaration(node, type);

  node->param_type = param_type;

  return node;
}

/**
 * @brief Sets the parent field in a query node list.
 * 
 * @param[in] node_list First node in a list.
 * @param[in] parent Parent value to set in all nodes of the list.
 */
static void pars_set_parent_in_list(que_node_t *node_list, que_node_t *parent) noexcept {
  auto common = reinterpret_cast<que_common_t *>(node_list);

  while (common != nullptr) {
    common->parent = parent;

    common = reinterpret_cast<que_common_t *>(que_node_get_next(common));
  }
}

elsif_node_t *pars_elsif_element(que_node_t *cond, que_node_t *stat_list) {
  auto node = reinterpret_cast<elsif_node_t *>(mem_heap_alloc(pars_sym_tab_global->heap, sizeof(elsif_node_t)));

  node->common.type = QUE_NODE_ELSIF;

  node->cond = cond;

  pars_resolve_exp_variables_and_types(nullptr, cond);

  node->stat_list = stat_list;

  return node;
}

if_node_t *pars_if_statement(que_node_t *cond, que_node_t *stat_list, que_node_t *else_part) {
  elsif_node_t *elsif_node;

  auto node = reinterpret_cast<if_node_t *>(mem_heap_alloc(pars_sym_tab_global->heap, sizeof(if_node_t)));

  node->common.type = QUE_NODE_IF;

  node->cond = cond;

  pars_resolve_exp_variables_and_types(nullptr, cond);

  node->stat_list = stat_list;

  if (else_part && que_node_get_type(else_part) == QUE_NODE_ELSIF) {

    /* There is a list of elsif conditions */

    node->else_part = nullptr;
    node->elsif_list = static_cast<elsif_node_t *>(else_part);

    elsif_node = static_cast<elsif_node_t *>(else_part);

    while (elsif_node != nullptr) {
      pars_set_parent_in_list(elsif_node->stat_list, node);

      elsif_node = static_cast<elsif_node_t *>(que_node_get_next(elsif_node));
    }
  } else {
    node->else_part = else_part;
    node->elsif_list = nullptr;

    pars_set_parent_in_list(else_part, node);
  }

  pars_set_parent_in_list(stat_list, node);

  return node;
}

while_node_t *pars_while_statement(que_node_t *cond, que_node_t *stat_list) {
  auto node = reinterpret_cast<while_node_t *>(mem_heap_alloc(pars_sym_tab_global->heap, sizeof(while_node_t)));

  node->common.type = QUE_NODE_WHILE;

  node->cond = cond;

  pars_resolve_exp_variables_and_types(nullptr, cond);

  node->stat_list = stat_list;

  pars_set_parent_in_list(stat_list, node);

  return (node);
}

for_node_t *pars_for_statement(sym_node_t *loop_var, que_node_t *loop_start_limit, que_node_t *loop_end_limit, que_node_t *stat_list) {
  auto node = reinterpret_cast<for_node_t *>(mem_heap_alloc(pars_sym_tab_global->heap, sizeof(for_node_t)));

  node->common.type = QUE_NODE_FOR;

  pars_resolve_exp_variables_and_types(nullptr, loop_var);
  pars_resolve_exp_variables_and_types(nullptr, loop_start_limit);
  pars_resolve_exp_variables_and_types(nullptr, loop_end_limit);

  node->loop_var = loop_var->indirection;

  ut_a(loop_var->indirection);

  node->loop_start_limit = loop_start_limit;
  node->loop_end_limit = loop_end_limit;

  node->stat_list = stat_list;

  pars_set_parent_in_list(stat_list, node);

  return node;
}

exit_node_t *pars_exit_statement() {
  auto node = reinterpret_cast<exit_node_t *>(mem_heap_alloc(pars_sym_tab_global->heap, sizeof(exit_node_t)));

  node->common.type = QUE_NODE_EXIT;

  return node;
}

return_node_t *pars_return_statement() {
  auto node = reinterpret_cast<return_node_t *>(mem_heap_alloc(pars_sym_tab_global->heap, sizeof(return_node_t)));

  node->common.type = QUE_NODE_RETURN;

  return node;
}

assign_node_t *pars_assignment_statement(sym_node_t *var, que_node_t *val) {
  auto node = reinterpret_cast<assign_node_t *>(mem_heap_alloc(pars_sym_tab_global->heap, sizeof(assign_node_t)));

  node->common.type = QUE_NODE_ASSIGNMENT;

  node->var = var;
  node->val = val;

  pars_resolve_exp_variables_and_types(nullptr, var);
  pars_resolve_exp_variables_and_types(nullptr, val);

  ut_a(dtype_get_mtype(dfield_get_type(que_node_get_val(var))) ==
       dtype_get_mtype(dfield_get_type(que_node_get_val(val))));

  return node;
}

func_node_t *pars_procedure_call(que_node_t *res_word, que_node_t *args) {
  auto node = pars_func(res_word, args);

  pars_resolve_exp_list_variables_and_types(nullptr, args);

  return node;
}

fetch_node_t *pars_fetch_statement(sym_node_t *cursor, sym_node_t *into_list, sym_node_t *user_func) {
  /* Logical XOR. */
  ut_a(!into_list != !user_func);

  auto ptr = mem_heap_alloc(pars_sym_tab_global->heap, sizeof(fetch_node_t));
  auto fetch_node = new (ptr) fetch_node_t();

  fetch_node->m_common.type = QUE_NODE_FETCH;

  pars_resolve_exp_variables_and_types(nullptr, cursor);

  if (into_list != nullptr) {
    pars_resolve_exp_list_variables_and_types(nullptr, into_list);
    fetch_node->m_into_list = into_list;
    fetch_node->m_func = nullptr;
  } else {
    pars_resolve_exp_variables_and_types(nullptr, user_func);

    fetch_node->m_func = pars_info_get_user_func(pars_sym_tab_global->info, user_func->name);
    ut_a(fetch_node->m_func);

    fetch_node->m_into_list = nullptr;
  }

  auto cursor_decl = cursor->alias;

  ut_a(cursor_decl->token_type == SYM_CURSOR);

  fetch_node->m_cursor_def = cursor_decl->cursor_def;

  if (into_list) {
    ut_a(que_node_list_get_len(into_list) == que_node_list_get_len(fetch_node->m_cursor_def->m_select_list));
  }

  return fetch_node;
}

open_node_t *pars_open_statement(ulint type, sym_node_t *cursor) {
  auto open_node = reinterpret_cast<open_node_t *>(mem_heap_alloc(pars_sym_tab_global->heap, sizeof(open_node_t)));

  open_node->m_common.type = QUE_NODE_OPEN;

  pars_resolve_exp_variables_and_types(nullptr, cursor);

  auto cursor_decl = cursor->alias;

  ut_a(cursor_decl->token_type == SYM_CURSOR);

  open_node->m_op_type = static_cast<open_node_op>(type);
  open_node->m_cursor_def = cursor_decl->cursor_def;

  return open_node;
}

row_printf_node_t *pars_row_printf_statement(sel_node_t *sel_node) {
  auto row_printf_node = reinterpret_cast<row_printf_node_t *>(mem_heap_alloc(pars_sym_tab_global->heap, sizeof(row_printf_node_t)));

  row_printf_node->m_common.type = QUE_NODE_ROW_PRINTF;

  row_printf_node->m_sel_node = sel_node;

  sel_node->m_common.parent = row_printf_node;

  return row_printf_node;
}

Commit_node *pars_commit_statement() {
  return Trx::commit_node_create(pars_sym_tab_global->heap);
}

roll_node_t *pars_rollback_statement() {
  return roll_node_create(pars_sym_tab_global->heap);
}

sym_node_t *pars_column_def(sym_node_t *sym_node, pars_res_word_t *type, sym_node_t *len, void *is_unsigned, void *is_not_null) {
  auto len2 = len != nullptr ? eval_node_get_int_val(len) : 0;

  pars_set_dfield_type(que_node_get_val(sym_node), type, len2, is_unsigned != nullptr, is_not_null != nullptr);

  return sym_node;
}

Table_node *pars_create_table(sym_node_t *table_sym, sym_node_t *column_defs, void *not_fit_in_memory __attribute__((unused))) {
  const auto n_cols = que_node_list_get_len(column_defs);
  auto table = Table::create(table_sym->name, DICT_HDR_SPACE, n_cols, 0, false, Current_location());

#ifdef UNIV_DEBUG
  if (not_fit_in_memory != nullptr) {
    table->m_does_not_fit_in_memory = true;
  }
#endif /* UNIV_DEBUG */

  auto column = column_defs;

  while (column != nullptr) {
    auto dtype = dfield_get_type(que_node_get_val(column));

    table->add_col(column->name, dtype->mtype, dtype->prtype, dtype->len);

    column->resolved = true;
    column->token_type = SYM_COLUMN;

    column = static_cast<sym_node_t *>(que_node_get_next(column));
  }

  auto node = Table_node::create(srv_dict_sys, table, pars_sym_tab_global->heap, true);

  table_sym->table = table;
  table_sym->resolved = true;
  table_sym->token_type = SYM_TABLE;

  return node;
}

Index_node *pars_create_index(pars_res_word_t *unique_def, pars_res_word_t *clustered_def, sym_node_t *index_sym, sym_node_t *table_sym, sym_node_t *column_list) {
  ulint ind_type{};
  const auto n_fields = que_node_list_get_len(column_list);

  if (unique_def != nullptr) {
    ind_type = ind_type | DICT_UNIQUE;
  }

  if (clustered_def) {
    ind_type = ind_type | DICT_CLUSTERED;
  }

  auto index = Index::create(table_sym->name, index_sym->name, 0, ind_type, n_fields);
  auto column = column_list;

  while (column != nullptr) {
    (void) index->add_field(column->name, 0);

    column->resolved = true;
    column->token_type = SYM_COLUMN;

    column = static_cast<sym_node_t *>(que_node_get_next(column));
  }

  auto node = Index_node::create(srv_dict_sys, index, pars_sym_tab_global->heap, true);

  table_sym->resolved = true;
  table_sym->token_type = SYM_TABLE;

  index_sym->resolved = true;
  index_sym->token_type = SYM_TABLE;

  return node;
}

que_fork_t *pars_procedure_definition(sym_node_t *sym_node, sym_node_t *param_list, que_node_t *stat_list) {
  auto heap = pars_sym_tab_global->heap;
  auto fork = que_fork_create(nullptr, nullptr, QUE_FORK_PROCEDURE, heap);

  fork->trx = nullptr;

  auto thr = que_thr_create(fork, heap);

  auto node = reinterpret_cast<proc_node_t *>( mem_heap_alloc(heap, sizeof(proc_node_t)));

  node->common.type = QUE_NODE_PROC;
  node->common.parent = thr;

  sym_node->token_type = SYM_PROCEDURE_NAME;
  sym_node->resolved = true;

  node->proc_id = sym_node;
  node->param_list = param_list;
  node->stat_list = stat_list;

  pars_set_parent_in_list(stat_list, node);

  node->sym_tab = pars_sym_tab_global;

  thr->child = node;

  pars_sym_tab_global->query_graph = fork;

  return fork;
}

que_fork_t *pars_stored_procedure_call(sym_node_t *) {
  ut_error;
  return nullptr;
}

int pars_get_lex_chars(char *buf, int max_size) {
  auto len = pars_sym_tab_global->string_len - pars_sym_tab_global->next_char_pos;

  if (len == 0) {
    return 0;
  }

  if (len > static_cast<ulint>(max_size)) {
    len = max_size;
  }

  memcpy(buf, pars_sym_tab_global->sql_string + pars_sym_tab_global->next_char_pos, len);

  pars_sym_tab_global->next_char_pos += len;

  return static_cast<int>(len);
}

void yyerror(const char* p) {
  log_fatal("PARSER ERROR: Syntax error in SQL string: ", p);
}

que_t *pars_sql(pars_info_t *info, const char *str) {
  auto heap = mem_heap_create(256);

  /* Currently, the parser is not reentrant: */
  ut_ad(mutex_own(&srv_dict_sys->m_mutex));

  pars_sym_tab_global = sym_tab_create(heap);

  pars_sym_tab_global->string_len = strlen(str);

  pars_sym_tab_global->sql_string = static_cast<char *>(mem_heap_dup(heap, str, pars_sym_tab_global->string_len + 1));

  pars_sym_tab_global->next_char_pos = 0;
  pars_sym_tab_global->info = info;

  yyparse();

  for (auto sym_node : pars_sym_tab_global->sym_list) {
    ut_a(sym_node->resolved);
  }

  auto graph = pars_sym_tab_global->query_graph;

  graph->info = info;
  graph->sym_tab = pars_sym_tab_global;

  return graph;
}

que_thr_t *pars_complete_graph_for_exec(que_node_t *node, Trx *trx, mem_heap_t *heap) {
  auto fork = que_fork_create(nullptr, nullptr, QUE_FORK_USER_INTERFACE, heap);

  fork->trx = trx;

  auto thr = que_thr_create(fork, heap);

  thr->child = node;

  que_node_set_parent(node, thr);

  trx->m_graph = nullptr;

  return thr;
}

pars_info_t *pars_info_create() {
  auto heap = mem_heap_create(512);
  auto ptr = mem_heap_alloc(heap, sizeof(pars_info_t));
  auto info = new (ptr) pars_info_t;

  info->m_heap = heap;
  info->m_graph_owns_us = true;

  return info;
}

void pars_info_free(pars_info_t *info) {
  auto heap = info->m_heap;
  call_destructor(info);
  mem_heap_free(heap);
}

void pars_info_add_literal(pars_info_t *info, const char *name, const void *address, ulint length, ulint type, ulint prtype) {
  ut_ad(!pars_info_get_bound_lit(info, name));

  auto pbl = reinterpret_cast<pars_bound_lit_t *>( mem_heap_alloc(info->m_heap, sizeof(pars_bound_lit_t)));

  pbl->name = name;
  pbl->address = address;
  pbl->length = length;
  pbl->type = type;
  pbl->prtype = prtype;

  info->m_bound_lits.emplace_back(pbl);
}

void pars_info_add_str_literal(pars_info_t *info, const char *name, const char *str) {
  pars_info_add_literal(info, name, str, strlen(str), DATA_VARCHAR, DATA_ENGLISH);
}

void pars_info_add_int4_literal(pars_info_t *info, const char *name, lint val) {
  auto buf = mem_heap_alloc(info->m_heap, 4);

  mach_write_to_4(buf, val);
  pars_info_add_literal(info, name, buf, 4, DATA_INT, 0);
}

void pars_info_add_int8_literal(pars_info_t *info, const char *name, uint64_t val) {
  auto buf = mem_heap_alloc(info->m_heap, sizeof(val));

  mach_write_to_8(buf, val);
  pars_info_add_literal(info, name, buf, sizeof(val), DATA_INT, 0);
}

void pars_info_add_uint64_literal(pars_info_t *info, const char *name, uint64_t val) {
  auto buf = mem_heap_alloc(info->m_heap, 8);

  mach_write_to_8(buf, val);

  pars_info_add_literal(info, name, buf, 8, DATA_FIXBINARY, 0);
}

void pars_info_add_function(pars_info_t *info, const char *name, pars_user_func_cb_t func, void *arg) {
  ut_ad(!pars_info_get_user_func(info, name));

  auto puf = reinterpret_cast<pars_user_func_t *>(mem_heap_alloc(info->m_heap, sizeof(pars_user_func_t)));

  puf->name = name;
  puf->func = func;
  puf->arg = arg;

  info->m_funcs.emplace_back(puf);
}

void pars_info_add_id(pars_info_t *info, const char *name, const char *id) {
  ut_ad(!pars_info_get_bound_id(info, name));

  auto bid = reinterpret_cast<pars_bound_id_t *>(mem_heap_alloc(info->m_heap, sizeof(pars_bound_id_t)));

  bid->id = id;
  bid->name = name;

  info->m_bound_ids.emplace_back(bid);
}

pars_user_func_t *pars_info_get_user_func(pars_info_t *info, const char *name) {
  if (info == nullptr || info->m_funcs.empty()) {
    return nullptr;
  }

  for (const auto puf : info->m_funcs) {

    if (strcmp(puf->name, name) == 0) {
      return puf;
    }
  }

  return nullptr;
}

pars_bound_lit_t *pars_info_get_bound_lit(pars_info_t *info, const char *name) {
  if (info == nullptr || info->m_bound_lits.empty()) {
    return nullptr;
  }

  for (const auto pbl : info->m_bound_lits) {

    if (strcmp(pbl->name, name) == 0) {
      return pbl;
    }
  }

  return nullptr;
}

pars_bound_id_t *pars_info_get_bound_id(pars_info_t *info, const char *name) {
  if (info == nullptr || info->m_bound_ids.empty()) {
    return nullptr;
  }

  for (const auto bid : info->m_bound_ids) {

    if (strcmp(bid->name, name) == 0) {
      return bid;
    }
  }

  return nullptr;
}
