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

/* Historical note: Innobase executed its first SQL string (CREATE TABLE) on
 * 1/27/1998 */

#include "data0data.h"
#include "data0type.h"
#include "dict0dict.h"
#include "eval0eval.h"
#include "lock0lock.h"
#include "pars0grm.h"
#include "pars0opt.h"
#include "pars0pars.h"
#include "pars0lex.h"
#include "que0que.h"
#include "row0sel.h"
#include "row0ins.h"
#include "row0upd.h"
#include "trx0trx.h"
#include "trx0roll.h"

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
 * @return FuncNode* Function node in a query tree.
 */
static FuncNode *pars_func_low(System &sys, int func, que_node_t *arg) {
  auto node = reinterpret_cast<FuncNode *>(
      mem_heap_alloc(sys.m_parser->m_sym_tab->heap, sizeof(FuncNode)));

  node->common.type = QUE_NODE_FUNC;
  dfield_set_data(&(node->common.val), nullptr, 0);
  node->common.val_buf_size = 0;

  node->func = func;

  node->func_class = pars_func_get_class(func);

  node->args = arg;

  sys.m_parser->m_sym_tab->func_node_list.push_back(node);

  return node;
}

FuncNode *pars_func(System &sys, que_node_t *res_word, que_node_t *arg) {
  return pars_func_low(sys, (reinterpret_cast<const ParsResWord *>(res_word))->code,
                       arg);
}

FuncNode *pars_op(System &sys, int func, que_node_t *arg1, que_node_t *arg2) {
  que_node_list_add_last(nullptr, arg1);

  if (arg2 != nullptr) {
    que_node_list_add_last(arg1, arg2);
  }

  return pars_func_low(sys, func, arg1);
}

OrderNode *pars_order_by(System &sys, SymNode *column, const ParsResWord *asc) {
  auto node = reinterpret_cast<OrderNode *>(
      mem_heap_alloc(sys.m_parser->m_sym_tab->heap, sizeof(OrderNode)));

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
 * @brief Determine if a data type is a built-in string data type of the InnoDB
 * SQL parser.
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
static void pars_resolve_func_data_type(FuncNode *node) {
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
 * @brief Resolves the meaning of variables in an expression and the data types
 * of functions.
 *
 * It is an error if some identifier cannot be resolved here.
 *
 * @param[in] sel_node Select node or nullptr; if this is not nullptr then the
 * variable sym nodes are added to the copy_variables list of sel_node.
 * @param[in] exp_node Expression node.
 */
static void pars_resolve_exp_variables_and_types(System &sys, SelNode *sel_node,
                                                 que_node_t *exp_node) {
  ut_a(exp_node != nullptr);

  if (que_node_get_type(exp_node) == QUE_NODE_FUNC) {
    auto func_node = reinterpret_cast<FuncNode *>(exp_node);
    auto arg = func_node->args;

    while (arg != nullptr) {
      pars_resolve_exp_variables_and_types(sys, sel_node, arg);

      arg = que_node_get_next(arg);
    }

    pars_resolve_func_data_type(func_node);

    return;
  }

  ut_a(que_node_get_type(exp_node) == QUE_NODE_SYMBOL);

  auto sym_node = reinterpret_cast<SymNode *>(exp_node);

  if (sym_node->resolved) {

    return;
  }

  /* Not resolved yet: look in the symbol table for a variable or a cursor or a
   * function with the same name */

  auto it = std::find_if(
      sys.m_parser->m_sym_tab->sym_list.begin(),
      sys.m_parser->m_sym_tab->sym_list.end(), [sym_node](const auto &node) {
        return node->resolved &&
               (node->token_type == SYM_VAR || node->token_type == SYM_CURSOR ||
                node->token_type == SYM_FUNCTION) &&
               node->name != nullptr && sym_node->name_len == node->name_len &&
               memcmp(sym_node->name, node->name, node->name_len) == 0;
      });

  if (it == sys.m_parser->m_sym_tab->sym_list.end()) {
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
static void
pars_resolve_exp_list_variables_and_types(System &sys, SelNode *sel_node,
                                          que_node_t *exp_node) noexcept {
  while (exp_node != nullptr) {
    pars_resolve_exp_variables_and_types(sys, sel_node, exp_node);

    exp_node = que_node_get_next(exp_node);
  }
}

/**
 * @brief Resolves the columns in an expression.
 *
 * @param[in] table_node First node in a table list.
 * @param[in] exp_node Expression.
 */
static void pars_resolve_exp_columns(SymNode *table_node,
                                     que_node_t *exp_node) noexcept {
  ut_a(exp_node != nullptr);

  if (que_node_get_type(exp_node) == QUE_NODE_FUNC) {
    auto func_node = reinterpret_cast<FuncNode *>(exp_node);
    auto arg = func_node->args;

    while (arg != nullptr) {
      pars_resolve_exp_columns(table_node, arg);

      arg = que_node_get_next(arg);
    }

    return;
  }

  ut_a(que_node_get_type(exp_node) == QUE_NODE_SYMBOL);

  auto sym_node = reinterpret_cast<SymNode *>(exp_node);

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

      if (sym_node->name_len == strlen(col_name) &&
          memcmp(sym_node->name, col_name, sym_node->name_len) == 0) {
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

    t_node = reinterpret_cast<SymNode *>(que_node_get_next(t_node));
  }
}

/**
 * @brief Resolves the meaning of columns in an expression list.
 *
 * @param[in] table_node First node in a table list.
 * @param[in] exp_node Expression list first node, or nullptr.
 */
static void pars_resolve_exp_list_columns(SymNode *table_node,
                                          que_node_t *exp_node) noexcept {

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
static void pars_retrieve_table_def(System &sys, SymNode *sym_node) noexcept {
  ut_a(sym_node != nullptr);
  ut_a(que_node_get_type(sym_node) == QUE_NODE_SYMBOL);

  sym_node->resolved = true;
  sym_node->token_type = SYM_TABLE;

  auto table_name = const_cast<char *>(sym_node->name);

  sym_node->table = sys.m_dict->table_get(table_name);

  ut_a(sym_node->table != nullptr);
}

/**
 * @brief Retrieves the table definitions for a list of table name ids.
 *
 * @param[in] sym_node First table node in the list.
 *
 * @return Number of tables.
 */
static ulint pars_retrieve_table_list_defs(System &sys, SymNode *sym_node) noexcept {
  if (sym_node == nullptr) {
    return 0;
  }

  ulint count{};

  while (sym_node != nullptr) {
    pars_retrieve_table_def(sys, sym_node);

    ++count;

    sym_node = reinterpret_cast<SymNode *>(que_node_get_next(sym_node));
  }

  return count;
}

/**
 * @brief Adds all columns to the select list if the query is SELECT * FROM ...
 *
 * @param[in] sel_node The select node already containing the table list.
 */
static void pars_select_all_columns(System &sys, SelNode *sel_node) noexcept {
  sel_node->m_select_list = nullptr;

  auto table_node = sel_node->m_table_list;

  while (table_node != nullptr) {
    auto table = table_node->table;

    for (ulint i{}; i < table->get_n_user_cols(); ++i) {
      const auto col_name = table->get_col_name(i);

      auto col_node = sym_tab_add_id(sys.m_parser->m_sym_tab, (byte *)col_name,
                                     strlen(col_name));

      sel_node->m_select_list =
          que_node_list_add_last(sel_node->m_select_list, col_node);
    }

    table_node = reinterpret_cast<SymNode *>(que_node_get_next(table_node));
  }
}

SelNode *pars_select_list(System &sys, que_node_t *select_list, SymNode *into_list) {
  auto sel_node = SelNode::create(sys.m_parser->m_sym_tab->heap);

  sel_node->m_select_list = select_list;
  sel_node->m_into_list = into_list;

  pars_resolve_exp_list_variables_and_types(sys, nullptr, into_list);

  return sel_node;
}

/**
 * @brief Checks if the query is an aggregate query.
 *
 * This function determines if the select list contains only aggregate function
 * items.
 *
 * @param[in] sel_node The select node already containing the select list.
 */
static void pars_check_aggregate(SelNode *sel_node) noexcept {
  ulint n_nodes{};
  ulint n_aggregate_nodes{};

  auto exp_node = sel_node->m_select_list;

  while (exp_node != nullptr) {

    ++n_nodes;

    if (que_node_get_type(exp_node) == QUE_NODE_FUNC) {

      auto func_node = reinterpret_cast<FuncNode *>(exp_node);

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

SelNode *pars_select_statement(System &sys, SelNode *sel_node, SymNode *table_list,
                                  que_node_t *search_cond,
                                  const ParsResWord *for_update,
                                  const ParsResWord *lock_shared,
                                  OrderNode *order_by) {
  sel_node->m_state = SEL_NODE_OPEN;

  sel_node->m_table_list = table_list;
  sel_node->m_n_tables = pars_retrieve_table_list_defs(sys, table_list);

  if (sel_node->m_select_list == &pars_star_denoter) {

    /* SELECT * FROM ... */
    pars_select_all_columns(sys, sel_node);
  }

  if (sel_node->m_into_list) {
    ut_a(que_node_list_get_len(sel_node->m_into_list) ==
         que_node_list_get_len(sel_node->m_select_list));
  }

  UT_LIST_INIT(sel_node->m_copy_variables);

  pars_resolve_exp_list_columns(table_list, sel_node->m_select_list);
  pars_resolve_exp_list_variables_and_types(sys, sel_node, sel_node->m_select_list);
  pars_check_aggregate(sel_node);

  sel_node->m_search_cond = search_cond;

  if (search_cond != nullptr) {
    pars_resolve_exp_columns(table_list, search_cond);
    pars_resolve_exp_variables_and_types(sys, sel_node, search_cond);
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

  opt_search_plan(sys, sel_node);

  return sel_node;
}

que_node_t *pars_cursor_declaration(SymNode *sym_node,
                                    SelNode *sel_node) {
  sym_node->resolved = true;
  sym_node->token_type = SYM_CURSOR;
  sym_node->cursor_def = sel_node;

  sel_node->m_state = SEL_NODE_CLOSED;
  sel_node->m_explicit_cursor = sym_node;

  return sym_node;
}

que_node_t *pars_function_declaration(System &sys, SymNode *sym_node) {
  sym_node->resolved = true;
  sym_node->token_type = SYM_FUNCTION;

  /* Check that the function exists. */
  ut_a(pars_info_get_user_func(sys.m_parser->m_sym_tab->info, sym_node->name));

  return sym_node;
}

UpdNode *pars_update_statement_start(System &sys, bool is_delete, SymNode *table_sym,
                                        ColAssignNode *col_assign_list) {
  auto node = sys.m_row_upd->node_create(sys.m_parser->m_sym_tab->heap);

  node->m_is_delete = is_delete;
  node->m_table_sym = table_sym;
  node->m_col_assign_list = col_assign_list;

  return node;
}

ColAssignNode *pars_column_assignment(System &sys, SymNode *column, que_node_t *exp) {
  auto node = reinterpret_cast<ColAssignNode *>(
      mem_heap_alloc(sys.m_parser->m_sym_tab->heap, sizeof(ColAssignNode)));

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
static void pars_process_assign_list(System &sys, UpdNode *node) noexcept {
  auto table_sym = node->m_table_sym;
  auto col_assign_list =
      reinterpret_cast<ColAssignNode *>(node->m_col_assign_list);
  auto clust_index = node->m_table->get_first_index();

  ulint n_assigns{};
  auto assign_node = col_assign_list;

  while (assign_node != nullptr) {
    pars_resolve_exp_columns(table_sym, assign_node->col);
    pars_resolve_exp_columns(table_sym, assign_node->val);
    pars_resolve_exp_variables_and_types(sys, nullptr, assign_node->val);

#if 0
		ut_a(dtype_get_mtype(dfield_get_type(que_node_get_val(assign_node->col))) == dtype_get_mtype( dfield_get_type(que_node_get_val(assign_node->val))));
#endif

    /* Add to the update node all the columns found in assignment
    values as columns to copy: therefore, true */

    opt_find_all_cols(true, clust_index, &node->m_columns, nullptr,
                      assign_node->val);
    ++n_assigns;

    assign_node =
        reinterpret_cast<ColAssignNode *>(que_node_get_next(assign_node));
  }

  node->m_update = RowUpdate::upd_create(n_assigns, sys.m_parser->m_sym_tab->heap);

  assign_node = col_assign_list;

  auto changes_field_size = UPD_NODE_NO_SIZE_CHANGE;

  for (ulint i{}; i < n_assigns; ++i) {
    auto upd_field = node->m_update->get_nth_field(i);
    auto col_sym = assign_node->col;

    upd_field->set_field_no(clust_index->get_nth_field_pos(col_sym->col_no),
                            clust_index, nullptr);
    upd_field->m_exp = assign_node->val;

    if (clust_index->get_nth_col(upd_field->m_field_no)->get_fixed_size() ==
        0) {
      changes_field_size = 0;
    }

    assign_node =
        reinterpret_cast<ColAssignNode *>(que_node_get_next(assign_node));
  }

  /* Find out if the update can modify an ordering field in any index */

  auto changes_ord_field = UPD_NODE_NO_ORD_CHANGE;

  if (sys.m_row_upd->changes_some_index_ord_field_binary(node->m_table,
                                                       node->m_update)) {
    changes_ord_field = 0;
  }

  node->m_cmpl_info = changes_ord_field | changes_field_size;
}

UpdNode *pars_update_statement(System &sys, UpdNode *node, SymNode *cursor_sym,
                                  que_node_t *search_cond) {
  auto table_sym = node->m_table_sym;

  pars_retrieve_table_def(sys, table_sym);
  node->m_table = table_sym->table;

  UT_LIST_INIT(node->m_columns);

  /* Make the single table node into a list of table nodes of length 1 */

  que_node_list_add_last(nullptr, table_sym);

  SelNode *sel_node;

  if (cursor_sym != nullptr) {
    pars_resolve_exp_variables_and_types(sys, nullptr, cursor_sym);

    sel_node = cursor_sym->alias->cursor_def;

    node->m_searched_update = false;
  } else {
    sel_node = pars_select_list(sys, nullptr, nullptr);

    pars_select_statement(sys, sel_node, table_sym, search_cond, nullptr,
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
    pars_process_assign_list(sys, node);
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

InsertNode *pars_insert_statement(System &sys, SymNode *table_sym,
                                  que_node_t *values_list,
                                  SelNode *sel_node) {
  ut_a(values_list != nullptr || sel_node != nullptr);
  ut_a(values_list == nullptr || sel_node == nullptr);

  auto ins_type = values_list != nullptr ? INS_VALUES : INS_SEARCHED;

  pars_retrieve_table_def(sys, table_sym);

  auto ins_node = reinterpret_cast<InsertNode *>(sys.m_row_ins->node_create(
      ins_type, table_sym->table, sys.m_parser->m_sym_tab->heap));
  auto row =
      dtuple_create(sys.m_parser->m_sym_tab->heap, ins_node->m_table->get_n_cols());

  table_sym->table->copy_types(row);

  sys.m_row_ins->set_new_row(ins_node, row);

  ins_node->m_select = sel_node;

  if (sel_node != nullptr) {
    sel_node->m_common.parent = ins_node;

    ut_a(que_node_list_get_len(sel_node->m_select_list) ==
         table_sym->table->get_n_user_cols());
  }

  ins_node->m_values_list = values_list;

  if (values_list != nullptr) {
    pars_resolve_exp_list_variables_and_types(sys, nullptr, values_list);

    ut_a(que_node_list_get_len(values_list) ==
         table_sym->table->get_n_user_cols());
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
static void pars_set_dfield_type(Dfield *dfield, const ParsResWord *type,
                                 ulint len, bool is_unsigned,
                                 bool is_not_null) noexcept {
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

SymNode *pars_variable_declaration(SymNode *node, const ParsResWord *type) {
  node->resolved = true;
  node->token_type = SYM_VAR;

  node->param_type = PARS_NOT_PARAM;

  pars_set_dfield_type(que_node_get_val(node), type, 0, false, false);

  return node;
}

SymNode *pars_parameter_declaration(SymNode *node, ulint param_type,
                                       const ParsResWord *type) {
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
static void pars_set_parent_in_list(que_node_t *node_list,
                                    que_node_t *parent) noexcept {
  auto common = reinterpret_cast<QueCommon *>(node_list);

  while (common != nullptr) {
    common->parent = parent;

    common = reinterpret_cast<QueCommon *>(que_node_get_next(common));
  }
}

ElsifNode *pars_elsif_element(System &sys, que_node_t *cond, que_node_t *stat_list) {
  auto node = reinterpret_cast<ElsifNode *>(
      mem_heap_alloc(sys.m_parser->m_sym_tab->heap, sizeof(ElsifNode)));

  node->common.type = QUE_NODE_ELSIF;

  node->cond = cond;

  pars_resolve_exp_variables_and_types(sys, nullptr, cond);

  node->stat_list = stat_list;

  return node;
}

IfNode *pars_if_statement(System &sys, que_node_t *cond, que_node_t *stat_list,
                             que_node_t *else_part) {
  ElsifNode *elsif_node;

  auto node = reinterpret_cast<IfNode *>(
      mem_heap_alloc(sys.m_parser->m_sym_tab->heap, sizeof(IfNode)));

  node->common.type = QUE_NODE_IF;

  node->cond = cond;

  pars_resolve_exp_variables_and_types(sys, nullptr, cond);

  node->stat_list = stat_list;

  if (else_part && que_node_get_type(else_part) == QUE_NODE_ELSIF) {

    /* There is a list of elsif conditions */

    node->else_part = nullptr;
    node->elsif_list = static_cast<ElsifNode *>(else_part);

    elsif_node = static_cast<ElsifNode *>(else_part);

    while (elsif_node != nullptr) {
      pars_set_parent_in_list(elsif_node->stat_list, node);

      elsif_node = static_cast<ElsifNode *>(que_node_get_next(elsif_node));
    }
  } else {
    node->else_part = else_part;
    node->elsif_list = nullptr;

    pars_set_parent_in_list(else_part, node);
  }

  pars_set_parent_in_list(stat_list, node);

  return node;
}

WhileNode *pars_while_statement(System &sys, que_node_t *cond, que_node_t *stat_list) {
  auto node = reinterpret_cast<WhileNode *>(
      mem_heap_alloc(sys.m_parser->m_sym_tab->heap, sizeof(WhileNode)));

  node->common.type = QUE_NODE_WHILE;

  node->cond = cond;

  pars_resolve_exp_variables_and_types(sys, nullptr, cond);

  node->stat_list = stat_list;

  pars_set_parent_in_list(stat_list, node);

  return (node);
}

ForNode *pars_for_statement(System &sys, SymNode *loop_var,
                               que_node_t *loop_start_limit,
                               que_node_t *loop_end_limit,
                               que_node_t *stat_list) {
  auto node = reinterpret_cast<ForNode *>(
      mem_heap_alloc(sys.m_parser->m_sym_tab->heap, sizeof(ForNode)));

  node->common.type = QUE_NODE_FOR;

  pars_resolve_exp_variables_and_types(sys, nullptr, loop_var);
  pars_resolve_exp_variables_and_types(sys, nullptr, loop_start_limit);
  pars_resolve_exp_variables_and_types(sys, nullptr, loop_end_limit);

  node->loop_var = loop_var->indirection;

  ut_a(loop_var->indirection);

  node->loop_start_limit = loop_start_limit;
  node->loop_end_limit = loop_end_limit;

  node->stat_list = stat_list;

  pars_set_parent_in_list(stat_list, node);

  return node;
}

ExitNode *pars_exit_statement(System &sys) {
  auto node = reinterpret_cast<ExitNode *>(
      mem_heap_alloc(sys.m_parser->m_sym_tab->heap, sizeof(ExitNode)));

  node->common.type = QUE_NODE_EXIT;

  return node;
}

ReturnNode *pars_return_statement(System &sys) {
  auto node = reinterpret_cast<ReturnNode *>(
      mem_heap_alloc(sys.m_parser->m_sym_tab->heap, sizeof(ReturnNode)));

  node->common.type = QUE_NODE_RETURN;

  return node;
}

AssignNode *pars_assignment_statement(System &sys, SymNode *var, que_node_t *val) {
  auto node = reinterpret_cast<AssignNode *>(
      mem_heap_alloc(sys.m_parser->m_sym_tab->heap, sizeof(AssignNode)));

  node->common.type = QUE_NODE_ASSIGNMENT;

  node->var = var;
  node->val = val;

  pars_resolve_exp_variables_and_types(sys, nullptr, var);
  pars_resolve_exp_variables_and_types(sys, nullptr, val);

  ut_a(dtype_get_mtype(dfield_get_type(que_node_get_val(var))) ==
       dtype_get_mtype(dfield_get_type(que_node_get_val(val))));

  return node;
}

FuncNode *pars_procedure_call(System &sys, que_node_t *res_word, que_node_t *args) {
  auto node = pars_func(sys, res_word, args);

  pars_resolve_exp_list_variables_and_types(sys, nullptr, args);

  return node;
}

FetchNode *pars_fetch_statement(System &sys, SymNode *cursor, SymNode *into_list,
                                   SymNode *user_func) {
  /* Logical XOR. */
  ut_a(!into_list != !user_func);

  auto ptr = mem_heap_alloc(sys.m_parser->m_sym_tab->heap, sizeof(FetchNode));
  auto fetch_node = new (ptr) FetchNode();

  fetch_node->m_common.type = QUE_NODE_FETCH;

  pars_resolve_exp_variables_and_types(sys, nullptr, cursor);

  if (into_list != nullptr) {
    pars_resolve_exp_list_variables_and_types(sys, nullptr, into_list);
    fetch_node->m_into_list = into_list;
    fetch_node->m_func = nullptr;
  } else {
    pars_resolve_exp_variables_and_types(sys, nullptr, user_func);

    fetch_node->m_func =
        pars_info_get_user_func(sys.m_parser->m_sym_tab->info, user_func->name);
    ut_a(fetch_node->m_func);

    fetch_node->m_into_list = nullptr;
  }

  auto cursor_decl = cursor->alias;

  ut_a(cursor_decl->token_type == SYM_CURSOR);

  fetch_node->m_cursor_def = cursor_decl->cursor_def;

  if (into_list) {
    ut_a(que_node_list_get_len(into_list) ==
         que_node_list_get_len(fetch_node->m_cursor_def->m_select_list));
  }

  return fetch_node;
}

OpenNode *pars_open_statement(System &sys, ulint type, SymNode *cursor) {
  auto open_node = reinterpret_cast<OpenNode *>(
      mem_heap_alloc(sys.m_parser->m_sym_tab->heap, sizeof(OpenNode)));

  open_node->m_common.type = QUE_NODE_OPEN;

  pars_resolve_exp_variables_and_types(sys, nullptr, cursor);

  auto cursor_decl = cursor->alias;

  ut_a(cursor_decl->token_type == SYM_CURSOR);

  open_node->m_op_type = static_cast<OpenNodeOp>(type);
  open_node->m_cursor_def = cursor_decl->cursor_def;

  return open_node;
}

RowPrintfNode *pars_row_printf_statement(System &sys, SelNode *sel_node) {
  auto row_printf_node = reinterpret_cast<RowPrintfNode *>(
      mem_heap_alloc(sys.m_parser->m_sym_tab->heap, sizeof(RowPrintfNode)));

  row_printf_node->m_common.type = QUE_NODE_ROW_PRINTF;

  row_printf_node->m_sel_node = sel_node;

  sel_node->m_common.parent = row_printf_node;

  return row_printf_node;
}

CommitNode *pars_commit_statement(System &sys) {
  return Trx::commit_node_create(sys.m_parser->m_sym_tab->heap);
}

TrxRollback *pars_rollback_statement(System &sys) {
  return roll_node_create(sys.m_parser->m_sym_tab->heap);
}

SymNode *pars_column_def(SymNode *sym_node, const ParsResWord *type,
                            SymNode *len, void *is_unsigned,
                            void *is_not_null) {
  auto len2 = len != nullptr ? eval_node_get_int_val(len) : 0;

  pars_set_dfield_type(que_node_get_val(sym_node), type, len2,
                       is_unsigned != nullptr, is_not_null != nullptr);

  return sym_node;
}

TableNode *pars_create_table(System &sys, SymNode *table_sym, SymNode *column_defs,
                              void *not_fit_in_memory __attribute__((unused))) {
  const auto n_cols = que_node_list_get_len(column_defs);
  auto table = Table::create(table_sym->name, DICT_HDR_SPACE, n_cols, 0, false,
                             Current_location());

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

    column = static_cast<SymNode *>(que_node_get_next(column));
  }

  auto node =
      TableNode::create(sys, table, sys.m_parser->m_sym_tab->heap, true);

  table_sym->table = table;
  table_sym->resolved = true;
  table_sym->token_type = SYM_TABLE;

  return node;
}

IndexNode *pars_create_index(System &sys, const ParsResWord *unique_def,
                              const ParsResWord *clustered_def,
                              SymNode *index_sym, SymNode *table_sym,
                              SymNode *column_list) {
  ulint ind_type{};
  const auto n_fields = que_node_list_get_len(column_list);

  if (unique_def != nullptr) {
    ind_type = ind_type | DICT_UNIQUE;
  }

  if (clustered_def) {
    ind_type = ind_type | DICT_CLUSTERED;
  }

  auto index =
      Index::create(table_sym->name, index_sym->name, 0, ind_type, n_fields);
  auto column = column_list;

  while (column != nullptr) {
    (void)index->add_field(column->name, 0);

    column->resolved = true;
    column->token_type = SYM_COLUMN;

    column = static_cast<SymNode *>(que_node_get_next(column));
  }

  auto node =
      IndexNode::create(sys, index, sys.m_parser->m_sym_tab->heap, true);

  table_sym->resolved = true;
  table_sym->token_type = SYM_TABLE;

  index_sym->resolved = true;
  index_sym->token_type = SYM_TABLE;

  return node;
}

QueFork *pars_procedure_definition(System &sys, SymNode *sym_node,
                                      SymNode *param_list,
                                      que_node_t *stat_list) {
  auto heap = sys.m_parser->m_sym_tab->heap;
  auto fork = que_fork_create(nullptr, nullptr, QUE_FORK_PROCEDURE, heap);

  fork->trx = nullptr;

  auto thr = que_thr_create(fork, heap);

  auto node = reinterpret_cast<ProcNode *>(
      mem_heap_alloc(heap, sizeof(ProcNode)));

  node->common.type = QUE_NODE_PROC;
  node->common.parent = thr;

  sym_node->token_type = SYM_PROCEDURE_NAME;
  sym_node->resolved = true;

  node->proc_id = sym_node;
  node->param_list = param_list;
  node->stat_list = stat_list;

  pars_set_parent_in_list(stat_list, node);

  node->sym_tab = sys.m_parser->m_sym_tab;

  thr->child = node;

  sys.m_parser->m_sym_tab->query_graph = fork;

  return fork;
}

QueFork *pars_stored_procedure_call(SymNode *) {
  ut_error;
  return nullptr;
}

Parser::Parser(System &sys) noexcept : m_sys(sys) {}

int Parser::get_lex_chars(char *buf, int max_size) noexcept {
  auto len = m_sym_tab->string_len - m_sym_tab->next_char_pos;

  if (len == 0) {
    return 0;
  }

  if (len > static_cast<ulint>(max_size)) {
    len = max_size;
  }

  memcpy(buf, m_sym_tab->sql_string + m_sym_tab->next_char_pos, len);

  m_sym_tab->next_char_pos += len;

  return static_cast<int>(len);
}

void yyerror(System &, const char *p) {
  log_fatal("PARSER ERROR: Syntax error in SQL string: ", p);
}

QueFork *pars_sql(System &sys, ParsInfo *info, const char *str) {
  auto heap = mem_heap_create(256);

  /* Currently, the parser is not reentrant: */
  ut_ad(mutex_own(&sys.m_dict->m_mutex));

  sys.m_parser->m_sym_tab = sym_tab_create(heap);

  sys.m_parser->m_sym_tab->string_len = strlen(str);

  sys.m_parser->m_sym_tab->sql_string = static_cast<char *>(
      mem_heap_dup(heap, str, sys.m_parser->m_sym_tab->string_len + 1));

  sys.m_parser->m_sym_tab->next_char_pos = 0;
  sys.m_parser->m_sym_tab->info = info;

  sys.m_parser->m_lexer = std::make_unique<ParsLexer>(sys);

  yyparse(sys);

  sys.m_parser->m_lexer.reset();

  for (auto sym_node : sys.m_parser->m_sym_tab->sym_list) {
    ut_a(sym_node->resolved);
  }

  auto graph = sys.m_parser->m_sym_tab->query_graph;

  graph->info = info;
  graph->sym_tab = sys.m_parser->m_sym_tab;

  return graph;
}

QueThr *pars_complete_graph_for_exec(que_node_t *node, Trx *trx,
                                        MemHeap *heap) {
  auto fork = que_fork_create(nullptr, nullptr, QUE_FORK_USER_INTERFACE, heap);

  fork->trx = trx;

  auto thr = que_thr_create(fork, heap);

  thr->child = node;

  que_node_set_parent(node, thr);

  trx->m_graph = nullptr;

  return thr;
}

ParsInfo *pars_info_create() {
  auto heap = mem_heap_create(512);
  auto ptr = mem_heap_alloc(heap, sizeof(ParsInfo));
  auto info = new (ptr) ParsInfo;

  info->m_heap = heap;
  info->m_graph_owns_us = true;

  return info;
}

void pars_info_free(ParsInfo *info) {
  auto heap = info->m_heap;
  call_destructor(info);
  mem_heap_free(heap);
}

void pars_info_add_literal(ParsInfo *info, const char *name,
                           const void *address, ulint length, ulint type,
                           ulint prtype) {
  ut_ad(!pars_info_get_bound_lit(info, name));

  auto pbl = reinterpret_cast<ParsBoundLit *>(
      mem_heap_alloc(info->m_heap, sizeof(ParsBoundLit)));

  pbl->name = name;
  pbl->address = address;
  pbl->length = length;
  pbl->type = type;
  pbl->prtype = prtype;

  info->m_bound_lits.emplace_back(pbl);
}

void pars_info_add_str_literal(ParsInfo *info, const char *name,
                               const char *str) {
  pars_info_add_literal(info, name, str, strlen(str), DATA_VARCHAR,
                        DATA_ENGLISH);
}

void pars_info_add_int4_literal(ParsInfo *info, const char *name, lint val) {
  auto buf = mem_heap_alloc(info->m_heap, 4);

  mach_write_to_4(buf, val);
  pars_info_add_literal(info, name, buf, 4, DATA_INT, 0);
}

void pars_info_add_int8_literal(ParsInfo *info, const char *name,
                                uint64_t val) {
  auto buf = mem_heap_alloc(info->m_heap, sizeof(val));

  mach_write_to_8(buf, val);
  pars_info_add_literal(info, name, buf, sizeof(val), DATA_INT, 0);
}

void pars_info_add_uint64_literal(ParsInfo *info, const char *name,
                                  uint64_t val) {
  auto buf = mem_heap_alloc(info->m_heap, 8);

  mach_write_to_8(buf, val);

  pars_info_add_literal(info, name, buf, 8, DATA_FIXBINARY, 0);
}

void pars_info_add_function(ParsInfo *info, const char *name,
                            pars_user_func_cb_t func, void *arg) {
  ut_ad(!pars_info_get_user_func(info, name));

  auto puf = reinterpret_cast<ParsUserFunc *>(
      mem_heap_alloc(info->m_heap, sizeof(ParsUserFunc)));

  puf->name = name;
  puf->func = func;
  puf->arg = arg;

  info->m_funcs.emplace_back(puf);
}

void pars_info_add_id(ParsInfo *info, const char *name, const char *id) {
  ut_ad(!pars_info_get_bound_id(info, name));

  auto bid = reinterpret_cast<ParsBoundId *>(
      mem_heap_alloc(info->m_heap, sizeof(ParsBoundId)));

  bid->id = id;
  bid->name = name;

  info->m_bound_ids.emplace_back(bid);
}

ParsUserFunc *pars_info_get_user_func(ParsInfo *info, const char *name) {
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

ParsBoundLit *pars_info_get_bound_lit(ParsInfo *info, const char *name) {
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

ParsBoundId *pars_info_get_bound_id(ParsInfo *info, const char *name) {
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
