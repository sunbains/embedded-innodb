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

/** @file pars/pars0opt.c
Simple SQL optimizer

Created 12/21/1997 Heikki Tuuri
*******************************************************/

#include "pars0opt.h"

#include "row0sel.h"
#include "row0ins.h"
#include "row0upd.h"
#include "dict0dict.h"
#include "que0que.h"
#include "pars0grm.h"
#include "pars0pars.h"
#include "lock0lock.h"

/** Comparison by = */
constexpr ulint OPT_EQUAL{1};

/** Comparison by <, >, <=, or >= */
constexpr ulint OPT_COMPARISON{2};

constexpr ulint OPT_NOT_COND{1};
constexpr ulint OPT_END_COND{2};
constexpr ulint OPT_TEST_COND{3};
constexpr ulint OPT_SCROLL_COND{4};

/**
 * @brief Inverts a comparison operator.
 *
 * This function takes a comparison operator and returns the equivalent operator
 * when the order of the arguments is switched.
 *
 * @param[in] op The comparison operator to invert.
 *
 * @return The equivalent operator when the order of the arguments is switched.
 */
static int opt_invert_cmp_op(int op) noexcept {
  if (op == '<') {
    return '>';
  } else if (op == '>') {
    return '<';
  } else if (op == '=') {
    return '=';
  } else if (op == PARS_LE_TOKEN) {
    return PARS_GE_TOKEN;
  } else if (op == PARS_GE_TOKEN) {
    return PARS_LE_TOKEN;
  } else {
    ut_error;
  }

  return 0;
}

/**
 * @brief Checks if the value of an expression can be calculated BEFORE the nth
 * table in a join is accessed.
 *
 * If this is the case, it can possibly be used in an index search for the nth
 * table.
 *
 * @param[in] exp The expression to check.
 * @param[in] sel_node The select node containing the query.
 * @param[in] nth_table The nth table that will be accessed.
 *
 * @return true if the value of the expression is already determined before the
 * nth table is accessed.
 */
static bool opt_check_exp_determined_before(que_node_t *exp,
                                            sel_node_t *sel_node,
                                            ulint nth_table) noexcept {
  ut_ad(exp != nullptr && sel_node != nullptr);

  if (que_node_get_type(exp) == QUE_NODE_FUNC) {
    auto func_node = static_cast<func_node_t *>(exp);

    for (auto arg = func_node->args; arg != nullptr;
         arg = que_node_get_next(arg)) {
      if (!opt_check_exp_determined_before(arg, sel_node, nth_table)) {
        return false;
      }
    }

    return true;
  }

  ut_a(que_node_get_type(exp) == QUE_NODE_SYMBOL);

  auto sym_node = static_cast<sym_node_t *>(exp);

  if (sym_node->token_type != SYM_COLUMN) {

    return true;
  }

  for (ulint i{}; i < nth_table; ++i) {

    auto table = sel_node->get_nth_plan(i)->m_table;

    if (sym_node->table == table) {

      return true;
    }
  }

  return false;
}

/**
 * @brief Looks in a comparison condition if a column value is already
 * restricted by it BEFORE the nth table is accessed.
 *
 * This function checks if a column value is restricted by a comparison
 * condition before the nth table in a join is accessed. It returns the
 * expression restricting the value of the column, or NULL if not known.
 *
 * @param[in] cmp_type Type of comparison (OPT_EQUAL, OPT_COMPARISON).
 * @param[in] col_no Column number.
 * @param[in] search_cond Comparison condition.
 * @param[in] sel_node Select node.
 * @param[in] nth_table Nth table in a join (a query from a single table is
 * considered a join of 1 table).
 * @param[out] op Comparison operator ('=', PARS_GE_TOKEN, ... ); this is
 * inverted if the column appears on the right side.
 *
 * @return Expression restricting the value of the column, or NULL if not known.
 */
static que_node_t *opt_look_for_col_in_comparison_before(
    ulint cmp_type, ulint col_no, func_node_t *search_cond,
    sel_node_t *sel_node, ulint nth_table, ulint *op) noexcept {
  ut_ad(search_cond != nullptr);

  ut_a(search_cond->func == '<' || search_cond->func == '>' ||
       search_cond->func == '=' || search_cond->func == PARS_GE_TOKEN ||
       search_cond->func == PARS_LE_TOKEN);

  auto table = sel_node->get_nth_plan(nth_table)->m_table;

  if (cmp_type == OPT_EQUAL && search_cond->func != '=') {

    return nullptr;

  } else if (cmp_type == OPT_COMPARISON && search_cond->func != '<' &&
             search_cond->func != '>' && search_cond->func != PARS_GE_TOKEN &&
             search_cond->func != PARS_LE_TOKEN) {

    return nullptr;
  }

  {
    auto arg = search_cond->args;

    if (que_node_get_type(arg) == QUE_NODE_SYMBOL) {
      auto sym_node = static_cast<sym_node_t *>(arg);

      if (sym_node->token_type == SYM_COLUMN && sym_node->table == table &&
          sym_node->col_no == col_no) {

        /* sym_node contains the desired column id */

        /* Check if the expression on the right side of the
        operator is already determined */

        auto exp = que_node_get_next(arg);

        if (opt_check_exp_determined_before(exp, sel_node, nth_table)) {
          *op = search_cond->func;

          return exp;
        }
      }
    }
  }

  {
    auto exp = search_cond->args;
    auto arg = que_node_get_next(exp);

    if (que_node_get_type(arg) == QUE_NODE_SYMBOL) {
      auto sym_node = static_cast<sym_node_t *>(arg);

      if (sym_node->token_type == SYM_COLUMN && sym_node->table == table &&
          sym_node->col_no == col_no) {

        if (opt_check_exp_determined_before(exp, sel_node, nth_table)) {
          *op = opt_invert_cmp_op(search_cond->func);

          return exp;
        }
      }
    }
  }

  return nullptr;
}

/**
 * @brief Looks in a search condition if a column value is already restricted by
 * the search condition BEFORE the nth table is accessed. Takes into account
 * that if we will fetch in an ascending order, we cannot utilize an upper limit
 * for a column value; in a descending order, respectively, a lower limit.
 *
 * @param[in] cmp_type Type of comparison (OPT_EQUAL, OPT_COMPARISON).
 * @param[in] col_no Column number.
 * @param[in] search_cond Search condition or NULL.
 * @param[in] sel_node Select node.
 * @param[in] nth_table Nth table in a join (a query from a single table is
 * considered a join of 1 table).
 * @param[out] op Comparison operator ('=', PARS_GE_TOKEN, ...).
 *
 * @return Expression restricting the value of the column, or NULL if not known.
 */
static que_node_t *opt_look_for_col_in_cond_before(ulint cmp_type, ulint col_no,
                                                   func_node_t *search_cond,
                                                   sel_node_t *sel_node,
                                                   ulint nth_table,
                                                   ulint *op) noexcept {
  if (search_cond == nullptr) {

    return nullptr;
  }

  ut_a(que_node_get_type(search_cond) == QUE_NODE_FUNC);
  ut_a(search_cond->func != PARS_OR_TOKEN);
  ut_a(search_cond->func != PARS_NOT_TOKEN);

  if (search_cond->func == PARS_AND_TOKEN) {
    auto new_cond = static_cast<func_node_t *>(search_cond->args);
    auto exp = opt_look_for_col_in_cond_before(cmp_type, col_no, new_cond,
                                               sel_node, nth_table, op);

    if (exp != nullptr) {
      return exp;
    }

    new_cond = static_cast<func_node_t *>(que_node_get_next(new_cond));

    return opt_look_for_col_in_cond_before(cmp_type, col_no, new_cond, sel_node,
                                           nth_table, op);
  }

  auto exp = opt_look_for_col_in_comparison_before(
      cmp_type, col_no, search_cond, sel_node, nth_table, op);

  if (exp == nullptr) {

    return nullptr;
  }

  /* If we will fetch in an ascending order, we cannot utilize an upper
  limit for a column value; in a descending order, respectively, a lower
  limit */

  if (sel_node->m_asc && (*op == '<' || *op == PARS_LE_TOKEN)) {

    return nullptr;

  } else if (!sel_node->m_asc && (*op == '>' || *op == PARS_GE_TOKEN)) {

    return nullptr;
  }

  return exp;
}

/**
 * @brief Calculates the goodness for an index according to a select node.
 *
 * The goodness is calculated as follows:
 * - 4 times the number of first fields in the index whose values are exactly
 * known in the query.
 * - If there is a comparison condition for an additional field, 2 points are
 * added.
 * - If the index is unique and all the unique fields for the index are known,
 * 1024 points are added.
 * - For a clustered index, 1 point is added.
 *
 * @param[in] index The index to evaluate.
 * @param[in] sel_node The parsed select node.
 * @param[in] nth_table The nth table in a join.
 * @param[in,out] index_plan Comparison expressions for this index.
 * @param[out] last_op The last comparison operator, if goodness > 1.
 *
 * @return The calculated goodness.
 */
static ulint opt_calc_index_goodness(Index *index, sel_node_t *sel_node,
                                     ulint nth_table, que_node_t **index_plan,
                                     ulint *last_op) {
  ulint goodness{};

  /* Note that as higher level node pointers in the B-tree contain
  page addresses as the last field, we must not put more fields in
  the search tuple than index_get_n_unique_in_tree(index); see
  the note in btr_cur_search_to_nth_level. */

  const auto n_fields = index->get_n_unique_in_tree();

  for (ulint i{}; i < n_fields; ++i) {
    ulint op{};
    const auto col_no = index->get_nth_table_col_no(i);

    auto exp = opt_look_for_col_in_cond_before(
        OPT_EQUAL, col_no, static_cast<func_node_t *>(sel_node->m_search_cond),
        sel_node, nth_table, &op);

    if (exp != nullptr) {
      /* The value for this column is exactly known already at this stage of the
       * join */

      *last_op = op;
      goodness += 4;
      index_plan[i] = exp;
    } else {
      /* Look for non-equality comparisons */

      auto exp = opt_look_for_col_in_cond_before(
          OPT_COMPARISON, col_no,
          static_cast<func_node_t *>(sel_node->m_search_cond), sel_node,
          nth_table, &op);

      if (exp != nullptr) {
        *last_op = op;
        goodness += 2;
        index_plan[i] = exp;
      }

      break;
    }
  }

  if (goodness >= 4 * index->get_n_unique()) {

    goodness += 1024;

    if (index->is_clustered()) {

      goodness += 1024;
    }
  }

  /* We have to test for goodness here, as last_op may note be set */
  if (goodness > 0 && index->is_clustered()) {

    ++goodness;
  }

  return goodness;
}

/**
 * @brief Calculates the number of matched fields based on an index goodness.
 *
 * This function determines the number of exactly or partially matched fields
 * from the given index goodness value.
 *
 * @param[in] goodness The goodness value of the index.
 *
 * @return The number of exactly or partially matched fields.
 */
inline ulint opt_calc_n_fields_from_goodness(ulint goodness) noexcept {
  return ((goodness % 1024) + 2) / 4;
}

/**
 * @brief Converts a comparison operator to the corresponding search mode.
 *
 * This function takes a comparison operator and determines the appropriate
 * search mode (e.g., PAGE_CUR_GE, PAGE_CUR_LE) based on whether the rows
 * should be fetched in ascending or descending order.
 *
 * @param[in] asc True if the rows should be fetched in an ascending order.
 * @param[in] op The comparison operator ('=', '<', '>', PARS_GE_TOKEN,
 * PARS_LE_TOKEN).
 *
 * @return The corresponding search mode.
 */
inline ib_srch_mode_t opt_op_to_search_mode(bool asc, ulint op) noexcept {
  if (op == '=') {
    if (asc) {
      return PAGE_CUR_GE;
    } else {
      return PAGE_CUR_LE;
    }
  } else if (op == '<') {
    ut_a(!asc);
    return PAGE_CUR_L;
  } else if (op == '>') {
    ut_a(asc);
    return PAGE_CUR_G;
  } else if (op == PARS_GE_TOKEN) {
    ut_a(asc);
    return PAGE_CUR_GE;
  } else if (op == PARS_LE_TOKEN) {
    ut_a(!asc);
    return PAGE_CUR_LE;
  } else {
    ut_error;
  }

  return PAGE_CUR_UNSUPP;
}

/**
 * @brief Determines if a node is an argument node of a function node.
 *
 * This function checks if the given node is an argument of the specified
 * function node.
 *
 * @param[in] arg_node The possible argument node.
 * @param[in] func_node The function node.
 *
 * @return true if the node is an argument of the function node, false
 * otherwise.
 */
static bool opt_is_arg(que_node_t *arg_node, func_node_t *func_node) noexcept {
  for (auto arg = func_node->args; arg != nullptr;
       arg = que_node_get_next(arg)) {
    if (arg == arg_node) {

      return true;
    }
  }

  return false;
}

/**
 * @brief Decides if the fetching of rows should be made in a descending order,
 * and also checks that the chosen query plan produces a result which satisfies
 * the order-by clause.
 *
 * This function ensures that the query plan aligns with the order-by clause
 * specified in the select node. If the plan does not agree with the order-by,
 * an error is asserted.
 *
 * @param[in] sel_node The select node containing the query plan and order-by
 * clause.
 */
static void opt_check_order_by(sel_node_t *sel_node) noexcept {
  if (sel_node->m_order_by == nullptr) {

    return;
  }

  auto order_node = sel_node->m_order_by;
  auto order_col_no = order_node->column->col_no;
  auto order_table = order_node->column->table;

  /* If there is an order-by clause, the first non-exactly matched field
  in the index used for the last table in the table list should be the
  column defined in the order-by clause, and for all the other tables
  we should get only at most a single row, otherwise we cannot presently
  calculate the order-by, as we have no sort utility */

  for (ulint i{}; i < sel_node->m_n_tables; ++i) {

    auto plan = sel_node->get_nth_plan(i);
    auto index = plan->m_index;

    if (i < sel_node->m_n_tables - 1) {
      ut_a(index->get_n_unique() <= plan->m_n_exact_match);
    } else {
      ut_a(plan->m_table == order_table);

      ut_a(index->get_n_unique() <= plan->m_n_exact_match ||
           index->get_nth_table_col_no(plan->m_n_exact_match) == order_col_no);
    }
  }
}

/**
 * @brief Optimizes a select statement by deciding which indexes to use for the
 * tables.
 *
 * This function determines the best indexes to use for the tables involved in
 * the select statement. The tables are accessed in the order that they were
 * written in the FROM part of the select statement.
 *
 * @param[in] sel_node Parsed select node.
 * @param[in] i The index of the current table in the select statement.
 * @param[in] table The table for which the index is being optimized.
 */
static void opt_search_plan_for_table(sel_node_t *sel_node, ulint i,
                                      Table *table) noexcept {
  using Que_nodes = std::array<que_node_t *, 256>;
  ulint last_op{}; /* Eliminate a Purify warning */
  ulint best_last_op{};
  Que_nodes index_plan{};
  Que_nodes best_index_plan{};
  auto plan = sel_node->get_nth_plan(i);

  plan->m_table = table;
  plan->m_asc = sel_node->m_asc;
  plan->m_pcur_is_open = false;
  plan->m_cursor_at_end = false;

  /* Calculate goodness for each index of the table */

  ulint best_goodness{};
  auto best_index = table->get_clustered_index();

  for (const auto index : table->m_indexes) {
    auto goodness = opt_calc_index_goodness(index, sel_node, i,
                                            index_plan.data(), &last_op);

    if (goodness > best_goodness) {

      best_index = index;
      best_goodness = goodness;
      const auto n_fields = opt_calc_n_fields_from_goodness(goodness);

      memcpy(best_index_plan.data(), index_plan.data(),
             n_fields * sizeof(Que_nodes::value_type));

      best_last_op = last_op;
    }
  }

  plan->m_index = best_index;

  const auto n_fields = opt_calc_n_fields_from_goodness(best_goodness);

  if (n_fields == 0) {
    plan->m_tuple = nullptr;
    plan->m_n_exact_match = 0;
  } else {
    plan->m_tuple = dtuple_create(pars_sym_tab_global->heap, n_fields);
    plan->m_index->copy_types(plan->m_tuple, n_fields);

    plan->m_tuple_exps = reinterpret_cast<que_node_t **>(
        mem_heap_alloc(pars_sym_tab_global->heap, n_fields * sizeof(void *)));

    memcpy(plan->m_tuple_exps, best_index_plan.data(),
           n_fields * sizeof(void *));

    if (best_last_op == '=') {
      plan->m_n_exact_match = n_fields;
    } else {
      plan->m_n_exact_match = n_fields - 1;
    }

    plan->m_mode = opt_op_to_search_mode(sel_node->m_asc, best_last_op);
  }

  if (best_index->is_clustered() &&
      plan->m_n_exact_match >= best_index->get_n_unique()) {

    plan->m_unique_search = true;
  } else {
    plan->m_unique_search = false;
  }

  plan->m_old_row_heap = nullptr;

  plan->m_pcur.init(0);
  plan->m_clust_pcur.init(0);
}

/**
 * @brief Looks at a comparison condition and decides if it can, and needs to be
 * tested for a table AFTER the table has been accessed.
 *
 * This function classifies a comparison condition for a specific table in a
 * join.
 *
 * @param[in] sel_node The select node containing the query.
 * @param[in] i The index of the table in the join.
 * @param[in] cond The comparison condition to classify.
 *
 * @return OPT_NOT_COND if not for this table, else OPT_END_COND,
 * OPT_TEST_COND, or OPT_SCROLL_COND, where the last means that the
 * condition need not be tested, except when scroll cursors are used.
 */
static ulint opt_classify_comparison(sel_node_t *sel_node, ulint i,
                                     func_node_t *cond) noexcept {
  ulint op;

  ut_ad(cond && sel_node);

  auto plan = sel_node->get_nth_plan(i);

  /* Check if the condition is determined after the ith table has been
  accessed, but not after the i - 1:th */

  if (!opt_check_exp_determined_before(cond, sel_node, i + 1)) {

    return OPT_NOT_COND;
  }

  if (i > 0 && opt_check_exp_determined_before(cond, sel_node, i)) {

    return OPT_NOT_COND;
  }

  /* If the condition is an exact match condition used in constructing
  the search tuple, it is classified as OPT_END_COND */

  ulint n_fields;

  if (plan->m_tuple != nullptr) {
    n_fields = dtuple_get_n_fields(plan->m_tuple);
  } else {
    n_fields = 0;
  }

  for (ulint j{}; j < plan->m_n_exact_match; ++j) {

    if (opt_is_arg(plan->m_tuple_exps[j], cond)) {

      return OPT_END_COND;
    }
  }

  /* If the condition is an non-exact match condition used in
  constructing the search tuple, it is classified as OPT_SCROLL_COND.
  When the cursor is positioned, and if a non-scroll cursor is used,
  there is no need to test this condition; if a scroll cursor is used
  the testing is necessary when the cursor is reversed. */

  if (n_fields > plan->m_n_exact_match &&
      opt_is_arg(plan->m_tuple_exps[n_fields - 1], cond)) {

    return OPT_SCROLL_COND;
  }

  /* If the condition is a non-exact match condition on the first field
  in index for which there is no exact match, and it limits the search
  range from the opposite side of the search tuple already BEFORE we
  access the table, it is classified as OPT_END_COND */

  if (plan->m_index->get_n_fields() > plan->m_n_exact_match &&
      opt_look_for_col_in_comparison_before(
          OPT_COMPARISON,
          plan->m_index->get_nth_table_col_no(plan->m_n_exact_match), cond,
          sel_node, i, &op)) {

    if (sel_node->m_asc && (op == '<' || op == PARS_LE_TOKEN)) {

      return OPT_END_COND;
    }

    if (!sel_node->m_asc && (op == '>' || op == PARS_GE_TOKEN)) {

      return OPT_END_COND;
    }
  }

  /* Otherwise, cond is classified as OPT_TEST_COND */

  return OPT_TEST_COND;
}

/**
 * @brief Recursively looks for test conditions for a table in a join.
 *
 * This function traverses the conjunction of search conditions and classifies
 * them as either end conditions or test conditions for the specified table in
 * the join.
 *
 * @param[in] sel_node The select node containing the query.
 * @param[in] i The index of the table in the join.
 * @param[in] cond The conjunction of search conditions or NULL.
 */
static void opt_find_test_conds(sel_node_t *sel_node, ulint i,
                                func_node_t *cond) noexcept {
  if (cond == nullptr) {

    return;
  }

  if (cond->func == PARS_AND_TOKEN) {
    auto new_cond = static_cast<func_node_t *>(cond->args);

    opt_find_test_conds(sel_node, i, new_cond);

    new_cond = static_cast<func_node_t *>(que_node_get_next(new_cond));

    opt_find_test_conds(sel_node, i, new_cond);

    return;
  }

  auto plan = sel_node->get_nth_plan(i);
  auto func_class = opt_classify_comparison(sel_node, i, cond);

  if (func_class == OPT_END_COND) {
    plan->m_end_conds.push_back(cond);

  } else if (func_class == OPT_TEST_COND) {
    plan->m_other_conds.push_back(cond);
  }
}

/**
 * @brief Normalizes a list of comparison conditions so that a column of the
 * table appears on the left side of the comparison if possible. This is
 * accomplished by switching the arguments of the operator.
 *
 * @param[in] cond The first in a list of comparison conditions, or NULL.
 * @param[in] table The table to which the conditions apply.
 */
static void opt_normalize_cmp_conds(func_node_t *cond, Table *table) noexcept {
  for (; cond != nullptr; cond = UT_LIST_GET_NEXT(cond_list, cond)) {
    auto arg1 = cond->args;
    auto arg2 = que_node_get_next(arg1);

    if (que_node_get_type(arg2) == QUE_NODE_SYMBOL) {

      auto sym_node = static_cast<sym_node_t *>(arg2);

      if (sym_node->token_type == SYM_COLUMN && sym_node->table == table) {

        /* Switch the order of the arguments */

        cond->args = arg2;
        que_node_list_add_last(nullptr, arg2);
        que_node_list_add_last(arg2, arg1);

        /* Invert the operator */
        cond->func = opt_invert_cmp_op(cond->func);
      }
    }
  }
}

/**
 * @brief Finds out the search condition conjuncts we can, and need, to test as
 * the ith table in a join is accessed.
 *
 * The search tuple can eliminate the need to test some conjuncts.
 *
 * @param[in] sel_node The select node.
 * @param[in] i The ith table in the join.
 */
static void opt_determine_and_normalize_test_conds(sel_node_t *sel_node,
                                                   ulint i) noexcept {
  auto plan = sel_node->get_nth_plan(i);

  /* Recursively go through the conjuncts and classify them */

  opt_find_test_conds(sel_node, i,
                      static_cast<func_node_t *>(sel_node->m_search_cond));

  opt_normalize_cmp_conds(plan->m_end_conds.front(), plan->m_table);

  ut_a(plan->m_end_conds.size() >= plan->m_n_exact_match);
}

void opt_find_all_cols(bool copy_val, Index *index, sym_node_list_t *col_list,
                       Plan *plan, que_node_t *exp) {
  if (exp == nullptr) {

    return;
  }

  if (que_node_get_type(exp) == QUE_NODE_FUNC) {
    auto func_node = static_cast<func_node_t *>(exp);
    auto arg = func_node->args;

    while (arg != nullptr) {
      opt_find_all_cols(copy_val, index, col_list, plan, arg);
      arg = que_node_get_next(arg);
    }

    return;
  }

  ut_a(que_node_get_type(exp) == QUE_NODE_SYMBOL);

  auto sym_node = static_cast<sym_node_t *>(exp);

  if (sym_node->token_type != SYM_COLUMN) {

    return;
  }

  if (sym_node->table != index->m_table) {

    return;
  }

  /* Look for an occurrence of the same column in the plan column list */

  for (auto col_node : *col_list) {
    if (col_node->col_no == sym_node->col_no) {

      if (col_node == sym_node) {
        /* sym_node was already in a list: do nothing */
        return;
      }

      /* Put an indirection */
      sym_node->alias = col_node;
      sym_node->indirection = col_node;

      return;
    }
  }

  /* The same column did not occur in the list: add it */

  UT_LIST_ADD_LAST(*col_list, sym_node);

  sym_node->copy_val = copy_val;

  /* Fill in the field_no fields in sym_node */

  sym_node->field_nos[SYM_CLUST_FIELD_NO] =
      index->get_clustered_index()->get_nth_field_pos(sym_node->col_no);

  if (!index->is_clustered()) {

    ut_a(plan != nullptr);

    const auto col_pos = index->get_nth_field_pos(sym_node->col_no);

    if (col_pos == ULINT_UNDEFINED) {

      plan->m_must_get_clust = true;
    }

    sym_node->field_nos[SYM_SEC_FIELD_NO] = col_pos;
  }
}

/**
 * @brief Looks for occurrences of the columns of the table in conditions which
 * are not yet determined AFTER the join operation has fetched a row in the ith
 * table. The values for these columns must be copied to dynamic memory for
 * later use.
 *
 * @param[in] sel_node Parsed select node.
 * @param[in] i The index of the current table in the join.
 * @param[in] search_cond Search condition or NULL.
 */
static void opt_find_copy_cols(sel_node_t *sel_node, ulint i,
                               func_node_t *search_cond) noexcept {
  if (search_cond == nullptr) {

    return;
  }

  ut_ad(que_node_get_type(search_cond) == QUE_NODE_FUNC);

  if (search_cond->func == PARS_AND_TOKEN) {
    auto new_cond = static_cast<func_node_t *>(search_cond->args);

    opt_find_copy_cols(sel_node, i, new_cond);

    new_cond = static_cast<func_node_t *>(que_node_get_next(new_cond));

    opt_find_copy_cols(sel_node, i, new_cond);

    return;
  }

  if (!opt_check_exp_determined_before(search_cond, sel_node, i + 1)) {

    /* Any ith table columns occurring in search_cond should be
    copied, as this condition cannot be tested already on the
    fetch from the ith table */

    auto plan = sel_node->get_nth_plan(i);

    opt_find_all_cols(true, plan->m_index, &plan->m_columns, plan, search_cond);
  }
}

/**
 * @brief Classifies the table columns according to their usage.
 *
 * This function classifies the table columns based on whether the column is
 * used only while holding the latch on the page or if the column value needs to
 * be copied to dynamic memory. It places the first occurrence of a column into
 * either list in the plan node and sets indirections for later occurrences of
 * the column.
 *
 * @param[in] sel_node The select node containing the query.
 * @param[in] i The index of the current table in the join.
 */
static void opt_classify_cols(sel_node_t *sel_node, ulint i) noexcept {
  auto plan = sel_node->get_nth_plan(i);

  /* The final value of the following field will depend on the
  environment of the select statement: */

  plan->m_must_get_clust = false;

  /* All select list columns should be copied: therefore true as the
  first argument */

  for (auto exp = sel_node->m_select_list; exp != nullptr;
       exp = que_node_get_next(exp)) {
    opt_find_all_cols(true, plan->m_index, &(plan->m_columns), plan, exp);
  }

  opt_find_copy_cols(sel_node, i,
                     static_cast<func_node_t *>(sel_node->m_search_cond));

  /* All remaining columns in the search condition are temporary
  columns: therefore false */

  opt_find_all_cols(false, plan->m_index, &(plan->m_columns), plan,
                    sel_node->m_search_cond);
}

/**
 * @brief Fills in the information in the plan used for accessing a clustered
 * index record.
 *
 * This function populates the plan with the necessary information to access a
 * clustered index record. The columns must already be classified for the plan
 * node.
 *
 * @param[in] sel_node The select node containing the query.
 * @param[in] n The index of the current table in the select statement.
 */
static void opt_clust_access(sel_node_t *sel_node, ulint n) {
  auto plan = sel_node->get_nth_plan(n);
  auto index = plan->m_index;

  /* The final value of the following field depends on the environment
  of the select statement: */

  plan->m_no_prefetch = false;

  if (index->is_clustered()) {
    plan->m_clust_map = nullptr;
    plan->m_clust_ref = nullptr;

    return;
  }

  auto table = index->m_table;
  auto clust_index = table->get_clustered_index();
  const auto n_fields = clust_index->get_n_unique();
  auto heap = pars_sym_tab_global->heap;

  plan->m_clust_ref = dtuple_create(heap, n_fields);

  clust_index->copy_types(plan->m_clust_ref, n_fields);

  plan->m_clust_map =
      reinterpret_cast<ulint *>(mem_heap_alloc(heap, n_fields * sizeof(ulint)));

  for (ulint i{}; i < n_fields; ++i) {
    const auto pos = index->get_nth_field_pos(clust_index, i);

    ut_a(pos != ULINT_UNDEFINED);

    /* We optimize here only queries to InnoDB's internal system
    tables, and they should not contain column prefix indexes. */

    if (index->get_nth_field(pos)->m_prefix_len != 0 ||
        clust_index->get_nth_field(i)->m_prefix_len != 0) {
      log_err(
          std::format("Table {} has prefix_len != 0", index->m_table->m_name));
    }

    plan->m_clust_map[i] = pos;
  }
}

void opt_search_plan(sel_node_t *sel_node) {
  order_node_t *order_by;

  auto ptr = mem_heap_alloc(pars_sym_tab_global->heap,
                            sel_node->m_n_tables * sizeof(Plan));
  sel_node->m_plans = reinterpret_cast<Plan *>(ptr);

  for (ulint i{}; i < sel_node->m_n_tables; ++i) {
    new (&sel_node->m_plans[i])
        Plan(srv_fsp, srv_btree_sys, srv_lock_sys, srv_row_sel);
  }

  /* Analyze the search condition to find out what we know at each
  join stage about the conditions that the columns of a table should
  satisfy */

  auto table_node = sel_node->m_table_list;

  if (sel_node->m_order_by == nullptr) {
    sel_node->m_asc = true;
  } else {
    order_by = sel_node->m_order_by;

    sel_node->m_asc = order_by->asc;
  }

  for (ulint i{}; i < sel_node->m_n_tables; ++i) {

    auto table = table_node->table;

    /* Choose index through which to access the table */

    opt_search_plan_for_table(sel_node, i, table);

    /* Determine the search condition conjuncts we can test at
    this table; normalize the end conditions */

    opt_determine_and_normalize_test_conds(sel_node, i);

    table_node = static_cast<sym_node_t *>(que_node_get_next(table_node));
  }

  table_node = sel_node->m_table_list;

  for (ulint i{}; i < sel_node->m_n_tables; ++i) {

    /* Classify the table columns into those we only need to access
    but not copy, and to those we must copy to dynamic memory */

    opt_classify_cols(sel_node, i);

    /* Calculate possible info for accessing the clustered index
    record */

    opt_clust_access(sel_node, i);

    table_node = static_cast<sym_node_t *>(que_node_get_next(table_node));
  }

  /* Check that the plan obeys a possible order-by clause: if not,
  an assertion error occurs */

  opt_check_order_by(sel_node);

#ifdef UNIV_SQL_DEBUG
  opt_print_query_plan(sel_node);
#endif
}

void opt_print_query_plan(sel_node_t *sel_node) {
  ulint n_fields;

  log_info("QUERY PLAN FOR A SELECT NODE");
  log_info(sel_node->m_asc ? "Asc. search; " : "Desc. search; ");

  if (sel_node->m_set_x_locks) {
    log_info("sets row x-locks; ");
    ut_a(sel_node->m_row_lock_mode == LOCK_X);
    ut_a(!sel_node->m_consistent_read);
  } else if (sel_node->m_consistent_read) {
    log_info("consistent read; ");
  } else {
    ut_a(sel_node->m_row_lock_mode == LOCK_S);
    log_info("sets row s-locks; ");
  }

  for (ulint i{}; i < sel_node->m_n_tables; ++i) {
    auto plan = sel_node->get_nth_plan(i);

    if (plan->m_tuple) {
      n_fields = dtuple_get_n_fields(plan->m_tuple);
    } else {
      n_fields = 0;
    }

    log_info(std::format("Table {}; exact m. {}, match {}, end conds {}",
                         plan->m_table->m_name, plan->m_n_exact_match, n_fields,
                         plan->m_end_conds.size()));
  }
}
