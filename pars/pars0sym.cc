/****************************************************************************
Copyright (c) 1997, 2009, Innobase Oy. All Rights Reserved.

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

/** @file pars/pars0sym.c
SQL parser symbol table

Created 12/15/1997 Heikki Tuuri
*******************************************************/

#include "pars0sym.h"

#ifdef UNIV_NONINL
#include "pars0sym.ic"
#endif

#include "mem0mem.h"
#include "data0type.h"
#include "data0data.h"
#include "pars0grm.h"
#include "pars0pars.h"
#include "que0que.h"
#include "eval0eval.h"
#include "row0sel.h"

sym_tab_t *sym_tab_create(mem_heap_t *heap) {
  auto sym_tab =
      reinterpret_cast<sym_tab_t *>(mem_heap_alloc(heap, sizeof(sym_tab_t)));

  UT_LIST_INIT(sym_tab->sym_list);
  UT_LIST_INIT(sym_tab->func_node_list);

  sym_tab->heap = heap;

  return (sym_tab);
}

void sym_tab_free_private(sym_tab_t *sym_tab) {
  auto sym = UT_LIST_GET_FIRST(sym_tab->sym_list);

  while (sym) {
    eval_node_free_val_buf(sym);

    if (sym->prefetch_buf) {
      sel_col_prefetch_buf_free(sym->prefetch_buf);
    }

    if (sym->cursor_def) {
      que_graph_free_recursive(sym->cursor_def);
    }

    sym = UT_LIST_GET_NEXT(sym_list, sym);
  }

  auto func = UT_LIST_GET_FIRST(sym_tab->func_node_list);

  while (func) {
    eval_node_free_val_buf(func);

    func = UT_LIST_GET_NEXT(func_node_list, func);
  }
}

sym_node_t *sym_tab_add_int_lit(sym_tab_t *sym_tab, ulint val) {
  auto node = reinterpret_cast<sym_node_t *>(
      mem_heap_alloc(sym_tab->heap, sizeof(sym_node_t)));

  node->common.type = QUE_NODE_SYMBOL;

  node->resolved = true;
  node->token_type = SYM_LIT;

  node->indirection = nullptr;

  dtype_set(dfield_get_type(&node->common.val), DATA_INT, 0, 4);

  auto data = mem_heap_alloc(sym_tab->heap, 4);
  mach_write_to_4(data, val);

  dfield_set_data(&(node->common.val), data, 4);

  node->common.val_buf_size = 0;
  node->prefetch_buf = nullptr;
  node->cursor_def = nullptr;

  UT_LIST_ADD_LAST(sym_tab->sym_list, node);

  node->sym_table = sym_tab;

  return (node);
}

sym_node_t *sym_tab_add_str_lit(sym_tab_t *sym_tab, byte *str, ulint len) {
  auto node = reinterpret_cast<sym_node_t *>(
      mem_heap_alloc(sym_tab->heap, sizeof(sym_node_t)));

  node->common.type = QUE_NODE_SYMBOL;

  node->resolved = true;
  node->token_type = SYM_LIT;

  node->indirection = nullptr;

  dtype_set(dfield_get_type(&node->common.val), DATA_VARCHAR, DATA_ENGLISH, 0);

  byte *data;

  if (len > 0) {
    data = mem_heap_alloc(sym_tab->heap, len + 1);
    memcpy(data, str, len);
  } else {
    data = nullptr;
  }

  dfield_set_data(&(node->common.val), data, len);

  node->common.val_buf_size = 0;
  node->prefetch_buf = nullptr;
  node->cursor_def = nullptr;

  UT_LIST_ADD_LAST(sym_tab->sym_list, node);

  node->sym_table = sym_tab;

  return (node);
}

sym_node_t *sym_tab_add_bound_lit(sym_tab_t *sym_tab, const char *name,
                                  ulint *lit_type) {
  auto blit = pars_info_get_bound_lit(sym_tab->info, name);
  ut_a(blit != nullptr);

  auto node = reinterpret_cast<sym_node_t *>(
      mem_heap_alloc(sym_tab->heap, sizeof(sym_node_t)));

  node->common.type = QUE_NODE_SYMBOL;

  node->resolved = true;
  node->token_type = SYM_LIT;

  node->indirection = nullptr;

  ulint len = 0;

  switch (blit->type) {
  case DATA_FIXBINARY:
    len = blit->length;
    *lit_type = PARS_FIXBINARY_LIT;
    break;

  case DATA_BLOB:
    *lit_type = PARS_BLOB_LIT;
    break;

  case DATA_VARCHAR:
    *lit_type = PARS_STR_LIT;
    break;

  case DATA_CHAR:
    ut_a(blit->length > 0);

    len = blit->length;
    *lit_type = PARS_STR_LIT;
    break;

  case DATA_INT:
    ut_a(blit->length > 0);
    ut_a(blit->length <= 8);

    len = blit->length;
    *lit_type = PARS_INT_LIT;
    break;

  default:
    ut_error;
  }

  dtype_set(dfield_get_type(&node->common.val), blit->type, blit->prtype, len);

  dfield_set_data(&(node->common.val), blit->address, blit->length);

  node->common.val_buf_size = 0;
  node->prefetch_buf = nullptr;
  node->cursor_def = nullptr;

  UT_LIST_ADD_LAST(sym_tab->sym_list, node);

  node->sym_table = sym_tab;

  return node;
}

sym_node_t *sym_tab_add_null_lit(sym_tab_t *sym_tab) {
  auto node = reinterpret_cast<sym_node_t *>(
      mem_heap_alloc(sym_tab->heap, sizeof(sym_node_t)));

  node->common.type = QUE_NODE_SYMBOL;

  node->resolved = true;
  node->token_type = SYM_LIT;

  node->indirection = nullptr;

  dfield_get_type(&node->common.val)->mtype = DATA_ERROR;

  dfield_set_null(&node->common.val);

  node->common.val_buf_size = 0;
  node->prefetch_buf = nullptr;
  node->cursor_def = nullptr;

  UT_LIST_ADD_LAST(sym_tab->sym_list, node);

  node->sym_table = sym_tab;

  return (node);
}

sym_node_t *sym_tab_add_id(sym_tab_t *sym_tab, byte *name, ulint len) {
  auto node = reinterpret_cast<sym_node_t *>(
      mem_heap_zalloc(sym_tab->heap, sizeof(sym_node_t)));

  node->common.type = QUE_NODE_SYMBOL;

  node->resolved = false;
  node->indirection = nullptr;

  node->name = mem_heap_strdupl(sym_tab->heap, (char *)name, len);
  node->name_len = len;

  UT_LIST_ADD_LAST(sym_tab->sym_list, node);

  dfield_set_null(&node->common.val);

  node->common.val_buf_size = 0;
  node->prefetch_buf = nullptr;
  node->cursor_def = nullptr;

  node->sym_table = sym_tab;

  return node;
}

sym_node_t *sym_tab_add_bound_id(sym_tab_t *sym_tab, const char *name) {
  auto bid = pars_info_get_bound_id(sym_tab->info, name);
  ut_a(bid);

  auto node = reinterpret_cast<sym_node_t *>(
      mem_heap_alloc(sym_tab->heap, sizeof(sym_node_t)));

  node->common.type = QUE_NODE_SYMBOL;

  node->resolved = false;
  node->indirection = nullptr;

  node->name = mem_heap_strdup(sym_tab->heap, bid->id);
  node->name_len = strlen(node->name);

  UT_LIST_ADD_LAST(sym_tab->sym_list, node);

  dfield_set_null(&node->common.val);

  node->common.val_buf_size = 0;
  node->prefetch_buf = nullptr;
  node->cursor_def = nullptr;

  node->sym_table = sym_tab;

  return node;
}
