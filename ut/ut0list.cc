/****************************************************************************
Copyright (c) 2006, 2009, Innobase Oy. All Rights Reserved.

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

/** @file ut/ut0list.c
A double-linked list

Created 4/26/2006 Osku Salerma
************************************************************************/

#include "ut0list.h"
#ifdef UNIV_NONINL
#include "ut0list.ic"
#endif

ib_list_t *ib_list_create() {
  auto list = static_cast<ib_list_t *>(mem_alloc(sizeof(ib_list_t)));

  list->first = NULL;
  list->last = NULL;
  list->is_heap_list = false;

  return list;
}

void ib_list_free(ib_list_t *list) {
  ut_a(!list->is_heap_list);

  /* We don't check that the list is empty because it's entirely valid
  to e.g. have all the nodes allocated from a single heap that is then
  freed after the list itself is freed. */

  mem_free(list);
}

ib_list_node_t *ib_list_add_last(ib_list_t *list, void *data,
                                 mem_heap_t *heap) {
  return ib_list_add_after(list, ib_list_get_last(list), data, heap);
}

ib_list_node_t *ib_list_add_after(ib_list_t *list, ib_list_node_t *prev_node,
                                  void *data, mem_heap_t *heap) {
  auto node = reinterpret_cast<ib_list_node_t *>(
      mem_heap_alloc(heap, sizeof(ib_list_node_t)));

  node->data = data;

  if (!list->first) {
    /* Empty list. */

    ut_a(!prev_node);

    node->prev = NULL;
    node->next = NULL;

    list->first = node;
    list->last = node;
  } else if (!prev_node) {
    /* Start of list. */

    node->prev = NULL;
    node->next = list->first;

    list->first->prev = node;

    list->first = node;
  } else {
    /* Middle or end of list. */

    node->prev = prev_node;
    node->next = prev_node->next;

    prev_node->next = node;

    if (node->next) {
      node->next->prev = node;
    } else {
      list->last = node;
    }
  }

  return node;
}

void ib_list_remove(ib_list_t *list, ib_list_node_t *node) {
  if (node->prev) {
    node->prev->next = node->next;
  } else {
    /* First item in list. */

    ut_ad(list->first == node);

    list->first = node->next;
  }

  if (node->next) {
    node->next->prev = node->prev;
  } else {
    /* Last item in list. */

    ut_ad(list->last == node);

    list->last = node->prev;
  }
}
