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

/** @file read/read0read.c
Cursor read

Created 2/16/1997 Heikki Tuuri
*******************************************************/

#include "read0read.h"

#ifdef UNIV_NONINL
#include "read0read.ic"
#endif

#include "srv0srv.h"
#include "trx0sys.h"

/*
-------------------------------------------------------------------------------
FACT A: Cursor read view on a secondary index sees only committed versions
-------
of the records in the secondary index or those versions of rows created
by transaction which created a cursor before cursor was created even
if transaction which created the cursor has changed that clustered index page.

PROOF: We must show that read goes always to the clustered index record
to see that record is visible in the cursor read view. Consider e.g.
following table and SQL-clauses:

create table t1(a int not null, b int, primary key(a), index(b));
insert into t1 values (1,1),(2,2);
commit;

Now consider that we have a cursor for a query

select b from t1 where b >= 1;

This query will use secondary key on the table t1. Now after the first fetch
on this cursor if we do a update:

update t1 set b = 5 where b = 2;

Now second fetch of the cursor should not see record (2,5) instead it should
see record (2,2).

We also should show that if we have delete t1 where b = 5; we still
can see record (2,2).

When we access a secondary key record maximum transaction id is fetched
from this record and this trx_id is compared to up_limit_id in the view.
If trx_id in the record is greater or equal than up_limit_id in the view
cluster record is accessed.  Because trx_id of the creating
transaction is stored when this view was created to the list of
trx_ids not seen by this read view previous version of the
record is requested to be built. This is build using clustered record.
If the secondary key record is delete  marked it's corresponding
clustered record can be already be purged only if records
trx_id < low_limit_no. Purge can't remove any record deleted by a
transaction which was active when cursor was created. But, we still
may have a deleted secondary key record but no clustered record. But,
this is not a problem because this case is handled in
row_sel_get_clust_rec() function which is called
whenever we note that this read view does not see trx_id in the
record. Thus, we see correct version. Q. E. D.

-------------------------------------------------------------------------------
FACT B: Cursor read view on a clustered index sees only committed versions
-------
of the records in the clustered index or those versions of rows created
by transaction which created a cursor before cursor was created even
if transaction which created the cursor has changed that clustered index page.

PROOF:  Consider e.g.following table and SQL-clauses:

create table t1(a int not null, b int, primary key(a));
insert into t1 values (1),(2);
commit;

Now consider that we have a cursor for a query

select a from t1 where a >= 1;

This query will use clustered key on the table t1. Now after the first fetch
on this cursor if we do a update:

update t1 set a = 5 where a = 2;

Now second fetch of the cursor should not see record (5) instead it should
see record (2).

We also should show that if we have execute delete t1 where a = 5; after
the cursor is opened we still can see record (2).

When accessing clustered record we always check if this read view sees
trx_id stored to clustered record. By default we don't see any changes
if record trx_id >= low_limit_id i.e. change was made transaction
which started after transaction which created the cursor. If row
was changed by the future transaction a previous version of the
clustered record is created. Thus we see only committed version in
this case. We see all changes made by committed transactions i.e.
record trx_id < up_limit_id. In this case we don't need to do anything,
we already see correct version of the record. We don't see any changes
made by active transaction except creating transaction. We have stored
trx_id of creating transaction to list of trx_ids when this view was
created. Thus we can easily see if this record was changed by the
creating transaction. Because we already have clustered record we can
access roll_ptr. Using this roll_ptr we can fetch undo record.
We can now check that undo_no of the undo record is less than undo_no of the
trancaction which created a view when cursor was created. We see this
clustered record only in case when record undo_no is less than undo_no
in the view. If this is not true we build based on undo_rec previous
version of the record. This record is found because purge can't remove
records accessed by active transaction. Thus we see correct version. Q. E. D.
-------------------------------------------------------------------------------
FACT C: Purge does not remove any delete marked row that is visible
-------
to cursor view.

TODO: proof this

*/

/** Creates a read view object.
@param[in] n                    Number of cells in the trx_ids array.
@param[in,out] heap             Memory heap to use for allocation.
@return	own: read view struct */
inline read_view_t *read_view_create_low(ulint n, mem_heap_t *heap) {
  auto view = reinterpret_cast<read_view_t *>(mem_heap_alloc(heap, sizeof(read_view_t)));

  view->n_trx_ids = n;
  view->trx_ids = reinterpret_cast<trx_id_t *>(mem_heap_alloc(heap, n * sizeof *view->trx_ids));

  return view;
}

read_view_t *read_view_oldest_copy_or_open_new(trx_id_t cr_trx_id, mem_heap_t *heap) {
  read_view_t *old_view;
  read_view_t *view_copy;
  bool needs_insert = true;
  ulint insert_done = 0;
  ulint n;
  ulint i;

  ut_ad(mutex_own(&srv_trx_sys->m_mutex));

  old_view = UT_LIST_GET_LAST(srv_trx_sys->m_view_list);

  if (old_view == nullptr) {

    return read_view_open_now(cr_trx_id, heap);
  }

  n = old_view->n_trx_ids;

  if (old_view->creator_trx_id > 0) {
    n++;
  } else {
    needs_insert = false;
  }

  view_copy = read_view_create_low(n, heap);

  /* Insert the id of the creator in the right place of the descending
  array of ids, if needs_insert is true: */

  i = 0;
  while (i < n) {
    if (needs_insert && (i >= old_view->n_trx_ids || old_view->creator_trx_id > read_view_get_nth_trx_id(old_view, i))) {

      read_view_set_nth_trx_id(view_copy, i, old_view->creator_trx_id);
      needs_insert = false;
      insert_done = 1;
    } else {
      read_view_set_nth_trx_id(view_copy, i, read_view_get_nth_trx_id(old_view, i - insert_done));
    }

    i++;
  }

  view_copy->creator_trx_id = cr_trx_id;

  view_copy->low_limit_no = old_view->low_limit_no;
  view_copy->low_limit_id = old_view->low_limit_id;

  if (n > 0) {
    /* The last active transaction has the smallest id: */
    view_copy->up_limit_id = read_view_get_nth_trx_id(view_copy, n - 1);
  } else {
    view_copy->up_limit_id = old_view->up_limit_id;
  }

  UT_LIST_ADD_LAST(srv_trx_sys->m_view_list, view_copy);

  return view_copy;
}

read_view_t *read_view_open_now(trx_id_t cr_trx_id, mem_heap_t *heap) {
  ut_ad(mutex_own(&srv_trx_sys->m_mutex));

  auto view = read_view_create_low(UT_LIST_GET_LEN(srv_trx_sys->m_trx_list), heap);

  view->creator_trx_id = cr_trx_id;
  view->type = VIEW_NORMAL;
  view->undo_no = 0;

  /* No future transactions should be visible in the view */

  view->low_limit_no = srv_trx_sys->m_max_trx_id;
  view->low_limit_id = view->low_limit_no;

  ulint n = 0;

  /* No active transaction should be visible, except cr_trx */
  for (auto trx : srv_trx_sys->m_trx_list) {
    ut_ad(trx->m_magic_n == TRX_MAGIC_N);

    if (trx->m_id != cr_trx_id && (trx->m_conc_state == TRX_ACTIVE || trx->m_conc_state == TRX_PREPARED)) {

      read_view_set_nth_trx_id(view, n, trx->m_id);

      ++n;

      /* NOTE that a transaction whose trx number is < srv_trx_sys->m_max_trx_id can still be active, if it is
      in the middle of its commit! Note that when a transaction starts, we initialize trx->no to LSN_MAX. */

      if (view->low_limit_no > trx->m_no) {

        view->low_limit_no = trx->m_no;
      }
    }
  }

  view->n_trx_ids = n;

  if (n > 0) {
    /* The last active transaction has the smallest id: */
    view->up_limit_id = read_view_get_nth_trx_id(view, n - 1);
  } else {
    view->up_limit_id = view->low_limit_id;
  }

  UT_LIST_ADD_FIRST(srv_trx_sys->m_view_list, view);

  return view;
}

void read_view_close(read_view_t *view) {
  ut_ad(mutex_own(&srv_trx_sys->m_mutex));

  UT_LIST_REMOVE(srv_trx_sys->m_view_list, view);
}

void read_view_close_for_read_committed(Trx *trx) {
  ut_a(trx->m_global_read_view != nullptr);

  mutex_enter(&trx->m_trx_sys->m_mutex);

  read_view_close(trx->m_global_read_view);

  mem_heap_empty(trx->m_global_read_view_heap);

  trx->m_read_view = nullptr;
  trx->m_global_read_view = nullptr;

  mutex_exit(&srv_trx_sys->m_mutex);
}

std::string to_string(const read_view_t *view) noexcept {
  std::string str{};

  if (view->type == VIEW_HIGH_GRANULARITY) {
    str = std::format("High-granularity read view undo_n:o {} {}\n", view->undo_no, view->undo_no);
  } else {
    str = "Normal read view\n";
  }

  str += std::format("Read view low limit trx n:o {} {}\n", view->low_limit_no, view->low_limit_no);

  str += std::format("Read view up limit trx id {}\n", view->up_limit_id);

  str += std::format("Read view low limit trx id {}\n", view->low_limit_id);

  str += "Read view individually stored trx ids:\n";

  const auto n_ids = view->n_trx_ids;

  for (ulint i{}; i < n_ids; ++i) {
    str += std::format("Read view trx id {}\n", read_view_get_nth_trx_id(view, i));
  }

  return str;
}

cursor_view_t *read_cursor_view_create(Trx *cr_trx) {
  auto trx_sys = cr_trx->m_trx_sys;

  /* Use larger heap than in trx_create when creating a read_view because cursors are quite big. */

  auto heap = mem_heap_create(512);

  auto curview = (cursor_view_t *)mem_heap_alloc(heap, sizeof(cursor_view_t));
  curview->heap = heap;

  /* Drop cursor tables from consideration when evaluating the need of
  auto-commit */
  curview->n_client_tables_in_use = cr_trx->m_n_client_tables_in_use;
  cr_trx->m_n_client_tables_in_use = 0;

  mutex_enter(&trx_sys->m_mutex);

  curview->read_view = read_view_create_low(UT_LIST_GET_LEN(trx_sys->m_trx_list), curview->heap);

  auto view = curview->read_view;
  view->creator_trx_id = cr_trx->m_id;
  view->type = VIEW_HIGH_GRANULARITY;
  view->undo_no = cr_trx->m_undo_no;

  /* No future transactions should be visible in the view */

  view->low_limit_no = trx_sys->m_max_trx_id;
  view->low_limit_id = view->low_limit_no;

  ulint n = 0;
  /* No active transaction should be visible */

  for (auto trx : trx_sys->m_trx_list) {
    if (trx->m_conc_state == TRX_ACTIVE || trx->m_conc_state == TRX_PREPARED) {

      read_view_set_nth_trx_id(view, n, trx->m_id);

      n++;

      /* NOTE that a transaction whose trx number is <
      srv_trx_sys->m_max_trx_id can still be active, if it is
      in the middle of its commit! Note that when a
      transaction starts, we initialize trx->no to
      LSN_MAX. */

      if (view->low_limit_no > trx->m_no) {

        view->low_limit_no = trx->m_no;
      }
    }
  }

  view->n_trx_ids = n;

  if (n > 0) {
    /* The last active transaction has the smallest id: */
    view->up_limit_id = read_view_get_nth_trx_id(view, n - 1);
  } else {
    view->up_limit_id = view->low_limit_id;
  }

  UT_LIST_ADD_FIRST(trx_sys->m_view_list, view);

  mutex_exit(&srv_trx_sys->m_mutex);

  return curview;
}

void read_cursor_view_close(Trx *trx, cursor_view_t *curview) {
  ut_a(curview->read_view);
  ut_a(curview->heap);

  /* Add cursor's tables to the global count of active tables that
  belong to this transaction */
  trx->m_n_client_tables_in_use += curview->n_client_tables_in_use;

  mutex_enter(&trx->m_trx_sys->m_mutex);

  read_view_close(curview->read_view);
  trx->m_read_view = trx->m_global_read_view;

  mutex_exit(&trx->m_trx_sys->m_mutex);

  mem_heap_free(curview->heap);
}

void read_cursor_set(Trx *trx, cursor_view_t *curview) {
  mutex_enter(&trx->m_trx_sys->m_mutex);

  if (likely(curview != nullptr)) {
    trx->m_read_view = curview->read_view;
  } else {
    trx->m_read_view = trx->m_global_read_view;
  }

  mutex_exit(&trx->m_trx_sys->m_mutex);
}
