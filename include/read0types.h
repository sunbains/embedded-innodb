#pragma once

#include "mem0mem.h"
#include "trx0types.h"

/** Normal consistent read view where transaction does not see
changes made by active transactions except creating transaction. */
constexpr ulint VIEW_NORMAL = 1;

/** High-granularity read view where transaction does not see
changes made by active transactions and own changes after a point in time when this read view was created. */
constexpr ulint VIEW_HIGH_GRANULARITY = 2;

/* @} */


/** Read view lists the trx ids of those transactions for which a consistent
read should not see the modifications to the database. */
struct read_view_t {
  /** Check whether the changes by id are visible.
  @param[in]    id      transaction id to check against the view
  @return whether the view sees the modifications of id. */
  [[nodiscard]] bool changes_visible(trx_id_t id) const;

  /** VIEW_NORMAL, VIEW_HIGH_GRANULARITY */
  ulint type;

  /** 0 or if type is VIEW_HIGH_GRANULARITY transaction undo_no when
  this high-granularity consistent read view was created */
  undo_no_t undo_no;

  /** The view does not need to see the undo logs for transactions
  whose transaction number is strictly smaller (<) than this value:
  they can be removed in purge if not needed by other views */
  trx_id_t low_limit_no;

  /** The read should not see any transaction with trx id >= this value.
  In other words, this is the "high water mark". */
  trx_id_t low_limit_id;

  /** The read should see all trx ids which are strictly smaller (<) than
  this value.  In other words, this is the "low water mark". */
  trx_id_t up_limit_id;

  /** Number of cells in the trx_ids array */
  ulint n_trx_ids;

  /** Additional trx ids which the read should not see: typically, these are
  the active transactions at the time when the read is serialized, except the
  reading transaction itself; the trx ids in this array are in a descending
  order. These trx_ids should be between the "low" and "high" water marks,
  that is, up_limit_id and low_limit_id. */
  trx_id_t *trx_ids;

  /** trx id of creating transaction, or 0 used in purge */
  trx_id_t creator_trx_id;

  /** List of read views in trx_sys */
  UT_LIST_NODE_T(read_view_t) view_list;
};

UT_LIST_NODE_GETTER_DEFINITION(read_view_t, view_list);

/** Implement InnoDB framework to support consistent read views in
cursors. This struct holds both heap where consistent read view
is allocated and pointer to a read view. */
struct cursor_view_t {
  /** Memory heap for the cursor view */
  mem_heap_t *heap;

  /** Consistent read view of the cursor*/
  read_view_t *read_view;

  /** number of Innobase tables used in the processing of this cursor */
  ulint n_client_tables_in_use;
};
