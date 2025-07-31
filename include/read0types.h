/** Copyright (c) 2024 Sunny Bains. All rights reserved. */

#pragma once

#include "mem0mem.h"
#include "trx0types.h"

/** Read view types */
enum class Read_view_type : ulint {
  /** Normal consistent read view where transaction does not see
  changes made by active transactions except creating transaction. */
  NORMAL = 1,

  /** High-granularity read view where transaction does not see
  changes made by active transactions and own changes after a point in time when this read view was created. */
  HIGH_GRANULARITY = 2
};

/* @} */

// Forward declaration
struct Read_view;

/** Support consistent read views in cursors. This struct holds both
 * heap where consistent read view is allocated and pointer to a read view. */
struct Cursor_view {
  /** Memory heap for the cursor view */
  mem_heap_t *heap;

  /** Consistent read view of the cursor*/
  Read_view *read_view;

  /** number of Innobase tables used in the processing of this cursor */
  ulint n_client_tables_in_use;
};
