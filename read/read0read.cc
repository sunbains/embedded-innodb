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

/** @file read/read0read.cc
Cursor read view implementation

Created 2/16/1997 Heikki Tuuri
*******************************************************/

#include "read0read.h"
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
cluster record is accessed. Because trx_id of the creating
transaction is stored when this view was created to the list of
trx_ids not seen by this read view previous version of the
record is requested to be built. This is build using clustered record.
If the secondary key record is delete marked its corresponding
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

PROOF: Consider e.g. following table and SQL-clauses:

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

When we access a clustered index record, the transaction id is fetched
from this record and this trx_id is compared to up_limit_id in the view.
If trx_id in the record is greater or equal than up_limit_id in the view,
the record is not visible in this read view. The trx_id of the creating
transaction is stored when this view was created to the list of
trx_ids not seen by this read view. If the record is not visible,
a previous version of the record is requested to be built using the undo log.

For the example above, when the cursor is created, it stores the current
transaction state. When we later update record (2) to (5), the new record
has a higher trx_id than up_limit_id in the cursor's read view. Therefore,
the cursor will not see the updated record (5). Instead, it will use the
undo log to reconstruct the previous committed version (2) that existed
when the cursor was created.

This mechanism ensures that the cursor sees only committed versions of
records as they existed at the time the cursor was created, providing
consistent read semantics. Q. E. D.

*/

std::string Read_view::to_string() const noexcept {
  std::string str{};

  if (type == Read_view_type::HIGH_GRANULARITY) {
    str = std::format("High-granularity read view undo_n:o {} {}\n", undo_no, undo_no);
  } else {
    str = "Normal read view\n";
  }

  str += std::format("Read view low limit trx n:o {} {}\n", low_limit_no, low_limit_no);

  str += std::format("Read view up limit trx id {}\n", up_limit_id);

  str += std::format("Read view low limit trx id {}\n", low_limit_id);

  str += "Read view individually stored trx ids:\n";

  const auto n_ids = n_trx_ids;

  for (ulint i{}; i < n_ids; ++i) {
    str += std::format("Read view trx id {}\n", get_nth_trx_id(i));
  }

  return str;
}
