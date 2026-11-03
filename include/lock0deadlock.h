/****************************************************************************
Copyright (c) 1996, 2010, Innobase Oy. All Rights Reserved.
Copyright (c) 2025 Sunny Bains. All rights reserved.

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

/** @file include/lock0deadlock.h
Deadlock detection: a one edge waits-for graph walked first, the full
waits-for graph searched when the walk can't decide.

THE GRAPHS

The full waits-for graph G has an edge T -> U when T waits for a lock L
and a lock of U, ahead of L in L's queue, is one that L has to wait for
(Lock::has_to_wait_for(): conflicting modes, U != T; for records, on L's
record). U may itself be granted or waiting.

Every waiting transaction also publishes one of its G edges,
Trx::m_blocking_trx (an atomic, nullptr when it does not wait), and whether
it has more than one blocker, Trx::m_blocking_multi. The edge is set when
the waiting lock is created (a request, or a waiting lock moved by a page
operation), moved by the grant pass of a release when the blocker it named
is gone but the lock still has to wait, and cleared when the wait ends
(Lock::reset()). When it chooses, it prefers a blocker that waits itself.
An edge always names a current blocker: the one edge graph E is a subgraph
of G.

THE CHECK (deadlock_find())

1. The fast path: walk E from the start, deadlock_search(), O(path). A
   cycle of E is a cycle of G.
2. If the walk ends without a cycle and every transaction on the path has
   one blocker, the path is all of G reachable from the start: no cycle.
3. Else the slow path: a depth first search of G from the start over the
   blocker sets read from the queues, deadlock_dfs(), visiting only waiting
   transactions, each once per search (an epoch per search, no clearing of
   marks on all transactions). It runs with the global latch of the lock
   system in X (it reads queues of any shard); it is the rare path.

It runs for every new waiting lock: on a request (enter_lock_wait() walks
latch free with the shard latch of the request held; if the walk is suspect,
deadlock_resolve() runs deadlock_occurs() with X once the shard latch is
released), with the requester as the start; when a page operation moves a
waiting lock, deadlock_check_moved(), after the page operation, with its
transaction as the start. Between the walk and the full check the graph may
change: the full check, under X, is the one that decides; a cycle that forms
in between is found by the request or the move that closes it.

PROOF

Soundness: a cycle found is a deadlock. Every edge of E and of the search
is a G edge, a current blocker. In a cycle T1 -> T2 -> ... -> Tk -> T1 each
Ti waits for a lock that has to wait for a lock of Ti+1 ahead of it; that
lock goes away only when Ti+1 commits or rolls back, or (a waiting lock)
when Ti+1 is granted or cancelled; Ti+1 waits, so it does neither until its
own blocker goes; around the cycle, no member can make progress except by
an abort (deadlock victim, timeout, interrupt).

Completeness: every cycle is found when it forms. A G edge T -> U appears
only when a waiting lock of T is created behind a lock of U: a lock is only
ever added to a queue at its end (rec_create_low(), table_create() append),
and a bit is only set in an existing lock struct of a record queue
(rec_add_to_queue(), rec_lock_fast()) when no lock waits on that record:
so a granted lock never comes to be ahead of a waiting one on its record,
and a G edge into an existing waiter never appears. Locks that become
granted keep their place and their mode (same edges). A release only
removes G edges. Page operations move locks with their queue order (a moved
waiting lock is created anew behind the moved locks ahead of it: new edges,
checked). So a new cycle contains a new edge, out of the transaction whose
waiting lock was just created; the check from it searches all of G
reachable from it (steps 2 and 3), and finds the cycle.

Cost: the fast path O(path length); the slow path O(the waiting component
of G reachable from the start: its transactions and the queues ahead of
their waiting locks), never O(active transactions).

LIMITS

LOCK_MAX_DEPTH_IN_DEADLOCK_CHECK: a path longer than this (200 edges) is
treated as a deadlock of the requester, which is rolled back, as before.
There is no step limit any more (it was 1,000,000 transactions visited):
each transaction is visited once per search.

VICTIM

The weight rule of the old search, unchanged: the cycle start -> ... ->
pred -> start was closed by start; if pred (the transaction waiting for
start) weighs less than start (Trx::weight_cmp(): locks and undo records),
pred is the victim, else start is. One victim per cycle: the victim's wait
is cancelled, which breaks the cycle, and the check is repeated from the
start until no cycle remains.

*******************************************************/

#pragma once

#include "innodb0types.h"

/** Restricts the depth of the walk in the waits-for graph of transactions:
a longer path is treated as a deadlock of the requester. */
constexpr ulint LOCK_MAX_DEPTH_IN_DEADLOCK_CHECK = 200;

/** The result of a walk of the waits-for graph from a transaction. */
enum class DeadlockWalk {
  /** The path ends at a transaction that does not wait. */
  NONE,

  /** The path comes back to the start: a cycle. */
  CYCLE,

  /** The path is longer than LOCK_MAX_DEPTH_IN_DEADLOCK_CHECK. */
  TOO_DEEP
};
