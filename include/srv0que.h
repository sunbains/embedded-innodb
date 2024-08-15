/****************************************************************************
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

/** @file include/srv0que.h
Server query execution

Created 6/5/1996 Heikki Tuuri
*******************************************************/

#pragma once

#include "innodb0types.h"

#include "que0types.h"

/**
 * Enqueues a task to server task queue and releases a worker thread, if there
 * is a suspended one.
 *
 * @param[in] thr query thread.
 */
void srv_que_task_enqueue_low(que_thr_t *thr);
