/****************************************************************************
Copyright (c) 1995, 2009, Innobase Oy. All Rights Reserved.

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

/*** @file include/buf0rea.h
The database buffer read

Created 11/5/1995 Heikki Tuuri
*******************************************************/

#pragma once

#include "innodb0types.h"

#include "buf0types.h"

/**
 * @brief High-level function which reads a page asynchronously
 *        from a file to the buffer srv_buf_pool if it is not already
 *        there. Sets the io_fix flag and sets an exclusive lock
 *        on the buffer frame. The flag is cleared and the
 *        x-lock released by the i/o-handler thread.
 * 
 * @param space The space id.
 * @param offset The page number.
 * @return true if the page has been read in, false in case of failure.
 */
bool buf_read_page(ulint space, ulint offset);

/**
 * @brief Applies linear read-ahead if the specified page is a border
 *        page of a linear read-ahead area and all the pages in the area
 *        have been accessed. Does not read any page if the read-ahead
 *        mechanism is not activated.  Note that the algorithm looks at
 *        the 'natural' adjacent successor and predecessor of the page,
 *        which on the leaf level of a B-tree are the next and previous page
 *        in the chain of leaves. To know these, the page specified in
 *        (space, offset) must already be present in the srv_buf_pool. Thus,
 *        the natural way to use this function is to call it when a page
 *        in the srv_buf_pool is accessed the first time, calling this function
 *        just after it has been bufferfixed.
 *   NOTE 1: as this function looks at the natural predecessor and successor
 *        fields on the page, what happens, if these are not initialized
 *        to any sensible value? No problem, before applying read-ahead
 *        we check that the area to read is within the span of the space,
 *        if not, read-ahead is not applied. An uninitialized value may
 *        result in a useless read operation, but only very improbably.
 *   NOTE 2: the calling thread may own latches on pages: to avoid deadlocks
 *        this function must be written such that it cannot end up waiting
 *        for these latches!
 * @param space The space id.
 * @param page_no The page number of a page. NOTE: the current thread must
 *  want access to this page (see NOTE 3 above).
 * @return The number of page read requests issued.
 */
ulint buf_read_ahead_linear(space_id_t space, page_no_t page_no);

/**
 * @brief Issues read requests for pages which recovery wants to read in.
 * 
 * @param sync true if the caller wants this function to wait for the
 *                  highest address page to get read in, before this
 *                  function returns
 * @param space space id
 * @param page_nos array of page numbers to read, with the highest page number the last in the array
 * @param n_stored number of page numbers in the array
 */
void buf_read_recv_pages(bool sync, space_id_t space, const page_no_t *page_nos, ulint n_stored);

/* @} */
