/****************************************************************************
Copyright (c) 2006, 2010, Innobase Oy. All Rights Reserved.

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

/** @file include/ut0vec.h
A vector of pointers to data items

Created 4/6/2006 Osku Salerma
************************************************************************/

#pragma once

#include "innodb0types.h"

#include "mem0mem.h"

/** An automatically resizing vector data type. */
typedef struct ib_vector_struct ib_vector_t;

/* An automatically resizing vector datatype with the following properties:

 -Contains void* items.

 -The items are owned by the caller.

 -All memory allocation is done through a heap owned by the caller, who is
 responsible for freeing it when done with the vector.

 -When the vector is resized, the old memory area is left allocated since it
 uses the same heap as the new memory area, so this is best used for
 relatively small or short-lived uses.
*/

/** Create a new vector with the given initial size.
@return	vector */
ib_vector_t *ib_vector_create(
  mem_heap_t *heap, /*!< in: heap */
  ulint size
); /*!< in: initial size */

/** Push a new element to the vector, increasing its size if necessary. */
void ib_vector_push(
  ib_vector_t *vec, /*!< in: vector */
  void *elem
); /*!< in: data element */

/** An automatically resizing vector data type. */
struct ib_vector_struct {
  mem_heap_t *heap; /*!< heap */
  void **data;      /*!< data elements */
  ulint used;       /*!< number of elements currently used */
  ulint total;      /*!< number of elements allocated */
};

/** Get number of elements in vector.
@return	number of elements in vector */
inline ulint ib_vector_size(const ib_vector_t *vec) /*!< in: vector */
{
  return (vec->used);
}

/** Get n'th element.
@return	n'th element */
inline void *ib_vector_get(
  ib_vector_t *vec, /*!< in: vector */
  ulint n
) /*!< in: element index to get */
{
  ut_a(n < ib_vector_size(vec));

  return (vec->data[n]);
}

/** Get n'th element as a const pointer.
@return	n'th element */
inline const void *ib_vector_get_const(
  const ib_vector_t *vec, /*!< in: vector */
  ulint n
) /*!< in: element index to get */
{
  ut_a(n < ib_vector_size(vec));

  return (vec->data[n]);
}

/** Set n'th element and return the previous value.
@return	n'th element */
inline void *ib_vector_set(
  ib_vector_t *vec, /*!< in: vector */
  ulint n,          /*!< in: element index to set */
  void *p
) /*!< in: new value to set */
{
  void *prev;

  ut_a(n < ib_vector_size(vec));

  prev = vec->data[n];
  vec->data[n] = p;

  return (prev);
}

/** Remove the last element from the vector.
@return	last vector element */
inline void *ib_vector_pop(ib_vector_t *vec) /*!< in/out: vector */
{
  void *elem;

  ut_a(vec->used > 0);
  --vec->used;
  elem = vec->data[vec->used];

  ut_d(vec->data[vec->used] = nullptr);
  UNIV_MEM_INVALID(&vec->data[vec->used], sizeof(*vec->data));

  return (elem);
}

/** Free the underlying heap of the vector. Note that vec is invalid
after this call. */
inline void ib_vector_free(ib_vector_t *vec) /*!< in, own: vector */
{
  mem_heap_free(vec->heap);
}

/** Test whether a vector is empty or not.
@return	true if empty */
inline bool ib_vector_is_empty(const ib_vector_t *vec) /*!< in: vector */
{
  return (ib_vector_size(vec) == 0);
}
