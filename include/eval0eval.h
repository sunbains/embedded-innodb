/**
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

/** @file include/eval0eval.h
SQL evaluator: evaluates simple data structures, like expressions, in
a query graph

Created 12/29/1997 Heikki Tuuri
*******************************************************/

#ifndef eval0eval_h
#define eval0eval_h

#include "innodb0types.h"

#include "pars0pars.h"
#include "pars0sym.h"
#include "que0types.h"

/** Free the buffer from global dynamic memory for a value of a que_node,
if it has been allocated in the above function. The freeing for pushed
column values is done in sel_col_prefetch_buf_free. */

void eval_node_free_val_buf(que_node_t *node); /*!< in: query graph node */
/** Evaluates a symbol table symbol. */
inline void eval_sym(sym_node_t *sym_node); /*!< in: symbol table node */
/** Evaluates an expression. */
inline void eval_exp(que_node_t *exp_node); /*!< in: expression */
/** Sets an integer value as the value of an expression node. */
inline void eval_node_set_int_val(que_node_t *node, /*!< in: expression node */
                                  lint val);        /*!< in: value to set */
/** Gets an integer value from an expression node.
@return	integer value */
inline lint eval_node_get_int_val(que_node_t *node); /*!< in: expression node */
/** Copies a binary string value as the value of a query graph node. Allocates a
new buffer if necessary. */
inline void eval_node_copy_and_alloc_val(
    que_node_t *node, /*!< in: query graph node */
    const byte *str,  /*!< in: binary string */
    ulint len);       /*!< in: string length or UNIV_SQL_NULL */
/** Copies a query node value to another node. */
inline void eval_node_copy_val(que_node_t *node1,  /*!< in: node to copy to */
                               que_node_t *node2); /*!< in: node to copy from */
/** Gets a boolean value from a query node.
@return	boolean value */
inline bool
eval_node_get_bool_val(que_node_t *node); /*!< in: query graph node */
/** Evaluates a comparison node.
@return	the result of the comparison */

bool eval_cmp(func_node_t *cmp_node); /*!< in: comparison node */

#ifndef UNIV_NONINL
#include "eval0eval.ic"
#endif

#endif
