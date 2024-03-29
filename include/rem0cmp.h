/****************************************************************************
Copyright (c) 1994, 2009, Innobase Oy. All Rights Reserved.

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

/** @file include/rem0cmp.h
Comparison services for records

Created 7/1/1994 Heikki Tuuri
************************************************************************/

#pragma once

#include "data0data.h"
#include "data0type.h"
#include "dict0dict.h"
#include "innodb0types.h"
#include "rem0rec.h"

/** Returns true if two columns are equal for comparison purposes.
@return	true if the columns are considered equal in comparisons */

bool cmp_cols_are_equal(
  const dict_col_t *col1, /*!< in: column 1 */
  const dict_col_t *col2, /*!< in: column 2 */
  bool check_charsets
);
/*!< in: whether to check charsets */
/** This function is used to compare two data fields for which we know the
data type.
@return	1, 0, -1, if data1 is greater, equal, less than data2, respectively */
inline int cmp_data_data(
  void *cmp_ctx,     /*!< in: client compare context */
  ulint mtype,       /*!< in: main type */
  ulint prtype,      /*!< in: precise type */
  const byte *data1, /*!< in: data field (== a pointer to a
                                 memory buffer) */
  ulint len1,        /*!< in: data field length or UNIV_SQL_NULL */
  const byte *data2, /*!< in: data field (== a pointer to a
                                 memory buffer) */
  ulint len2
); /*!< in: data field length or UNIV_SQL_NULL */
/** This function is used to compare two data fields for which we know the
data type.
@return	1, 0, -1, if data1 is greater, equal, less than data2, respectively */

int cmp_data_data_slow(
  void *cmp_ctx,     /*!< in: client compare context */
  ulint mtype,       /*!< in: main type */
  ulint prtype,      /*!< in: precise type */
  const byte *data1, /*!< in: data field (== a pointer to a memory
                       buffer) */
  ulint len1,        /*!< in: data field length or UNIV_SQL_NULL */
  const byte *data2, /*!< in: data field (== a pointer to a memory
                       buffer) */
  ulint len2
); /*!< in: data field length or UNIV_SQL_NULL */
/** This function is used to compare two dfields where at least the first
has its data type field set.
@return 1, 0, -1, if dfield1 is greater, equal, less than dfield2,
respectively */
inline int cmp_dfield_dfield(
  void *cmp_ctx,           /*!< in: client compare context */
  const dfield_t *dfield1, /*!< in: data field; must have type field set */
  const dfield_t *dfield2
); /*!< in: data field */
/** This function is used to compare a data tuple to a physical record.
Only dtuple->n_fields_cmp first fields are taken into account for
the data tuple! If we denote by n = n_fields_cmp, then rec must
have either m >= n fields, or it must differ from dtuple in some of
the m fields rec has. If rec has an externally stored field we do not
compare it but return with value 0 if such a comparison should be
made.
@return 1, 0, -1, if dtuple is greater, equal, less than rec,
respectively, when only the common first fields are compared, or until
the first externally stored field in rec */

int cmp_dtuple_rec_with_match(
  void *cmp_ctx,          /*!< in: client compare context */
  const dtuple_t *dtuple, /*!< in: data tuple */
  const rec_t *rec,       /*!< in: physical record which differs from
                            dtuple in some of the common fields, or which
                            has an equal number or more fields than
                            dtuple */
  const ulint *offsets,   /*!< in: array returned by rec_get_offsets() */
  ulint *matched_fields,  /*!< in/out: number of already completely
                    matched fields; when function returns,
                    contains the value for current comparison */
  ulint *matched_bytes
); /*!< in/out: number of already matched
                    bytes within the first field not completely
                    matched; when function returns, contains the
                    value for current comparison */
/** Compares a data tuple to a physical record.
@see cmp_dtuple_rec_with_match
@return 1, 0, -1, if dtuple is greater, equal, less than rec, respectively */

int cmp_dtuple_rec(
  void *cmp_ctx,          /*!< in: client compare context */
  const dtuple_t *dtuple, /*!< in: data tuple */
  const rec_t *rec,       /*!< in: physical record */
  const ulint *offsets
); /*!< in: array returned by rec_get_offsets() */
/** Checks if a dtuple is a prefix of a record. The last field in dtuple
is allowed to be a prefix of the corresponding field in the record.
@return	true if prefix */

bool cmp_dtuple_is_prefix_of_rec(
  void *cmp_ctx,          /*!< in: client compare context */
  const dtuple_t *dtuple, /*!< in: data tuple */
  const rec_t *rec,       /*!< in: physical record */
  const ulint *offsets
); /*!< in: array returned by rec_get_offsets() */
/** Compare two physical records that contain the same number of columns,
none of which are stored externally.
@return	1, 0, -1 if rec1 is greater, equal, less, respectively, than rec2 */

int cmp_rec_rec_simple(
  const rec_t *rec1,     /*!< in: physical record */
  const rec_t *rec2,     /*!< in: physical record */
  const ulint *offsets1, /*!< in: rec_get_offsets(rec1, ...) */
  const ulint *offsets2, /*!< in: rec_get_offsets(rec2, ...) */
  const dict_index_t *index
); /*!< in: data dictionary index */
/** This function is used to compare two physical records. Only the common
first fields are compared, and if an externally stored field is
encountered, then 0 is returned.
@return 1, 0, -1 if rec1 is greater, equal, less, respectively */

int cmp_rec_rec_with_match(
  const rec_t *rec1,     /*!< in: physical record */
  const rec_t *rec2,     /*!< in: physical record */
  const ulint *offsets1, /*!< in: rec_get_offsets(rec1, index) */
  const ulint *offsets2, /*!< in: rec_get_offsets(rec2, index) */
  dict_index_t *index,   /*!< in: data dictionary index */
  ulint *matched_fields, /*!< in/out: number of already completely
                   matched fields; when the function returns,
                   contains the value the for current
                   comparison */
  ulint *matched_bytes
); /*!< in/out: number of already matched
                    bytes within the first field not completely
                    matched; when the function returns, contains
                    the value for the current comparison */
/** This function is used to compare two physical records. Only the common
first fields are compared.
@return 1, 0 , -1 if rec1 is greater, equal, less, respectively, than
rec2; only the common first fields are compared */
inline int cmp_rec_rec(
  const rec_t *rec1,     /*!< in: physical record */
  const rec_t *rec2,     /*!< in: physical record */
  const ulint *offsets1, /*!< in: rec_get_offsets(rec1, index) */
  const ulint *offsets2, /*!< in: rec_get_offsets(rec2, index) */
  dict_index_t *index
); /*!< in: data dictionary index */


/** This function is used to compare two data fields for which we know the
data type.
@return	1, 0, -1, if data1 is greater, equal, less than data2, respectively */
inline int cmp_data_data(
  void *cmp_ctx,     /*!< in: client compare context */
  ulint mtype,       /*!< in: main type */
  ulint prtype,      /*!< in: precise type */
  const byte *data1, /*!< in: data field (== a pointer to a
                                 memory buffer) */
  ulint len1,        /*!< in: data field length or UNIV_SQL_NULL */
  const byte *data2, /*!< in: data field (== a pointer to a
                                 memory buffer) */
  ulint len2
) /*!< in: data field length or UNIV_SQL_NULL */
{
  return (cmp_data_data_slow(cmp_ctx, mtype, prtype, data1, len1, data2, len2));
}

/** This function is used to compare two dfields where at least the first
has its data type field set.
@return 1, 0, -1, if dfield1 is greater, equal, less than dfield2,
respectively */
inline int cmp_dfield_dfield(
  void *cmp_ctx,           /*!< in: client compare context */
  const dfield_t *dfield1, /*!< in: data field; must have type field set */
  const dfield_t *dfield2
) /*!< in: data field */
{
  const dtype_t *type;

  ut_ad(dfield_check_typed(dfield1));

  type = dfield_get_type(dfield1);

  return (cmp_data_data(
    cmp_ctx,
    type->mtype,
    type->prtype,
    (const byte *)dfield_get_data(dfield1),
    dfield_get_len(dfield1),
    (const byte *)dfield_get_data(dfield2),
    dfield_get_len(dfield2)
  ));
}

/** This function is used to compare two physical records. Only the common
first fields are compared.
@return 1, 0 , -1 if rec1 is greater, equal, less, respectively, than
rec2; only the common first fields are compared */
inline int cmp_rec_rec(
  const rec_t *rec1,     /*!< in: physical record */
  const rec_t *rec2,     /*!< in: physical record */
  const ulint *offsets1, /*!< in: rec_get_offsets(rec1, index) */
  const ulint *offsets2, /*!< in: rec_get_offsets(rec2, index) */
  dict_index_t *dict_index
) /*!< in: data dictionary index */
{
  ulint match_f = 0;
  ulint match_b = 0;

  return (cmp_rec_rec_with_match(rec1, rec2, offsets1, offsets2, dict_index, &match_f, &match_b));
}
