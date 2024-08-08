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

/**
 * @brief Compares two columns for equality.
 *
 * This function compares the given columns, `col1` and `col2`, and checks if they are equal.
 * The comparison can optionally take into account the character sets of the columns if `check_charsets` is set to true.
 *
 * @param[in] col1 Pointer to the first column.
 * @param[in] col2 Pointer to the second column.
 * @param[in] check_charsets Flag indicating whether to check the character sets of the columns.
 * 
 * @return True if the columns are equal, false otherwise.
 */
bool cmp_cols_are_equal(const dict_col_t *col1, const dict_col_t *col2, bool check_charsets) noexcept;

/**
 * Compares two data fields.
 *
 * @param[in] cmp_ctx   Client compare context.
 * @param[in] mtype     Main type.
 * @param[in] prtype    Precise type.
 * @param[in] data1     Pointer to the first data field.
 * @param[in] len1      Length of the first data field or UNIV_SQL_NULL.
 * @param[in] data2     Pointer to the second data field.
 * @param[in] len2      Length of the second data field or UNIV_SQL_NULL.
 * 
 * @return	1, 0, -1, if data1 is greater, equal, less than data2, respectively
 */
inline int cmp_data_data(
  void *cmp_ctx,
  ulint mtype,
  ulint prtype,
  const byte *data1,
  ulint len1,
  const byte *data2,
  ulint len2
) noexcept ;

/**
 * This function is used to compare two data fields for which we know the
 * data type.
 * 
 * @param[in] cmp_ctx   Client compare context.
 * @param[in] mtype     Main type.
 * @param[in] prtype    Precise type.
 * @param[in] data1     Pointer to the first data field.
 * @param[in] len1      Length of the first data field or UNIV_SQL_NULL.
 * @param[in] data2     Pointer to the second data field.
 * @param[in] len2      Length of the second data field or UNIV_SQL_NULL.
 * 
 * @return	1, 0, -1, if data1 is greater, equal, less than data2, respectively
 */
int cmp_data_data_slow(
  void *cmp_ctx,
  ulint mtype,
  ulint prtype,
  const byte *data1,
  ulint len1,
  const byte *data2,
  ulint len2
) noexcept;

/**
 * This function is used to compare two dfields where at least the first
 * has its data type field set.
 * 
 * @param[in] cmp_ctx   Client compare context.
 * @param[in] dfield1   Pointer to the first data field.
 * @param[in] dfield2   Pointer to the second data field.
 * 
 * @return 1, 0, -1, if dfield1 is greater, equal, less than dfield2, respectively
 */
inline int cmp_dfield_dfield(void *cmp_ctx, const dfield_t *dfield1, const dfield_t *dfield2) noexcept;

/**
 * This function is used to compare a data tuple to a physical record.
 * Only dtuple->n_fields_cmp first fields are taken into account for
 * the data tuple! If we denote by n = n_fields_cmp, then rec must
 * have either m >= n fields, or it must differ from dtuple in some of
 * the m fields rec has. If rec has an externally stored field we do not
 * compare it but return with value 0 if such a comparison should be
 * made.
 * 
 * @param[in] cmp_ctx          Client compare context.
 * @param[in] dtuple           Pointer to the data tuple.
 * @param[in] rec              Pointer to the physical record which differs
 *                             from dtuple in some of the common fields,
 *                             or which has an equal number or more fields
 *                             than dtuple.
 * @param[in] offsets          Array returned by Phy_rec::get_col_offsets().
 * @param[in] matched_fields   Number of already completely matched fields
 *                             when the function returns., contains the value
 *                             for current comparison.
 * @param[in] matched_bytes    Number of already matched bytes within the first
 *                             field not completely matched when the function returns,
 *                             contains the value for current comparison.
 * 
 * @return 1, 0, -1, if dtuple is greater, equal, less than rec,
 *    respectively, when only the common first fields are compared, or until
 *    the first externally stored field in rec
 */
int cmp_dtuple_rec_with_match(
  void *cmp_ctx,
  const dtuple_t *dtuple,
  const rec_t *rec,
  const ulint *offsets,
  ulint *matched_fields,
  ulint *matched_bytes
) noexcept;

/**
 * Compares a data tuple to a physical record.
 * @see cmp_dtuple_rec_with_match
 * 
 * @param[in] cmp_ctx          Client compare context.
 * @param[in] dtuple           Pointer to the data tuple.
 * @param[in] rec              Pointer to the physical record which differs
 * @param[in] offsets          Array returned by Phy_rec::get_col_offsets().
 * 
 * @return 1, 0, -1, if dtuple is greater, equal, less than rec, respectively
 */
int cmp_dtuple_rec(
  void *cmp_ctx,
  const dtuple_t *dtuple,
  const rec_t *rec,
  const ulint *offsets
) noexcept;

/**
 * Checks if a dtuple is a prefix of a record. The last field in dtuple
 * is allowed to be a prefix of the corresponding field in the record.
 * 
 * @param[in] cmp_ctx          Client compare context.
 * @param[in] dtuple           Pointer to the data tuple.
 * @param[in] rec              Pointer to the physical record.
 * @param[in] offsets          Array returned by Phy_rec::get_col_offsets().
 * 
 * @return	true if prefix
 */
bool cmp_dtuple_is_prefix_of_rec(
  void *cmp_ctx,
  const dtuple_t *dtuple,
  const rec_t *rec,
  const ulint *offsets
) noexcept;

/**
 * Compare two physical records that contain the same number of columns,
 * none of which are stored externally.
 * 
 * @param[in] rec1             Pointer to the first physical record.
 * @param[in] rec2             Pointer to the second physical record.
 * @param[in] offsets1         Array returned by Phy_rec::get_col_offsets().
 * @param[in] offsets2         Array returned by Phy_rec::get_col_offsets().
 * @param[in] index            Data dictionary index.
 * 
 * @return	1, 0, -1 if rec1 is greater, equal, less, respectively, than rec2
 */
int cmp_rec_rec_simple(
  const rec_t *rec1,
  const rec_t *rec2,
  const ulint *offsets1,
  const ulint *offsets2,
  const dict_index_t *index
) noexcept;

/**
 * This function is used to compare two physical records. Only the common
 * first fields are compared, and if an externally stored field is
 * encountered, then 0 is returned.
 * 
 * @param[in] rec1             Pointer to the first physical record.
 * @param[in] rec2             Pointer to the second physical record.
 * @param[in] offsets1         Array returned by Phy_rec::get_col_offsets().
 * @param[in] offsets2         Array returned by Phy_rec::get_col_offsets().
 * @param[in] index            Data dictionary index.
 * @param[out] matched_fields  Number of already completely matched fields when
 *                             the function returns, contains the value for
 *                             current comparison.
 * @param[out] matched_bytes   Number of already matched bytes within the first
 *                             field not completely matched when the function returns,
 *                             contains the value for current comparison.
 * 
 * @return 1, 0, -1 if rec1 is greater, equal, less, respectively
 */
int cmp_rec_rec_with_match(
  const rec_t *rec1,
  const rec_t *rec2,
  const ulint *offsets1,
  const ulint *offsets2,
  dict_index_t *index,
  ulint *matched_fields,
  ulint *matched_bytes
) noexcept;

/**
 * This function is used to compare two physical records. Only the common
 * first fields are compared.
 * 
 * @param[in] rec1             Pointer to the first physical record.
 * @param[in] rec2             Pointer to the second physical record.
 * @param[in] offsets1         Array returned by Phy_rec::get_col_offsets().
 * @param[in] offsets2         Array returned by Phy_rec::get_col_offsets().
 * @param[in] index            Data dictionary index.
 * 
 * @return 1, 0 , -1 if rec1 is greater, equal, less, respectively, than
 *  rec2; only the common first fields are compared
 */
inline int cmp_rec_rec(
  const rec_t *rec1,
  const rec_t *rec2,
  const ulint *offsets1,
  const ulint *offsets2,
  dict_index_t *index
) noexcept;

/**
 * This function is used to compare two data fields for which we know the
 * data type.
 * 
 * @param[in] cmp_ctx   Client compare context.
 * @param[in] mtype     Main type.
 * @param[in] prtype    Precise type.
 * @param[in] data1     Pointer to the first data field.
 * @param[in] len1      Length of the first data field or UNIV_SQL_NULL.
 * @param[in] data2     Pointer to the second data field.
 * @param[in] len2      Length of the second data field or UNIV_SQL_NULL.
 * 
 * @return	1, 0, -1, if data1 is greater, equal, less than data2, respectively
 */
inline int cmp_data_data(
  void *cmp_ctx,
  ulint mtype,
  ulint prtype,
  const byte *data1,
  ulint len1,
  const byte *data2,
  ulint len2
) noexcept {
  return cmp_data_data_slow(cmp_ctx, mtype, prtype, data1, len1, data2, len2);
}

/**
 * This function is used to compare two dfields where at least the first
 * has its data type field set.
 * 
 * @param[in] cmp_ctx   Client compare context.
 * @param[in] dfield1   Pointer to the first data field.
 * @param[in] dfield2   Pointer to the second data field.
 * 
 * @return 1, 0, -1, if dfield1 is greater, equal, less than dfield2, respectively
 */
inline int cmp_dfield_dfield(void *cmp_ctx, const dfield_t *dfield1, const dfield_t *dfield2) noexcept {
  ut_ad(dfield_check_typed(dfield1));

  const auto type = dfield_get_type(dfield1);

  return cmp_data_data(
    cmp_ctx,
    type->mtype,
    type->prtype,
    static_cast<const byte*>(dfield_get_data(dfield1)),
    dfield_get_len(dfield1),
    static_cast<const byte*>(dfield_get_data(dfield2)),
    dfield_get_len(dfield2)
  );
}

/**
 * This function is used to compare two physical records. Only the common
 * first fields are compared.
 * 
 * @param[in] rec1             Pointer to the first physical record.
 * @param[in] rec2             Pointer to the second physical record.
 * @param[in] offsets1         Array returned by Phy_rec::get_col_offsets().
 * @param[in] offsets2         Array returned by Phy_rec::get_col_offsets().
 * @param[in] index            Data dictionary index.
 * 
 * @return 1, 0 , -1 if rec1 is greater, equal, less, respectively, than
 *    rec2; only the common first fields are compared
 */
inline int cmp_rec_rec(
  const rec_t *rec1,
  const rec_t *rec2,
  const ulint *offsets1,
  const ulint *offsets2,
  dict_index_t *dict_index
) noexcept {
  ulint match_f{};
  ulint match_b{};

  return cmp_rec_rec_with_match(rec1, rec2, offsets1, offsets2, dict_index, &match_f, &match_b);
}
