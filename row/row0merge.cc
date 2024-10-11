/****************************************************************************
Copyright (c) 2005, 2010, Innobase Oy. All Rights Reserved.
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

/** @file row/row0merge.c
New index creation routines using a merge sort

Created 12/4/2005 Jan Lindstrom
Completed by Sunny Bains and Marko Makela
*******************************************************/

#include "row0merge.h"
#include "api0misc.h"
#include "btr0btr.h"
#include "btr0blob.h"
#include "data0data.h"
#include "data0type.h"
#include "ddl0ddl.h"
#include "dict0store.h"
#include "dict0dict.h"
#include "dict0load.h"
#include "lock0lock.h"
#include "log0log.h"
#include "mach0data.h"
#include "mem0mem.h"
#include "os0file.h"
#include "os0proc.h"
#include "pars0pars.h"
#include "read0read.h"
#include "rem0cmp.h"
#include "row0ext.h"
#include "row0ins.h"
#include "row0row.h"
#include "row0sel.h"
#include "row0upd.h"
#include "trx0purge.h"
#include "trx0rec.h"
#include "trx0roll.h"
#include "trx0rseg.h"
#include "trx0trx.h"
#include "trx0undo.h"
#include "ut0sort.h"

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif

#ifdef UNIV_DEBUG
/** Set these in order ot enable debug printout. */
/* @{ */
/** Log the outcome of each row_merge_cmp() call, comparing records. */
static bool row_merge_print_cmp;
/** Log each record write to temporary file. */
static bool row_merge_print_write;
/** Log each row_merge_blocks() call, merging two blocks of records to
a bigger one. */
static bool row_merge_print_block;
/* @} */
#endif /* UNIV_DEBUG */

/** @brief Block size for I/O operations in merge sort.

The minimum is UNIV_PAGE_SIZE, or page_get_free_space_of_empty()
rounded to a power of 2.

When not creating a PRIMARY KEY that contains column prefixes, this
can be set as small as UNIV_PAGE_SIZE / 2.  See the comment above
ut_ad(data_size < sizeof(row_merge_block_t)). */
typedef byte row_merge_block_t[1048576];

/** @brief Secondary buffer for I/O operations of merge records.

This buffer is used for writing or reading a record that spans two
row_merge_block_t.  Thus, it must be able to hold one merge record,
whose maximum size is the same as the minimum size of
row_merge_block_t. */
typedef byte mrec_buf_t[UNIV_PAGE_SIZE];

/** @brief Merge record in row_merge_block_t.

The format is the same as a record in ROW_FORMAT=COMPACT with the
exception that the REC_N_NEW_EXTRA_BYTES are omitted. */
typedef byte mrec_t;

/** Buffer for sorting in main memory. */
struct row_merge_buf_struct {
  /** memory heap where allocated */
  mem_heap_t *heap;

  /** the index the rows belong to */
  Index *index;

  /** total amount of data bytes */
  ulint total_size;

  /** number of data rows */
  ulint n_recs;

  /** maximum number of data rows */
  ulint max_tuples;

  /** array of pointers to arrays of fields that form the data rows */
  const dfield_t **rows;

  /** temporary copy of rows, for sorting */
  const dfield_t **tmp_tuples;
};

/** Buffer for sorting in main memory. */
typedef struct row_merge_buf_struct row_merge_buf_t;

/** Information about temporary files used in merge sort */
struct merge_file_struct {
  /** file descriptor */
  int fd;
  /** file offset (end of file) */
  ulint offset;
  /** number of records in the file */
  uint64_t n_rec;
};

/** Information about temporary files used in merge sort */
typedef struct merge_file_struct merge_file_t;

/**
 * @brief Allocate a sort buffer.
 * 
 * @param[in] heap Memory heap where allocated.
 * @param[in] index Secondary index.
 * @param[in] max_tuples Maximum number of data rows.
 * @param[in] buf_size Size of the buffer, in bytes.
 * 
 * @return Pointer to the allocated sort buffer.
 */
static row_merge_buf_t *row_merge_buf_create_low(mem_heap_t *heap, Index *index, ulint max_tuples, ulint buf_size) {
  ut_ad(max_tuples > 0);
  ut_ad(max_tuples <= sizeof(row_merge_block_t));
  ut_ad(max_tuples < buf_size);

  auto buf = reinterpret_cast<row_merge_buf_t *>(mem_heap_zalloc(heap, buf_size));

  buf->heap = heap;
  buf->index = index;
  buf->max_tuples = max_tuples;
  buf->rows = reinterpret_cast<const dfield_t **>(mem_heap_alloc(heap, 2 * max_tuples * sizeof *buf->rows));
  buf->tmp_tuples = buf->rows + max_tuples;

  return buf;
}

/**
 * @brief Allocate a sort buffer.
 * 
 * @param[in] index Secondary index.
 * 
 * @return Pointer to the allocated sort buffer.
 */
static row_merge_buf_t *row_merge_buf_create(Index *index) noexcept {
  auto max_tuples = sizeof(row_merge_block_t) / ut_max(1, index->get_min_size());
  auto buf_size = (sizeof(row_merge_buf_t)) + (max_tuples - 1) * sizeof row_merge_buf_t::rows;
  auto heap = static_cast<mem_heap_t *>(mem_heap_create(buf_size + sizeof(row_merge_block_t)));
  auto row_merge_buf = row_merge_buf_create_low(heap, index, max_tuples, buf_size);

  return row_merge_buf;
}

/**
 * @brief Empty a sort buffer.
 * 
 * @param[in] buf The sort buffer to empty.
 * 
 * @return Pointer to the emptied sort buffer.
 */
static row_merge_buf_t *row_merge_buf_empty(row_merge_buf_t *buf) {
  ulint buf_size;
  ulint max_tuples = buf->max_tuples;
  mem_heap_t *heap = buf->heap;
  auto index = buf->index;

  buf_size = (sizeof *buf) + (max_tuples - 1) * sizeof *buf->rows;

  mem_heap_empty(heap);

  return (row_merge_buf_create_low(heap, index, max_tuples, buf_size));
}

/** Deallocate a sort buffer. */
static void row_merge_buf_free(row_merge_buf_t *buf) /*!< in,own: sort buffer, to be freed */
{
  mem_heap_free(buf->heap);
}

/**
 *  Insert a data tuple into a sort buffer.
 *
 * @param buf[in,out] The sort buffer to add the row to.
 * @param row[in] The row in the clustered index to add.
 * @param ext[in] Cache of externally stored column prefixes, or NULL.
 * 
 * @return True if the row was successfully added, false if the sort buffer is full.
 */
static bool row_merge_buf_add(row_merge_buf_t *buf, const DTuple *row, const row_ext_t *ext) {
  if (buf->n_recs >= buf->max_tuples) {
    return false;
  }

  prefetch_r(row->fields);

  auto index = buf->index;
  const auto n_fields = index->get_n_fields();
  auto entry = reinterpret_cast<dfield_t *>(mem_heap_alloc(buf->heap, n_fields * sizeof(dfield_t)));
  auto dfield = entry;

  buf->rows[buf->n_recs] = entry;

  ulint data_size{};
  ulint extra_size = UT_BITS_IN_BYTES(index->m_n_nullable);

  for (ulint i{}; i < n_fields; ++i, ++dfield) {
    auto ifield = index->get_nth_field(i);
    auto col = ifield->m_col;
    auto col_no = col->get_no();
    auto row_field = dtuple_get_nth_field(row, col_no);

    dfield_copy(dfield, row_field);

    auto len = dfield_get_len(dfield);

    if (dfield_is_null(dfield)) {
      ut_ad(!(col->prtype & DATA_NOT_NULL));
      continue;
    } else if (likely(!ext)) {
      ;
    } else if (index->is_clustered()) {
      /* Flag externally stored fields. */
      const byte *buf = row_ext_lookup(ext, col_no, &len);

      if (likely_null(buf)) {

        ut_a(buf != field_ref_zero);

        if (i < index->get_n_unique()) {
          dfield_set_data(dfield, buf, len);
        } else {
          dfield_set_ext(dfield);
          len = dfield_get_len(dfield);
        }
      }

    } else {

      const byte *buf = row_ext_lookup(ext, col_no, &len);

      if (likely_null(buf)) {
        ut_a(buf != field_ref_zero);
        dfield_set_data(dfield, buf, len);
      }
    }

    /* If a column prefix index, take only the prefix */

    if (ifield->m_prefix_len) {
      len = dtype_get_at_most_n_mbchars(
        col->prtype, col->mbminlen, col->mbmaxlen, ifield->m_prefix_len, len, (char *)dfield_get_data(dfield)
      );

      dfield_set_len(dfield, len);
    }

    ut_ad(len <= col->len || col->mtype == DATA_BLOB);

    if (ifield->m_fixed_len) {
      ut_ad(len == ifield->m_fixed_len);
      ut_ad(!dfield_is_ext(dfield));
    } else if (dfield_is_ext(dfield)) {
      extra_size += 2;
    } else if (len < 128 || (col->len < 256 && col->mtype != DATA_BLOB)) {
      ++extra_size;
    } else {
      /* For variable-length columns, we look up the
      maximum length from the column itself.  If this
      is a prefix index column shorter than 256 bytes,
      this will waste one byte. */
      extra_size += 2;
    }
    data_size += len;
  }

#ifdef UNIV_DEBUG
  {
    auto size = Phy_rec::get_encoded_size(index, REC_STATUS_ORDINARY, {entry, n_fields});

    ut_ad(extra_size == size.first);
    ut_ad(data_size == size.second);
  }
#endif /* UNIV_DEBUG */

  /* Add to the total size of the record in row_merge_block_t
  the encoded length of extra_size and the extra bytes (extra_size).
  See row_merge_buf_write() for the variable-length encoding
  of extra_size. */
  data_size += (extra_size + 1) + ((extra_size + 1) >= 0x80);

  ut_ad(data_size < sizeof(row_merge_block_t));

  /* Reserve one byte for the end marker of row_merge_block_t. */
  if (buf->total_size + data_size >= sizeof(row_merge_block_t) - 1) {
    return false;
  }

  buf->total_size += data_size;
  ++buf->n_recs;

  dfield = entry;

  /* Copy the data fields. */

  {
    auto i{n_fields};

    do {
      dfield_dup(dfield++, buf->heap);
    } while (--i);
  }

  return true;
}

/** Structure for reporting duplicate records. */
struct row_merge_dup_t {
  const Index *index; /*!< index being sorted */
  table_handle_t table;      /*!< table object */
  ulint n_dup;               /*!< number of duplicates */
};

/** Report a duplicate key. */
static void row_merge_dup_report(
  row_merge_dup_t *dup, /*!< in/out: for reporting duplicates */
  const dfield_t *entry
) /*!< in: duplicate index entry */
{
  if (dup->n_dup++ > 0) {
    /* Only report the first duplicate record, but count all duplicate records. */
    return;
  }

  mrec_buf_t buf;
  DTuple tuple_store;
  const auto index = dup->index;
  const auto n_fields = index->get_n_fields();

  /* Convert the tuple to a record and then to client format. */

  auto tuple = dtuple_from_fields(&tuple_store, entry, n_fields);
  auto n_ext = index->is_clustered() ? dtuple_get_n_ext(tuple) : 0;

  rec_convert_dtuple_to_rec(buf, index, tuple, n_ext);
}

/** Compare two rows.
@return	1, 0, -1 if a is greater, equal, less, respectively, than b */
static int row_merge_tuple_cmp(
  void *cmp_ctx,     /*!< in: compare context, required
                          for BLOBs and user defined types */
  ulint n_field,     /*!< in: number of fields */
  const dfield_t *a, /*!< in: first tuple to be compared */
  const dfield_t *b, /*!< in: second tuple to be compared */
  row_merge_dup_t *dup
) /*!< in/out: for reporting duplicates */
{
  int cmp;
  const dfield_t *field = a;

  /* Compare the fields of the rows until a difference is
  found or we run out of fields to compare.  If !cmp at the
  end, the rows are equal. */
  do {
    cmp = cmp_dfield_dfield(cmp_ctx, a++, b++);
  } while (!cmp && --n_field);

  if (unlikely(!cmp) && likely_null(dup)) {
    /* Report a duplicate value error if the rows are
    logically equal.  NULL columns are logically inequal,
    although they are equal in the sorting order.  Find
    out if any of the fields are NULL. */
    for (b = field; b != a; b++) {
      if (dfield_is_null(b)) {

        return cmp;
      }
    }

    row_merge_dup_report(dup, field);
  }

  return cmp;
}

/** Wrapper for row_merge_tuple_sort() to inject some more context to
UT_SORT_FUNCTION_BODY().
@param a	array of rows that being sorted
@param b	aux (work area), same size as rows[]
@param c	lower bound of the sorting area, inclusive
@param d	upper bound of the sorting area, inclusive */
#define row_merge_tuple_sort_ctx(x, a, b, c, d) row_merge_tuple_sort(x, n_field, dup, a, b, c, d)
/** Wrapper for row_merge_tuple_cmp() to inject some more context to
UT_SORT_FUNCTION_BODY().
@param a	first tuple to be compared
@param b	second tuple to be compared
@return	1, 0, -1 if a is greater, equal, less, respectively, than b */
#define row_merge_tuple_cmp_ctx(x, a, b) row_merge_tuple_cmp(x, n_field, a, b, dup)

/** Merge sort the tuple buffer in main memory. */
static void row_merge_tuple_sort(
  void *cmp_ctx,           /*!< in: compare context, required
                             for BLOBs and user defined types */
  ulint n_field,           /*!< in: number of fields */
  row_merge_dup_t *dup,    /*!< in/out: for reporting duplicates */
  const dfield_t **rows, /*!< in/out: rows */
  const dfield_t **aux,    /*!< in/out: work area */
  ulint low,               /*!< in: lower bound of the
                             sorting area, inclusive */
  ulint high
) /*!< in: upper bound of the
                             sorting area, exclusive */
{
  UT_SORT_FUNCTION_BODY(cmp_ctx, row_merge_tuple_sort_ctx, rows, aux, low, high, row_merge_tuple_cmp_ctx);
}

/** Sort a buffer. */
static void row_merge_buf_sort(
  row_merge_buf_t *buf, /*!< in/out: sort buffer */
  row_merge_dup_t *dup
) /*!< in/out: for reporting duplicates */
{
  row_merge_tuple_sort(buf->index->m_cmp_ctx, buf->index->get_n_unique(), dup, buf->rows, buf->tmp_tuples, 0, buf->n_recs);
}

/** Write a buffer to a block. */
static void row_merge_buf_write(
  const row_merge_buf_t *buf, /*!< in: sorted buffer */
#ifdef UNIV_DEBUG
  const merge_file_t *of, /*!< in: output file */
#endif                    /* UNIV_DEBUG */
  row_merge_block_t *block
) /*!< out: buffer for writing to file */
#ifndef UNIV_DEBUG
#define row_merge_buf_write(buf, of, block) row_merge_buf_write(buf, block)
#endif /* !UNIV_DEBUG */
{
  auto b = &(*block)[0];
  const auto index = buf->index;
  const auto n_fields = index->get_n_fields();

  for (ulint i{}; i < buf->n_recs; ++i) {
    const auto dfield = buf->rows[i];
    auto size = Phy_rec::get_encoded_size(index, REC_STATUS_ORDINARY, {dfield, n_fields});
    auto total_size = size.first + size.second;

    /* Encode extra_size + 1 */
    if (size.first + 1 < 0x80) {
      *b++ = (byte)(size.first + 1);
    } else {
      ut_ad((size.first + 1) < 0x8000);
      *b++ = (byte)(0x80 | ((size.first + 1) >> 8));
      *b++ = (byte)(size.first + 1);
    }

    ut_ad(b + total_size < block[1]);

    /* Encode into b + size.first */
    Phy_rec::encode(index, b + size.first, REC_STATUS_ORDINARY, {dfield, n_fields});

    b += total_size;
  }

  /* Write an "end-of-chunk" marker. */
  ut_a(b < block[1]);
  ut_a(b == block[0] + buf->total_size);

  *b++ = 0;

#ifdef UNIV_DEBUG_VALGRIND
  /* The rest of the block is uninitialized. Initialize it to avoid bogus warnings. */
  memset(b, 0xff, block[1] - b);
#endif /* UNIV_DEBUG_VALGRIND */
}

/** Create a memory heap and allocate space for row_merge_rec_offsets().
@return	memory heap */
static mem_heap_t *row_merge_heap_create(
  const Index *index, /*!< in: record descriptor */
  ulint **offsets1,          /*!< out: offsets */
  ulint **offsets2
) /*!< out: offsets */
{
  ulint i = 1 + REC_OFFS_HEADER_SIZE + index->get_n_fields();
  mem_heap_t *heap = mem_heap_create(2 * i * sizeof *offsets1);

  *offsets1 = reinterpret_cast<ulint *>(mem_heap_alloc(heap, i * sizeof(*offsets1)));
  *offsets2 = reinterpret_cast<ulint *>(mem_heap_alloc(heap, i * sizeof(*offsets2)));

  (*offsets1)[0] = (*offsets2)[0] = i;
  (*offsets1)[1] = (*offsets2)[1] = index->get_n_fields();

  return (heap);
}

/** Search an index object by name and column names.  If several indexes match,
return the index with the max id.
@return	matching index, NULL if not found */
static Index *row_merge_dict_table_get_index(
  Table *table, /*!< in: table */
  const merge_index_def_t *index_def
) /*!< in: index definition */
{
  ulint i;
  Index *index;
  auto column_names = static_cast<const char **>(mem_alloc(index_def->n_fields * sizeof(char *)));

  for (i = 0; i < index_def->n_fields; ++i) {
    column_names[i] = index_def->fields[i].field_name;
  }

  index = table->get_index_by_max_id(index_def->name, column_names, index_def->n_fields);

  mem_free((void *)column_names);

  return (index);
}

/** Read a merge block from the file system.
@return	true if request was successful, false if fail */
static bool row_merge_read(
  int fd,       /*!< in: file descriptor */
  ulint offset, /*!< in: offset where to read */
  row_merge_block_t *buf
) /*!< out: data */
{
  off_t off = ((off_t)offset) * sizeof(*buf);

  auto success = os_file_read_no_error_handling(OS_FILE_FROM_FD(fd), buf, sizeof(*buf), off);

  if (!success) {
    ut_print_timestamp(ib_stream);
    ib_logger(ib_stream, "  failed to read merge block at %lu\n", off);
  }

  return success;
}

/** Read a merge block from the file system.
@return	true if request was successful, false if fail */
static bool row_merge_write(
  int fd,       /*!< in: file descriptor */
  ulint offset, /*!< in: offset where to write */
  const void *buf
) /*!< in: data */
{
  off_t off = off_t(offset) * sizeof(row_merge_block_t);

  return os_file_write("(merge)", OS_FILE_FROM_FD(fd), buf, sizeof(row_merge_block_t), off);
}

/**
 * Reads a record from the specified file buffer and returns a pointer to the merge record.
 * 
 * @param block The file buffer.
 * @param buf The secondary buffer.
 * @param b Pointer to the record.
 * @param index Index of the record.
 * @param fd File descriptor.
 * @param foffs File offset.
 * @param mrec Pointer to the merge record.
 * @param offsets Column offsets of the merge record.
 * @return Pointer to the next record, or nullptr on end of list (non-NULL on I/O error).
 */
static const byte *row_merge_read_rec(
  row_merge_block_t *block,
  mrec_buf_t *buf,
  const byte *b,
  const Index *index,
  int fd,
  ulint *foffs,
  const mrec_t **mrec,
  ulint *offsets
) {
  ulint extra_size;
  ulint avail_size;

  ut_ad(b >= block[0]);
  ut_ad(b < block[1]);

  ut_ad(*offsets == 1 + REC_OFFS_HEADER_SIZE + index->get_n_fields());

  extra_size = *b++;

  if (unlikely(extra_size == 0)) {
    /* End of list */
    *mrec = nullptr;
    return nullptr;
  }

  if (extra_size >= 0x80) {
    /* Read another byte of extra_size. */

    if (unlikely(b >= block[1])) {
      if (!row_merge_read(fd, ++(*foffs), block)) {
      err_exit:
        /* Signal I/O error. */
        *mrec = b;
        return nullptr;
      }

      /* Wrap around to the beginning of the buffer. */
      b = block[0];
    }

    extra_size = (extra_size & 0x7f) << 8;
    extra_size |= *b;
    ++b;
  }

  /* Normalize extra_size.  Above, value 0 signals "end of list". */
  extra_size--;

  /* Read the extra bytes. */

  if (unlikely(b + extra_size >= block[1])) {
    /* The record spans two blocks.  Copy the entire record
    to the auxiliary buffer and handle this as a special
    case. */

    avail_size = block[1] - b;

    memcpy(*buf, b, avail_size);

    if (!row_merge_read(fd, ++(*foffs), block)) {

      goto err_exit;
    }

    /* Wrap around to the beginning of the buffer. */
    b = block[0];

    /* Copy the record. */
    memcpy(*buf + avail_size, b, extra_size - avail_size);
    b += extra_size - avail_size;

    *mrec = *buf + extra_size;

    {
      Phy_rec rec{index, *mrec};
      rec.get_col_offsets(offsets);
    }

    auto data_size = rec_offs_data_size(offsets);

    /* These overflows should be impossible given that
    records are much smaller than either buffer, and
    the record starts near the beginning of each buffer. */
    ut_a(extra_size + data_size < sizeof(*buf));
    ut_a(b + data_size < block[1]);

    /* Copy the data bytes. */
    memcpy(*buf + extra_size, b, data_size);
    b += data_size;

    return b;
  }

  *mrec = b + extra_size;

  {
    Phy_rec rec{index, *mrec};
    rec.get_col_offsets(offsets);
  }

  auto data_size = rec_offs_data_size(offsets);
  ut_ad(extra_size + data_size < sizeof *buf);

  b += extra_size + data_size;

  if (likely(b < block[1])) {
    /* The record fits entirely in the block.  This is the normal case. */
    return b;
  }

  /* The record spans two blocks.  Copy it to buf. */

  b -= extra_size + data_size;
  avail_size = block[1] - b;
  memcpy(*buf, b, avail_size);
  *mrec = *buf + extra_size;

#ifdef UNIV_DEBUG
  /* We cannot invoke rec_offs_make_valid() here, because there
  are no REC_N_NEW_EXTRA_BYTES between extra_size and data_size.
  Similarly, rec_offs_validate() would fail, because it invokes
  rec_get_status(). */
  offsets[2] = (ulint)*mrec;
  offsets[3] = (ulint)index;
#endif /* UNIV_DEBUG */

  if (!row_merge_read(fd, ++(*foffs), block)) {

    goto err_exit;
  }

  /* Wrap around to the beginning of the buffer. */
  b = block[0];

  /* Copy the rest of the record. */
  memcpy(*buf + avail_size, b, extra_size + data_size - avail_size);
  b += extra_size + data_size - avail_size;

  return b;
}

/** Write a merge record. */
static void row_merge_write_rec_low(
  byte *b, /*!< out: buffer */
  ulint e, /*!< in: encoded extra_size */
#ifdef UNIV_DEBUG
  ulint size,         /*!< in: total size to write */
  int fd,             /*!< in: file descriptor */
  ulint foffs,        /*!< in: file offset */
#endif                /* UNIV_DEBUG */
  const mrec_t *mrec, /*!< in: record to write */
  const ulint *offsets
) /*!< in: offsets of mrec */
#ifndef UNIV_DEBUG
#define row_merge_write_rec_low(b, e, size, fd, foffs, mrec, offsets) row_merge_write_rec_low(b, e, mrec, offsets)
#endif /* !UNIV_DEBUG */
{
  if (e < 0x80) {
    *b++ = (byte)e;
  } else {
    *b++ = (byte)(0x80 | (e >> 8));
    *b++ = (byte)e;
  }

  memcpy(b, mrec - rec_offs_extra_size(offsets), rec_offs_size(offsets));
  ut_ad(b + rec_offs_size(offsets) == b + size);
}

/** Write a merge record.
@return	pointer to end of block, or NULL on error */
static byte *row_merge_write_rec(
  row_merge_block_t *block, /*!< in/out: file buffer */
  mrec_buf_t *buf,          /*!< in/out: secondary buffer */
  byte *b,                  /*!< in: pointer to end of block */
  int fd,                   /*!< in: file descriptor */
  ulint *foffs,             /*!< in/out: file offset */
  const mrec_t *mrec,       /*!< in: record to write */
  const ulint *offsets
) /*!< in: offsets of mrec */
{
  ut_ad(block);
  ut_ad(buf);
  ut_ad(b >= block[0]);
  ut_ad(b < block[1]);
  ut_ad(mrec);
  ut_ad(foffs);
  ut_ad(mrec < block[0] || mrec > block[1]);
  ut_ad(mrec < buf[0] || mrec > buf[1]);

  /* Normalize extra_size.  Value 0 signals "end of list". */
  auto extra_size = rec_offs_extra_size(offsets);

  auto size = extra_size + (extra_size >= 0x80) + rec_offs_data_size(offsets);

  ++extra_size;

  if (unlikely(b + size >= block[1])) {
    /* The record spans two blocks.
    Copy it to the temporary buffer first. */
    auto avail_size = block[1] - b;

    row_merge_write_rec_low(buf[0], extra_size, size, fd, *foffs, mrec, offsets);

    /* Copy the head of the temporary buffer, write
    the completed block, and copy the tail of the
    record to the head of the new block. */
    memcpy(b, buf[0], avail_size);

    if (!row_merge_write(fd, (*foffs)++, block)) {
      return (nullptr);
    }

    UNIV_MEM_INVALID(block[0], sizeof block[0]);

    /* Copy the rest. */
    b = block[0];
    memcpy(b, buf[0] + avail_size, size - avail_size);
    b += size - avail_size;
  } else {
    row_merge_write_rec_low(b, extra_size, size, fd, *foffs, mrec, offsets);
    b += size;
  }

  return (b);
}

/** Write an end-of-list marker.
@return	pointer to end of block, or NULL on error */
static byte *row_merge_write_eof(
  row_merge_block_t *block, /*!< in/out: file buffer */
  byte *b,                  /*!< in: pointer to end of block */
  int fd,                   /*!< in: file descriptor */
  ulint *foffs
) /*!< in/out: file offset */
{
  ut_ad(block);
  ut_ad(b >= block[0]);
  ut_ad(b < block[1]);
  ut_ad(foffs);
#ifdef UNIV_DEBUG
  if (row_merge_print_write) {
    ib_logger(ib_stream, "row_merge_write %p,%p,%d,%lu EOF\n", (void *)b, (void *)block, fd, (ulong)*foffs);
  }
#endif /* UNIV_DEBUG */

  *b++ = 0;
  UNIV_MEM_ASSERT_RW(block[0], b - block[0]);
  UNIV_MEM_ASSERT_W(block[0], sizeof block[0]);
#ifdef UNIV_DEBUG_VALGRIND
  /* The rest of the block is uninitialized.  Initialize it
  to avoid bogus warnings. */
  memset(b, 0xff, block[1] - b);
#endif /* UNIV_DEBUG_VALGRIND */

  if (!row_merge_write(fd, (*foffs)++, block)) {
    return (nullptr);
  }

  UNIV_MEM_INVALID(block[0], sizeof block[0]);
  return (block[0]);
}

/** Compare two merge records.
@return	1, 0, -1 if mrec1 is greater, equal, less, respectively, than mrec2 */
static int row_merge_cmp(
  const mrec_t *mrec1,   /*!< in: first merge
                                         record to be compared */
  const mrec_t *mrec2,   /*!< in: second merge
                                         record to be compared */
  const ulint *offsets1, /*!< in: first record offsets */
  const ulint *offsets2, /*!< in: second record offsets */
  const Index *index
) /*!< in: index */
{
  int cmp;

  cmp = cmp_rec_rec_simple(mrec1, mrec2, offsets1, offsets2, index);

#ifdef UNIV_DEBUG
  if (row_merge_print_cmp) {
    log_err("row_merge_cmp1 ");
    log_err(rec_to_string(mrec1));
    log_err("row_merge_cmp2 ");
    log_err(rec_to_string(mrec2));
    log_err("row_merge_cmp=%d", cmp);
  }
#endif /* UNIV_DEBUG */

  return (cmp);
}

/**
 * Reads clustered index of the table and create temporary files
 * containing the index entries for the indexes to be built.
 *
 * @param trx Transaction
 * @param table Client table object, for reporting erroneous records
 * @param old_table Table where rows are read from
 * @param new_table Table where indexes are created; identical to old_table unless creating a PRIMARY KEY
 * @param index Indexes to be created
 * @param files Temporary files
 * @param n_index Number of indexes to create
 * @param block File buffer
 * @return DB_SUCCESS or error
 */
static db_err row_merge_read_clustered_index(
  trx_t *trx,
  table_handle_t table,
  const Table *old_table,
  const Table *new_table,
  Index **index,
  merge_file_t *files,
  ulint n_index,
  row_merge_block_t *block) noexcept
{
  ulint *nonnull{};
  ulint n_nonnull{};
  db_err err = DB_SUCCESS;
  Btree_pcursor pcur(srv_fsp, srv_btree_sys);

  trx->m_op_info = "reading clustered index";

  ut_ad(trx);
  ut_ad(old_table);
  ut_ad(new_table);
  ut_ad(index);
  ut_ad(files);

  /* Create and initialize memory for record buffers */

  auto merge_buf = static_cast<row_merge_buf_t **>(mem_alloc(n_index * sizeof(row_merge_buf_t *)));

  for (ulint i{}; i < n_index; ++i) {
    merge_buf[i] = row_merge_buf_create(index[i]);
  }

  mtr_t mtr;

  mtr.start();

  /* Find the clustered index and create a persistent cursor
  based on that. */

  auto clust_index = old_table->get_clustered_index();

  pcur.open_at_index_side(true, clust_index, BTR_SEARCH_LEAF, true, 0, &mtr);

  if (unlikely(old_table != new_table)) {
    ulint n_cols = old_table->get_n_cols();

    /* A primary key will be created.  Identify the
    columns that were flagged NOT NULL in the new table,
    so that we can quickly check that the records in the
    (old) clustered index do not violate the added NOT
    NULL constraints. */

    ut_a(n_cols == new_table->get_n_cols());

    nonnull = static_cast<ulint *>(mem_alloc(n_cols * sizeof *nonnull));

    for (ulint i{}; i < n_cols; ++i) {
      if (old_table->get_nth_col(i)->prtype & DATA_NOT_NULL) {

        continue;
      }

      if (new_table->get_nth_col(i)->prtype & DATA_NOT_NULL) {

        nonnull[n_nonnull++] = i;
      }
    }

    if (!n_nonnull) {
      mem_free(nonnull);
      nonnull = nullptr;
    }
  }

  auto row_heap = mem_heap_create(sizeof(mrec_buf_t));

  auto func_exit = [&](db_err err) -> auto {
    pcur.close();

    mtr.commit();

    mem_heap_free(row_heap);

    if (likely_null(nonnull)) {
      mem_free(nonnull);
    }

    for (ulint i{}; i < n_index; ++i) {
      row_merge_buf_free(merge_buf[i]);
    }

    mem_free(merge_buf);

    trx->m_op_info = "";

    return err;
  };
  /* Scan the clustered index. */
  for (;;) {
    const rec_t *rec;
    ulint *offsets;
    DTuple *row{};
    row_ext_t *ext;
    bool has_next = true;

    pcur.move_to_next_on_page();

    /* When switching pages, commit the mini-transaction
    in order to release the latch on the old page. */

    merge_file_t *file{};
    const Index *index{};

    if (pcur.is_after_last_on_page()) {
      if (unlikely(trx_is_interrupted(trx))) {
        trx->error_key_num = ULINT_UNDEFINED;
        return func_exit(DB_INTERRUPTED);
      }

      pcur.store_position(&mtr);

      mtr.commit();

      mtr.start();

      (void) pcur.restore_position(BTR_SEARCH_LEAF, &mtr, Current_location());
      has_next = pcur.move_to_next_user_rec(&mtr);
    }

    row_merge_buf_t *buf;

    if (likely(has_next)) {
      rec = pcur.get_rec();

      {
        Phy_rec record{clust_index, rec};

        offsets = record.get_col_offsets(nullptr, ULINT_UNDEFINED, &row_heap, Current_location());
      }

      /* Skip delete marked records. */
      if (rec_get_deleted_flag(rec)) {
        continue;
      }

      ++srv_n_rows_inserted;

      /* Build a row based on the clustered index. */

      row = row_build(ROW_COPY_POINTERS, clust_index, rec, offsets, new_table, &ext, row_heap);

      if (likely_null(nonnull)) {
        for (ulint i = 0; i < n_nonnull; i++) {
          dfield_t *field = &row->fields[nonnull[i]];
          dtype_t *field_type = dfield_get_type(field);

          ut_a(!(field_type->prtype & DATA_NOT_NULL));

          if (dfield_is_null(field)) {
            trx->error_key_num = i;
            return func_exit(DB_PRIMARY_KEY_IS_NULL);
          }

          field_type->prtype |= DATA_NOT_NULL;
        }
      }
    }

    /* Build all entries for all the indexes to be created
    in a single scan of the clustered index. */

    for (ulint i{}; i < n_index; ++i) {
      file = &files[i];
      buf = merge_buf[i];
      index = buf->index;

      if (likely(row && row_merge_buf_add(buf, row, ext))) {
        file->n_rec++;
        continue;
      }

      /* The buffer must be sufficiently large
      to hold at least one record. */
      ut_ad(buf->n_recs || !has_next);

      /* We have enough data rows to form a block.
      Sort them and write to disk. */

      if (buf->n_recs) {
        if (index->is_unique()) {
          row_merge_dup_t dup;
          dup.index = buf->index;
          dup.table = table;
          dup.n_dup = 0;

          row_merge_buf_sort(buf, &dup);

          if (dup.n_dup) {
            err = DB_DUPLICATE_KEY;
            trx->error_key_num = i;
            return func_exit(DB_DUPLICATE_KEY);
          }
        } else {
          row_merge_buf_sort(buf, nullptr);
        }
      }

      row_merge_buf_write(buf, file, block);

      if (!row_merge_write(file->fd, file->offset++, block)) {
        trx->error_key_num = i;
        return func_exit(DB_OUT_OF_FILE_SPACE);
      }

      UNIV_MEM_INVALID(block[0], sizeof block[0]);
      merge_buf[i] = row_merge_buf_empty(buf);

      if (likely(row != nullptr)) {
        /* Try writing the record again, now
        that the buffer has been written out
        and emptied. */

        if (unlikely(!row_merge_buf_add(buf, row, ext))) {
          /* An empty buffer should have enough
          room for at least one record. */
          ut_error;
        }

        file->n_rec++;
      }
    }

    mem_heap_empty(row_heap);

    if (unlikely(!has_next)) {
      return func_exit(err);
    }
  }

  return func_exit(err);
}

/** Write a record via buffer 2 and read the next record to buffer N.
@param N	number of the buffer (0 or 1)
@param AT_END	statement to execute at end of input */
#define ROW_MERGE_WRITE_GET_NEXT(N, AT_END)                                                               \
  do {                                                                                                    \
    b2 = row_merge_write_rec(&block[2], &buf[2], b2, of->fd, &of->offset, mrec##N, offsets##N);           \
    if (unlikely(!b2 || ++of->n_rec > file->n_rec)) {                                                     \
      goto corrupt;                                                                                       \
    }                                                                                                     \
    b##N = row_merge_read_rec(&block[N], &buf[N], b##N, index, file->fd, foffs##N, &mrec##N, offsets##N); \
    if (unlikely(!b##N)) {                                                                                \
      if (mrec##N) {                                                                                      \
        goto corrupt;                                                                                     \
      }                                                                                                   \
      AT_END;                                                                                             \
    }                                                                                                     \
  } while (0)

/** Merge two blocks of records on disk and write a bigger block.
@return	DB_SUCCESS or error code */
static db_err row_merge_blocks(
  const Index *index, /*!< in: index being created */
  const merge_file_t *file,  /*!< in: file containing
                                            index entries */
  row_merge_block_t *block,  /*!< in/out: 3 buffers */
  ulint *foffs0,             /*!< in/out: offset of first
                                            source list in the file */
  ulint *foffs1,             /*!< in/out: offset of second
                                            source list in the file */
  merge_file_t *of,          /*!< in/out: output file */
  table_handle_t table
) /*!< in/out: Client table, for
                                            reporting erroneous key value
                                            if applicable */
{
  mem_heap_t *heap; /*!< memory heap for offsets0, offsets1 */

  mrec_buf_t buf[3];   /*!< buffer for handling split mrec in block[] */
  const byte *b0;      /*!< pointer to block[0] */
  const byte *b1;      /*!< pointer to block[1] */
  byte *b2;            /*!< pointer to block[2] */
  const mrec_t *mrec0; /*!< merge rec, points to block[0] or buf[0] */
  const mrec_t *mrec1; /*!< merge rec, points to block[1] or buf[1] */
  ulint *offsets0;     /* offsets of mrec0 */
  ulint *offsets1;     /* offsets of mrec1 */

#ifdef UNIV_DEBUG
  if (row_merge_print_block) {
    ib_logger(
      ib_stream,
      "row_merge_blocks fd=%d ofs=%lu + fd=%d ofs=%lu"
      " = fd=%d ofs=%lu\n",
      file->fd,
      (ulong)*foffs0,
      file->fd,
      (ulong)*foffs1,
      of->fd,
      (ulong)of->offset
    );
  }
#endif /* UNIV_DEBUG */

  heap = row_merge_heap_create(index, &offsets0, &offsets1);

  /* Write a record and read the next record.  Split the output
  file in two halves, which can be merged on the following pass. */

  if (!row_merge_read(file->fd, *foffs0, &block[0]) || !row_merge_read(file->fd, *foffs1, &block[1])) {
  corrupt:
    mem_heap_free(heap);
    return (DB_CORRUPTION);
  }

  b0 = block[0];
  b1 = block[1];
  b2 = block[2];

  b0 = row_merge_read_rec(&block[0], &buf[0], b0, index, file->fd, foffs0, &mrec0, offsets0);
  b1 = row_merge_read_rec(&block[1], &buf[1], b1, index, file->fd, foffs1, &mrec1, offsets1);
  if (unlikely(!b0 && mrec0) || unlikely(!b1 && mrec1)) {

    goto corrupt;
  }

  while (mrec0 != nullptr && mrec1 != nullptr) {
    switch (row_merge_cmp(mrec0, mrec1, offsets0, offsets1, index)) {
      case 0:
        if (unlikely(index->is_unique())) {
          mem_heap_free(heap);
          return (DB_DUPLICATE_KEY);
        }
        /* fall through */
      case -1:
        ROW_MERGE_WRITE_GET_NEXT(0, goto merged);
        break;
      case 1:
        ROW_MERGE_WRITE_GET_NEXT(1, goto merged);
        break;
      default:
        ut_error;
    }
  }

merged:
  if (mrec0 != nullptr) {
    /* append all mrec0 to output */
    for (;;) {
      ROW_MERGE_WRITE_GET_NEXT(0, goto done0);
    }
  }
done0:
  if (mrec1 != nullptr) {
    /* append all mrec1 to output */
    for (;;) {
      ROW_MERGE_WRITE_GET_NEXT(1, goto done1);
    }
  }
done1:

  mem_heap_free(heap);
  b2 = row_merge_write_eof(&block[2], b2, of->fd, &of->offset);
  return (b2 ? DB_SUCCESS : DB_CORRUPTION);
}

/**
 * @brief Copy a block of index entries.
 *
 * @param[in] index The index being created.
 * @param[in] file The input file.
 * @param[in,out] block The 3 buffers.
 * @param[in,out] foffs0 The input file offset.
 * @param[in,out] of The output file.
 * 
 * @return true on success, false on failure.
 */
static bool row_merge_blocks_copy(const Index *index, const merge_file_t *file, row_merge_block_t *block, ulint *foffs0, merge_file_t *of) {
  mem_heap_t *heap; /*!< memory heap for offsets0, offsets1 */

  mrec_buf_t buf[3];   /*!< buffer for handling
                       split mrec in block[] */
  const byte *b0;      /*!< pointer to block[0] */
  byte *b2;            /*!< pointer to block[2] */
  const mrec_t *mrec0; /*!< merge rec, points to block[0] */
  ulint *offsets0;     /* offsets of mrec0 */
  ulint *offsets1;     /* dummy offsets */

#ifdef UNIV_DEBUG
  if (row_merge_print_block) {
    ib_logger(
      ib_stream,
      "row_merge_blocks_copy fd=%d ofs=%lu"
      " = fd=%d ofs=%lu\n",
      file->fd,
      (ulong)foffs0,
      of->fd,
      (ulong)of->offset
    );
  }
#endif /* UNIV_DEBUG */

  heap = row_merge_heap_create(index, &offsets0, &offsets1);

  /* Write a record and read the next record.  Split the output
  file in two halves, which can be merged on the following pass. */

  if (!row_merge_read(file->fd, *foffs0, &block[0])) {
  corrupt:
    mem_heap_free(heap);
    return (false);
  }

  b0 = block[0];
  b2 = block[2];

  b0 = row_merge_read_rec(&block[0], &buf[0], b0, index, file->fd, foffs0, &mrec0, offsets0);
  if (unlikely(!b0 && mrec0)) {

    goto corrupt;
  }

  if (mrec0) {
    /* append all mrec0 to output */
    for (;;) {
      ROW_MERGE_WRITE_GET_NEXT(0, goto done0);
    }
  }
done0:

  /* The file offset points to the beginning of the last page
  that has been read.  Update it to point to the next block. */
  (*foffs0)++;

  mem_heap_free(heap);
  return (row_merge_write_eof(&block[2], b2, of->fd, &of->offset) != nullptr);
}

/** Merge disk files.
@return	DB_SUCCESS or error code */
static __attribute__((nonnull)) db_err row_merge(
  trx_t *trx,                /*!< in: transaction */
  const Index *index, /*!< in: index being created */
  merge_file_t *file,        /*!< in/out: file containing
                                     index entries */
  ulint *half,               /*!< in/out: half the file */
  row_merge_block_t *block,  /*!< in/out: 3 buffers */
  int *tmpfd,                /*!< in/out: temporary file handle */
  table_handle_t table
) /*!< in/out: Client table, for
                                     reporting erroneous key value
                                     if applicable */
{
  ulint foffs0;    /*!< first input offset */
  ulint foffs1;    /*!< second input offset */
  db_err err;      /*!< error code */
  merge_file_t of; /*!< output file */
  const ulint ihalf = *half;
  /*!< half the input file */
  ulint ohalf; /*!< half the output file */

  UNIV_MEM_ASSERT_W(block[0], 3 * sizeof block[0]);
  ut_ad(ihalf < file->offset);

  of.fd = *tmpfd;
  of.offset = 0;
  of.n_rec = 0;

  /* Merge blocks to the output file. */
  ohalf = 0;
  foffs0 = 0;
  foffs1 = ihalf;

  for (; foffs0 < ihalf && foffs1 < file->offset; foffs0++, foffs1++) {
    ulint ahalf; /*!< arithmetic half the input file */

    if (unlikely(trx_is_interrupted(trx))) {
      return (DB_INTERRUPTED);
    }

    err = row_merge_blocks(index, file, block, &foffs0, &foffs1, &of, table);

    if (err != DB_SUCCESS) {
      return err;
    }

    /* Record the offset of the output file when
    approximately half the output has been generated.  In
    this way, the next invocation of row_merge() will
    spend most of the time in this loop.  The initial
    estimate is ohalf==0. */
    ahalf = file->offset / 2;
    ut_ad(ohalf <= of.offset);

    /* Improve the estimate until reaching half the input
    file size, or we can not get any closer to it.  All
    comparands should be non-negative when !(ohalf < ahalf)
    because ohalf <= of.offset. */
    if (ohalf < ahalf || of.offset - ahalf < ohalf - ahalf) {
      ohalf = of.offset;
    }
  }

  /* Copy the last blocks, if there are any. */

  while (foffs0 < ihalf) {
    if (unlikely(trx_is_interrupted(trx))) {
      return (DB_INTERRUPTED);
    }

    if (!row_merge_blocks_copy(index, file, block, &foffs0, &of)) {
      return (DB_CORRUPTION);
    }
  }

  ut_ad(foffs0 == ihalf);

  while (foffs1 < file->offset) {
    if (unlikely(trx_is_interrupted(trx))) {
      return (DB_INTERRUPTED);
    }

    if (!row_merge_blocks_copy(index, file, block, &foffs1, &of)) {
      return (DB_CORRUPTION);
    }
  }

  ut_ad(foffs1 == file->offset);

  if (unlikely(of.n_rec != file->n_rec)) {
    return (DB_CORRUPTION);
  }

  /* Swap file descriptors for the next pass. */
  *tmpfd = file->fd;
  *file = of;
  *half = ohalf;

  UNIV_MEM_INVALID(block[0], 3 * sizeof block[0]);

  return (DB_SUCCESS);
}

/** Merge disk files.
@return	DB_SUCCESS or error code */
static db_err row_merge_sort(
  trx_t *trx,                /*!< in: transaction */
  const Index *index, /*!< in: index being created */
  merge_file_t *file,        /*!< in/out: file containing
                                          index entries */
  row_merge_block_t *block,  /*!< in/out: 3 buffers */
  int *tmpfd,                /*!< in/out: temporary file handle */
  table_handle_t table
) /*!< in/out: User table, for
                                          reporting erroneous key value
                                          if applicable */
{
  ulint half = file->offset / 2;

  /* The file should always contain at least one byte (the end
  of file marker).  Thus, it must be at least one block. */
  ut_ad(file->offset > 0);

  do {
    db_err err;

    err = row_merge(trx, index, file, &half, block, tmpfd, table);

    if (err != DB_SUCCESS) {
      return err;
    }

    /* half > 0 should hold except when the file consists
    of one block.  No need to merge further then. */
    ut_ad(half > 0 || file->offset == 1);
  } while (half < file->offset && half > 0);

  return (DB_SUCCESS);
}

/** Copy externally stored columns to the data tuple. */
static void row_merge_copy_blobs(
  const mrec_t *mrec,   /*!< in: merge record */
  const ulint *offsets, /*!< in: offsets of mrec */
  DTuple *tuple,      /*!< in/out: data tuple */
  mem_heap_t *heap
) /*!< in/out: memory heap */
{
  ulint i;
  Blob blob(srv_fsp, srv_btree_sys);
  ulint n_fields = dtuple_get_n_fields(tuple);

  for (i = 0; i < n_fields; i++) {
    ulint len;
    const void *data;
    dfield_t *field = dtuple_get_nth_field(tuple, i);

    if (!dfield_is_ext(field)) {
      continue;
    }

    ut_ad(!dfield_is_null(field));

    /* The table is locked during index creation.
    Therefore, externally stored columns cannot possibly
    be freed between the time the BLOB pointers are read
    (row_merge_read_clustered_index()) and dereferenced
    (below). */
    data = blob.copy_externally_stored_field(mrec, offsets, i, &len, heap);

    dfield_set_data(field, data, len);
  }
}

/**
 * @brief Read sorted file containing index data rows and insert these data rows to the index.
 * 
 * @param[in] trx Transaction.
 * @param[in] index Index.
 * @param[in] table New table.
 * @param[in] fd File descriptor.
 * @param[in] block File buffer.
 * 
 * @return DB_SUCCESS or error number.
 */
static db_err row_merge_insert_index_tuples(trx_t *trx, Index *index, Table *table, int fd, row_merge_block_t *block) {
  mrec_buf_t buf;
  const byte *b;
  que_thr_t *thr;
  ins_node_t *node;
  mem_heap_t *tuple_heap;
  mem_heap_t *graph_heap;
  enum db_err err = DB_SUCCESS;
  ulint foffs = 0;
  ulint *offsets;

  ut_ad(trx);
  ut_ad(index);
  ut_ad(table);

  /* We use the insert query graph as the dummy graph
  needed in the row module call */

  trx->m_op_info = "inserting index entries";

  graph_heap = mem_heap_create(500);
  node = row_ins_node_create(INS_DIRECT, table, graph_heap);

  thr = pars_complete_graph_for_exec(node, trx, graph_heap);

  que_thr_move_to_run_state(thr);

  tuple_heap = mem_heap_create(1000);

  {
    ulint i = 1 + REC_OFFS_HEADER_SIZE + index->get_n_fields();
    offsets = reinterpret_cast<ulint *>(mem_heap_alloc(graph_heap, i * sizeof *offsets));
    offsets[0] = i;
    offsets[1] = index->get_n_fields();
  }

  b = *block;

  if (!row_merge_read(fd, foffs, block)) {
    err = DB_CORRUPTION;
  } else {
    for (;;) {
      const mrec_t *mrec;
      DTuple *dtuple;
      ulint n_ext;

      b = row_merge_read_rec(block, &buf, b, index, fd, &foffs, &mrec, offsets);
      if (unlikely(!b)) {
        /* End of list, or I/O error */
        if (mrec) {
          err = DB_CORRUPTION;
        }
        break;
      }

      dtuple = row_rec_to_index_entry_low(mrec, index, offsets, &n_ext, tuple_heap);

      if (unlikely(n_ext)) {
        row_merge_copy_blobs(mrec, offsets, dtuple, tuple_heap);
      }

      node->row = dtuple;
      node->table = table;
      node->trx_id = trx->m_id;

      ut_ad(dtuple_validate(dtuple));

      do {
        thr->run_node = thr;
        thr->prev_node = thr->common.parent;

        err = row_ins_index_entry(index, dtuple, 0, false, thr);

        if (likely(err == DB_SUCCESS)) {

          goto next_rec;
        }

        thr->lock_state = QUE_THR_LOCK_ROW;
        trx->error_state = err;
        que_thr_stop_client(thr);
        thr->lock_state = QUE_THR_LOCK_NOLOCK;
      } while (ib_handle_errors(&err, trx, thr, nullptr));

      goto err_exit;
    next_rec:
      mem_heap_empty(tuple_heap);
    }
  }

  que_thr_stop_for_client_no_error(thr, trx);

err_exit:
  que_graph_free(thr->graph);

  trx->m_op_info = "";

  mem_heap_free(tuple_heap);

  return (err);
}

/** Drop an index from the InnoDB system tables.  The data dictionary must
have been locked exclusively by the caller, because the transaction
will not be committed. */

void row_merge_drop_index(
  Index *index, /*!< in: index to be removed */
  Table *table, /*!< in: table */
  trx_t *trx
) /*!< in: transaction handle */
{
  if (index != nullptr) {
    ddl_drop_index(table, index, trx);
  }
}

/** Drop those indexes which were created before an error occurred when
building an index.  The data dictionary must have been locked
exclusively by the caller, because the transaction will not be
committed. */

void row_merge_drop_indexes(
  trx_t *trx,           /*!< in: transaction */
  Table *table,  /*!< in: table containing the indexes */
  Index **index, /*!< in: indexes to drop */
  ulint num_created
) /*!< in: number of elements in index[] */
{
  ulint key_num;

  for (key_num = 0; key_num < num_created; key_num++) {
    row_merge_drop_index(index[key_num], table, trx);
  }
}

/** Create a merge file. */
static void row_merge_file_create(merge_file_t *merge_file) /*!< out: merge file structure */
{
  merge_file->fd = ib_create_tempfile("ibmrg");
  merge_file->offset = 0;
  merge_file->n_rec = 0;
}

/** Destroy a merge file. */
static void row_merge_file_destroy(merge_file_t *merge_file) /*!< out: merge file structure */
{
  if (merge_file->fd != -1) {
    close(merge_file->fd);
    merge_file->fd = -1;
  }
}

/** Determine the precise type of a column that is added to a tem
if a column must be constrained NOT NULL.
@return	col->prtype, possibly ORed with DATA_NOT_NULL */
inline ulint row_merge_col_prtype(
  const Column *col, /*!< in: column */
  const char *col_name,  /*!< in: name of the column */
  const merge_index_def_t *index_def
) /*!< in: the index definition
                                        of the primary key */
{
  ulint prtype = col->prtype;

  ut_ad(index_def->ind_type & DICT_CLUSTERED);

  if (prtype & DATA_NOT_NULL) {

    return prtype;
  }

  /* All columns that are included
  in the PRIMARY KEY must be NOT NULL. */

  for (ulint i{}; i < index_def->n_fields; i++) {
    if (!strcmp(col_name, index_def->fields[i].field_name)) {
      return (prtype | DATA_NOT_NULL);
    }
  }

  return prtype;
}

Table *row_merge_create_temporary_table(const char *table_name, const merge_index_def_t *index_def, const Table *table, trx_t *trx) {
  db_err err;
  const auto n_cols = table->get_n_user_cols();

  ut_ad(table);
  ut_ad(index_def);
  ut_ad(table_name);
  ut_ad(mutex_own(&srv_dict_sys->m_mutex));

  auto new_table = Table::create(table_name, DICT_HDR_SPACE, n_cols, table->m_flags, false, Current_location());

  for (ulint i{}; i < n_cols; ++i) {
    const auto col = table->get_nth_col(i);
    const auto col_name = table->get_col_name(i);

    new_table->add_col(col_name, col->mtype, row_merge_col_prtype(col, col_name, index_def), col->len);
  }

  err = ddl_create_table(new_table, trx);

  if (err != DB_SUCCESS) {
    trx->error_state = err;
    new_table = nullptr;
  }

  return (new_table);
}

db_err row_merge_rename_indexes(trx_t *trx, Table *table) {
  db_err err = DB_SUCCESS;
  pars_info_t *info = pars_info_create();

  /* We use the private SQL parser of Innobase to generate the
  query graphs needed in renaming indexes. */

  static const char rename_indexes[] =
    "PROCEDURE RENAME_INDEXES_PROC () IS\n"
    "BEGIN\n"
    "UPDATE SYS_INDEXES SET NAME=SUBSTR(NAME,1,LENGTH(NAME)-1)\n"
    "WHERE TABLE_ID = :tableid AND SUBSTR(NAME,0,1)='" TEMP_INDEX_PREFIX_STR
    "';\n"
    "END;\n";

  ut_ad(table);
  ut_ad(trx);
  ut_a(trx->m_dict_operation_lock_mode == RW_X_LATCH);

  trx->m_op_info = "renaming indexes";

  pars_info_add_uint64_literal(info, "tableid", table->m_id);

  err = que_eval_sql(info, rename_indexes, false, trx);

  if (err == DB_SUCCESS) {
    for (auto index : table->m_indexes) {
      if (*index->m_name == TEMP_INDEX_PREFIX) {
        ++index->m_name;
      }
    }
  }

  trx->m_op_info = "";

  return err;
}

db_err row_merge_rename_tables(Table *old_table, Table *new_table, const char *tmp_name, trx_t *trx) {
  db_err err = DB_ERROR;
  pars_info_t *info;
  const char *old_name = old_table->m_name;

  ut_ad(old_table != new_table);
  ut_ad(mutex_own(&srv_dict_sys->m_mutex));

  ut_a(trx->m_dict_operation_lock_mode == RW_X_LATCH);

  trx->m_op_info = "renaming tables";

  /* We use the private SQL parser of Innobase to generate the query
  graphs needed in updating the dictionary data in system tables. */

  info = pars_info_create();

  pars_info_add_str_literal(info, "new_name", new_table->m_name);
  pars_info_add_str_literal(info, "old_name", old_name);
  pars_info_add_str_literal(info, "tmp_name", tmp_name);

  err = que_eval_sql(
    info,
    "PROCEDURE RENAME_TABLES () IS\n"
    "BEGIN\n"
    "UPDATE SYS_TABLES SET NAME = :tmp_name\n"
    " WHERE NAME = :old_name;\n"
    "UPDATE SYS_TABLES SET NAME = :old_name\n"
    " WHERE NAME = :new_name;\n"
    "END;\n",
    false,
    trx
  );

  if (err != DB_SUCCESS) {

    goto err_exit;
  }

  /* The following calls will also rename the .ibd data files if
  the tables are stored in a single-table tablespace */

  if (!srv_dict_sys->table_rename_in_cache(old_table, tmp_name, false) || !srv_dict_sys->table_rename_in_cache(new_table, old_name, false)) {

    err = DB_ERROR;
    goto err_exit;
  }

  err = srv_dict_sys->m_loader.load_foreigns(old_name, true);

  if (err != DB_SUCCESS) {
  err_exit:
    trx->error_state = DB_SUCCESS;
    trx_general_rollback(trx, false, nullptr);
    trx->error_state = DB_SUCCESS;
  }

  trx->m_op_info = "";

  return (err);
}

Index *row_merge_create_index(trx_t *trx, Table *table, const merge_index_def_t *index_def) {
  const auto n_fields = index_def->n_fields;

  /* Create the index prototype, using the passed in def, this is not
  a persistent operation. We pass 0 as the space id, and determine at
  a lower level the space id where to store the table. */

  auto index = Index::create(table, index_def->name, Page_id{DICT_HDR_SPACE, NULL_PAGE_NO}, index_def->ind_type, n_fields);
  ut_a(index != nullptr);

  for (ulint i{}; i < n_fields; ++i) {
    auto ifield = &index_def->fields[i];

    (void) index->add_field(ifield->field_name, ifield->prefix_len);
  }

  /* Add the index to SYS_INDEXES, using the index prototype. */
  index->m_table = table;

  auto err = ddl_create_index(index, trx);

  if (err == DB_SUCCESS) {

    index = row_merge_dict_table_get_index(table, index_def);
    ut_a(index != nullptr);

    /* Note the id of the transaction that created this
    index, we use it to restrict readers from accessing
    this index, to ensure read consistency. */
    index->m_trx_id = trx->m_id;
  } else {
    index = nullptr;
  }

  return index;
}

bool row_merge_is_index_usable(const trx_t *trx, const Index *index) {
  return !trx->read_view || read_view_sees_trx_id(trx->read_view, index->m_trx_id);
}

db_err row_merge_drop_table(trx_t *trx, Table *table) {
  /* There must be no open transactions on the table. */
  ut_a(table->m_n_handles_opened == 0);

  auto err = ddl_drop_table(table->m_name, trx, false);
  auto err_commit = trx_commit(trx);
  ut_a(err_commit == DB_SUCCESS);

  return err;
}

db_err row_merge_build_indexes(
  trx_t *trx,
  Table *old_table,
  Table *new_table,
  Index **indexes,
  ulint n_indexes,
  table_handle_t table)
{
  ut_ad(trx);
  ut_ad(old_table);
  ut_ad(new_table);
  ut_ad(indexes);
  ut_ad(n_indexes);

  ut_a(trx->m_conc_state != TRX_NOT_STARTED);

  /* Allocate memory for merge file data structure and initialize fields */

  auto block_size = 3 * sizeof(row_merge_block_t);
  auto block = static_cast<row_merge_block_t *>(os_mem_alloc_large(&block_size));
  auto merge_files = static_cast<merge_file_t *>(mem_alloc(n_indexes * sizeof(merge_file_t)));

  for (ulint i{}; i < n_indexes; ++i) {
    row_merge_file_create(&merge_files[i]);
  }

  auto tmpfd = ib_create_tempfile("mrg");

  /* Read clustered index of the table and create files for
  secondary index entries for merge sort */

  auto err = row_merge_read_clustered_index(trx, table, old_table, new_table, indexes, merge_files, n_indexes, block);

  if (err != DB_SUCCESS) {

    goto func_exit;
  }

  /* Now we have files containing index entries ready for
  sorting and inserting. */

  for (ulint i{}; i < n_indexes; ++i) {
    err = row_merge_sort(trx, indexes[i], &merge_files[i], block, &tmpfd, table);

    if (err == DB_SUCCESS) {
      err = row_merge_insert_index_tuples(trx, indexes[i], new_table, merge_files[i].fd, block);
    }

    /* Close the temporary file to free up space. */
    row_merge_file_destroy(&merge_files[i]);

    if (err != DB_SUCCESS) {
      trx->error_key_num = i;
      goto func_exit;
    }
  }

func_exit:
  close(tmpfd);

  for (ulint i{}; i < n_indexes; ++i) {
    row_merge_file_destroy(&merge_files[i]);
  }

  mem_free(merge_files);
  os_mem_free_large(block, block_size);

  return err;
}
