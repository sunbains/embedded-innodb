/****************************************************************************
Copyright (c) 1995, 2010, Innobase Oy. All Rights Reserved.

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

/** @file fsp/fsp0fsp.c
File space management

Created 11/29/1995 Heikki Tuuri
***********************************************************************/

#include "fsp0fsp.h"
#include "btr0btr.h"
#include "buf0buf.h"
#include "dict0boot.h"
#include "dict0mem.h"
#include "fil0fil.h"
#include "fut0fut.h"
#include "log0log.h"
#include "mtr0log.h"
#include "page0page.h"
#include "srv0srv.h"
#include "sync0sync.h"
#include "ut0byte.h"

/* The data structures in files are defined just as byte strings in C */
using fsp_header_t = byte;
using xdes_t = byte;

/*			SPACE HEADER
                        ============

File space header data structure: this data structure is contained in the
first page of a space. The space for this header is reserved in every extent
descriptor page, but used only in the first. */

/** Offset of the space header within a file page */
constexpr auto FSP_HEADER_OFFSET = FIL_PAGE_DATA;

/** Tanlespace id */
constexpr ulint FSP_SPACE_ID = 0;

/** This field contained a value up to which we know that the modifications
in the database have been flushed to the file space; not used now */
constexpr ulint FSP_NOT_USED = 4;

/** Current size of the space in pages */
constexpr ulint FSP_SIZE = 8;

/** Minimum page number for which the free list has not been initialized:
the pages >= this limit are, by definition, free; note that in a single-table
tablespace where size < 64 pages, this number is 64, i.e., we have initialized
the space about the first extent, but have not physically allocted those pages
to the file */
constexpr ulint FSP_FREE_LIMIT = 12;

/** Table->flags & ~DICT_TF_COMPACT */
constexpr ulint FSP_SPACE_FLAGS = 16;

/** Number of used pages in the FSP_FREE_FRAG list */
constexpr ulint FSP_FRAG_N_USED = 20;

/** List of free extents */
constexpr ulint FSP_FREE = 24;

/** List of partially free extents not belonging to any segment */
constexpr ulint FSP_FREE_FRAG = 24 + FLST_BASE_NODE_SIZE;

/** List of full extents not belonging to any segment */
constexpr ulint FSP_FULL_FRAG = (24 + 2 * FLST_BASE_NODE_SIZE);

/** 8 bytes which give the first unused segment id */
constexpr ulint FSP_SEG_ID = (24 + 3 * FLST_BASE_NODE_SIZE);

/** List of pages containing segment headers, where all the segment inode
slots are reserved */
constexpr ulint FSP_SEG_INODES_FULL = 32 + 3 * FLST_BASE_NODE_SIZE;

/** List of pages containing segment headers, where not all the segment
header slots are reserved */
constexpr ulint FSP_SEG_INODES_FREE = 32 + 4 * FLST_BASE_NODE_SIZE;

/*-------------------------------------*/
/* File space header size */
constexpr ulint FSP_HEADER_SIZE = 32 + 5 * FLST_BASE_NODE_SIZE;

/** This many free extents are added to the free list from above FSP_FREE_LIMIT at a time */
constexpr ulint FSP_FREE_ADD = 4;

/*			FILE SEGMENT INODE
                        ==================

Segment inode which is created for each segment in a tablespace. NOTE: in
purge we assume that a segment having only one currently used page can be
freed in a few steps, so that the freeing cannot fill the file buffer with
bufferfixed file pages. */

typedef byte fseg_inode_t;

/** The list node for linking segment inode pages */
constexpr ulint FSEG_INODE_PAGE_NODE = FSEG_PAGE_DATA;

constexpr ulint FSEG_ARR_OFFSET = FSEG_PAGE_DATA + FLST_NODE_SIZE;

/*-------------------------------------*/
/** 8 bytes of segment id: if this is 0, it means that the header is unused */
constexpr ulint FSEG_ID = 0;

/** Number of used segment pages in the FSEG_NOT_FULL list */
constexpr ulint FSEG_NOT_FULL_N_USED = 8;

/** List of free extents of this segment */
constexpr ulint FSEG_FREE = 12;

/** list of partially free extents */
constexpr ulint FSEG_NOT_FULL = 12 + FLST_BASE_NODE_SIZE;

/** List of full extents */
constexpr ulint FSEG_FULL = 12 + 2 * FLST_BASE_NODE_SIZE;

/** magic number used in debugging */
constexpr ulint FSEG_MAGIC_N = 12 + 3 * FLST_BASE_NODE_SIZE;

/** array of individual pages belonging to this segment in fsp
fragment extent lists */
constexpr ulint FSEG_FRAG_ARR = 16 + 3 * FLST_BASE_NODE_SIZE;

/** Number of slots in the array for the fragment pages */
constexpr ulint FSEG_FRAG_ARR_N_SLOTS = FSP_EXTENT_SIZE / 2;

/** A fragment page slot contains its page number within space,
 FIL_NULL means that the slot is not in use */
constexpr ulint FSEG_FRAG_SLOT_SIZE = 4;

/*-------------------------------------*/

constexpr ulint FSEG_INODE_SIZE = 16 + 3 * FLST_BASE_NODE_SIZE + FSEG_FRAG_ARR_N_SLOTS * FSEG_FRAG_SLOT_SIZE;

/** Number of segment inodes which fit on a single page */
constexpr ulint FSP_SEG_INODES_PER_PAGE = ((UNIV_PAGE_SIZE)-FSEG_ARR_OFFSET - 10) / FSEG_INODE_SIZE;

/** Magic value for debugging. */
constexpr ulint FSEG_MAGIC_N_VALUE = 97937874;

/** If this value is x, then if the number of unused but reserved pages
in a segment is less than reserved pages * 1/x, and there are at least
FSEG_FRAG_LIMIT used pages, then we allow a new empty extent to be added
to the segment in fseg_alloc_free_page. Otherwise, we use unused pages
of the segment. */
constexpr ulint FSEG_FILLFACTOR = 8;

/** If the segment has >= this many used pages, it may be expanded by
allocating extents to the segment; until that only individual fragment
pages are allocated from the space */
constexpr ulint FSEG_FRAG_LIMIT = FSEG_FRAG_ARR_N_SLOTS;

/** If the reserved size of a segment is at least this many extents,
we allow extents to be put to the free list of the extent: at most
FSEG_FREE_LIST_MAX_LEN many */
constexpr ulint FSEG_FREE_LIST_LIMIT = 40;

constexpr ulint FSEG_FREE_LIST_MAX_LEN = 4;

/*			EXTENT DESCRIPTOR
                        =================

File extent descriptor data structure: contains bits to tell which pages in
the extent are free and which contain old tuple version to clean. */

/** The identifier of the segmentto which this extent belongs */
constexpr ulint XDES_ID = 0;

/** The list node data structure for the descriptors */
constexpr ulint XDES_FLST_NODE = 8;

/** Contains state information of the extent */
constexpr ulint XDES_STATE = FLST_NODE_SIZE + 8;

/** Descriptor bitmap of the pages in the extent */
constexpr ulint XDES_BITMAP = FLST_NODE_SIZE + 12;

/*-------------------------------------*/

/** How many bits are there per page */
constexpr ulint XDES_BITS_PER_PAGE = 2;

/** Index of the bit which tells if the page is free */
constexpr ulint XDES_FREE_BIT = 0;

/** NOTE: currently not used!  Index of the bit which tells if there
are old versions of tuples on the page */
constexpr ulint XDES_CLEAN_BIT = 1;

/** States of a descriptor */
constexpr ulint XDES_FREE = 1;

/** Extent is in free list of space */

/** Extent is in free fragment list of space */
constexpr ulint XDES_FREE_FRAG = 2;

/** Extent is in full fragment list of space */
constexpr ulint XDES_FULL_FRAG = 3;

/** Extent belongs to a segment */
constexpr ulint XDES_FSEG = 4;

/** File extent data structure size in bytes. */
constexpr ulint XDES_SIZE = XDES_BITMAP + UT_BITS_IN_BYTES(FSP_EXTENT_SIZE * XDES_BITS_PER_PAGE);

/** Offset of the descriptor array on a descriptor page */
constexpr ulint XDES_ARR_OFFSET = FSP_HEADER_OFFSET + FSP_HEADER_SIZE;

/* Flag to indicate if we have printed the tablespace full error. */
static bool fsp_tbs_full_error_printed = false;

/** Returns an extent to the free list of a space.
@param[in] space                Tablespace ID
@param[in] page                 Page offset in the extent
@param[in,out] mtr              Mini-transaction covering the operation. */
static void fsp_free_extent(ulint space, ulint page, mtr_t *mtr);

/** Frees an extent of a segment to the space free list.
@param[in,out] seg_inode        Segment inode
@param[in] space                Tablespace ID
@param[in] page                 Page offset in the extent
@param[in,out] mtr              Mini-transaction covering the operation. */
static void fseg_free_extent(fseg_inode_t *seg_inode, ulint space, ulint page, mtr_t *mtr);

/** Calculates the number of pages reserved by a segment, and how
many pages are currently used.
@return	number of reserved pages */
static ulint fseg_n_reserved_pages_low(
  fseg_inode_t *header, /*!< in: segment inode */
  ulint *used,          /*!< out: number of pages used (not
                                       more than reserved) */
  mtr_t *mtr
); /*!< in: mtr handle */

/** Marks a page used. The page must reside within the extents of the given
segment. */
static void fseg_mark_page_used(
  fseg_inode_t *seg_inode, /*!< in: segment inode */
  ulint space,             /*!< in: space id */
  ulint page,              /*!< in: page offset */
  mtr_t *mtr
); /*!< in: mtr */

/** Returns the first extent descriptor for a segment. We think of the extent
lists of the segment catenated in the order FSEG_FULL -> FSEG_NOT_FULL
-> FSEG_FREE.
@return	the first extent descriptor, or nullptr if none */
static xdes_t *fseg_get_first_extent(
  fseg_inode_t *inode, /*!< in: segment inode */
  ulint space,         /*!< in: space id */
  mtr_t *mtr
); /*!< in: mtr */

/** Puts new extents to the free list if
there are free extents above the free limit. If an extent happens
to contain an extent descriptor page, the extent is put to
the FSP_FREE_FRAG list with the page marked as used. */
static void fsp_fill_free_list(
  bool init_space,      /*!< in: true if this is a single-table
                                          tablespace and we are only initing
                                          the tablespace's first extent
                                          descriptor page;
                                          then we do not allocate more extents */
  ulint space,          /*!< in: space */
  fsp_header_t *header, /*!< in: space header */
  mtr_t *mtr
); /*!< in: mtr */

/** Allocates a single free page from a segment. This function implements
the intelligent allocation strategy which tries to minimize file space
fragmentation.
@return	the allocated page number, FIL_NULL if no page could be allocated */
static ulint fseg_alloc_free_page_low(
  ulint space,             /*!< in: space */
  fseg_inode_t *seg_inode, /*!< in: segment inode */
  ulint hint,              /*!< in: hint of which page would be desirable */
  byte direction,          /*!< in: if the new page is needed because
                 of an index page split, and records are
                 inserted there in order, into which
                 direction they go alphabetically: FSP_DOWN,
                 FSP_UP, FSP_NO_DIR */
  mtr_t *mtr
); /*!< in: mtr handle */

/** Gets a pointer to the space header and x-locks its page.
@return	pointer to the space header, page x-locked */
inline fsp_header_t *fsp_get_space_header(
  ulint id, /*!< in: space id */
  mtr_t *mtr
) /*!< in: mtr */
{
  auto block = buf_page_get(id, 0, 0, RW_X_LATCH, mtr);
  auto header = FSP_HEADER_OFFSET + buf_block_get_frame(block);
  buf_block_dbg_add_level(block, SYNC_FSP_PAGE);

  ut_ad(id == mach_read_from_4(FSP_SPACE_ID + header));
  return (header);
}

/** Gets a descriptor bit of a page.
@return	true if free */
inline bool xdes_get_bit(
  const xdes_t *descr, /*!< in: descriptor */
  ulint bit,           /*!< in: XDES_FREE_BIT or XDES_CLEAN_BIT */
  ulint offset,        /*!< in: page offset within extent:
                                       0 ... FSP_EXTENT_SIZE - 1 */
  mtr_t *mtr
) /*!< in: mtr */
{
  ulint index;
  ulint byte_index;
  ulint bit_index;

  ut_ad(mtr_memo_contains_page(mtr, descr, MTR_MEMO_PAGE_X_FIX));
  ut_ad((bit == XDES_FREE_BIT) || (bit == XDES_CLEAN_BIT));
  ut_ad(offset < FSP_EXTENT_SIZE);

  index = bit + XDES_BITS_PER_PAGE * offset;

  byte_index = index / 8;
  bit_index = index % 8;

  return ut_bit_get_nth(mtr_read_ulint(descr + XDES_BITMAP + byte_index, MLOG_1BYTE, mtr), bit_index);
}

/** Sets a descriptor bit of a page. */
inline void xdes_set_bit(
  xdes_t *descr, /*!< in: descriptor */
  ulint bit,     /*!< in: XDES_FREE_BIT or XDES_CLEAN_BIT */
  ulint offset,  /*!< in: page offset within extent:
                                       0 ... FSP_EXTENT_SIZE - 1 */
  bool val,      /*!< in: bit value */
  mtr_t *mtr
) /*!< in: mtr */
{
  ulint index;
  ulint byte_index;
  ulint bit_index;
  ulint descr_byte;

  ut_ad(mtr_memo_contains_page(mtr, descr, MTR_MEMO_PAGE_X_FIX));
  ut_ad((bit == XDES_FREE_BIT) || (bit == XDES_CLEAN_BIT));
  ut_ad(offset < FSP_EXTENT_SIZE);

  index = bit + XDES_BITS_PER_PAGE * offset;

  byte_index = index / 8;
  bit_index = index % 8;

  descr_byte = mtr_read_ulint(descr + XDES_BITMAP + byte_index, MLOG_1BYTE, mtr);
  descr_byte = ut_bit_set_nth(descr_byte, bit_index, val);

  mlog_write_ulint(descr + XDES_BITMAP + byte_index, descr_byte, MLOG_1BYTE, mtr);
}

/** Looks for a descriptor bit having the desired value. Starts from hint
and scans upward; at the end of the extent the search is wrapped to
the start of the extent.
@return	bit index of the bit, ULINT_UNDEFINED if not found */
inline ulint xdes_find_bit(
  xdes_t *descr, /*!< in: descriptor */
  ulint bit,     /*!< in: XDES_FREE_BIT or XDES_CLEAN_BIT */
  bool val,      /*!< in: desired bit value */
  ulint hint,    /*!< in: hint of which bit position would be desirable */
  mtr_t *mtr
) /*!< in: mtr */
{
  ulint i;

  ut_ad(descr && mtr);
  ut_ad(hint < FSP_EXTENT_SIZE);
  ut_ad(mtr_memo_contains_page(mtr, descr, MTR_MEMO_PAGE_X_FIX));
  for (i = hint; i < FSP_EXTENT_SIZE; i++) {
    if (val == xdes_get_bit(descr, bit, i, mtr)) {

      return (i);
    }
  }

  for (i = 0; i < hint; i++) {
    if (val == xdes_get_bit(descr, bit, i, mtr)) {

      return (i);
    }
  }

  return (ULINT_UNDEFINED);
}

/** Returns the number of used pages in a descriptor.
@return	number of pages used */
inline ulint xdes_get_n_used(
  const xdes_t *descr, /*!< in: descriptor */
  mtr_t *mtr
) /*!< in: mtr */
{
  ulint i;
  ulint count = 0;

  ut_ad(descr && mtr);
  ut_ad(mtr_memo_contains_page(mtr, descr, MTR_MEMO_PAGE_X_FIX));
  for (i = 0; i < FSP_EXTENT_SIZE; i++) {
    if (false == xdes_get_bit(descr, XDES_FREE_BIT, i, mtr)) {
      count++;
    }
  }

  return (count);
}

/** Returns true if extent contains no used pages.
@return	true if totally free */
inline bool xdes_is_free(
  const xdes_t *descr, /*!< in: descriptor */
  mtr_t *mtr
) /*!< in: mtr */
{
  if (0 == xdes_get_n_used(descr, mtr)) {

    return (true);
  }

  return (false);
}

/** Returns true if extent contains no free pages.
@return	true if full */
inline bool xdes_is_full(
  const xdes_t *descr, /*!< in: descriptor */
  mtr_t *mtr
) /*!< in: mtr */
{
  if (FSP_EXTENT_SIZE == xdes_get_n_used(descr, mtr)) {

    return (true);
  }

  return (false);
}

/** Sets the state of an xdes. */
inline void xdes_set_state(
  xdes_t *descr, /*!< in/out: descriptor */
  ulint state,   /*!< in: state to set */
  mtr_t *mtr
) /*!< in: mtr handle */
{
  ut_ad(descr && mtr);
  ut_ad(state >= XDES_FREE);
  ut_ad(state <= XDES_FSEG);
  ut_ad(mtr_memo_contains_page(mtr, descr, MTR_MEMO_PAGE_X_FIX));

  mlog_write_ulint(descr + XDES_STATE, state, MLOG_4BYTES, mtr);
}

/** Gets the state of an xdes.
@return	state */
inline ulint xdes_get_state(
  const xdes_t *descr, /*!< in: descriptor */
  mtr_t *mtr
) /*!< in: mtr handle */
{
  ulint state;

  ut_ad(descr && mtr);
  ut_ad(mtr_memo_contains_page(mtr, descr, MTR_MEMO_PAGE_X_FIX));

  state = mtr_read_ulint(descr + XDES_STATE, MLOG_4BYTES, mtr);
  ut_ad(state - 1 < XDES_FSEG);
  return (state);
}

/** Inits an extent descriptor to the free and clean state. */
inline void xdes_init(
  xdes_t *descr, /*!< in: descriptor */
  mtr_t *mtr
) /*!< in: mtr */
{
  ulint i;

  ut_ad(descr && mtr);
  ut_ad(mtr_memo_contains_page(mtr, descr, MTR_MEMO_PAGE_X_FIX));
  ut_ad((XDES_SIZE - XDES_BITMAP) % 4 == 0);

  for (i = XDES_BITMAP; i < XDES_SIZE; i += 4) {
    mlog_write_ulint(descr + i, 0xFFFFFFFFUL, MLOG_4BYTES, mtr);
  }

  xdes_set_state(descr, XDES_FREE, mtr);
}

/** Calculates the page where the descriptor of a page resides.
@return	descriptor page offset */
inline ulint xdes_calc_descriptor_page(ulint offset) /*!< in: page offset */
{
  return (ut_2pow_round(offset, UNIV_PAGE_SIZE));
}

/** Calculates the descriptor index within a descriptor page.
@return	descriptor index */
inline ulint xdes_calc_descriptor_index(ulint offset) /*!< in: page offset */
{
  return (ut_2pow_remainder(offset, UNIV_PAGE_SIZE) / FSP_EXTENT_SIZE);
}

/** Gets pointer to a the extent descriptor of a page. The page where the extent
descriptor resides is x-locked. If the page offset is equal to the free limit
of the space, adds new extents from above the free limit to the space free
list, if not free limit == space size. This adding is necessary to make the
descriptor defined, as they are uninitialized above the free limit.
@return pointer to the extent descriptor, nullptr if the page does not
exist in the space or if the offset exceeds the free limit */
inline xdes_t *xdes_get_descriptor_with_space_hdr(
  fsp_header_t *sp_header, /*!< in/out: space header, x-latched */
  ulint space,             /*!< in: space id */
  ulint offset,            /*!< in: page offset;
                             if equal to the free limit,
                             we try to add new extents to
                             the space free list */
  mtr_t *mtr
) /*!< in: mtr handle */
{
  ulint limit;
  ulint size;
  ulint descr_page_no;
  page_t *descr_page;

  ut_ad(mtr);
  ut_ad(mtr_memo_contains(mtr, fil_space_get_latch(space), MTR_MEMO_X_LOCK));
  ut_ad(mtr_memo_contains_page(mtr, sp_header, MTR_MEMO_PAGE_S_FIX) || mtr_memo_contains_page(mtr, sp_header, MTR_MEMO_PAGE_X_FIX));
  ut_ad(page_offset(sp_header) == FSP_HEADER_OFFSET);
  /* Read free limit and space size */
  limit = mach_read_from_4(sp_header + FSP_FREE_LIMIT);
  size = mach_read_from_4(sp_header + FSP_SIZE);

  /* If offset is >= size or > limit, return nullptr */

  if ((offset >= size) || (offset > limit)) {

    return (nullptr);
  }

  /* If offset is == limit, fill free list of the space. */

  if (offset == limit) {
    fsp_fill_free_list(false, space, sp_header, mtr);
  }

  descr_page_no = xdes_calc_descriptor_page(offset);

  if (descr_page_no == 0) {
    /* It is on the space header page */

    descr_page = page_align(sp_header);
  } else {
    buf_block_t *block;

    block = buf_page_get(space, 0, descr_page_no, RW_X_LATCH, mtr);
    buf_block_dbg_add_level(block, SYNC_FSP_PAGE);

    descr_page = buf_block_get_frame(block);
  }

  return (descr_page + XDES_ARR_OFFSET + XDES_SIZE * xdes_calc_descriptor_index(offset));
}

/** Gets pointer to a the extent descriptor of a page. The page where the
extent descriptor resides is x-locked. If the page offset is equal to
the free limit of the space, adds new extents from above the free limit
to the space free list, if not free limit == space size. This adding
is necessary to make the descriptor defined, as they are uninitialized
above the free limit.
@return pointer to the extent descriptor, nullptr if the page does not
exist in the space or if the offset exceeds the free limit */
static xdes_t *xdes_get_descriptor(
  ulint space,  /*!< in: space id */
  ulint offset, /*!< in: page offset; if equal to the free limit,
                         we try to add new extents to the space free list */
  mtr_t *mtr
) /*!< in: mtr handle */
{
  buf_block_t *block;
  fsp_header_t *sp_header;

  block = buf_page_get(space, 0, 0, RW_X_LATCH, mtr);
  buf_block_dbg_add_level(block, SYNC_FSP_PAGE);

  sp_header = FSP_HEADER_OFFSET + buf_block_get_frame(block);
  return (xdes_get_descriptor_with_space_hdr(sp_header, space, offset, mtr));
}

/** Gets pointer to a the extent descriptor if the file address
of the descriptor list node is known. The page where the
extent descriptor resides is x-locked.
@return	pointer to the extent descriptor */
inline xdes_t *xdes_lst_get_descriptor(
  ulint space,         /*!< in: space id */
  fil_addr_t lst_node, /*!< in: file address of the list
                                            node contained in the descriptor */
  mtr_t *mtr
) /*!< in: mtr handle */
{
  xdes_t *descr;

  ut_ad(mtr);
  ut_ad(mtr_memo_contains(mtr, fil_space_get_latch(space), MTR_MEMO_X_LOCK));
  descr = fut_get_ptr(space, lst_node, RW_X_LATCH, mtr) - XDES_FLST_NODE;

  return (descr);
}

/** Returns page offset of the first page in extent described by a descriptor.
@return	offset of the first page in extent */
inline ulint xdes_get_offset(xdes_t *descr) /*!< in: extent descriptor */
{
  ut_ad(descr);

  return (page_get_page_no(page_align(descr)) + ((page_offset(descr) - XDES_ARR_OFFSET) / XDES_SIZE) * FSP_EXTENT_SIZE);
}

/** Inits a file page whose prior contents should be ignored. */
static void fsp_init_file_page_low(buf_block_t *block) /*!< in: pointer to a page */
{
  page_t *page = buf_block_get_frame(block);

  block->check_index_page_at_flush = false;

  UNIV_MEM_INVALID(page, UNIV_PAGE_SIZE);

  memset(page, 0, UNIV_PAGE_SIZE);
  mach_write_to_4(page + FIL_PAGE_OFFSET, buf_block_get_page_no(block));
  memset(page + FIL_PAGE_LSN, 0, 8);
  mach_write_to_4(page + FIL_PAGE_SPACE_ID, buf_block_get_space(block));
  memset(page + UNIV_PAGE_SIZE - FIL_PAGE_END_LSN_OLD_CHKSUM, 0, 8);
  mach_write_to_4(page + FIL_PAGE_SPACE_ID, buf_block_get_space(block));
}

/** Inits a file page whose prior contents should be ignored. */
static void fsp_init_file_page(
  buf_block_t *block, /*!< in: pointer to a page */
  mtr_t *mtr
) /*!< in: mtr */
{
  fsp_init_file_page_low(block);

  mlog_write_initial_log_record(buf_block_get_frame(block), MLOG_INIT_FILE_PAGE, mtr);
}

/** Parses a redo log record of a file page init.
@return	end of log record or nullptr */

byte *fsp_parse_init_file_page(
  byte *ptr,                             /*!< in: buffer */
  byte *end_ptr __attribute__((unused)), /*!< in: buffer end */
  buf_block_t *block
) /*!< in: block or nullptr */
{
  ut_ad(ptr && end_ptr);

  if (block) {
    fsp_init_file_page_low(block);
  }

  return (ptr);
}

/** Initializes the fsp system. */

void fsp_init(void) { /* Does nothing at the moment */
}

void fsp_header_init_fields(page_t *page, ulint space_id, ulint flags) {
  /* The tablespace flags (FSP_SPACE_FLAGS) should be 0 for
  ROW_FORMAT=COMPACT (table->flags == DICT_TF_COMPACT) and
  ROW_FORMAT=REDUNDANT (table->flags == 0).  For any other
  format, the tablespace flags should equal table->flags. */
  ut_a(flags != DICT_TF_COMPACT);

  mach_write_to_4(FSP_HEADER_OFFSET + FSP_SPACE_ID + page, space_id);
  mach_write_to_4(FSP_HEADER_OFFSET + FSP_SPACE_FLAGS + page, flags);
}

void fsp_header_init(ulint space, ulint size, mtr_t *mtr) {
  ut_ad(mtr != nullptr);

  mtr_x_lock(fil_space_get_latch(space), mtr);

  auto block = buf_page_create(space, 0, mtr);
  buf_page_get(space, 0, 0, RW_X_LATCH, mtr);
  buf_block_dbg_add_level(block, SYNC_FSP_PAGE);

  /* The prior contents of the file page should be ignored */

  fsp_init_file_page(block, mtr);

  auto page = buf_block_get_frame(block);

  mlog_write_ulint(page + FIL_PAGE_TYPE, FIL_PAGE_TYPE_FSP_HDR, MLOG_2BYTES, mtr);

  auto header = FSP_HEADER_OFFSET + page;

  mlog_write_ulint(header + FSP_SPACE_ID, space, MLOG_4BYTES, mtr);
  mlog_write_ulint(header + FSP_NOT_USED, 0, MLOG_4BYTES, mtr);

  mlog_write_ulint(header + FSP_SIZE, size, MLOG_4BYTES, mtr);
  mlog_write_ulint(header + FSP_FREE_LIMIT, 0, MLOG_4BYTES, mtr);
  mlog_write_ulint(header + FSP_SPACE_FLAGS, 0, MLOG_4BYTES, mtr);
  mlog_write_ulint(header + FSP_FRAG_N_USED, 0, MLOG_4BYTES, mtr);

  flst_init(header + FSP_FREE, mtr);
  flst_init(header + FSP_FREE_FRAG, mtr);
  flst_init(header + FSP_FULL_FRAG, mtr);
  flst_init(header + FSP_SEG_INODES_FULL, mtr);
  flst_init(header + FSP_SEG_INODES_FREE, mtr);

  mlog_write_uint64(header + FSP_SEG_ID, 1, mtr);

  fsp_fill_free_list(space != SYS_TABLESPACE, space, header, mtr);
}

ulint fsp_header_get_space_id(const page_t *page) {
  auto fsp_id = mach_read_from_4(FSP_HEADER_OFFSET + page + FSP_SPACE_ID);
  auto id = mach_read_from_4(page + FIL_PAGE_SPACE_ID);

  if (id != fsp_id) {
    ib_logger(
      ib_stream,
      "Error: space id in fsp header %lu,"
      " but in the page header %lu\n",
      (ulong)fsp_id,
      (ulong)id
    );

    return ULINT_UNDEFINED;
  } else {
    return id;
  }
}

ulint fsp_header_get_flags(const page_t *page) {
  ut_ad(!page_offset(page));

  return mach_read_from_4(FSP_HEADER_OFFSET + FSP_SPACE_FLAGS + page);
}

void fsp_header_inc_size(ulint space, ulint size_inc, mtr_t *mtr) {
  fsp_header_t *header;
  ulint size;

  ut_ad(mtr);

  mtr_x_lock(fil_space_get_latch(space), mtr);

  header = fsp_get_space_header(space, mtr);

  size = mtr_read_ulint(header + FSP_SIZE, MLOG_4BYTES, mtr);

  mlog_write_ulint(header + FSP_SIZE, size + size_inc, MLOG_4BYTES, mtr);
}

ulint fsp_header_get_free_limit() {
  mtr_t mtr;

  mtr_start(&mtr);

  mtr_x_lock(fil_space_get_latch(0), &mtr);

  auto header = fsp_get_space_header(0, &mtr);

  auto limit = mtr_read_ulint(header + FSP_FREE_LIMIT, MLOG_4BYTES, &mtr);

  limit /= ((1024 * 1024) / UNIV_PAGE_SIZE);

  log_fsp_current_free_limit_set_and_checkpoint(limit);

  mtr_commit(&mtr);

  return (limit);
}

ulint fsp_header_get_tablespace_size() {
  fsp_header_t *header;
  ulint size;
  mtr_t mtr;

  mtr_start(&mtr);

  mtr_x_lock(fil_space_get_latch(0), &mtr);

  header = fsp_get_space_header(0, &mtr);

  size = mtr_read_ulint(header + FSP_SIZE, MLOG_4BYTES, &mtr);

  mtr_commit(&mtr);

  return (size);
}

/** Tries to extend a single-table tablespace so that a page would fit in the
data file.
@return	true if success */
static bool fsp_try_extend_data_file_with_pages(
  ulint space,          /*!< in: space */
  ulint page_no,        /*!< in: page number */
  fsp_header_t *header, /*!< in: space header */
  mtr_t *mtr
) /*!< in: mtr */
{
  bool success;
  ulint actual_size;
  ulint size;

  ut_a(space != 0);

  size = mtr_read_ulint(header + FSP_SIZE, MLOG_4BYTES, mtr);

  ut_a(page_no >= size);

  success = fil_extend_space_to_desired_size(&actual_size, space, page_no + 1);
  /* actual_size now has the space size in pages; it may be less than
  we wanted if we ran out of disk space */

  mlog_write_ulint(header + FSP_SIZE, actual_size, MLOG_4BYTES, mtr);

  return (success);
}

/** Tries to extend the last data file of a tablespace if it is auto-extending.
@return	false if not auto-extending */
static bool fsp_try_extend_data_file(
  ulint *actual_increase, /*!< out: actual increase in pages, where
                            we measure the tablespace size from
                            what the header field says; it may be
                            the actual file size rounded down to
                            megabyte */
  ulint space,            /*!< in: space */
  fsp_header_t *header,   /*!< in: space header */
  mtr_t *mtr
) /*!< in: mtr */
{
  ulint size;
  ulint new_size;
  ulint old_size;
  ulint size_increase;
  ulint actual_size;
  bool success;

  *actual_increase = 0;

  if (space == 0 && !srv_auto_extend_last_data_file) {

    /* We print the error message only once to avoid
    spamming the error log. Note that we don't need
    to reset the flag to false as dealing with this
    error requires server restart. */
    if (fsp_tbs_full_error_printed == false) {
      ib_logger(
        ib_stream,
        "Error: Data file(s) ran"
        " out of space.\n"
        "Please add another data file or"
        " use \'autoextend\' for the last"
        " data file.\n"
      );
      fsp_tbs_full_error_printed = true;
    }
    return (false);
  }

  size = mtr_read_ulint(header + FSP_SIZE, MLOG_4BYTES, mtr);

  old_size = size;

  if (space == 0) {
    if (!srv_last_file_size_max) {
      size_increase = SRV_AUTO_EXTEND_INCREMENT;
    } else {
      if (srv_last_file_size_max < srv_data_file_sizes[srv_n_data_files - 1]) {

        ib_logger(
          ib_stream,
          "Error: Last data file size"
          " is %lu, max size allowed %lu\n",
          (ulong)srv_data_file_sizes[srv_n_data_files - 1],
          (ulong)srv_last_file_size_max
        );
      }

      size_increase = srv_last_file_size_max - srv_data_file_sizes[srv_n_data_files - 1];
      if (size_increase > SRV_AUTO_EXTEND_INCREMENT) {
        size_increase = SRV_AUTO_EXTEND_INCREMENT;
      }
    }
  } else {
    /* We extend single-table tablespaces first one extent
    at a time, but for bigger tablespaces more. It is not
    enough to extend always by one extent, because some
    extents are frag page extents. */
    ulint extent_size; /*!< one megabyte, in pages */

    extent_size = FSP_EXTENT_SIZE;

    if (size < extent_size) {
      /* Let us first extend the file to extent_size */
      success = fsp_try_extend_data_file_with_pages(space, extent_size - 1, header, mtr);
      if (!success) {
        new_size = mtr_read_ulint(header + FSP_SIZE, MLOG_4BYTES, mtr);

        *actual_increase = new_size - old_size;

        return (false);
      }

      size = extent_size;
    }

    if (size < 32 * extent_size) {
      size_increase = extent_size;
    } else {
      /* Below in fsp_fill_free_list() we assume
      that we add at most FSP_FREE_ADD extents at
      a time */
      size_increase = FSP_FREE_ADD * extent_size;
    }
  }

  if (size_increase == 0) {

    return (true);
  }

  success = fil_extend_space_to_desired_size(&actual_size, space, size + size_increase);
  /* We ignore any fragments of a full megabyte when storing the size
  to the space header */

  new_size = ut_calc_align_down(actual_size, (1024 * 1024) / UNIV_PAGE_SIZE);
  mlog_write_ulint(header + FSP_SIZE, new_size, MLOG_4BYTES, mtr);

  *actual_increase = new_size - old_size;

  return (true);
}

/** Puts new extents to the free list if there are free extents above the free
limit. If an extent happens to contain an extent descriptor page, the extent
is put to the FSP_FREE_FRAG list with the page marked as used.
@param[in] init_space           true if this is a single-table tablespace and
                                we are only initing the tablespace's first
				extent descriptor page; then we do not allocate
				more extents
@param[in] space                Tablespace
@param[in,out] header           Tablespace header page
@param[in] mtr                  Mini-transaction covering the operation.  */
static void fsp_fill_free_list(bool init_space, space_id_t space, fsp_header_t *header, mtr_t *mtr) {
  ut_ad(mtr != nullptr);
  ut_ad(header != nullptr);
  ut_ad(page_offset(header) == FSP_HEADER_OFFSET);

  /* Check if we can fill free list from above the free list limit */
  auto size = mtr_read_ulint(header + FSP_SIZE, MLOG_4BYTES, mtr);
  auto limit = mtr_read_ulint(header + FSP_FREE_LIMIT, MLOG_4BYTES, mtr);
  bool extend_file = size < limit + FSP_EXTENT_SIZE * FSP_FREE_ADD;

  if (space == SYS_TABLESPACE) {
    extend_file = extend_file && srv_auto_extend_last_data_file;
  } else {
    extend_file = extend_file && !init_space;
  }

  if (extend_file) {
    ulint actual_increase{};

    fsp_try_extend_data_file(&actual_increase, space, header, mtr);
    size = mtr_read_ulint(header + FSP_SIZE, MLOG_4BYTES, mtr);
  }

  ulint count{};
  auto i = limit;

  while ((init_space && i < 1) || (i + FSP_EXTENT_SIZE <= size && count < FSP_FREE_ADD)) {

    auto init_xdes = ut_2pow_remainder(i, UNIV_PAGE_SIZE) == 0;

    mlog_write_ulint(header + FSP_FREE_LIMIT, i + FSP_EXTENT_SIZE, MLOG_4BYTES, mtr);

    /* Update the free limit info in the log system and make
    a checkpoint */
    if (space == SYS_TABLESPACE) {
      log_fsp_current_free_limit_set_and_checkpoint((i + FSP_EXTENT_SIZE) / ((1024 * 1024) / UNIV_PAGE_SIZE));
    }

    if (unlikely(init_xdes)) {

      buf_block_t *block;

      /* We are going to initialize a new descriptor page:
      the prior contents of the pages should be ignored. */

      if (i > 0) {
        block = buf_page_create(space, i, mtr);

        buf_page_get(space, 0, i, RW_X_LATCH, mtr);

        buf_block_dbg_add_level(block, SYNC_FSP_PAGE);

        fsp_init_file_page(block, mtr);

        mlog_write_ulint(buf_block_get_frame(block) + FIL_PAGE_TYPE, FIL_PAGE_TYPE_XDES, MLOG_2BYTES, mtr);
      }
    }

    auto descr = xdes_get_descriptor_with_space_hdr(header, space, i, mtr);
    xdes_init(descr, mtr);

    static_assert(!(UNIV_PAGE_SIZE % FSP_EXTENT_SIZE), "error UNIV_PAGE_SIZE % FSP_EXTENT_SIZE != 0");

    if (unlikely(init_xdes)) {

      /* The first page in the extent is a descriptor page
      mark them used. */

      xdes_set_bit(descr, XDES_FREE_BIT, 0, false, mtr);
      xdes_set_state(descr, XDES_FREE_FRAG, mtr);

      flst_add_last(header + FSP_FREE_FRAG, descr + XDES_FLST_NODE, mtr);

      const auto frag_n_used = mtr_read_ulint(header + FSP_FRAG_N_USED, MLOG_4BYTES, mtr);
      mlog_write_ulint(header + FSP_FRAG_N_USED, frag_n_used + 2, MLOG_4BYTES, mtr);
    } else {
      flst_add_last(header + FSP_FREE, descr + XDES_FLST_NODE, mtr);
      count++;
    }

    i += FSP_EXTENT_SIZE;
  }
}

/** Allocates a new free extent.
@return	extent descriptor, nullptr if cannot be allocated */
static xdes_t *fsp_alloc_free_extent(
  ulint space, /*!< in: space id */
  ulint hint,  /*!< in: hint of which extent would be
                       desirable: any     page offset in the extent goes;
                       the hint must not     be > FSP_FREE_LIMIT */
  mtr_t *mtr
) /*!< in: mtr */
{
  ut_ad(mtr);

  auto header = fsp_get_space_header(space, mtr);
  auto descr = xdes_get_descriptor_with_space_hdr(header, space, hint, mtr);

  if (descr && (xdes_get_state(descr, mtr) == XDES_FREE)) {
    /* Ok, we can take this extent */
  } else {
    /* Take the first extent in the free list */
    auto first = flst_get_first(header + FSP_FREE, mtr);

    if (fil_addr_is_null(first)) {
      fsp_fill_free_list(false, space, header, mtr);

      first = flst_get_first(header + FSP_FREE, mtr);
    }

    if (fil_addr_is_null(first)) {

      return (nullptr); /* No free extents left */
    }

    descr = xdes_lst_get_descriptor(space, first, mtr);
  }

  flst_remove(header + FSP_FREE, descr + XDES_FLST_NODE, mtr);

  return (descr);
}

/** Allocates a single free page from a space. The page is marked as used.
@return	the page offset, FIL_NULL if no page could be allocated */
static ulint fsp_alloc_free_page(
  ulint space, /*!< in: space id */
  ulint hint,  /*!< in: hint of which page would be desirable */
  mtr_t *mtr
) /*!< in: mtr handle */
{
  fsp_header_t *header;
  fil_addr_t first;
  xdes_t *descr;
  buf_block_t *block;
  ulint free;
  ulint frag_n_used;
  ulint page_no;
  ulint space_size;
  bool success;

  ut_ad(mtr);

  header = fsp_get_space_header(space, mtr);

  /* Get the hinted descriptor */
  descr = xdes_get_descriptor_with_space_hdr(header, space, hint, mtr);

  if (descr && (xdes_get_state(descr, mtr) == XDES_FREE_FRAG)) {
    /* Ok, we can take this extent */
  } else {
    /* Else take the first extent in free_frag list */
    first = flst_get_first(header + FSP_FREE_FRAG, mtr);

    if (fil_addr_is_null(first)) {
      /* There are no partially full fragments: allocate
      a free extent and add it to the FREE_FRAG list. NOTE
      that the allocation may have as a side-effect that an
      extent containing a descriptor page is added to the
      FREE_FRAG list. But we will allocate our page from the
      the free extent anyway. */

      descr = fsp_alloc_free_extent(space, hint, mtr);

      if (descr == nullptr) {
        /* No free space left */

        return (FIL_NULL);
      }

      xdes_set_state(descr, XDES_FREE_FRAG, mtr);
      flst_add_last(header + FSP_FREE_FRAG, descr + XDES_FLST_NODE, mtr);
    } else {
      descr = xdes_lst_get_descriptor(space, first, mtr);
    }

    /* Reset the hint */
    hint = 0;
  }

  /* Now we have in descr an extent with at least one free page. Look
  for a free page in the extent. */

  free = xdes_find_bit(descr, XDES_FREE_BIT, true, hint % FSP_EXTENT_SIZE, mtr);
  if (free == ULINT_UNDEFINED) {

    ut_print_buf(ib_stream, ((byte *)descr) - 500, 1000);
    ib_logger(ib_stream, "\n");

    ut_error;
  }

  page_no = xdes_get_offset(descr) + free;

  space_size = mtr_read_ulint(header + FSP_SIZE, MLOG_4BYTES, mtr);

  if (space_size <= page_no) {
    /* It must be that we are extending a single-table tablespace
    whose size is still < 64 pages */

    ut_a(space != 0);
    if (page_no >= FSP_EXTENT_SIZE) {
      ib_logger(
        ib_stream,
        "Error: trying to extend a"
        " single-table tablespace %lu\n"
        "by single page(s) though the"
        " space size %lu. Page no %lu.\n",
        (ulong)space,
        (ulong)space_size,
        (ulong)page_no
      );
      return (FIL_NULL);
    }
    success = fsp_try_extend_data_file_with_pages(space, page_no, header, mtr);
    if (!success) {
      /* No disk space left */
      return (FIL_NULL);
    }
  }

  xdes_set_bit(descr, XDES_FREE_BIT, free, false, mtr);

  /* Update the FRAG_N_USED field */
  frag_n_used = mtr_read_ulint(header + FSP_FRAG_N_USED, MLOG_4BYTES, mtr);
  frag_n_used++;
  mlog_write_ulint(header + FSP_FRAG_N_USED, frag_n_used, MLOG_4BYTES, mtr);
  if (xdes_is_full(descr, mtr)) {
    /* The fragment is full: move it to another list */
    flst_remove(header + FSP_FREE_FRAG, descr + XDES_FLST_NODE, mtr);
    xdes_set_state(descr, XDES_FULL_FRAG, mtr);

    flst_add_last(header + FSP_FULL_FRAG, descr + XDES_FLST_NODE, mtr);
    mlog_write_ulint(header + FSP_FRAG_N_USED, frag_n_used - FSP_EXTENT_SIZE, MLOG_4BYTES, mtr);
  }

  /* Initialize the allocated page to the buffer pool, so that it can
  be obtained immediately with buf_page_get without need for a disk
  read. */

  buf_page_create(space, page_no, mtr);

  block = buf_page_get(space, 0, page_no, RW_X_LATCH, mtr);
  buf_block_dbg_add_level(block, SYNC_FSP_PAGE);

  /* Prior contents of the page should be ignored */
  fsp_init_file_page(block, mtr);

  return (page_no);
}

/** Frees a single page of a space. The page is marked as free and clean. */
static void fsp_free_page(
  ulint space, /*!< in: space id */
  ulint page,  /*!< in: page offset */
  mtr_t *mtr
) /*!< in: mtr handle */
{
  fsp_header_t *header;
  xdes_t *descr;
  ulint state;
  ulint frag_n_used;

  ut_ad(mtr);

  /* ib_logger(ib_stream,
          "Freeing page %lu in space %lu\n", page, space); */

  header = fsp_get_space_header(space, mtr);

  descr = xdes_get_descriptor_with_space_hdr(header, space, page, mtr);

  state = xdes_get_state(descr, mtr);

  if (state != XDES_FREE_FRAG && state != XDES_FULL_FRAG) {
    ib_logger(
      ib_stream,
      "Error: File space extent descriptor"
      " of page %lu has state %lu\n",
      (ulong)page,
      (ulong)state
    );
    ib_logger(ib_stream, "Dump of descriptor: ");
    ut_print_buf(ib_stream, ((byte *)descr) - 50, 200);
    ib_logger(ib_stream, "\n");

    if (state == XDES_FREE) {
      /* We put here some fault tolerance: if the page
      is already free, return without doing anything! */

      return;
    }

    ut_error;
  }

  if (xdes_get_bit(descr, XDES_FREE_BIT, page % FSP_EXTENT_SIZE, mtr)) {
    ib_logger(
      ib_stream,
      "Error: File space extent descriptor"
      " of page %lu says it is free\n"
      "Dump of descriptor: ",
      (ulong)page
    );
    ut_print_buf(ib_stream, ((byte *)descr) - 50, 200);
    ib_logger(ib_stream, "\n");

    /* We put here some fault tolerance: if the page
    is already free, return without doing anything! */

    return;
  }

  xdes_set_bit(descr, XDES_FREE_BIT, page % FSP_EXTENT_SIZE, true, mtr);
  xdes_set_bit(descr, XDES_CLEAN_BIT, page % FSP_EXTENT_SIZE, true, mtr);

  frag_n_used = mtr_read_ulint(header + FSP_FRAG_N_USED, MLOG_4BYTES, mtr);
  if (state == XDES_FULL_FRAG) {
    /* The fragment was full: move it to another list */
    flst_remove(header + FSP_FULL_FRAG, descr + XDES_FLST_NODE, mtr);
    xdes_set_state(descr, XDES_FREE_FRAG, mtr);
    flst_add_last(header + FSP_FREE_FRAG, descr + XDES_FLST_NODE, mtr);
    mlog_write_ulint(header + FSP_FRAG_N_USED, frag_n_used + FSP_EXTENT_SIZE - 1, MLOG_4BYTES, mtr);
  } else {
    ut_a(frag_n_used > 0);
    mlog_write_ulint(header + FSP_FRAG_N_USED, frag_n_used - 1, MLOG_4BYTES, mtr);
  }

  if (xdes_is_free(descr, mtr)) {
    /* The extent has become free: move it to another list */
    flst_remove(header + FSP_FREE_FRAG, descr + XDES_FLST_NODE, mtr);
    fsp_free_extent(space, page, mtr);
  }
}

/** Returns an extent to the free list of a space. */
static void fsp_free_extent(
  ulint space, /*!< in: space id */
  ulint page,  /*!< in: page offset in the extent */
  mtr_t *mtr
) /*!< in: mtr */
{
  fsp_header_t *header;
  xdes_t *descr;

  ut_ad(mtr);

  header = fsp_get_space_header(space, mtr);

  descr = xdes_get_descriptor_with_space_hdr(header, space, page, mtr);

  if (xdes_get_state(descr, mtr) == XDES_FREE) {

    ut_print_buf(ib_stream, (byte *)descr - 500, 1000);
    ib_logger(ib_stream, "\n");

    ut_error;
  }

  xdes_init(descr, mtr);

  flst_add_last(header + FSP_FREE, descr + XDES_FLST_NODE, mtr);
}

/** Returns the nth inode slot on an inode page.
@return	segment inode */
inline fseg_inode_t *fsp_seg_inode_page_get_nth_inode(
  page_t *page, /*!< in: segment inode page */
  ulint i,      /*!< in: inode index on page */
  mtr_t *mtr __attribute__((unused))
)
/*!< in: mini-transaction handle */
{
  ut_ad(i < FSP_SEG_INODES_PER_PAGE);
  ut_ad(mtr_memo_contains_page(mtr, page, MTR_MEMO_PAGE_X_FIX));

  return (page + FSEG_ARR_OFFSET + FSEG_INODE_SIZE * i);
}

/** Looks for a used segment inode on a segment inode page.
@return	segment inode index, or ULINT_UNDEFINED if not found */
static ulint fsp_seg_inode_page_find_used(
  page_t *page, /*!< in: segment inode page */
  mtr_t *mtr
) /*!< in: mini-transaction handle */
{
  ulint i;
  fseg_inode_t *inode;

  for (i = 0; i < FSP_SEG_INODES_PER_PAGE; i++) {

    inode = fsp_seg_inode_page_get_nth_inode(page, i, mtr);

    if (mach_read_from_8(inode + FSEG_ID) > 0) {
      /* This is used */

      ut_ad(mach_read_from_4(inode + FSEG_MAGIC_N) == FSEG_MAGIC_N_VALUE);
      return (i);
    }
  }

  return (ULINT_UNDEFINED);
}

/** Looks for an unused segment inode on a segment inode page.
@return	segment inode index, or ULINT_UNDEFINED if not found */
static ulint fsp_seg_inode_page_find_free(
  page_t *page, /*!< in: segment inode page */
  ulint i,      /*!< in: search forward starting from this index */
  mtr_t *mtr
) /*!< in: mini-transaction handle */
{
  fseg_inode_t *inode;

  for (; i < FSP_SEG_INODES_PER_PAGE; i++) {

    inode = fsp_seg_inode_page_get_nth_inode(page, i, mtr);

    if (mach_read_from_8(inode + FSEG_ID) == 0) {
      /* This is unused */

      return i;
    }

    ut_ad(mach_read_from_4(inode + FSEG_MAGIC_N) == FSEG_MAGIC_N_VALUE);
  }

  return ULINT_UNDEFINED;
}

/** Allocates a new file segment inode page.
@return	true if could be allocated */
static bool fsp_alloc_seg_inode_page(
  fsp_header_t *space_header, /*!< in: space header */
  mtr_t *mtr
) /*!< in: mini-transaction handle */
{
  fseg_inode_t *inode;
  buf_block_t *block;
  page_t *page;
  ulint page_no;
  ulint space;
  ulint i;

  ut_ad(page_offset(space_header) == FSP_HEADER_OFFSET);

  space = page_get_space_id(page_align(space_header));

  page_no = fsp_alloc_free_page(space, 0, mtr);

  if (page_no == FIL_NULL) {

    return (false);
  }

  block = buf_page_get(space, 0, page_no, RW_X_LATCH, mtr);
  buf_block_dbg_add_level(block, SYNC_FSP_PAGE);

  block->check_index_page_at_flush = false;

  page = buf_block_get_frame(block);

  mlog_write_ulint(page + FIL_PAGE_TYPE, FIL_PAGE_INODE, MLOG_2BYTES, mtr);

  for (i = 0; i < FSP_SEG_INODES_PER_PAGE; i++) {

    inode = fsp_seg_inode_page_get_nth_inode(page, i, mtr);

    mlog_write_uint64(inode + FSEG_ID, 0, mtr);
  }

  flst_add_last(space_header + FSP_SEG_INODES_FREE, page + FSEG_INODE_PAGE_NODE, mtr);
  return (true);
}

/** Allocates a new file segment inode.
@return	segment inode, or nullptr if not enough space */
static fseg_inode_t *fsp_alloc_seg_inode(
  fsp_header_t *space_header, /*!< in: space header */
  mtr_t *mtr
) /*!< in: mini-transaction handle */
{
  ulint page_no;
  buf_block_t *block;
  page_t *page;
  fseg_inode_t *inode;
  bool success;
  ulint n;

  ut_ad(page_offset(space_header) == FSP_HEADER_OFFSET);

  if (flst_get_len(space_header + FSP_SEG_INODES_FREE, mtr) == 0) {
    /* Allocate a new segment inode page */

    success = fsp_alloc_seg_inode_page(space_header, mtr);

    if (!success) {

      return (nullptr);
    }
  }

  page_no = flst_get_first(space_header + FSP_SEG_INODES_FREE, mtr).page;

  block = buf_page_get(page_get_space_id(page_align(space_header)), 0, page_no, RW_X_LATCH, mtr);
  buf_block_dbg_add_level(block, SYNC_FSP_PAGE);

  page = buf_block_get_frame(block);

  n = fsp_seg_inode_page_find_free(page, 0, mtr);

  ut_a(n != ULINT_UNDEFINED);

  inode = fsp_seg_inode_page_get_nth_inode(page, n, mtr);

  if (ULINT_UNDEFINED == fsp_seg_inode_page_find_free(page, n + 1, mtr)) {
    /* There are no other unused headers left on the page: move it
    to another list */

    flst_remove(space_header + FSP_SEG_INODES_FREE, page + FSEG_INODE_PAGE_NODE, mtr);

    flst_add_last(space_header + FSP_SEG_INODES_FULL, page + FSEG_INODE_PAGE_NODE, mtr);
  }

  ut_ad(mach_read_from_8(inode + FSEG_ID) == 0 || mach_read_from_4(inode + FSEG_MAGIC_N) == FSEG_MAGIC_N_VALUE);
  return (inode);
}

/** Frees a file segment inode. */
static void fsp_free_seg_inode(
  ulint space,         /*!< in: space id */
  fseg_inode_t *inode, /*!< in: segment inode */
  mtr_t *mtr
) /*!< in: mini-transaction handle */
{
  page_t *page;
  fsp_header_t *space_header;

  page = page_align(inode);

  space_header = fsp_get_space_header(space, mtr);

  ut_ad(mach_read_from_4(inode + FSEG_MAGIC_N) == FSEG_MAGIC_N_VALUE);

  if (ULINT_UNDEFINED == fsp_seg_inode_page_find_free(page, 0, mtr)) {

    /* Move the page to another list */

    flst_remove(space_header + FSP_SEG_INODES_FULL, page + FSEG_INODE_PAGE_NODE, mtr);

    flst_add_last(space_header + FSP_SEG_INODES_FREE, page + FSEG_INODE_PAGE_NODE, mtr);
  }

  mlog_write_uint64(inode + FSEG_ID, 0, mtr);
  mlog_write_ulint(inode + FSEG_MAGIC_N, 0xfa051ce3, MLOG_4BYTES, mtr);

  if (ULINT_UNDEFINED == fsp_seg_inode_page_find_used(page, mtr)) {

    /* There are no other used headers left on the page: free it */

    flst_remove(space_header + FSP_SEG_INODES_FREE, page + FSEG_INODE_PAGE_NODE, mtr);

    fsp_free_page(space, page_get_page_no(page), mtr);
  }
}

/** Returns the file segment inode, page x-latched.
@return	segment inode, page x-latched; nullptr if the inode is free */
static fseg_inode_t *fseg_inode_try_get(
  fseg_header_t *header, /*!< in: segment header */
  ulint space,           /*!< in: space id */
  mtr_t *mtr
) /*!< in: mtr handle */
{
  fil_addr_t inode_addr;
  fseg_inode_t *inode;

  inode_addr.page = mach_read_from_4(header + FSEG_HDR_PAGE_NO);
  inode_addr.boffset = mach_read_from_2(header + FSEG_HDR_OFFSET);
  ut_ad(space == mach_read_from_4(header + FSEG_HDR_SPACE));

  inode = fut_get_ptr(space, inode_addr, RW_X_LATCH, mtr);

  if (unlikely(mach_read_from_8(inode + FSEG_ID) == 0)) {

    inode = nullptr;
  } else {
    ut_ad(mach_read_from_4(inode + FSEG_MAGIC_N) == FSEG_MAGIC_N_VALUE);
  }

  return (inode);
}

/** Returns the file segment inode, page x-latched.
@return	segment inode, page x-latched */
static fseg_inode_t *fseg_inode_get(
  fseg_header_t *header, /*!< in: segment header */
  ulint space,           /*!< in: space id */
  mtr_t *mtr
) /*!< in: mtr handle */
{
  fseg_inode_t *inode = fseg_inode_try_get(header, space, mtr);
  ut_a(inode);
  return (inode);
}

/** Gets the page number from the nth fragment page slot.
@return	page number, FIL_NULL if not in use */
inline ulint fseg_get_nth_frag_page_no(
  fseg_inode_t *inode, /*!< in: segment inode */
  ulint n,             /*!< in: slot index */
  mtr_t *mtr __attribute__((unused))
) /*!< in: mtr handle */
{
  ut_ad(inode && mtr);
  ut_ad(n < FSEG_FRAG_ARR_N_SLOTS);
  ut_ad(mtr_memo_contains_page(mtr, inode, MTR_MEMO_PAGE_X_FIX));
  ut_ad(mach_read_from_4(inode + FSEG_MAGIC_N) == FSEG_MAGIC_N_VALUE);
  return (mach_read_from_4(inode + FSEG_FRAG_ARR + n * FSEG_FRAG_SLOT_SIZE));
}

/** Sets the page number in the nth fragment page slot. */
inline void fseg_set_nth_frag_page_no(
  fseg_inode_t *inode, /*!< in: segment inode */
  ulint n,             /*!< in: slot index */
  ulint page_no,       /*!< in: page number to set */
  mtr_t *mtr
) /*!< in: mtr handle */
{
  ut_ad(inode && mtr);
  ut_ad(n < FSEG_FRAG_ARR_N_SLOTS);
  ut_ad(mtr_memo_contains_page(mtr, inode, MTR_MEMO_PAGE_X_FIX));
  ut_ad(mach_read_from_4(inode + FSEG_MAGIC_N) == FSEG_MAGIC_N_VALUE);

  mlog_write_ulint(inode + FSEG_FRAG_ARR + n * FSEG_FRAG_SLOT_SIZE, page_no, MLOG_4BYTES, mtr);
}

/** Finds a fragment page slot which is free.
@return	slot index; ULINT_UNDEFINED if none found */
static ulint fseg_find_free_frag_page_slot(
  fseg_inode_t *inode, /*!< in: segment inode */
  mtr_t *mtr
) /*!< in: mtr handle */
{
  ulint i;
  ulint page_no;

  ut_ad(inode && mtr);

  for (i = 0; i < FSEG_FRAG_ARR_N_SLOTS; i++) {
    page_no = fseg_get_nth_frag_page_no(inode, i, mtr);

    if (page_no == FIL_NULL) {

      return (i);
    }
  }

  return (ULINT_UNDEFINED);
}

/** Finds a fragment page slot which is used and last in the array.
@return	slot index; ULINT_UNDEFINED if none found */
static ulint fseg_find_last_used_frag_page_slot(
  fseg_inode_t *inode, /*!< in: segment inode */
  mtr_t *mtr
) /*!< in: mtr handle */
{
  ulint i;
  ulint page_no;

  ut_ad(inode && mtr);

  for (i = 0; i < FSEG_FRAG_ARR_N_SLOTS; i++) {
    page_no = fseg_get_nth_frag_page_no(inode, FSEG_FRAG_ARR_N_SLOTS - i - 1, mtr);

    if (page_no != FIL_NULL) {

      return (FSEG_FRAG_ARR_N_SLOTS - i - 1);
    }
  }

  return (ULINT_UNDEFINED);
}

/** Calculates reserved fragment page slots.
@return	number of fragment pages */
static ulint fseg_get_n_frag_pages(
  fseg_inode_t *inode, /*!< in: segment inode */
  mtr_t *mtr
) /*!< in: mtr handle */
{
  ulint i;
  ulint count = 0;

  ut_ad(inode && mtr);

  for (i = 0; i < FSEG_FRAG_ARR_N_SLOTS; i++) {
    if (FIL_NULL != fseg_get_nth_frag_page_no(inode, i, mtr)) {
      count++;
    }
  }

  return (count);
}

buf_block_t *fseg_create_general(space_id_t space_id, page_no_t page_no, ulint byte_offset, bool has_done_reservation, mtr_t *mtr) {
  ut_ad(mtr != nullptr);
  ut_ad(byte_offset + FSEG_HEADER_SIZE <= UNIV_PAGE_SIZE - FIL_PAGE_DATA_END);

  auto latch = fil_space_get_latch(space_id);

  buf_block_t *block{};
  fseg_header_t *header{};

  if (page_no != 0) {
    block = buf_page_get(space_id, 0, page_no, RW_X_LATCH, mtr);
    header = byte_offset + buf_block_get_frame(block);
  }

  ut_ad(!mutex_own(&kernel_mutex) || mtr_memo_contains(mtr, latch, MTR_MEMO_X_LOCK));

  mtr_x_lock(latch, mtr);

  ulint n_reserved{};

  if (!has_done_reservation) {
    auto success = fsp_reserve_free_extents(&n_reserved, space_id, 2, FSP_NORMAL, mtr);

    if (!success) {
      return nullptr;
    }
  }

  auto space_header = fsp_get_space_header(space_id, mtr);
  auto inode = fsp_alloc_seg_inode(space_header, mtr);

  if (inode != nullptr) {

    /* Read the next segment id from space header and increment the
    value in space header */

    auto seg_id = mtr_read_uint64(space_header + FSP_SEG_ID, mtr);

    mlog_write_uint64(space_header + FSP_SEG_ID, seg_id + 1, mtr);

    mlog_write_uint64(inode + FSEG_ID, seg_id, mtr);
    mlog_write_ulint(inode + FSEG_NOT_FULL_N_USED, 0, MLOG_4BYTES, mtr);

    flst_init(inode + FSEG_FREE, mtr);
    flst_init(inode + FSEG_NOT_FULL, mtr);
    flst_init(inode + FSEG_FULL, mtr);

    mlog_write_ulint(inode + FSEG_MAGIC_N, FSEG_MAGIC_N_VALUE, MLOG_4BYTES, mtr);

    for (ulint i = 0; i < FSEG_FRAG_ARR_N_SLOTS; i++) {
      fseg_set_nth_frag_page_no(inode, i, FIL_NULL, mtr);
    }

    if (page_no == 0) {
      page_no = fseg_alloc_free_page_low(space_id, inode, page_no, FSP_UP, mtr);

      if (unlikely(page_no != FIL_NULL)) {

        block = buf_page_get(space_id, 0, page_no, RW_X_LATCH, mtr);

        header = byte_offset + buf_block_get_frame(block);

        mlog_write_ulint(header - byte_offset + FIL_PAGE_TYPE, FIL_PAGE_TYPE_SYS, MLOG_2BYTES, mtr);
      } else {

        fsp_free_seg_inode(space_id, inode, mtr);

        if (!has_done_reservation) {

          fil_space_release_free_extents(space_id, n_reserved);
        }

        return nullptr;
      }
    }

    mlog_write_ulint(header + FSEG_HDR_OFFSET, page_offset(inode), MLOG_2BYTES, mtr);

    mlog_write_ulint(header + FSEG_HDR_PAGE_NO, page_get_page_no(page_align(inode)), MLOG_4BYTES, mtr);

    mlog_write_ulint(header + FSEG_HDR_SPACE, space_id, MLOG_4BYTES, mtr);
  }

  if (!has_done_reservation) {

    fil_space_release_free_extents(space_id, n_reserved);
  }

  return block;
}

buf_block_t *fseg_create(space_id_t space, page_no_t page, ulint byte_offset, mtr_t *mtr) {
  return fseg_create_general(space, page, byte_offset, false, mtr);
}

/** Calculates the number of pages reserved by a segment, and how many pages are
currently used.
@return	number of reserved pages */
static ulint fseg_n_reserved_pages_low(
  fseg_inode_t *inode, /*!< in: segment inode */
  ulint *used,         /*!< out: number of pages used (not
                                       more than reserved) */
  mtr_t *mtr
) /*!< in: mtr handle */
{
  ulint ret;

  ut_ad(inode && used && mtr);
  ut_ad(mtr_memo_contains_page(mtr, inode, MTR_MEMO_PAGE_X_FIX));

  *used = mtr_read_ulint(inode + FSEG_NOT_FULL_N_USED, MLOG_4BYTES, mtr) + FSP_EXTENT_SIZE * flst_get_len(inode + FSEG_FULL, mtr) +
          fseg_get_n_frag_pages(inode, mtr);

  ret = fseg_get_n_frag_pages(inode, mtr) + FSP_EXTENT_SIZE * flst_get_len(inode + FSEG_FREE, mtr) +
        FSP_EXTENT_SIZE * flst_get_len(inode + FSEG_NOT_FULL, mtr) + FSP_EXTENT_SIZE * flst_get_len(inode + FSEG_FULL, mtr);

  return (ret);
}

ulint fseg_n_reserved_pages(fseg_header_t *header, ulint *used, mtr_t *mtr) {
  auto space = page_get_space_id(page_align(header));
  auto latch = fil_space_get_latch(space);

  ut_ad(!mutex_own(&kernel_mutex) || mtr_memo_contains(mtr, latch, MTR_MEMO_X_LOCK));

  mtr_x_lock(latch, mtr);

  auto inode = fseg_inode_get(header, space, mtr);

  return fseg_n_reserved_pages_low(inode, used, mtr);
}

/** Tries to fill the free list of a segment with consecutive free extents.
This happens if the segment is big enough to allow extents in the free list,
the free list is empty, and the extents can be allocated consecutively from
the hint onward. */
static void fseg_fill_free_list(
  fseg_inode_t *inode, /*!< in: segment inode */
  ulint space,         /*!< in: space id */
  ulint hint,          /*!< in: hint which extent would be
                                            good as the first extent */
  mtr_t *mtr
) /*!< in: mtr */
{
  xdes_t *descr;
  ulint i;
  uint64_t seg_id;
  ulint reserved;
  ulint used;

  ut_ad(inode && mtr);
  ut_ad(!((page_offset(inode) - FSEG_ARR_OFFSET) % FSEG_INODE_SIZE));

  reserved = fseg_n_reserved_pages_low(inode, &used, mtr);

  if (reserved < FSEG_FREE_LIST_LIMIT * FSP_EXTENT_SIZE) {

    /* The segment is too small to allow extents in free list */

    return;
  }

  if (flst_get_len(inode + FSEG_FREE, mtr) > 0) {
    /* Free list is not empty */

    return;
  }

  for (i = 0; i < FSEG_FREE_LIST_MAX_LEN; i++) {
    descr = xdes_get_descriptor(space, hint, mtr);

    if ((descr == nullptr) || (XDES_FREE != xdes_get_state(descr, mtr))) {

      /* We cannot allocate the desired extent: stop */

      return;
    }

    descr = fsp_alloc_free_extent(space, hint, mtr);

    xdes_set_state(descr, XDES_FSEG, mtr);

    seg_id = mtr_read_uint64(inode + FSEG_ID, mtr);
    ut_ad(mach_read_from_4(inode + FSEG_MAGIC_N) == FSEG_MAGIC_N_VALUE);
    mlog_write_uint64(descr + XDES_ID, seg_id, mtr);

    flst_add_last(inode + FSEG_FREE, descr + XDES_FLST_NODE, mtr);
    hint += FSP_EXTENT_SIZE;
  }
}

/** Allocates a free extent for the segment: looks first in the free list of the
segment, then tries to allocate from the space free list. NOTE that the extent
returned still resides in the segment free list, it is not yet taken off it!
@return allocated extent, still placed in the segment free list, nullptr
if could not be allocated */
static xdes_t *fseg_alloc_free_extent(
  fseg_inode_t *inode, /*!< in: segment inode */
  ulint space,         /*!< in: space id */
  mtr_t *mtr
) /*!< in: mtr */
{
  xdes_t *descr;
  uint64_t seg_id;
  fil_addr_t first;

  ut_ad(!((page_offset(inode) - FSEG_ARR_OFFSET) % FSEG_INODE_SIZE));
  ut_ad(mach_read_from_4(inode + FSEG_MAGIC_N) == FSEG_MAGIC_N_VALUE);

  if (flst_get_len(inode + FSEG_FREE, mtr) > 0) {
    /* Segment free list is not empty, allocate from it */

    first = flst_get_first(inode + FSEG_FREE, mtr);

    descr = xdes_lst_get_descriptor(space, first, mtr);
  } else {
    /* Segment free list was empty, allocate from space */
    descr = fsp_alloc_free_extent(space, 0, mtr);

    if (descr == nullptr) {

      return (nullptr);
    }

    seg_id = mtr_read_uint64(inode + FSEG_ID, mtr);

    xdes_set_state(descr, XDES_FSEG, mtr);
    mlog_write_uint64(descr + XDES_ID, seg_id, mtr);
    flst_add_last(inode + FSEG_FREE, descr + XDES_FLST_NODE, mtr);

    /* Try to fill the segment free list */
    fseg_fill_free_list(inode, space, xdes_get_offset(descr) + FSP_EXTENT_SIZE, mtr);
  }

  return (descr);
}

/** Allocates a single free page from a segment. This function implements
the intelligent allocation strategy which tries to minimize file space
fragmentation.
@param[in] space                Tablespace ID where to allocate the page.
@param[in] seg_inode            Segment inode in the tablespace.
@param[in] hint                 Hint of which page would be desirable
@param[in] direction            If the new page is needed because
                                of an index page split, and records are
                                inserted there in order, into which
                                direction they go alphabetically: FSP_DOWN,
                                FSP_UP, FSP_NO_DIR
@param[in,out] mtr              Mini-transaction covering the operation.
@return	the allocated page number, FIL_NULL if no page could be allocated */
static ulint fseg_alloc_free_page_low(space_id_t space, fseg_inode_t *seg_inode, page_no_t hint, byte direction, mtr_t *mtr) {
  bool frag_page_allocated{};

  ut_ad(mtr != nullptr);
  ut_ad((direction >= FSP_UP) && (direction <= FSP_NO_DIR));
  ut_ad(mach_read_from_4(seg_inode + FSEG_MAGIC_N) == FSEG_MAGIC_N_VALUE);
  ut_ad(!((page_offset(seg_inode) - FSEG_ARR_OFFSET) % FSEG_INODE_SIZE));

  auto seg_id = mtr_read_uint64(seg_inode + FSEG_ID, mtr);

  ut_ad(seg_id != 0);

  ulint used;

  auto reserved = fseg_n_reserved_pages_low(seg_inode, &used, mtr);
  auto space_header = fsp_get_space_header(space, mtr);
  auto hint_descr = xdes_get_descriptor_with_space_hdr(space_header, space, hint, mtr);

  if (hint_descr == nullptr) {
    /* Hint outside space or too high above free limit: reset hint */
    hint = 0;
    hint_descr = xdes_get_descriptor(space, hint, mtr);
  }

  /* The allocated page offset, FIL_NULL if could not be allocated */
  ulint ret_page;

  /* the extent of the allocated page */
  xdes_t *ret_descr;

  /* In the big if-else below we look for ret_page and ret_descr */
  if (xdes_get_state(hint_descr, mtr) == XDES_FSEG && mtr_read_uint64(hint_descr + XDES_ID, mtr) == seg_id && xdes_get_bit(hint_descr, XDES_FREE_BIT, hint % FSP_EXTENT_SIZE, mtr)) {

    /* 1. We can take the hinted page */

    ret_page = hint;
    ret_descr = hint_descr;

  } else if (xdes_get_state(hint_descr, mtr) == XDES_FREE && (reserved - used) < reserved / FSEG_FILLFACTOR && used >= FSEG_FRAG_LIMIT) {

    /* 2. We allocate the free extent from space and can take
    the hinted page. */

    ret_descr = fsp_alloc_free_extent(space, hint, mtr);

    ut_a(ret_descr == hint_descr);

    xdes_set_state(ret_descr, XDES_FSEG, mtr);
    mlog_write_uint64(ret_descr + XDES_ID, seg_id, mtr);
    flst_add_last(seg_inode + FSEG_FREE, ret_descr + XDES_FLST_NODE, mtr);

    /* Try to fill the segment free list */
    fseg_fill_free_list(seg_inode, space, hint + FSP_EXTENT_SIZE, mtr);
    ret_page = hint;

  } else if (direction != FSP_NO_DIR && (reserved - used) < reserved / FSEG_FILLFACTOR && used >= FSEG_FRAG_LIMIT && !!(ret_descr = fseg_alloc_free_extent(seg_inode, space, mtr))) {

    /* 3. We take any free extent (which was already assigned above
    in the if-condition to ret_descr) and take the lowest or
    highest page in it, depending on the direction. */
    ret_page = xdes_get_offset(ret_descr);

    if (direction == FSP_DOWN) {
      ret_page += FSP_EXTENT_SIZE - 1;
    }

  } else if (xdes_get_state(hint_descr, mtr) == XDES_FSEG && mtr_read_uint64(hint_descr + XDES_ID, mtr) == seg_id && !xdes_is_full(hint_descr, mtr)) {

    /* 4. We can take the page from the same extent as the
    hinted page (and the extent already belongs to the segment). */

    ret_descr = hint_descr;

    ret_page = xdes_get_offset(ret_descr) + xdes_find_bit(ret_descr, XDES_FREE_BIT, true, hint % FSP_EXTENT_SIZE, mtr);

  } else if (reserved - used > 0) {

    /* 5. We take any unused page from the segment */
    fil_addr_t first;

    if (flst_get_len(seg_inode + FSEG_NOT_FULL, mtr) > 0) {
      first = flst_get_first(seg_inode + FSEG_NOT_FULL, mtr);
    } else if (flst_get_len(seg_inode + FSEG_FREE, mtr) > 0) {
      first = flst_get_first(seg_inode + FSEG_FREE, mtr);
    } else {
      ut_error;
      return FIL_NULL;
    }

    ret_descr = xdes_lst_get_descriptor(space, first, mtr);

    ret_page = xdes_get_offset(ret_descr) + xdes_find_bit(ret_descr, XDES_FREE_BIT, true, 0, mtr);

  } else if (used < FSEG_FRAG_LIMIT) {

    /* 6. We allocate an individual page from the space. */

    ret_page = fsp_alloc_free_page(space, hint, mtr);

    ret_descr = nullptr;

    frag_page_allocated = true;

    if (ret_page != FIL_NULL) {
      /* Put the page in the fragment page array of the segment */
      auto n = fseg_find_free_frag_page_slot(seg_inode, mtr);

      ut_a(n != FIL_NULL);

      fseg_set_nth_frag_page_no(seg_inode, n, ret_page, mtr);
    }
  } else {

    /* 7. We allocate a new extent and take its first page. */
    ret_descr = fseg_alloc_free_extent(seg_inode, space, mtr);

    if (ret_descr == nullptr) {
      ret_page = FIL_NULL;
    } else {
      ret_page = xdes_get_offset(ret_descr);
    }
  }

  if (ret_page == FIL_NULL) {

    /* Page could not be allocated */

    return FIL_NULL;
  }

  if (space != 0) {
    auto space_size = fil_space_get_size(space);

    if (space_size <= ret_page) {
      /* It must be that we are extending a single-table
      tablespace whose size is still < 64 pages */

      if (ret_page >= FSP_EXTENT_SIZE) {

        ib_logger(
          ib_stream,
          "Error (2): trying to extend a single-table tablespace %lu"
          " by single page(s) though the space size %lu."
          "  Page no %lu.\n",
          (ulong)space,
          (ulong)space_size,
          (ulong)ret_page
        );

        return (FIL_NULL);
      }

      auto success = fsp_try_extend_data_file_with_pages(space, ret_page, space_header, mtr);

      if (!success) {
        /* No disk space left */
        return FIL_NULL;
      }
    }
  }

  if (!frag_page_allocated) {
    /* Initialize the allocated page to buffer pool, so that it
    can be obtained immediately with buf_page_get without need
    for a disk read */

    auto block = buf_page_create(space, ret_page, mtr);

    buf_block_dbg_add_level(block, SYNC_FSP_PAGE);

    if (block != buf_page_get(space, 0, ret_page, RW_X_LATCH, mtr)) {
      ut_error;
    }

    /* The prior contents of the page should be ignored */
    fsp_init_file_page(block, mtr);

    /* At this point we know the extent and the page offset.
    The extent is still in the appropriate list (FSEG_NOT_FULL
    or FSEG_FREE), and the page is not yet marked as used. */

    ut_ad(xdes_get_descriptor(space, ret_page, mtr) == ret_descr);

    ut_ad(xdes_get_bit(ret_descr, XDES_FREE_BIT, ret_page % FSP_EXTENT_SIZE, mtr));

    fseg_mark_page_used(seg_inode, space, ret_page, mtr);
  }

  buf_reset_check_index_page_at_flush(space, ret_page);

  return ret_page;
}

ulint fseg_alloc_free_page_general(fseg_header_t *seg_header, ulint hint, byte direction, bool has_done_reservation, mtr_t *mtr) {
  ulint space;
  rw_lock_t *latch;
  bool success;
  ulint page_no;
  ulint n_reserved;

  space = page_get_space_id(page_align(seg_header));

  latch = fil_space_get_latch(space);

  ut_ad(!mutex_own(&kernel_mutex) || mtr_memo_contains(mtr, latch, MTR_MEMO_X_LOCK));

  mtr_x_lock(latch, mtr);

  auto inode = fseg_inode_get(seg_header, space, mtr);

  if (!has_done_reservation) {
    success = fsp_reserve_free_extents(&n_reserved, space, 2, FSP_NORMAL, mtr);
    if (!success) {
      return (FIL_NULL);
    }
  }

  page_no = fseg_alloc_free_page_low(space, inode, hint, direction, mtr);
  if (!has_done_reservation) {
    fil_space_release_free_extents(space, n_reserved);
  }

  return page_no;
}

ulint fseg_alloc_free_page(fseg_header_t *seg_header, ulint hint, byte direction, mtr_t *mtr) {
  return fseg_alloc_free_page_general(seg_header, hint, direction, false, mtr);
}

/** Checks that we have at least 2 frag pages free in the first extent of a
single-table tablespace, and they are also physically initialized to the data
file. That is we have already extended the data file so that those pages are
inside the data file. If not, this function extends the tablespace with
pages.
@return	true if there were >= 3 free pages, or we were able to extend */
static bool fsp_reserve_free_pages(
  ulint space,                /*!< in: space id, must be != 0 */
  fsp_header_t *space_header, /*!< in: header of that
                                                   space, x-latched */
  ulint size,                 /*!< in: size of the tablespace in pages,
                                   must be < FSP_EXTENT_SIZE / 2 */
  mtr_t *mtr
) /*!< in: mtr */
{
  xdes_t *descr;
  ulint n_used;

  ut_a(space != 0);
  ut_a(size < FSP_EXTENT_SIZE / 2);

  descr = xdes_get_descriptor_with_space_hdr(space_header, space, 0, mtr);
  n_used = xdes_get_n_used(descr, mtr);

  ut_a(n_used <= size);

  if (size >= n_used + 2) {

    return (true);
  }

  return (fsp_try_extend_data_file_with_pages(space, n_used + 1, space_header, mtr));
}

bool fsp_reserve_free_extents(ulint *n_reserved, ulint space, ulint n_ext, ulint alloc_type, mtr_t *mtr) {
  fsp_header_t *space_header;
  ulint n_free_list_ext;
  ulint free_limit;
  ulint size;
  ulint n_free;
  ulint n_free_up;
  ulint reserve;
  bool success;
  ulint n_pages_added;

  ut_ad(mtr);
  *n_reserved = n_ext;

  auto latch = fil_space_get_latch(space);

  ut_ad(!mutex_own(&kernel_mutex) || mtr_memo_contains(mtr, latch, MTR_MEMO_X_LOCK));

  mtr_x_lock(latch, mtr);

  space_header = fsp_get_space_header(space, mtr);
try_again:
  size = mtr_read_ulint(space_header + FSP_SIZE, MLOG_4BYTES, mtr);

  if (size < FSP_EXTENT_SIZE / 2) {
    /* Use different rules for small single-table tablespaces */
    *n_reserved = 0;
    return (fsp_reserve_free_pages(space, space_header, size, mtr));
  }

  n_free_list_ext = flst_get_len(space_header + FSP_FREE, mtr);

  free_limit = mtr_read_ulint(space_header + FSP_FREE_LIMIT, MLOG_4BYTES, mtr);

  /* Below we play safe when counting free extents above the free limit:
  some of them will contain extent descriptor pages, and therefore
  will not be free extents */

  n_free_up = (size - free_limit) / FSP_EXTENT_SIZE;

  if (n_free_up > 0) {
    n_free_up--;
    n_free_up -= n_free_up / (UNIV_PAGE_SIZE / FSP_EXTENT_SIZE);
  }

  n_free = n_free_list_ext + n_free_up;

  if (alloc_type == FSP_NORMAL) {
    /* We reserve 1 extent + 0.5 % of the space size to undo logs
    and 1 extent + 0.5 % to cleaning operations; NOTE: this source
    code is duplicated in the function below! */

    reserve = 2 + ((size / FSP_EXTENT_SIZE) * 2) / 200;

    if (n_free <= reserve + n_ext) {

      goto try_to_extend;
    }
  } else if (alloc_type == FSP_UNDO) {
    /* We reserve 0.5 % of the space size to cleaning operations */

    reserve = 1 + ((size / FSP_EXTENT_SIZE) * 1) / 200;

    if (n_free <= reserve + n_ext) {

      goto try_to_extend;
    }
  } else {
    ut_a(alloc_type == FSP_CLEANING);
  }

  success = fil_space_reserve_free_extents(space, n_free, n_ext);

  if (success) {
    return (true);
  }
try_to_extend:
  success = fsp_try_extend_data_file(&n_pages_added, space, space_header, mtr);
  if (success && n_pages_added > 0) {

    goto try_again;
  }

  return (false);
}

uint64_t fsp_get_available_space_in_free_extents(ulint space) {
  fsp_header_t *space_header;
  ulint n_free_list_ext;
  ulint free_limit;
  ulint size;
  ulint n_free;
  ulint n_free_up;
  ulint reserve;
  rw_lock_t *latch;
  mtr_t mtr;

  ut_ad(!mutex_own(&kernel_mutex));

  mtr_start(&mtr);

  latch = fil_space_get_latch(space);

  mtr_x_lock(latch, &mtr);

  space_header = fsp_get_space_header(space, &mtr);

  size = mtr_read_ulint(space_header + FSP_SIZE, MLOG_4BYTES, &mtr);

  n_free_list_ext = flst_get_len(space_header + FSP_FREE, &mtr);

  free_limit = mtr_read_ulint(space_header + FSP_FREE_LIMIT, MLOG_4BYTES, &mtr);
  mtr_commit(&mtr);

  if (size < FSP_EXTENT_SIZE) {
    ut_a(space != 0); /* This must be a single-table
                      tablespace */

    return (0); /* TODO: count free frag pages and
                return a value based on that */
  }

  /* Below we play safe when counting free extents above the free limit:
  some of them will contain extent descriptor pages, and therefore
  will not be free extents */

  n_free_up = (size - free_limit) / FSP_EXTENT_SIZE;

  if (n_free_up > 0) {
    n_free_up--;
    n_free_up -= n_free_up / (UNIV_PAGE_SIZE / FSP_EXTENT_SIZE);
  }

  n_free = n_free_list_ext + n_free_up;

  /* We reserve 1 extent + 0.5 % of the space size to undo logs
  and 1 extent + 0.5 % to cleaning operations; NOTE: this source
  code is duplicated in the function above! */

  reserve = 2 + ((size / FSP_EXTENT_SIZE) * 2) / 200;

  if (reserve > n_free) {
    return (0);
  }

  return ((uint64_t)(n_free - reserve) * FSP_EXTENT_SIZE * (UNIV_PAGE_SIZE / 1024));
}

/** Marks a page used. The page must reside within the extents of the given
segment. */
static void fseg_mark_page_used(
  fseg_inode_t *seg_inode, /*!< in: segment inode */
  ulint space,             /*!< in: space id */
  ulint page,              /*!< in: page offset */
  mtr_t *mtr
) /*!< in: mtr */
{
  xdes_t *descr;
  ulint not_full_n_used;

  ut_ad(seg_inode && mtr);
  ut_ad(!((page_offset(seg_inode) - FSEG_ARR_OFFSET) % FSEG_INODE_SIZE));
  ut_ad(mach_read_from_4(seg_inode + FSEG_MAGIC_N) == FSEG_MAGIC_N_VALUE);

  descr = xdes_get_descriptor(space, page, mtr);

  ut_ad(mtr_read_ulint(seg_inode + FSEG_ID, MLOG_4BYTES, mtr) == mtr_read_ulint(descr + XDES_ID, MLOG_4BYTES, mtr));

  if (xdes_is_free(descr, mtr)) {
    /* We move the extent from the free list to the
    NOT_FULL list */
    flst_remove(seg_inode + FSEG_FREE, descr + XDES_FLST_NODE, mtr);
    flst_add_last(seg_inode + FSEG_NOT_FULL, descr + XDES_FLST_NODE, mtr);
  }

  ut_ad(xdes_get_bit(descr, XDES_FREE_BIT, page % FSP_EXTENT_SIZE, mtr) == true);
  /* We mark the page as used */
  xdes_set_bit(descr, XDES_FREE_BIT, page % FSP_EXTENT_SIZE, false, mtr);

  not_full_n_used = mtr_read_ulint(seg_inode + FSEG_NOT_FULL_N_USED, MLOG_4BYTES, mtr);
  not_full_n_used++;
  mlog_write_ulint(seg_inode + FSEG_NOT_FULL_N_USED, not_full_n_used, MLOG_4BYTES, mtr);
  if (xdes_is_full(descr, mtr)) {
    /* We move the extent from the NOT_FULL list to the
    FULL list */
    flst_remove(seg_inode + FSEG_NOT_FULL, descr + XDES_FLST_NODE, mtr);
    flst_add_last(seg_inode + FSEG_FULL, descr + XDES_FLST_NODE, mtr);

    mlog_write_ulint(seg_inode + FSEG_NOT_FULL_N_USED, not_full_n_used - FSP_EXTENT_SIZE, MLOG_4BYTES, mtr);
  }
}

/** Frees a single page of a segment. */
static void fseg_free_page_low(
  fseg_inode_t *seg_inode, /*!< in: segment inode */
  ulint space,             /*!< in: space id */
  ulint page,              /*!< in: page offset */
  mtr_t *mtr
) /*!< in: mtr handle */
{
  xdes_t *descr;
  ulint not_full_n_used;
  ulint state;
  uint64_t descr_id;
  uint64_t seg_id;
  ulint i;

  ut_ad(seg_inode && mtr);
  ut_ad(mach_read_from_4(seg_inode + FSEG_MAGIC_N) == FSEG_MAGIC_N_VALUE);
  ut_ad(!((page_offset(seg_inode) - FSEG_ARR_OFFSET) % FSEG_INODE_SIZE));

  /* Drop search system page hash index if the page is found in
  the pool and is hashed */

  descr = xdes_get_descriptor(space, page, mtr);

  ut_a(descr);
  if (xdes_get_bit(descr, XDES_FREE_BIT, page % FSP_EXTENT_SIZE, mtr)) {
    ib_logger(ib_stream, "Dump of the tablespace extent descriptor: ");
    ut_print_buf(ib_stream, descr, 40);

    ib_logger(
      ib_stream,
      "\n"
      "Serious error! InnoDB is trying to"
      " free page %lu\n"
      "though it is already marked as free"
      " in the tablespace!\n"
      "The tablespace free space info is corrupt.\n"
      "You may need to dump your"
      " InnoDB tables and recreate the whole\n"
      "database!\n",
      (ulong)page
    );
  crash:
    ib_logger(
      ib_stream,
      "Please refer to the Embdedded InnoDB GitHub repository for details about forcing recovery."
    );
    ut_error;
  }

  state = xdes_get_state(descr, mtr);

  if (state != XDES_FSEG) {
    /* The page is in the fragment pages of the segment */

    for (i = 0;; i++) {
      if (fseg_get_nth_frag_page_no(seg_inode, i, mtr) == page) {

        fseg_set_nth_frag_page_no(seg_inode, i, FIL_NULL, mtr);
        break;
      }
    }

    fsp_free_page(space, page, mtr);

    return;
  }

  /* If we get here, the page is in some extent of the segment */

  descr_id = mtr_read_uint64(descr + XDES_ID, mtr);
  seg_id = mtr_read_uint64(seg_inode + FSEG_ID, mtr);

  if (descr_id != seg_id) {
    ib_logger(ib_stream, "Dump of the tablespace extent descriptor: ");
    ut_print_buf(ib_stream, descr, 40);
    ib_logger(ib_stream, "\nDump of the segment inode: ");
    ut_print_buf(ib_stream, seg_inode, 40);
    ib_logger(ib_stream, "\n");

    ib_logger(
      ib_stream,
      "Serious error: InnoDB is trying to"
      " free space %lu page %lu,\n"
      "which does not belong to"
      " segment %lu %lu but belongs\n"
      "to segment %lu %lu.\n",
      (ulong)space,
      (ulong)page,
      (ulong)descr_id,
      (ulong)descr_id,
      (ulong)seg_id,
      (ulong)seg_id
    );
    goto crash;
  }

  not_full_n_used = mtr_read_ulint(seg_inode + FSEG_NOT_FULL_N_USED, MLOG_4BYTES, mtr);
  if (xdes_is_full(descr, mtr)) {
    /* The fragment is full: move it to another list */
    flst_remove(seg_inode + FSEG_FULL, descr + XDES_FLST_NODE, mtr);
    flst_add_last(seg_inode + FSEG_NOT_FULL, descr + XDES_FLST_NODE, mtr);
    mlog_write_ulint(seg_inode + FSEG_NOT_FULL_N_USED, not_full_n_used + FSP_EXTENT_SIZE - 1, MLOG_4BYTES, mtr);
  } else {
    ut_a(not_full_n_used > 0);
    mlog_write_ulint(seg_inode + FSEG_NOT_FULL_N_USED, not_full_n_used - 1, MLOG_4BYTES, mtr);
  }

  xdes_set_bit(descr, XDES_FREE_BIT, page % FSP_EXTENT_SIZE, true, mtr);
  xdes_set_bit(descr, XDES_CLEAN_BIT, page % FSP_EXTENT_SIZE, true, mtr);

  if (xdes_is_free(descr, mtr)) {
    /* The extent has become free: free it to space */
    flst_remove(seg_inode + FSEG_NOT_FULL, descr + XDES_FLST_NODE, mtr);
    fsp_free_extent(space, page, mtr);
  }
}

void fseg_free_page(fseg_header_t *seg_header, ulint space, ulint page, mtr_t *mtr) {
  fseg_inode_t *seg_inode;

  auto latch = fil_space_get_latch(space);

  ut_ad(!mutex_own(&kernel_mutex) || mtr_memo_contains(mtr, latch, MTR_MEMO_X_LOCK));

  mtr_x_lock(latch, mtr);

  seg_inode = fseg_inode_get(seg_header, space, mtr);

  fseg_free_page_low(seg_inode, space, page, mtr);

#ifdef UNIV_DEBUG
  buf_page_set_file_page_was_freed(space, page);
#endif
}

/** Frees an extent of a segment to the space free list. */
static void fseg_free_extent(
  fseg_inode_t *seg_inode, /*!< in: segment inode */
  space_id_t space,        /*!< in: space id */
  page_no_t page,          /*!< in: a page in the extent */
  mtr_t *mtr
) /*!< in: mtr handle */
{
  ut_ad(mtr != nullptr);
  ut_ad(seg_inode != nullptr);

  auto descr = xdes_get_descriptor(space, page, mtr);

  ut_a(xdes_get_state(descr, mtr) == XDES_FSEG);
  ut_a(mtr_read_uint64(descr + XDES_ID, mtr) == mtr_read_uint64(seg_inode + FSEG_ID, mtr));
  ut_ad(mach_read_from_4(seg_inode + FSEG_MAGIC_N) == FSEG_MAGIC_N_VALUE);

  if (xdes_is_full(descr, mtr)) {
    flst_remove(seg_inode + FSEG_FULL, descr + XDES_FLST_NODE, mtr);
  } else if (xdes_is_free(descr, mtr)) {
    flst_remove(seg_inode + FSEG_FREE, descr + XDES_FLST_NODE, mtr);
  } else {
    flst_remove(seg_inode + FSEG_NOT_FULL, descr + XDES_FLST_NODE, mtr);

    auto not_full_n_used = mtr_read_ulint(seg_inode + FSEG_NOT_FULL_N_USED, MLOG_4BYTES, mtr);

    auto descr_n_used = xdes_get_n_used(descr, mtr);

    ut_a(not_full_n_used >= descr_n_used);

    mlog_write_ulint(seg_inode + FSEG_NOT_FULL_N_USED, not_full_n_used - descr_n_used, MLOG_4BYTES, mtr);
  }

  fsp_free_extent(space, page, mtr);

#ifdef UNIV_DEBUG
  const auto first_page_in_extent = page - (page % FSP_EXTENT_SIZE);

  for (ulint i = 0; i < FSP_EXTENT_SIZE; i++) {

    buf_page_set_file_page_was_freed(space, first_page_in_extent + i);
  }
#endif /* UNIV_DEBUG */
}

bool fseg_free_step(fseg_header_t *header, mtr_t *mtr) {
  ulint n;
  ulint page;
  xdes_t *descr;
  fseg_inode_t *inode;

  auto space = page_get_space_id(page_align(header));
  auto header_page = page_get_page_no(page_align(header));
  auto latch = fil_space_get_latch(space);

  ut_ad(!mutex_own(&kernel_mutex) || mtr_memo_contains(mtr, latch, MTR_MEMO_X_LOCK));

  mtr_x_lock(latch, mtr);

  descr = xdes_get_descriptor(space, header_page, mtr);

  /* Check that the header resides on a page which has not been
  freed yet */

  ut_a(descr);
  ut_a(xdes_get_bit(descr, XDES_FREE_BIT, header_page % FSP_EXTENT_SIZE, mtr) == false);
  inode = fseg_inode_try_get(header, space, mtr);

  if (unlikely(inode == nullptr)) {
    ib_logger(ib_stream, "double free of inode from %u:%u\n", (unsigned)space, (unsigned)header_page);
    return (true);
  }

  descr = fseg_get_first_extent(inode, space, mtr);

  if (descr != nullptr) {
    /* Free the extent held by the segment */
    page = xdes_get_offset(descr);

    fseg_free_extent(inode, space, page, mtr);

    return (false);
  }

  /* Free a frag page */
  n = fseg_find_last_used_frag_page_slot(inode, mtr);

  if (n == ULINT_UNDEFINED) {
    /* Freeing completed: free the segment inode */
    fsp_free_seg_inode(space, inode, mtr);

    return (true);
  }

  fseg_free_page_low(inode, space, fseg_get_nth_frag_page_no(inode, n, mtr), mtr);

  n = fseg_find_last_used_frag_page_slot(inode, mtr);

  if (n == ULINT_UNDEFINED) {
    /* Freeing completed: free the segment inode */
    fsp_free_seg_inode(space, inode, mtr);

    return (true);
  }

  return (false);
}

bool fseg_free_step_not_header(fseg_header_t *header, mtr_t *mtr) {
  ulint n;
  ulint page;
  xdes_t *descr;
  fseg_inode_t *inode;
  ulint space;
  ulint page_no;
  rw_lock_t *latch;

  space = page_get_space_id(page_align(header));

  latch = fil_space_get_latch(space);

  ut_ad(!mutex_own(&kernel_mutex) || mtr_memo_contains(mtr, latch, MTR_MEMO_X_LOCK));

  mtr_x_lock(latch, mtr);

  inode = fseg_inode_get(header, space, mtr);

  descr = fseg_get_first_extent(inode, space, mtr);

  if (descr != nullptr) {
    /* Free the extent held by the segment */
    page = xdes_get_offset(descr);

    fseg_free_extent(inode, space, page, mtr);

    return (false);
  }

  /* Free a frag page */

  n = fseg_find_last_used_frag_page_slot(inode, mtr);

  if (n == ULINT_UNDEFINED) {
    ut_error;
  }

  page_no = fseg_get_nth_frag_page_no(inode, n, mtr);

  if (page_no == page_get_page_no(page_align(header))) {

    return (true);
  }

  fseg_free_page_low(inode, space, page_no, mtr);

  return (false);
}

/** Returns the first extent descriptor for a segment. We think of the extent
lists of the segment catenated in the order FSEG_FULL -> FSEG_NOT_FULL
-> FSEG_FREE.
@return	the first extent descriptor, or nullptr if none */
static xdes_t *fseg_get_first_extent(
  fseg_inode_t *inode, /*!< in: segment inode */
  ulint space,         /*!< in: space id */
  mtr_t *mtr
) /*!< in: mtr */
{
  fil_addr_t first;
  xdes_t *descr;

  ut_ad(inode && mtr);

  ut_ad(space == page_get_space_id(page_align(inode)));
  ut_ad(mach_read_from_4(inode + FSEG_MAGIC_N) == FSEG_MAGIC_N_VALUE);

  first = fil_addr_null;

  if (flst_get_len(inode + FSEG_FULL, mtr) > 0) {

    first = flst_get_first(inode + FSEG_FULL, mtr);

  } else if (flst_get_len(inode + FSEG_NOT_FULL, mtr) > 0) {

    first = flst_get_first(inode + FSEG_NOT_FULL, mtr);

  } else if (flst_get_len(inode + FSEG_FREE, mtr) > 0) {

    first = flst_get_first(inode + FSEG_FREE, mtr);
  }

  if (first.page == FIL_NULL) {

    return (nullptr);
  }
  descr = xdes_lst_get_descriptor(space, first, mtr);

  return (descr);
}

/** Validates a segment.
@return	true if ok */
static bool fseg_validate_low(
  fseg_inode_t *inode, /*!< in: segment inode */
  mtr_t *mtr2
) /*!< in: mtr */
{
  ulint space;
  uint64_t seg_id;
  mtr_t mtr;
  xdes_t *descr;
  fil_addr_t node_addr;
  ulint n_used = 0;
  ulint n_used2 = 0;

  ut_ad(mtr_memo_contains_page(mtr2, inode, MTR_MEMO_PAGE_X_FIX));
  ut_ad(mach_read_from_4(inode + FSEG_MAGIC_N) == FSEG_MAGIC_N_VALUE);

  space = page_get_space_id(page_align(inode));

  seg_id = mtr_read_uint64(inode + FSEG_ID, mtr2);
  n_used = mtr_read_ulint(inode + FSEG_NOT_FULL_N_USED, MLOG_4BYTES, mtr2);
  flst_validate(inode + FSEG_FREE, mtr2);
  flst_validate(inode + FSEG_NOT_FULL, mtr2);
  flst_validate(inode + FSEG_FULL, mtr2);

  /* Validate FSEG_FREE list */
  node_addr = flst_get_first(inode + FSEG_FREE, mtr2);

  while (!fil_addr_is_null(node_addr)) {
    mtr_start(&mtr);
    mtr_x_lock(fil_space_get_latch(space), &mtr);

    descr = xdes_lst_get_descriptor(space, node_addr, &mtr);

    ut_a(xdes_get_n_used(descr, &mtr) == 0);
    ut_a(xdes_get_state(descr, &mtr) == XDES_FSEG);
    ut_a(mtr_read_uint64(descr + XDES_ID, &mtr) == seg_id);

    node_addr = flst_get_next_addr(descr + XDES_FLST_NODE, &mtr);
    mtr_commit(&mtr);
  }

  /* Validate FSEG_NOT_FULL list */

  node_addr = flst_get_first(inode + FSEG_NOT_FULL, mtr2);

  while (!fil_addr_is_null(node_addr)) {
    mtr_start(&mtr);
    mtr_x_lock(fil_space_get_latch(space), &mtr);

    descr = xdes_lst_get_descriptor(space, node_addr, &mtr);

    ut_a(xdes_get_n_used(descr, &mtr) > 0);
    ut_a(xdes_get_n_used(descr, &mtr) < FSP_EXTENT_SIZE);
    ut_a(xdes_get_state(descr, &mtr) == XDES_FSEG);
    ut_a(mtr_read_uint64(descr + XDES_ID, &mtr) == seg_id);

    n_used2 += xdes_get_n_used(descr, &mtr);

    node_addr = flst_get_next_addr(descr + XDES_FLST_NODE, &mtr);
    mtr_commit(&mtr);
  }

  /* Validate FSEG_FULL list */

  node_addr = flst_get_first(inode + FSEG_FULL, mtr2);

  while (!fil_addr_is_null(node_addr)) {
    mtr_start(&mtr);
    mtr_x_lock(fil_space_get_latch(space), &mtr);

    descr = xdes_lst_get_descriptor(space, node_addr, &mtr);

    ut_a(xdes_get_n_used(descr, &mtr) == FSP_EXTENT_SIZE);
    ut_a(xdes_get_state(descr, &mtr) == XDES_FSEG);
    ut_a(mtr_read_uint64(descr + XDES_ID, &mtr) == seg_id);

    node_addr = flst_get_next_addr(descr + XDES_FLST_NODE, &mtr);
    mtr_commit(&mtr);
  }

  ut_a(n_used == n_used2);

  return (true);
}

#ifdef UNIV_DEBUG
bool fseg_validate(fseg_header_t *header, mtr_t *mtr) {
  fseg_inode_t *inode;
  bool ret;
  ulint space;

  space = page_get_space_id(page_align(header));

  mtr_x_lock(fil_space_get_latch(space), mtr);

  inode = fseg_inode_get(header, space, mtr);

  ret = fseg_validate_low(inode, mtr);

  return (ret);
}
#endif /* UNIV_DEBUG */

/** Writes info of a segment. */
static void fseg_print_low(
  fseg_inode_t *inode, /*!< in: segment inode */
  mtr_t *mtr
) /*!< in: mtr */
{
  ulint space;
  ulint seg_id_low;
  ulint seg_id_high;
  ulint n_used;
  ulint n_frag;
  ulint n_free;
  ulint n_not_full;
  ulint n_full;
  ulint reserved;
  ulint used;
  ulint page_no;
  uint64_t d_var;

  ut_ad(mtr_memo_contains_page(mtr, inode, MTR_MEMO_PAGE_X_FIX));
  space = page_get_space_id(page_align(inode));
  page_no = page_get_page_no(page_align(inode));

  reserved = fseg_n_reserved_pages_low(inode, &used, mtr);

  d_var = mtr_read_uint64(inode + FSEG_ID, mtr);

  seg_id_low = d_var;
  seg_id_high = d_var;

  n_used = mtr_read_ulint(inode + FSEG_NOT_FULL_N_USED, MLOG_4BYTES, mtr);
  n_frag = fseg_get_n_frag_pages(inode, mtr);
  n_free = flst_get_len(inode + FSEG_FREE, mtr);
  n_not_full = flst_get_len(inode + FSEG_NOT_FULL, mtr);
  n_full = flst_get_len(inode + FSEG_FULL, mtr);

  ib_logger(
    ib_stream,
    "SEGMENT id %lu %lu space %lu; page %lu;"
    " res %lu used %lu; full ext %lu\n"
    "fragm pages %lu; free extents %lu;"
    " not full extents %lu: pages %lu\n",
    (ulong)seg_id_high,
    (ulong)seg_id_low,
    (ulong)space,
    (ulong)page_no,
    (ulong)reserved,
    (ulong)used,
    (ulong)n_full,
    (ulong)n_frag,
    (ulong)n_free,
    (ulong)n_not_full,
    (ulong)n_used
  );
  ut_ad(mach_read_from_4(inode + FSEG_MAGIC_N) == FSEG_MAGIC_N_VALUE);
}

#ifdef UNIV_BTR_PRINT
void fseg_print(fseg_header_t *header, mtr_t *mtr) {
  fseg_inode_t *inode;
  ulint space;

  space = page_get_space_id(page_align(header));

  mtr_x_lock(fil_space_get_latch(space), mtr);

  inode = fseg_inode_get(header, space, mtr);

  fseg_print_low(inode, mtr);
}
#endif /* UNIV_BTR_PRINT */

bool fsp_validate(ulint space) {
  fsp_header_t *header;
  fseg_inode_t *seg_inode;
  page_t *seg_inode_page;
  rw_lock_t *latch;
  ulint size;
  ulint free_limit;
  ulint frag_n_used;
  mtr_t mtr;
  mtr_t mtr2;
  xdes_t *descr;
  fil_addr_t node_addr;
  fil_addr_t next_node_addr;
  ulint descr_count = 0;
  ulint n_used = 0;
  ulint n_used2 = 0;
  ulint n_full_frag_pages;
  ulint n;
  ulint seg_inode_len_free;
  ulint seg_inode_len_full;

  latch = fil_space_get_latch(space);

  /* Start first a mini-transaction mtr2 to lock out all other threads
  from the fsp system */
  mtr_start(&mtr2);
  mtr_x_lock(latch, &mtr2);

  mtr_start(&mtr);
  mtr_x_lock(latch, &mtr);

  header = fsp_get_space_header(space, &mtr);

  size = mtr_read_ulint(header + FSP_SIZE, MLOG_4BYTES, &mtr);
  free_limit = mtr_read_ulint(header + FSP_FREE_LIMIT, MLOG_4BYTES, &mtr);
  frag_n_used = mtr_read_ulint(header + FSP_FRAG_N_USED, MLOG_4BYTES, &mtr);

  n_full_frag_pages = FSP_EXTENT_SIZE * flst_get_len(header + FSP_FULL_FRAG, &mtr);

  if (unlikely(free_limit > size)) {

    ut_a(space != 0);
    ut_a(size < FSP_EXTENT_SIZE);
  }

  flst_validate(header + FSP_FREE, &mtr);
  flst_validate(header + FSP_FREE_FRAG, &mtr);
  flst_validate(header + FSP_FULL_FRAG, &mtr);

  mtr_commit(&mtr);

  /* Validate FSP_FREE list */
  mtr_start(&mtr);
  mtr_x_lock(latch, &mtr);

  header = fsp_get_space_header(space, &mtr);
  node_addr = flst_get_first(header + FSP_FREE, &mtr);

  mtr_commit(&mtr);

  while (!fil_addr_is_null(node_addr)) {
    mtr_start(&mtr);
    mtr_x_lock(latch, &mtr);

    descr_count++;
    descr = xdes_lst_get_descriptor(space, node_addr, &mtr);

    ut_a(xdes_get_n_used(descr, &mtr) == 0);
    ut_a(xdes_get_state(descr, &mtr) == XDES_FREE);

    node_addr = flst_get_next_addr(descr + XDES_FLST_NODE, &mtr);
    mtr_commit(&mtr);
  }

  /* Validate FSP_FREE_FRAG list */
  mtr_start(&mtr);
  mtr_x_lock(latch, &mtr);

  header = fsp_get_space_header(space, &mtr);
  node_addr = flst_get_first(header + FSP_FREE_FRAG, &mtr);

  mtr_commit(&mtr);

  while (!fil_addr_is_null(node_addr)) {
    mtr_start(&mtr);
    mtr_x_lock(latch, &mtr);

    descr_count++;
    descr = xdes_lst_get_descriptor(space, node_addr, &mtr);

    ut_a(xdes_get_n_used(descr, &mtr) > 0);
    ut_a(xdes_get_n_used(descr, &mtr) < FSP_EXTENT_SIZE);
    ut_a(xdes_get_state(descr, &mtr) == XDES_FREE_FRAG);

    n_used += xdes_get_n_used(descr, &mtr);
    node_addr = flst_get_next_addr(descr + XDES_FLST_NODE, &mtr);

    mtr_commit(&mtr);
  }

  /* Validate FSP_FULL_FRAG list */
  mtr_start(&mtr);
  mtr_x_lock(latch, &mtr);

  header = fsp_get_space_header(space, &mtr);
  node_addr = flst_get_first(header + FSP_FULL_FRAG, &mtr);

  mtr_commit(&mtr);

  while (!fil_addr_is_null(node_addr)) {
    mtr_start(&mtr);
    mtr_x_lock(latch, &mtr);

    descr_count++;
    descr = xdes_lst_get_descriptor(space, node_addr, &mtr);

    ut_a(xdes_get_n_used(descr, &mtr) == FSP_EXTENT_SIZE);
    ut_a(xdes_get_state(descr, &mtr) == XDES_FULL_FRAG);

    node_addr = flst_get_next_addr(descr + XDES_FLST_NODE, &mtr);
    mtr_commit(&mtr);
  }

  /* Validate segments */
  mtr_start(&mtr);
  mtr_x_lock(latch, &mtr);

  header = fsp_get_space_header(space, &mtr);

  node_addr = flst_get_first(header + FSP_SEG_INODES_FULL, &mtr);

  seg_inode_len_full = flst_get_len(header + FSP_SEG_INODES_FULL, &mtr);

  mtr_commit(&mtr);

  while (!fil_addr_is_null(node_addr)) {

    n = 0;
    do {
      mtr_start(&mtr);
      mtr_x_lock(latch, &mtr);

      seg_inode_page = fut_get_ptr(space, node_addr, RW_X_LATCH, &mtr) - FSEG_INODE_PAGE_NODE;

      seg_inode = fsp_seg_inode_page_get_nth_inode(seg_inode_page, n, &mtr);

      ut_a(mach_read_from_8(seg_inode + FSEG_ID) != 0);
      fseg_validate_low(seg_inode, &mtr);

      descr_count += flst_get_len(seg_inode + FSEG_FREE, &mtr);
      descr_count += flst_get_len(seg_inode + FSEG_FULL, &mtr);
      descr_count += flst_get_len(seg_inode + FSEG_NOT_FULL, &mtr);

      n_used2 += fseg_get_n_frag_pages(seg_inode, &mtr);

      next_node_addr = flst_get_next_addr(seg_inode_page + FSEG_INODE_PAGE_NODE, &mtr);
      mtr_commit(&mtr);
    } while (++n < FSP_SEG_INODES_PER_PAGE);

    node_addr = next_node_addr;
  }

  mtr_start(&mtr);
  mtr_x_lock(latch, &mtr);

  header = fsp_get_space_header(space, &mtr);

  node_addr = flst_get_first(header + FSP_SEG_INODES_FREE, &mtr);

  seg_inode_len_free = flst_get_len(header + FSP_SEG_INODES_FREE, &mtr);

  mtr_commit(&mtr);

  while (!fil_addr_is_null(node_addr)) {

    n = 0;

    do {
      mtr_start(&mtr);
      mtr_x_lock(latch, &mtr);

      seg_inode_page = fut_get_ptr(space, node_addr, RW_X_LATCH, &mtr) - FSEG_INODE_PAGE_NODE;

      seg_inode = fsp_seg_inode_page_get_nth_inode(seg_inode_page, n, &mtr);
      if (mach_read_from_8(seg_inode + FSEG_ID) != 0) {
        fseg_validate_low(seg_inode, &mtr);

        descr_count += flst_get_len(seg_inode + FSEG_FREE, &mtr);
        descr_count += flst_get_len(seg_inode + FSEG_FULL, &mtr);
        descr_count += flst_get_len(seg_inode + FSEG_NOT_FULL, &mtr);
        n_used2 += fseg_get_n_frag_pages(seg_inode, &mtr);
      }

      next_node_addr = flst_get_next_addr(seg_inode_page + FSEG_INODE_PAGE_NODE, &mtr);
      mtr_commit(&mtr);
    } while (++n < FSP_SEG_INODES_PER_PAGE);

    node_addr = next_node_addr;
  }

  ut_a(descr_count * FSP_EXTENT_SIZE == free_limit);
  ut_a(
    n_used + n_full_frag_pages ==
    n_used2 + 2 * ((free_limit + (UNIV_PAGE_SIZE - 1)) / UNIV_PAGE_SIZE) + seg_inode_len_full + seg_inode_len_free
  );
  ut_a(frag_n_used == n_used);

  mtr_commit(&mtr2);

  return (true);
}

void fsp_print(ulint space) {
  fsp_header_t *header;
  fseg_inode_t *seg_inode;
  page_t *seg_inode_page;
  rw_lock_t *latch;
  ulint size;
  ulint free_limit;
  ulint frag_n_used;
  fil_addr_t node_addr;
  fil_addr_t next_node_addr;
  ulint n_free;
  ulint n_free_frag;
  ulint n_full_frag;
  ulint seg_id_low;
  ulint seg_id_high;
  ulint n;
  ulint n_segs = 0;
  uint64_t d_var;
  mtr_t mtr;
  mtr_t mtr2;

  latch = fil_space_get_latch(space);

  /* Start first a mini-transaction mtr2 to lock out all other threads
  from the fsp system */

  mtr_start(&mtr2);

  mtr_x_lock(latch, &mtr2);

  mtr_start(&mtr);

  mtr_x_lock(latch, &mtr);

  header = fsp_get_space_header(space, &mtr);

  size = mtr_read_ulint(header + FSP_SIZE, MLOG_4BYTES, &mtr);

  free_limit = mtr_read_ulint(header + FSP_FREE_LIMIT, MLOG_4BYTES, &mtr);
  frag_n_used = mtr_read_ulint(header + FSP_FRAG_N_USED, MLOG_4BYTES, &mtr);
  n_free = flst_get_len(header + FSP_FREE, &mtr);
  n_free_frag = flst_get_len(header + FSP_FREE_FRAG, &mtr);
  n_full_frag = flst_get_len(header + FSP_FULL_FRAG, &mtr);

  d_var = mtr_read_uint64(header + FSP_SEG_ID, &mtr);

  seg_id_low = d_var;
  seg_id_high = d_var;

  ib_logger(
    ib_stream,
    "FILE SPACE INFO: id %lu\n"
    "size %lu, free limit %lu, free extents %lu\n"
    "not full frag extents %lu: used pages %lu,"
    " full frag extents %lu\n"
    "first seg id not used %lu %lu\n",
    (ulong)space,
    (ulong)size,
    (ulong)free_limit,
    (ulong)n_free,
    (ulong)n_free_frag,
    (ulong)frag_n_used,
    (ulong)n_full_frag,
    (ulong)seg_id_high,
    (ulong)seg_id_low
  );

  mtr_commit(&mtr);

  /* Print segments */

  mtr_start(&mtr);
  mtr_x_lock(latch, &mtr);

  header = fsp_get_space_header(space, &mtr);

  node_addr = flst_get_first(header + FSP_SEG_INODES_FULL, &mtr);

  mtr_commit(&mtr);

  while (!fil_addr_is_null(node_addr)) {

    n = 0;

    do {

      mtr_start(&mtr);
      mtr_x_lock(latch, &mtr);

      seg_inode_page = fut_get_ptr(space, node_addr, RW_X_LATCH, &mtr) - FSEG_INODE_PAGE_NODE;

      seg_inode = fsp_seg_inode_page_get_nth_inode(seg_inode_page, n, &mtr);

      ut_a(mach_read_from_8(seg_inode + FSEG_ID) != 0);
      fseg_print_low(seg_inode, &mtr);

      n_segs++;

      next_node_addr = flst_get_next_addr(seg_inode_page + FSEG_INODE_PAGE_NODE, &mtr);
      mtr_commit(&mtr);
    } while (++n < FSP_SEG_INODES_PER_PAGE);

    node_addr = next_node_addr;
  }

  mtr_start(&mtr);
  mtr_x_lock(latch, &mtr);

  header = fsp_get_space_header(space, &mtr);

  node_addr = flst_get_first(header + FSP_SEG_INODES_FREE, &mtr);

  mtr_commit(&mtr);

  while (!fil_addr_is_null(node_addr)) {

    n = 0;

    do {

      mtr_start(&mtr);
      mtr_x_lock(latch, &mtr);

      seg_inode_page = fut_get_ptr(space, node_addr, RW_X_LATCH, &mtr) - FSEG_INODE_PAGE_NODE;

      seg_inode = fsp_seg_inode_page_get_nth_inode(seg_inode_page, n, &mtr);
      if (mach_read_from_8(seg_inode + FSEG_ID) != 0) {

        fseg_print_low(seg_inode, &mtr);
        n_segs++;
      }

      next_node_addr = flst_get_next_addr(seg_inode_page + FSEG_INODE_PAGE_NODE, &mtr);

      mtr_commit(&mtr);

      ++n;

    } while (n < FSP_SEG_INODES_PER_PAGE);

    node_addr = next_node_addr;
  }

  mtr_commit(&mtr2);

  ib_logger(ib_stream, "NUMBER of file segments: %lu\n", (ulong)n_segs);
}

ulint fsp_get_size_low(page_t *page) {
  return mach_read_from_4(page + FSP_HEADER_OFFSET + FSP_SIZE);
}
