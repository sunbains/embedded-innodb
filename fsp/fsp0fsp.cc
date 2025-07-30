/****************************************************************************
Copyright (c) 1995, 2010, Innobase Oy. All Rights Reserved.
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

/** @file fsp/fsp0fsp.c
File space management

Created 11/29/1995 Heikki Tuuri
***********************************************************************/

#include "fsp0fsp.h"
#include "btr0btr.h"
#include "buf0buf.h"
#include "dict0dict.h"
#include "dict0store.h"
#include "fil0fil.h"
#include "fut0fut.h"
#include "log0log.h"
#include "mtr0log.h"
#include "page0page.h"
#include "srv0srv.h"
#include "sync0sync.h"
#include "trx0sys.h"
#include "ut0byte.h"

/** Offset of the space header within a file page */
constexpr auto FSP_HEADER_OFFSET = FIL_PAGE_DATA;

/** Tablespace id */
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

FSP *srv_fsp{};

/**
 * Calculates the number of pages reserved by a segment, and how
 * many pages are currently used.
 *
 * @param[in] header            Segment inode
 * @param[out] used             Number of pages used (not more than reserved)
 * @param[in] mtr               mini-transaction handle
 *
 * @return number of reserved pages
 */
static ulint fseg_n_reserved_pages_low(FSP::fseg_inode_t *header, ulint *used, mtr_t *mtr) noexcept;

/**
 * @brief Gets a descriptor bit of a page.
 *
 * @param[in] descr             Descriptor data
 * @param[in] bit               XDES_FREE_BIT or XDES_CLEAN_BIT
 * @param[in] offset            Page offset within extent: 0 ... FSP_EXTENT_SIZE - 1
 * @param[in,out] mtr           mini-transaction handle
 *
 * @return true if free
 */
inline bool xdes_get_bit(const FSP::xdes_t *descr, ulint bit, ulint offset, mtr_t *mtr) noexcept {
  ut_ad(mtr->memo_contains_page(descr, MTR_MEMO_PAGE_X_FIX));
  ut_ad(bit == XDES_FREE_BIT || bit == XDES_CLEAN_BIT);
  ut_ad(offset < FSP_EXTENT_SIZE);

  auto index = bit + XDES_BITS_PER_PAGE * offset;
  auto byte_index = index / 8;
  auto bit_index = index % 8;

  return ut_bit_get_nth(mtr->read_ulint(descr + XDES_BITMAP + byte_index, MLOG_1BYTE), bit_index);
}

/**
 * Sets a descriptor bit of a page.
 *
 * @param[in] descr             Descriptor data
 * @param[in] bit               XDES_FREE_BIT or XDES_CLEAN_BIT
 * @param[in] offset            Page offset within extent: 0 ... FSP_EXTENT_SIZE - 1
 * @param[in] val               Bit value
 * @param[in,out] mtr           Mini-transaction handle
 */
inline void xdes_set_bit(FSP::xdes_t *descr, ulint bit, ulint offset, bool val, mtr_t *mtr) noexcept {
  ut_ad(mtr->memo_contains_page(descr, MTR_MEMO_PAGE_X_FIX));
  ut_ad(bit == XDES_FREE_BIT || bit == XDES_CLEAN_BIT);
  ut_ad(offset < FSP_EXTENT_SIZE);

  auto index = bit + XDES_BITS_PER_PAGE * offset;

  auto byte_index = index / 8;
  auto bit_index = index % 8;
  auto descr_byte = mtr->read_ulint(descr + XDES_BITMAP + byte_index, MLOG_1BYTE);

  descr_byte = ut_bit_set_nth(descr_byte, bit_index, val);

  mlog_write_ulint(descr + XDES_BITMAP + byte_index, descr_byte, MLOG_1BYTE, mtr);
}

/**
 * Looks for a descriptor bit having the desired value. Starts from hint
 * and scans upward; at the end of the extent the search is wrapped to
 * the start of the extent.
 *
 * @param[in] descr             Descriptor
 * @param[in] bit               XDES_FREE_BIT or XDES_CLEAN_BIT
 * @param[in] val               Desired bit value
 * @param[in] hint              Hint of which bit position would be desirable
 * @param[in,out] mtr           Mini-transaction handle
 *
 * @return bit index of the bit, ULINT_UNDEFINED if not found
 */
inline ulint xdes_find_bit(FSP::xdes_t *descr, ulint bit, bool val, ulint hint, mtr_t *mtr) noexcept {
  ut_ad(hint < FSP_EXTENT_SIZE);
  ut_ad(mtr->memo_contains_page(descr, MTR_MEMO_PAGE_X_FIX));

  for (ulint i = hint; i < FSP_EXTENT_SIZE; ++i) {
    if (val == xdes_get_bit(descr, bit, i, mtr)) {

      return i;
    }
  }

  for (ulint i = 0; i < hint; ++i) {
    if (val == xdes_get_bit(descr, bit, i, mtr)) {

      return i;
    }
  }

  return ULINT_UNDEFINED;
}

/**
 * @brief Returns the number of used pages in a descriptor.
 *
 * @param[in] descr             Descriptor data
 * @param[in] mtr               Mini-transaction handle
 *
 * @return number of pages used
 */
inline ulint xdes_get_n_used(const FSP::xdes_t *descr, mtr_t *mtr) noexcept {
  ut_ad(mtr->memo_contains_page(descr, MTR_MEMO_PAGE_X_FIX));

  ulint count{};

  for (ulint i{}; i < FSP_EXTENT_SIZE; ++i) {
    if (false == xdes_get_bit(descr, XDES_FREE_BIT, i, mtr)) {
      ++count;
    }
  }

  return count;
}

/**
 * @brief Returns true if extent contains no used pages.
 *
 * @param[in] descr             Descriptor data
 * @param[in] mtr               mini-transaction handle
 *
 * @return true if totally free
 */
inline bool xdes_is_free(const FSP::xdes_t *descr, mtr_t *mtr) noexcept {
  return xdes_get_n_used(descr, mtr) == 0;
}

/**
 * @brief Returns true if extent contains no free pages.
 *
 * @param[in] descr             Descriptor data
 * @param[in] mtr               Mini-transaction handle
 *
 * @return true if full
 */
inline bool xdes_is_full(const FSP::xdes_t *descr, mtr_t *mtr) noexcept {
  return FSP_EXTENT_SIZE == xdes_get_n_used(descr, mtr);
}

/** Sets the state of an xdes.
 *
 * @param[in] descr             Descriptor data
 * @param[in] state             State to set
 * @param[in] mtr               Mini-transaction handle
 */
inline void xdes_set_state(FSP::xdes_t *descr, ulint state, mtr_t *mtr) noexcept {
  ut_ad(state >= XDES_FREE);
  ut_ad(state <= XDES_FSEG);
  ut_ad(mtr->memo_contains_page(descr, MTR_MEMO_PAGE_X_FIX));

  mlog_write_ulint(descr + XDES_STATE, state, MLOG_4BYTES, mtr);
}

/**
 * Gets the state of an xdes.
 *
 * @param[in] descr             Descriptor
 * @param[in] mtr               Mini-transaction handle
 *
 * @return state
 */
inline ulint xdes_get_state(const FSP::xdes_t *descr, mtr_t *mtr) noexcept {
  ut_ad(mtr->memo_contains_page(descr, MTR_MEMO_PAGE_X_FIX));

  auto state = mtr->read_ulint(descr + XDES_STATE, MLOG_4BYTES);
  ut_ad(state - 1 < XDES_FSEG);

  return state;
}

/**
 * @brief Inits an extent descriptor to the free and clean state.
 *
 * @param[in] descr             Descriptor data
 * @param[in] mtr               Mini-transaction handle
 */
inline void xdes_init(FSP::xdes_t *descr, mtr_t *mtr) noexcept {
  ut_ad(mtr->memo_contains_page(descr, MTR_MEMO_PAGE_X_FIX));
  ut_ad((XDES_SIZE - XDES_BITMAP) % 4 == 0);

  for (ulint i = XDES_BITMAP; i < XDES_SIZE; i += 4) {
    mlog_write_ulint(descr + i, 0xFFFFFFFFUL, MLOG_4BYTES, mtr);
  }

  xdes_set_state(descr, XDES_FREE, mtr);
}

/**
 * Calculates the page where the descriptor of a page resides.
 *
 * @return	descriptor page offset
 */
inline page_no_t xdes_calc_descriptor_page(page_no_t page_no) noexcept {
  return ut_2pow_round(page_no, UNIV_PAGE_SIZE);
}

/**
 * Calculates the descriptor index within a descriptor page.
 *
 * @param[in] page_no           Page number of the descriptor page
 *
 * @return descriptor index
 */
inline ulint xdes_calc_descriptor_index(page_no_t page_no) noexcept {
  return ut_2pow_remainder(page_no, UNIV_PAGE_SIZE) / FSP_EXTENT_SIZE;
}

/**
 * @brief Returns page offset of the first page in extent described by a descriptor.
 *
 * @param[in] descr             Extent descriptor
 *
 * @return offset of the first page in extent
 */
inline ulint xdes_get_offset(FSP::xdes_t *descr) noexcept {
  return page_get_page_no(page_align(descr)) + ((page_offset(descr) - XDES_ARR_OFFSET) / XDES_SIZE) * FSP_EXTENT_SIZE;
}

/**
 * @brief Inits a file page whose prior contents should be ignored.
 *
 * @param[in] block             Pointer to block
 */
static void fsp_init_file_page_low(Buf_block *block) noexcept {
  auto page = block->get_frame();

  block->m_check_index_page_at_flush = false;

  UNIV_MEM_INVALID(page, UNIV_PAGE_SIZE);

  memset(page, 0, UNIV_PAGE_SIZE);
  mach_write_to_4(page + FIL_PAGE_OFFSET, block->get_page_no());
  memset(page + FIL_PAGE_LSN, 0, 8);
  mach_write_to_4(page + FIL_PAGE_SPACE_ID, block->get_space());
  memset(page + UNIV_PAGE_SIZE - FIL_PAGE_END_LSN_CHKSUM, 0, 8);
  mach_write_to_4(page + FIL_PAGE_SPACE_ID, block->get_space());
}

/**
 * @brief Inits a file page whose prior contents should be ignored.
 *
 * @param[in] block             Pointer to block
 * @param[in] mtr               Mini-transaction handle
 */
static void fsp_init_file_page(Buf_block *block, mtr_t *mtr) noexcept {
  fsp_init_file_page_low(block);

  mlog_write_initial_log_record(block->get_frame(), MLOG_INIT_FILE_PAGE, mtr);
}

/**
 * Returns the nth inode slot on an inode page.
 *
 * @param[in] page              Segment inode page
 * @param[in] i                 inode index on page
 *
 * @return	segment inode
 */
inline FSP::fseg_inode_t *fsp_seg_inode_page_get_nth_inode(page_t *page, ulint i, mtr_t *mtr __attribute__((unused))) noexcept {
  ut_ad(i < FSP_SEG_INODES_PER_PAGE);
  ut_ad(mtr->memo_contains_page(page, MTR_MEMO_PAGE_X_FIX));

  return page + FSEG_ARR_OFFSET + FSEG_INODE_SIZE * i;
}

/**
 * Looks for a used segment inode on a segment inode page.
 *
 * @param[in] page              Segment inode page
 * @param[in] mtr               Mini-transaction handle
 *
 * @return	segment inode index, or ULINT_UNDEFINED if not found
 */
static ulint fsp_seg_inode_page_find_used(page_t *page, mtr_t *mtr) noexcept {
  for (ulint i{}; i < FSP_SEG_INODES_PER_PAGE; ++i) {

    auto inode = fsp_seg_inode_page_get_nth_inode(page, i, mtr);

    if (mach_read_from_8(inode + FSEG_ID) > 0) {
      /* This is used */
      ut_ad(mach_read_from_4(inode + FSEG_MAGIC_N) == FSEG_MAGIC_N_VALUE);

      return i;
    }
  }

  return ULINT_UNDEFINED;
}

/**
 * Looks for an unused segment inode on a segment inode page.
 *
 * @param[in] page              Segment inode page
 * @param[in] i                 Search forward starting from this index
 * @param[in] mtr               Mini-transaction handle
 *
 * @return segment inode index, or ULINT_UNDEFINED if not found
 */
static ulint fsp_seg_inode_page_find_free(page_t *page, ulint i, mtr_t *mtr) noexcept {
  for (; i < FSP_SEG_INODES_PER_PAGE; ++i) {

    auto inode = fsp_seg_inode_page_get_nth_inode(page, i, mtr);

    if (mach_read_from_8(inode + FSEG_ID) == 0) {
      /* This is unused */
      return i;
    }

    ut_ad(mach_read_from_4(inode + FSEG_MAGIC_N) == FSEG_MAGIC_N_VALUE);
  }

  return ULINT_UNDEFINED;
}

/**
 * Frees a segment inode in the specified space.
 *
 * @param[in] space             The space id of the segment.
 * @param[in] inode             The segment inode to be freed.
 * @param[in] mtr               Mini-transaction handle.
 */
void FSP::free_seg_inode(space_id_t space, fseg_inode_t *inode, mtr_t *mtr) noexcept {
  auto page = page_align(inode);
  auto space_header = get_space_header(space, mtr);

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

    free_page(space, page_get_page_no(page), mtr);
  }
}

/**
 * Tries to get the file segment inode for the specified space from the segment header.
 *
 * @param[in] header            The segment header.
 * @param[in] space             Tablespace id.
 * @param[in] mtr               Mini-transaction handle.
 *
 * @return A pointer to the file segment inode if found - x-latched, otherwise nullptr.
 */
static FSP::fseg_inode_t *fseg_inode_try_get(fseg_header_t *header, space_id_t space, mtr_t *mtr) noexcept {
  Fil_addr inode_addr;

  inode_addr.m_page_no = mach_read_from_4(header + FSEG_HDR_PAGE_NO);
  inode_addr.m_boffset = mach_read_from_2(header + FSEG_HDR_OFFSET);

  ut_ad(space == mach_read_from_4(header + FSEG_HDR_SPACE));

  auto inode = fut_get_ptr(space, inode_addr, RW_X_LATCH, mtr);

  if (unlikely(mach_read_from_8(inode + FSEG_ID) == 0)) {

    inode = nullptr;
  } else {
    ut_ad(mach_read_from_4(inode + FSEG_MAGIC_N) == FSEG_MAGIC_N_VALUE);
  }

  return inode;
}

/**
 * Retrieves the fseg_inode_t structure for a given space from the segment header.
 *
 * @param[in] header            The segment header.
 * @param[in] space             Tablespace id.
 * @param[in] mtr               Mini-transaction handle.
 *
 * @return A pointer to the fseg_inode_t structure if found, otherwise NULL.
 */
static FSP::fseg_inode_t *fseg_inode_get(fseg_header_t *header, space_id_t space, mtr_t *mtr) noexcept {
  auto inode = fseg_inode_try_get(header, space, mtr);
  ut_a(inode != nullptr);
  return inode;
}

/**
 * @brief Gets the page number from the nth fragment page slot.
 *
 * @param[in] inode             The segment inode.
 * @param[in] n                 The slot index.
 * @param[in] mtr               The mtr handle.
 *
 * @return The page number, FIL_NULL if not in use.
 */
inline ulint fseg_get_nth_frag_page_no(FSP::fseg_inode_t *inode, ulint n, mtr_t *mtr __attribute__((unused))) noexcept {
  ut_ad(n < FSEG_FRAG_ARR_N_SLOTS);
  ut_ad(mtr->memo_contains_page(inode, MTR_MEMO_PAGE_X_FIX));
  ut_ad(mach_read_from_4(inode + FSEG_MAGIC_N) == FSEG_MAGIC_N_VALUE);

  return mach_read_from_4(inode + FSEG_FRAG_ARR + n * FSEG_FRAG_SLOT_SIZE);
}

/**
 * Sets the page number in the nth fragment page slot.
 *
 * @param[in] inode             The segment inode.
 * @param[in] n                 The slot index.
 * @param[in] page_no           The page number to set.
 * @param[in] mtr               Mini-transaction handle.
 */
inline void fseg_set_nth_frag_page_no(FSP::fseg_inode_t *inode, ulint n, ulint page_no, mtr_t *mtr) noexcept {
  ut_ad(n < FSEG_FRAG_ARR_N_SLOTS);
  ut_ad(mtr->memo_contains_page(inode, MTR_MEMO_PAGE_X_FIX));
  ut_ad(mach_read_from_4(inode + FSEG_MAGIC_N) == FSEG_MAGIC_N_VALUE);

  mlog_write_ulint(inode + FSEG_FRAG_ARR + n * FSEG_FRAG_SLOT_SIZE, page_no, MLOG_4BYTES, mtr);
}

/** Finds a fragment page slot which is free.
 *
 * @param[in] inode             The segment inode.
 * @param[in] mtr               Mini-transaction handle.
 *
 * @return Slot index; ULINT_UNDEFINED if none found.
 */
static ulint fseg_find_free_frag_page_slot(FSP::fseg_inode_t *inode, mtr_t *mtr) noexcept {
  for (ulint i{}; i < FSEG_FRAG_ARR_N_SLOTS; ++i) {
    const auto page_no = fseg_get_nth_frag_page_no(inode, i, mtr);

    if (page_no == FIL_NULL) {

      return i;
    }
  }

  return ULINT_UNDEFINED;
}

/**
 * Finds a fragment page slot which is used and last in the array.
 *
 * @param[in] inode             The segment inode.
 * @param[in] mtr               Mini-transaction handle.
 *
 * @return Slot index; ULINT_UNDEFINED if none found.
 */
static ulint fseg_find_last_used_frag_page_slot(FSP::fseg_inode_t *inode, mtr_t *mtr) noexcept {
  for (ulint i{}; i < FSEG_FRAG_ARR_N_SLOTS; ++i) {
    const ulint page_no = fseg_get_nth_frag_page_no(inode, FSEG_FRAG_ARR_N_SLOTS - i - 1, mtr);

    if (page_no != FIL_NULL) {
      return FSEG_FRAG_ARR_N_SLOTS - i - 1;
    }
  }

  return ULINT_UNDEFINED;
}

/**
 * @brief Calculates reserved fragment page slots.
 *
 * @param[in] inode             The segment inode.
 * @param[in] mtr               Mini-transaction handle.
 *
 * @return The number of fragment pages.
 */
static ulint fseg_get_n_frag_pages(FSP::fseg_inode_t *inode, mtr_t *mtr) noexcept {
  ulint count{};

  for (ulint i{}; i < FSEG_FRAG_ARR_N_SLOTS; ++i) {
    if (FIL_NULL != fseg_get_nth_frag_page_no(inode, i, mtr)) {
      ++count;
    }
  }

  return count;
}

/**
 * Writes info of a segment.
 *
 * @param[in] inode             Segment inode
 * @param[in] mtr               Mini-transaction handle
 */
static void fseg_print_low(FSP::fseg_inode_t *inode, mtr_t *mtr) noexcept {
  ut_ad(mtr->memo_contains_page(inode, MTR_MEMO_PAGE_X_FIX));

  ulint used{};
  auto space = page_get_space_id(page_align(inode));
  auto page_no = page_get_page_no(page_align(inode));
  auto reserved = fseg_n_reserved_pages_low(inode, &used, mtr);
  auto d_var = mtr->read_uint64(inode + FSEG_ID);
  auto seg_id_low = d_var;
  auto seg_id_high = d_var;

  auto n_used = mtr->read_ulint(inode + FSEG_NOT_FULL_N_USED, MLOG_4BYTES);
  auto n_frag = fseg_get_n_frag_pages(inode, mtr);
  auto n_free = flst_get_len(inode + FSEG_FREE, mtr);
  auto n_not_full = flst_get_len(inode + FSEG_NOT_FULL, mtr);
  auto n_full = flst_get_len(inode + FSEG_FULL, mtr);

  log_info(std::format(
    "SEGMENT id {} {} space {}; page {}; res {} used {}; full ext {}"
    " fragm pages {}; free extents {}; not full extents {}: pages {}\n",
    seg_id_high,
    seg_id_low,
    space,
    page_no,
    reserved,
    used,
    n_full,
    n_frag,
    n_free,
    n_not_full,
    n_used
  ));

  ut_ad(mach_read_from_4(inode + FSEG_MAGIC_N) == FSEG_MAGIC_N_VALUE);
}

static ulint fseg_n_reserved_pages_low(FSP::fseg_inode_t *inode, ulint *used, mtr_t *mtr) noexcept {
  ut_ad(mtr->memo_contains_page(inode, MTR_MEMO_PAGE_X_FIX));

  *used = mtr->read_ulint(inode + FSEG_NOT_FULL_N_USED, MLOG_4BYTES) + FSP_EXTENT_SIZE * flst_get_len(inode + FSEG_FULL, mtr) +
          fseg_get_n_frag_pages(inode, mtr);

  auto ret = fseg_get_n_frag_pages(inode, mtr) + FSP_EXTENT_SIZE * flst_get_len(inode + FSEG_FREE, mtr) +
             FSP_EXTENT_SIZE * flst_get_len(inode + FSEG_NOT_FULL, mtr) + FSP_EXTENT_SIZE * flst_get_len(inode + FSEG_FULL, mtr);

  return ret;
}

bool FSP::try_extend_data_file_with_pages(space_id_t space, page_no_t page_no, fsp_header_t *header, mtr_t *mtr) noexcept {
  ut_a(space != SYS_TABLESPACE);

  auto size = mtr->read_ulint(header + FSP_SIZE, MLOG_4BYTES);

  ut_a(page_no >= size);

  page_no_t actual_size;
  auto success = m_fil->extend_space_to_desired_size(&actual_size, space, page_no + 1);

  /* actual_size now has the space size in pages; it may be less than
  we wanted if we ran out of disk space */

  mlog_write_ulint(header + FSP_SIZE, actual_size, MLOG_4BYTES, mtr);

  return success;
}

bool FSP::try_extend_data_file(ulint *actual_increase, space_id_t space, fsp_header_t *header, mtr_t *mtr) noexcept {
  *actual_increase = 0;

  auto extent_size = FSP_EXTENT_SIZE;
  auto size = mtr->read_ulint(header + FSP_SIZE, MLOG_4BYTES);
  auto old_size = size;

  /* We extend single-table tablespaces first one extent at a time, but for bigger
  tablespaces more. It is not enough to extend always by one extent, because some
  extents are frag page extents. */

  if (size < extent_size) {
    /* Let us first extend the file to extent_size */
    auto success = try_extend_data_file_with_pages(space, extent_size - 1, header, mtr);

    if (!success) {
      auto new_size = mtr->read_ulint(header + FSP_SIZE, MLOG_4BYTES);

      *actual_increase = new_size - old_size;

      return false;
    }

    size = extent_size;
  }

  page_no_t size_increase;

  if (size < 32 * extent_size) {
    size_increase = extent_size;
  } else {
    /* Below in fsp_fill_free_list() we assume that we add at most FSP_FREE_ADD
    extents at a time */
    size_increase = FSP_FREE_ADD * extent_size;
  }

  if (size_increase == 0) {

    return true;
  }

  page_no_t actual_size;

  {
    auto success = m_fil->extend_space_to_desired_size(&actual_size, space, size + size_increase);
    ut_a(success);
  }

  /* We ignore any fragments of a full megabyte when storing the size
  to the space header */

  const auto new_size = ut_calc_align_down(actual_size, (1024 * 1024) / UNIV_PAGE_SIZE);
  mlog_write_ulint(header + FSP_SIZE, new_size, MLOG_4BYTES, mtr);

  *actual_increase = new_size - old_size;

  return true;
}

void FSP::free_page(space_id_t space, page_no_t page_no, mtr_t *mtr) noexcept {
  auto header = get_space_header(space, mtr);
  auto descr = xdes_get_descriptor_with_space_hdr(header, space, page_no, mtr);
  auto state = xdes_get_state(descr, mtr);

  if (state != XDES_FREE_FRAG && state != XDES_FULL_FRAG) {
    log_err(std::format("File space extent descriptor of page {} has state {}", page_no, state));
    log_err("Dump of descriptor: ");
    log_warn_buf(reinterpret_cast<byte *>(descr) - 50, 200);

    if (state == XDES_FREE) {
      /* We put here some fault tolerance: if the page
      is already free, return without doing anything! */

      return;
    }

    ut_error;
  }

  if (xdes_get_bit(descr, XDES_FREE_BIT, page_no % FSP_EXTENT_SIZE, mtr)) {
    log_err(std::format("File space extent descriptor of page {} says it is free. Dump of descriptor: ", page_no));
    log_warn_buf(reinterpret_cast<byte *>(descr) - 50, 200);

    /* We put here some fault tolerance: if the page is already free,
    return without doing anything! */

    return;
  }

  xdes_set_bit(descr, XDES_FREE_BIT, page_no % FSP_EXTENT_SIZE, true, mtr);
  xdes_set_bit(descr, XDES_CLEAN_BIT, page_no % FSP_EXTENT_SIZE, true, mtr);

  auto frag_n_used = mtr->read_ulint(header + FSP_FRAG_N_USED, MLOG_4BYTES);

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
    free_extent(space, page_no, mtr);
  }
}

void FSP::free_extent(space_id_t space, ulint page, mtr_t *mtr) noexcept {
  auto header = get_space_header(space, mtr);
  auto descr = xdes_get_descriptor_with_space_hdr(header, space, page, mtr);

  if (xdes_get_state(descr, mtr) == XDES_FREE) {

    log_warn_buf(reinterpret_cast<byte *>(descr) - 500, 1000);

    ut_error;
  }

  xdes_init(descr, mtr);

  flst_add_last(header + FSP_FREE, descr + XDES_FLST_NODE, mtr);
}

void FSP::fseg_fill_free_list(FSP::fseg_inode_t *inode, space_id_t space, page_no_t hint, mtr_t *mtr) noexcept {
  ut_ad(!((page_offset(inode) - FSEG_ARR_OFFSET) % FSEG_INODE_SIZE));

  ulint used;
  const auto reserved = fseg_n_reserved_pages_low(inode, &used, mtr);

  if (reserved < FSEG_FREE_LIST_LIMIT * FSP_EXTENT_SIZE) {

    /* The segment is too small to allow extents in free list */

    return;
  }

  if (flst_get_len(inode + FSEG_FREE, mtr) > 0) {
    /* Free list is not empty */

    return;
  }

  for (ulint i{}; i < FSEG_FREE_LIST_MAX_LEN; ++i) {
    auto descr = xdes_get_descriptor(space, hint, mtr);

    if (descr == nullptr || XDES_FREE != xdes_get_state(descr, mtr)) {

      /* We cannot allocate the desired extent: stop */

      return;
    }

    descr = alloc_free_extent(space, hint, mtr);

    xdes_set_state(descr, XDES_FSEG, mtr);

    const auto seg_id = mtr->read_uint64(inode + FSEG_ID);
    ut_ad(mach_read_from_4(inode + FSEG_MAGIC_N) == FSEG_MAGIC_N_VALUE);

    mlog_write_uint64(descr + XDES_ID, seg_id, mtr);

    flst_add_last(inode + FSEG_FREE, descr + XDES_FLST_NODE, mtr);

    hint += FSP_EXTENT_SIZE;
  }
}

FSP::xdes_t *FSP::fseg_alloc_free_extent(fseg_inode_t *inode, space_id_t space, mtr_t *mtr) noexcept {
  xdes_t *descr;

  ut_ad(!((page_offset(inode) - FSEG_ARR_OFFSET) % FSEG_INODE_SIZE));
  ut_ad(mach_read_from_4(inode + FSEG_MAGIC_N) == FSEG_MAGIC_N_VALUE);

  if (flst_get_len(inode + FSEG_FREE, mtr) > 0) {
    /* Segment free list is not empty, allocate from it */

    const auto first = flst_get_first(inode + FSEG_FREE, mtr);

    descr = xdes_lst_get_descriptor(space, first, mtr);
  } else {
    /* Segment free list was empty, allocate from space */
    descr = alloc_free_extent(space, 0, mtr);

    if (descr == nullptr) {

      return nullptr;
    }

    const auto seg_id = mtr->read_uint64(inode + FSEG_ID);

    xdes_set_state(descr, XDES_FSEG, mtr);
    mlog_write_uint64(descr + XDES_ID, seg_id, mtr);
    flst_add_last(inode + FSEG_FREE, descr + XDES_FLST_NODE, mtr);

    /* Try to fill the segment free list */
    fseg_fill_free_list(inode, space, xdes_get_offset(descr) + FSP_EXTENT_SIZE, mtr);
  }

  return descr;
}

bool FSP::reserve_free_pages(space_id_t space, fsp_header_t *space_header, ulint size, mtr_t *mtr) noexcept {
  ut_a(size < FSP_EXTENT_SIZE / 2);

  auto descr = xdes_get_descriptor_with_space_hdr(space_header, space, 0, mtr);
  const auto n_used = xdes_get_n_used(descr, mtr);

  ut_a(n_used <= size);

  if (size >= n_used + 2) {
    return true;
  } else {
    return try_extend_data_file_with_pages(space, n_used + 1, space_header, mtr);
  }
}

void FSP::fseg_free_page_low(fseg_inode_t *seg_inode, space_id_t space, ulint page, mtr_t *mtr) noexcept {
  ut_ad(mach_read_from_4(seg_inode + FSEG_MAGIC_N) == FSEG_MAGIC_N_VALUE);
  ut_ad(!((page_offset(seg_inode) - FSEG_ARR_OFFSET) % FSEG_INODE_SIZE));

  /* Drop search system page hash index if the page is found in
  the pool and is hashed */

  auto descr = xdes_get_descriptor(space, page, mtr);
  ut_a(descr != nullptr);

  if (xdes_get_bit(descr, XDES_FREE_BIT, page % FSP_EXTENT_SIZE, mtr)) {
    log_warn("Dump of the tablespace extent descriptor: ");
    log_warn_buf(descr, 40);

    log_warn(std::format(
      "\n"
      "Serious error! InnoDB is trying to free page {} though it is already marked as free"
      " in the tablespace! The tablespace free space info is corrupt. You may need to dump"
      " your InnoDB tables and recreate the whole database!",
      page
    ));

    log_fatal("Please refer to the Embdedded InnoDB GitHub repository for details about forcing recovery.");
  }

  auto state = xdes_get_state(descr, mtr);

  if (state != XDES_FSEG) {
    /* The page is in the fragment pages of the segment */

    for (ulint i = 0;; ++i) {
      if (fseg_get_nth_frag_page_no(seg_inode, i, mtr) == page) {

        fseg_set_nth_frag_page_no(seg_inode, i, FIL_NULL, mtr);
        break;
      }
    }

    free_page(space, page, mtr);

    return;
  }

  /* If we get here, the page is in some extent of the segment */

  const auto descr_id = mtr->read_uint64(descr + XDES_ID);
  const auto seg_id = mtr->read_uint64(seg_inode + FSEG_ID);

  if (descr_id != seg_id) {
    log_warn("Dump of the tablespace extent descriptor: ");
    log_warn_buf(descr, 40);
    log_warn("Dump of the segment inode: ");
    log_warn_buf(seg_inode, 40);

    log_warn(std::format(
      "Serious error: InnoDB is trying to free space {} page {}, which does not belong to"
      " segment {}  {} but belongs to segment {}  {}.",
      space,
      page,
      descr_id,
      descr_id,
      seg_id,
      seg_id
    ));

    log_fatal("Please refer to the Embdedded InnoDB GitHub repository for details about forcing recovery.");
  }

  const auto not_full_n_used = mtr->read_ulint(seg_inode + FSEG_NOT_FULL_N_USED, MLOG_4BYTES);

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
    free_extent(space, page, mtr);
  }
}

void FSP::fseg_free_extent(fseg_inode_t *seg_inode, space_id_t space, page_no_t page, mtr_t *mtr) noexcept {
  auto descr = xdes_get_descriptor(space, page, mtr);

  ut_a(xdes_get_state(descr, mtr) == XDES_FSEG);
  ut_a(mtr->read_uint64(descr + XDES_ID) == mtr->read_uint64(seg_inode + FSEG_ID));
  ut_ad(mach_read_from_4(seg_inode + FSEG_MAGIC_N) == FSEG_MAGIC_N_VALUE);

  if (xdes_is_full(descr, mtr)) {
    flst_remove(seg_inode + FSEG_FULL, descr + XDES_FLST_NODE, mtr);
  } else if (xdes_is_free(descr, mtr)) {
    flst_remove(seg_inode + FSEG_FREE, descr + XDES_FLST_NODE, mtr);
  } else {
    flst_remove(seg_inode + FSEG_NOT_FULL, descr + XDES_FLST_NODE, mtr);

    auto not_full_n_used = mtr->read_ulint(seg_inode + FSEG_NOT_FULL_N_USED, MLOG_4BYTES);

    auto descr_n_used = xdes_get_n_used(descr, mtr);

    ut_a(not_full_n_used >= descr_n_used);

    mlog_write_ulint(seg_inode + FSEG_NOT_FULL_N_USED, not_full_n_used - descr_n_used, MLOG_4BYTES, mtr);
  }

  free_extent(space, page, mtr);

#ifdef UNIV_DEBUG
  const auto first_page_in_extent = page - (page % FSP_EXTENT_SIZE);

  Page_id page_id(space, NULL_PAGE_NO);
  for (ulint i{}; i < FSP_EXTENT_SIZE; ++i) {
    page_id.set_page_no(first_page_in_extent + i);
    m_buf_pool->set_file_page_was_freed(page_id);
  }
#endif /* UNIV_DEBUG */
}

FSP::xdes_t *FSP::fseg_get_first_extent(fseg_inode_t *inode, space_id_t space, mtr_t *mtr) noexcept {
  ut_ad(space == page_get_space_id(page_align(inode)));
  ut_ad(mach_read_from_4(inode + FSEG_MAGIC_N) == FSEG_MAGIC_N_VALUE);

  auto first = fil_addr_null;

  if (flst_get_len(inode + FSEG_FULL, mtr) > 0) {

    first = flst_get_first(inode + FSEG_FULL, mtr);

  } else if (flst_get_len(inode + FSEG_NOT_FULL, mtr) > 0) {

    first = flst_get_first(inode + FSEG_NOT_FULL, mtr);

  } else if (flst_get_len(inode + FSEG_FREE, mtr) > 0) {

    first = flst_get_first(inode + FSEG_FREE, mtr);
  }

  if (first.m_page_no == FIL_NULL) {

    return nullptr;

  } else {

    return xdes_lst_get_descriptor(space, first, mtr);
  }
}

bool FSP::fseg_validate_low(fseg_inode_t *inode, mtr_t *mtr2) noexcept {
  mtr_t mtr;

  ut_ad(mtr2->memo_contains_page(inode, MTR_MEMO_PAGE_X_FIX));
  ut_ad(mach_read_from_4(inode + FSEG_MAGIC_N) == FSEG_MAGIC_N_VALUE);

  auto space = page_get_space_id(page_align(inode));
  auto seg_id = mtr2->read_uint64(inode + FSEG_ID);
  const auto n_used = mtr2->read_ulint(inode + FSEG_NOT_FULL_N_USED, MLOG_4BYTES);

  flst_validate(inode + FSEG_FREE, mtr2);
  flst_validate(inode + FSEG_NOT_FULL, mtr2);
  flst_validate(inode + FSEG_FULL, mtr2);

  /* Validate FSEG_FREE list */
  auto node_addr = flst_get_first(inode + FSEG_FREE, mtr2);

  while (!m_fil->addr_is_null(node_addr)) {
    mtr.start();

    mtr_x_lock(m_fil->space_get_latch(space), &mtr);

    auto descr = xdes_lst_get_descriptor(space, node_addr, &mtr);

    ut_a(xdes_get_n_used(descr, &mtr) == 0);
    ut_a(xdes_get_state(descr, &mtr) == XDES_FSEG);
    ut_a(mtr.read_uint64(descr + XDES_ID) == seg_id);

    node_addr = flst_get_next_addr(descr + XDES_FLST_NODE, &mtr);

    mtr.commit();
  }

  /* Validate FSEG_NOT_FULL list */

  ulint n_used2{};

  node_addr = flst_get_first(inode + FSEG_NOT_FULL, mtr2);

  while (!m_fil->addr_is_null(node_addr)) {
    mtr.start();
    mtr_x_lock(m_fil->space_get_latch(space), &mtr);

    auto descr = xdes_lst_get_descriptor(space, node_addr, &mtr);

    ut_a(xdes_get_n_used(descr, &mtr) > 0);
    ut_a(xdes_get_n_used(descr, &mtr) < FSP_EXTENT_SIZE);
    ut_a(xdes_get_state(descr, &mtr) == XDES_FSEG);
    ut_a(mtr.read_uint64(descr + XDES_ID) == seg_id);

    n_used2 += xdes_get_n_used(descr, &mtr);

    node_addr = flst_get_next_addr(descr + XDES_FLST_NODE, &mtr);

    mtr.commit();
  }

  /* Validate FSEG_FULL list */

  node_addr = flst_get_first(inode + FSEG_FULL, mtr2);

  while (!m_fil->addr_is_null(node_addr)) {
    mtr.start();
    mtr_x_lock(m_fil->space_get_latch(space), &mtr);

    auto descr = xdes_lst_get_descriptor(space, node_addr, &mtr);

    ut_a(xdes_get_n_used(descr, &mtr) == FSP_EXTENT_SIZE);
    ut_a(xdes_get_state(descr, &mtr) == XDES_FSEG);
    ut_a(mtr.read_uint64(descr + XDES_ID) == seg_id);

    node_addr = flst_get_next_addr(descr + XDES_FLST_NODE, &mtr);

    mtr.commit();
  }

  ut_a(n_used == n_used2);

  return true;
}

FSP *FSP::create(Log *log, Fil *fil, Buf_pool *buf_pool) noexcept {
  auto ptr = static_cast<log_t *>(ut_new(sizeof(FSP)));
  return new (ptr) FSP(log, fil, buf_pool);
}

void FSP::destroy(FSP *&fsp) noexcept {
  call_destructor(fsp);
  ut_delete(fsp);
  fsp = nullptr;
}

FSP::fsp_header_t *FSP::get_space_header(space_id_t id, mtr_t *mtr) noexcept {
  Buf_pool::Request req{
    .m_rw_latch = RW_X_LATCH, .m_page_id = {id, 0}, .m_mode = BUF_GET, .m_file = __FILE__, .m_line = __LINE__, .m_mtr = mtr
  };

  auto block = m_buf_pool->get(req, nullptr);
  auto header = FSP_HEADER_OFFSET + block->get_frame();

  buf_block_dbg_add_level(IF_SYNC_DEBUG(block, SYNC_FSP_PAGE));

  ut_ad(id == mach_read_from_4(FSP_SPACE_ID + header));

  return header;
}

FSP::xdes_t *FSP::xdes_lst_get_descriptor(space_id_t space, Fil_addr lst_node, mtr_t *mtr) noexcept {
  ut_ad(mtr->memo_contains(m_fil->space_get_latch(space), MTR_MEMO_X_LOCK));
  auto descr = fut_get_ptr(space, lst_node, RW_X_LATCH, mtr) - XDES_FLST_NODE;

  return descr;
}

void FSP::fill_free_list(bool init_space, space_id_t space, fsp_header_t *header, mtr_t *mtr) noexcept {
  ut_ad(page_offset(header) == FSP_HEADER_OFFSET);

  /* Check if we can fill free list from above the free list limit */
  auto size = mtr->read_ulint(header + FSP_SIZE, MLOG_4BYTES);
  auto limit = mtr->read_ulint(header + FSP_FREE_LIMIT, MLOG_4BYTES);
  bool extend_file = (size < limit + FSP_EXTENT_SIZE * FSP_FREE_ADD) && !init_space;

  if (extend_file) {
    ulint actual_increase{};

    try_extend_data_file(&actual_increase, space, header, mtr);
    size = mtr->read_ulint(header + FSP_SIZE, MLOG_4BYTES);
  }

  ulint count{};
  auto i = limit;

  while ((init_space && i < 1) || (i + FSP_EXTENT_SIZE <= size && count < FSP_FREE_ADD)) {

    auto init_xdes = ut_2pow_remainder(i, UNIV_PAGE_SIZE) == 0;

    mlog_write_ulint(header + FSP_FREE_LIMIT, i + FSP_EXTENT_SIZE, MLOG_4BYTES, mtr);

    /* Update the free limit info in the log system and make a checkpoint */
    if (space == SYS_TABLESPACE) {
      m_log->fsp_current_free_limit_set_and_checkpoint((i + FSP_EXTENT_SIZE) / ((1024 * 1024) / UNIV_PAGE_SIZE));
    }

    if (unlikely(init_xdes)) {

      Buf_block *block;

      /* We are going to initialize a new descriptor page:
      the prior contents of the pages should be ignored. */

      if (i > 0) {
        block = m_buf_pool->create(Page_id(space, i), mtr);

        Buf_pool::Request req{
          .m_rw_latch = RW_X_LATCH,
          .m_page_id = {space, static_cast<page_no_t>(i)},
          .m_mode = BUF_GET,
          .m_file = __FILE__,
          .m_line = __LINE__,
          .m_mtr = mtr
        };

        {
          const auto b = m_buf_pool->get(req, nullptr);
          ut_a(block == b);
        }

        buf_block_dbg_add_level(IF_SYNC_DEBUG(block, SYNC_FSP_PAGE));

        fsp_init_file_page(block, mtr);

        mlog_write_ulint(block->get_frame() + FIL_PAGE_TYPE, FIL_PAGE_TYPE_XDES, MLOG_2BYTES, mtr);
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

      const auto frag_n_used = mtr->read_ulint(header + FSP_FRAG_N_USED, MLOG_4BYTES);
      mlog_write_ulint(header + FSP_FRAG_N_USED, frag_n_used + 2, MLOG_4BYTES, mtr);
    } else {
      flst_add_last(header + FSP_FREE, descr + XDES_FLST_NODE, mtr);
      count++;
    }

    i += FSP_EXTENT_SIZE;
  }
}

FSP::xdes_t *FSP::alloc_free_extent(space_id_t space, ulint hint, mtr_t *mtr) noexcept {
  auto header = get_space_header(space, mtr);
  auto descr = xdes_get_descriptor_with_space_hdr(header, space, hint, mtr);

  if (descr != nullptr && (xdes_get_state(descr, mtr) == XDES_FREE)) {
    /* Ok, we can take this extent */
  } else {
    /* Take the first extent in the free list */
    auto first = flst_get_first(header + FSP_FREE, mtr);

    if (m_fil->addr_is_null(first)) {
      fill_free_list(false, space, header, mtr);

      first = flst_get_first(header + FSP_FREE, mtr);
    }

    if (m_fil->addr_is_null(first)) {
      /* No free extents left */
      return nullptr;
    }

    descr = xdes_lst_get_descriptor(space, first, mtr);
  }

  flst_remove(header + FSP_FREE, descr + XDES_FLST_NODE, mtr);

  return descr;
}

ulint FSP::alloc_free_page(space_id_t space, page_no_t hint, mtr_t *mtr) noexcept {
  auto header = get_space_header(space, mtr);

  /* Get the hinted descriptor */
  auto descr = xdes_get_descriptor_with_space_hdr(header, space, hint, mtr);

  if (descr != nullptr && xdes_get_state(descr, mtr) == XDES_FREE_FRAG) {
    /* Ok, we can take this extent */
  } else {
    /* Else take the first extent in free_frag list */
    auto first = flst_get_first(header + FSP_FREE_FRAG, mtr);

    if (m_fil->addr_is_null(first)) {
      /* There are no partially full fragments: allocate
      a free extent and add it to the FREE_FRAG list. NOTE
      that the allocation may have as a side-effect that an
      extent containing a descriptor page is added to the
      FREE_FRAG list. But we will allocate our page from the
      the free extent anyway. */

      descr = alloc_free_extent(space, hint, mtr);

      if (descr == nullptr) {
        /* No free space left */

        return FIL_NULL;
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

  auto free = xdes_find_bit(descr, XDES_FREE_BIT, true, hint % FSP_EXTENT_SIZE, mtr);

  if (free == ULINT_UNDEFINED) {

    log_warn_buf(((byte *)descr) - 500, 1000);

    ut_error;
  }

  page_no_t page_no = xdes_get_offset(descr) + free;

  auto space_size = mtr->read_ulint(header + FSP_SIZE, MLOG_4BYTES);

  if (space_size <= page_no) {
    /* It must be that we are extending a single-table tablespace
    whose size is still < 64 pages */

    ut_a(space != SYS_TABLESPACE);

    if (page_no >= FSP_EXTENT_SIZE) {
      log_err(std::format(
        "Trying to extend a single-table tablespace {}"
        " by single page(s) though the space size {}. Page no {}.",
        space,
        space_size,
        page_no
      ));
      return FIL_NULL;
    }

    auto success = try_extend_data_file_with_pages(space, page_no, header, mtr);

    if (!success) {
      /* No disk space left */
      return FIL_NULL;
    }
  }

  xdes_set_bit(descr, XDES_FREE_BIT, free, false, mtr);

  /* Update the FRAG_N_USED field */
  auto frag_n_used = mtr->read_ulint(header + FSP_FRAG_N_USED, MLOG_4BYTES);

  ++frag_n_used;

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

  auto block = m_buf_pool->create(Page_id(space, page_no), mtr);

  Buf_pool::Request req{
    .m_rw_latch = RW_X_LATCH, .m_page_id = {space, page_no}, .m_mode = BUF_GET, .m_file = __FILE__, .m_line = __LINE__, .m_mtr = mtr
  };

  {
    auto b = m_buf_pool->get(req, nullptr);
    ut_a(block == b);
  }

  buf_block_dbg_add_level(IF_SYNC_DEBUG(block, SYNC_FSP_PAGE));

  /* Prior contents of the page should be ignored */
  fsp_init_file_page(block, mtr);

  return page_no;
}

FSP::xdes_t *FSP::xdes_get_descriptor_with_space_hdr(fsp_header_t *sp_header, space_id_t space, ulint offset, mtr_t *mtr) noexcept {
  ut_ad(mtr->memo_contains(m_fil->space_get_latch(space), MTR_MEMO_X_LOCK));
  ut_ad(mtr->memo_contains_page(sp_header, MTR_MEMO_PAGE_S_FIX) || mtr->memo_contains_page(sp_header, MTR_MEMO_PAGE_X_FIX));
  ut_ad(page_offset(sp_header) == FSP_HEADER_OFFSET);

  /* Read free limit and space size */
  auto limit = mach_read_from_4(sp_header + FSP_FREE_LIMIT);
  auto size = mach_read_from_4(sp_header + FSP_SIZE);

  /* If offset is >= size or > limit, return nullptr */

  if (offset >= size || offset > limit) {

    return nullptr;
  }

  /* If offset is == limit, fill free list of the space. */

  if (offset == limit) {
    fill_free_list(false, space, sp_header, mtr);
  }

  page_t *descr_page;
  auto descr_page_no = xdes_calc_descriptor_page(offset);

  if (descr_page_no == 0) {
    /* It is on the space header page */

    descr_page = page_align(sp_header);
  } else {

    Buf_pool::Request req{
      .m_rw_latch = RW_X_LATCH,
      .m_page_id = {space, descr_page_no},
      .m_mode = BUF_GET,
      .m_file = __FILE__,
      .m_line = __LINE__,
      .m_mtr = mtr
    };

    auto block = m_buf_pool->get(req, nullptr);

    buf_block_dbg_add_level(IF_SYNC_DEBUG(block, SYNC_FSP_PAGE));

    descr_page = block->get_frame();
  }

  return descr_page + XDES_ARR_OFFSET + XDES_SIZE * xdes_calc_descriptor_index(offset);
}

FSP::xdes_t *FSP::xdes_get_descriptor(space_id_t space_id, page_no_t page_no, mtr_t *mtr) noexcept {
  Buf_pool::Request req{
    .m_rw_latch = RW_X_LATCH, .m_page_id = {space_id, 0}, .m_mode = BUF_GET, .m_file = __FILE__, .m_line = __LINE__, .m_mtr = mtr
  };

  auto block = m_buf_pool->get(req, nullptr);

  buf_block_dbg_add_level(IF_SYNC_DEBUG(block, SYNC_FSP_PAGE));

  auto sp_header = FSP_HEADER_OFFSET + block->get_frame();

  return xdes_get_descriptor_with_space_hdr(sp_header, space_id, page_no, mtr);
}

bool FSP::alloc_seg_inode_page(fsp_header_t *space_header, mtr_t *mtr) noexcept {
  ut_ad(page_offset(space_header) == FSP_HEADER_OFFSET);

  space_id_t space = page_get_space_id(page_align(space_header));
  page_no_t page_no = alloc_free_page(space, 0, mtr);

  if (page_no == FIL_NULL) {

    return false;
  }

  Buf_pool::Request req{
    .m_rw_latch = RW_X_LATCH, .m_page_id = {space, page_no}, .m_mode = BUF_GET, .m_file = __FILE__, .m_line = __LINE__, .m_mtr = mtr
  };

  auto block = m_buf_pool->get(req, nullptr);

  buf_block_dbg_add_level(IF_SYNC_DEBUG(block, SYNC_FSP_PAGE));

  block->m_check_index_page_at_flush = false;

  auto page = block->get_frame();

  mlog_write_ulint(page + FIL_PAGE_TYPE, FIL_PAGE_TYPE_INODE, MLOG_2BYTES, mtr);

  for (ulint i{}; i < FSP_SEG_INODES_PER_PAGE; ++i) {

    auto inode = fsp_seg_inode_page_get_nth_inode(page, i, mtr);

    mlog_write_uint64(inode + FSEG_ID, 0, mtr);
  }

  flst_add_last(space_header + FSP_SEG_INODES_FREE, page + FSEG_INODE_PAGE_NODE, mtr);

  return true;
}

FSP::fseg_inode_t *FSP::alloc_seg_inode(fsp_header_t *space_header, mtr_t *mtr) noexcept {
  ut_ad(page_offset(space_header) == FSP_HEADER_OFFSET);

  if (flst_get_len(space_header + FSP_SEG_INODES_FREE, mtr) == 0) {
    /* Allocate a new segment inode page */

    auto success = alloc_seg_inode_page(space_header, mtr);

    if (!success) {

      return nullptr;
    }
  }

  auto ptr = page_align(space_header);
  auto page_ptr = space_header + FSP_SEG_INODES_FREE;

  Buf_pool::Request req{
    .m_rw_latch = RW_X_LATCH,
    .m_page_id = {page_get_space_id(ptr), flst_get_first(page_ptr, mtr).m_page_no},
    .m_mode = BUF_GET,
    .m_file = __FILE__,
    .m_line = __LINE__,
    .m_mtr = mtr
  };

  auto block = m_buf_pool->get(req, nullptr);

  buf_block_dbg_add_level(IF_SYNC_DEBUG(block, SYNC_FSP_PAGE));

  auto page = block->get_frame();
  auto n = fsp_seg_inode_page_find_free(page, 0, mtr);

  ut_a(n != ULINT_UNDEFINED);

  auto inode = fsp_seg_inode_page_get_nth_inode(page, n, mtr);

  if (ULINT_UNDEFINED == fsp_seg_inode_page_find_free(page, n + 1, mtr)) {
    /* There are no other unused headers left on the page: move it to another list */

    flst_remove(space_header + FSP_SEG_INODES_FREE, page + FSEG_INODE_PAGE_NODE, mtr);

    flst_add_last(space_header + FSP_SEG_INODES_FULL, page + FSEG_INODE_PAGE_NODE, mtr);
  }

  ut_ad(mach_read_from_8(inode + FSEG_ID) == 0 || mach_read_from_4(inode + FSEG_MAGIC_N) == FSEG_MAGIC_N_VALUE);

  return inode;
}

page_no_t FSP::fseg_alloc_free_page_low(
  space_id_t space, fseg_inode_t *seg_inode, page_no_t hint, byte direction, mtr_t *mtr
) noexcept {
  bool frag_page_allocated{};

  ut_ad((direction >= FSP_UP) && (direction <= FSP_NO_DIR));
  ut_ad(mach_read_from_4(seg_inode + FSEG_MAGIC_N) == FSEG_MAGIC_N_VALUE);
  ut_ad(!((page_offset(seg_inode) - FSEG_ARR_OFFSET) % FSEG_INODE_SIZE));

  const auto seg_id = mtr->read_uint64(seg_inode + FSEG_ID);

  ut_ad(seg_id != 0);

  ulint used;

  auto reserved = fseg_n_reserved_pages_low(seg_inode, &used, mtr);
  auto space_header = get_space_header(space, mtr);
  auto hint_descr = xdes_get_descriptor_with_space_hdr(space_header, space, hint, mtr);

  if (hint_descr == nullptr) {
    /* Hint outside space or too high above free limit: reset hint */
    hint = 0;
    hint_descr = xdes_get_descriptor(space, hint, mtr);
  }

  /* The allocated page offset, FIL_NULL if could not be allocated */
  page_no_t ret_page;

  /* the extent of the allocated page */
  FSP::xdes_t *ret_descr;

  /* In the big if-else below we look for ret_page and ret_descr */
  if (xdes_get_state(hint_descr, mtr) == XDES_FSEG && mtr->read_uint64(hint_descr + XDES_ID) == seg_id &&
      xdes_get_bit(hint_descr, XDES_FREE_BIT, hint % FSP_EXTENT_SIZE, mtr)) {

    /* 1. We can take the hinted page */

    ret_page = hint;
    ret_descr = hint_descr;

  } else if (xdes_get_state(hint_descr, mtr) == XDES_FREE && (reserved - used) < reserved / FSEG_FILLFACTOR &&
             used >= FSEG_FRAG_LIMIT) {

    /* 2. We allocate the free extent from space and can take
    the hinted page. */

    ret_descr = alloc_free_extent(space, hint, mtr);

    ut_a(ret_descr == hint_descr);

    xdes_set_state(ret_descr, XDES_FSEG, mtr);
    mlog_write_uint64(ret_descr + XDES_ID, seg_id, mtr);
    flst_add_last(seg_inode + FSEG_FREE, ret_descr + XDES_FLST_NODE, mtr);

    /* Try to fill the segment free list */
    fseg_fill_free_list(seg_inode, space, hint + FSP_EXTENT_SIZE, mtr);
    ret_page = hint;

  } else if (direction != FSP_NO_DIR && (reserved - used) < reserved / FSEG_FILLFACTOR && used >= FSEG_FRAG_LIMIT &&
             !!(ret_descr = fseg_alloc_free_extent(seg_inode, space, mtr))) {

    /* 3. We take any free extent (which was already assigned above
    in the if-condition to ret_descr) and take the lowest or
    highest page in it, depending on the direction. */
    ret_page = xdes_get_offset(ret_descr);

    if (direction == FSP_DOWN) {
      ret_page += FSP_EXTENT_SIZE - 1;
    }

  } else if (xdes_get_state(hint_descr, mtr) == XDES_FSEG && mtr->read_uint64(hint_descr + XDES_ID) == seg_id &&
             !xdes_is_full(hint_descr, mtr)) {

    /* 4. We can take the page from the same extent as the
    hinted page (and the extent already belongs to the segment). */

    ret_descr = hint_descr;

    ret_page = xdes_get_offset(ret_descr) + xdes_find_bit(ret_descr, XDES_FREE_BIT, true, hint % FSP_EXTENT_SIZE, mtr);

  } else if (reserved - used > 0) {

    /* 5. We take any unused page from the segment */
    Fil_addr first;

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

    ret_page = alloc_free_page(space, hint, mtr);

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

  if (space != SYS_TABLESPACE) {
    auto space_size = m_fil->space_get_size(space);

    if (space_size <= ret_page) {
      /* It must be that we are extending a single-table
      tablespace whose size is still < 64 pages */

      if (ret_page >= FSP_EXTENT_SIZE) {

        log_err(std::format(
          "Trying to extend a single-table tablespace {} by single page(s) though the space size {}."
          "  Page no {}.",
          space,
          space_size,
          ret_page
        ));

        return FIL_NULL;
      }

      auto success = try_extend_data_file_with_pages(space, ret_page, space_header, mtr);

      if (!success) {
        /* No disk space left */
        return FIL_NULL;
      }
    }
  }

  if (!frag_page_allocated) {

    /* Initialize the allocated page to buffer pool, so that it
    can be obtained immediately with buf_pool->get without need
    for a disk read */

    auto block = m_buf_pool->create(Page_id(space, ret_page), mtr);

    buf_block_dbg_add_level(IF_SYNC_DEBUG(block, SYNC_FSP_PAGE));

    Buf_pool::Request req{
      .m_rw_latch = RW_X_LATCH,
      .m_page_id = {space, ret_page},
      .m_mode = BUF_GET,
      .m_file = __FILE__,
      .m_line = __LINE__,
      .m_mtr = mtr
    };

    if (block != m_buf_pool->get(req, nullptr)) {
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

  m_buf_pool->check_index_page_at_flush(Page_id(space, ret_page));

  return ret_page;
}

byte *FSP::parse_init_file_page(byte *ptr, byte *, Buf_block *block) noexcept {
  if (block != nullptr) {
    fsp_init_file_page_low(block);
  }

  return ptr;
}

void FSP::init_fields(page_t *page, space_id_t space_id, ulint flags) noexcept {
  ut_a(flags == DICT_TF_FORMAT_V1);

  mach_write_to_4(FSP_HEADER_OFFSET + FSP_SPACE_ID + page, space_id);
  mach_write_to_4(FSP_HEADER_OFFSET + FSP_SPACE_FLAGS + page, flags);
}

void FSP::header_init(space_id_t space, ulint size, mtr_t *mtr) noexcept {
  mtr_x_lock(m_fil->space_get_latch(space), mtr);

  auto block = m_buf_pool->create(Page_id(space, 0), mtr);

  Buf_pool::Request req{
    .m_rw_latch = RW_X_LATCH, .m_page_id = {space, 0}, .m_mode = BUF_GET, .m_file = __FILE__, .m_line = __LINE__, .m_mtr = mtr
  };

  {
    const auto b = m_buf_pool->get(req, nullptr);
    ut_a(b == block);
  }

  buf_block_dbg_add_level(IF_SYNC_DEBUG(block, SYNC_FSP_PAGE));

  /* The prior contents of the file page should be ignored */

  fsp_init_file_page(block, mtr);

  auto page = block->get_frame();

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

  fill_free_list(space != SYS_TABLESPACE, space, header, mtr);
}

space_id_t FSP::get_space_id(const page_t *page) noexcept {
  auto fsp_id = mach_read_from_4(FSP_HEADER_OFFSET + page + FSP_SPACE_ID);
  auto id = mach_read_from_4(page + FIL_PAGE_SPACE_ID);

  if (id != fsp_id) {
    log_err(std::format("space id in fsp header {}, but in the page header {}", fsp_id, id));
    return ULINT32_UNDEFINED;
  } else {
    return id;
  }
}

ulint FSP::get_flags(const page_t *page) noexcept {
  ut_ad(page_offset(page) == 0);

  return mach_read_from_4(FSP_HEADER_OFFSET + FSP_SPACE_FLAGS + page);
}

ulint FSP::init_system_space_free_limit() noexcept {
  mtr_t mtr;

  mtr.start();

  mtr_x_lock(m_fil->space_get_latch(SYS_TABLESPACE), &mtr);

  auto header = get_space_header(SYS_TABLESPACE, &mtr);
  auto limit = mtr.read_ulint(header + FSP_FREE_LIMIT, MLOG_4BYTES);

  limit /= ((1024 * 1024) / UNIV_PAGE_SIZE);

  m_log->fsp_current_free_limit_set_and_checkpoint(limit);

  mtr.commit();

  return limit;
}

ulint FSP::get_system_space_size() noexcept {
  mtr_t mtr;

  mtr.start();

  mtr_x_lock(m_fil->space_get_latch(SYS_TABLESPACE), &mtr);

  auto header = get_space_header(SYS_TABLESPACE, &mtr);
  auto size = mtr.read_ulint(header + FSP_SIZE, MLOG_4BYTES);

  mtr.commit();

  return size;
}

Buf_block *FSP::fseg_create_general(space_id_t space_id, page_no_t page_no, ulint byte_offset, bool reserved, mtr_t *mtr) noexcept {
  ut_ad(byte_offset + FSEG_HEADER_SIZE <= UNIV_PAGE_SIZE - FIL_PAGE_DATA_END);

  auto latch = m_fil->space_get_latch(space_id);

  Buf_block *block{};
  fseg_header_t *header{};

  if (page_no != 0) {

    Buf_pool::Request req{
      .m_rw_latch = RW_X_LATCH,
      .m_page_id = {space_id, page_no},
      .m_mode = BUF_GET,
      .m_file = __FILE__,
      .m_line = __LINE__,
      .m_mtr = mtr
    };

    block = m_buf_pool->get(req, nullptr);

    header = byte_offset + block->get_frame();
  }

  ut_ad(!mutex_own(&m_buf_pool->m_trx_sys->m_mutex) || mtr->memo_contains(latch, MTR_MEMO_X_LOCK));

  mtr_x_lock(latch, mtr);

  ulint n_reserved{};

  if (!reserved) {
    auto success = reserve_free_extents(&n_reserved, space_id, 2, FSP_NORMAL, mtr);

    if (!success) {
      return nullptr;
    }
  }

  auto space_header = get_space_header(space_id, mtr);
  auto inode = alloc_seg_inode(space_header, mtr);

  if (inode != nullptr) {

    /* Read the next segment id from space header and increment the
    value in space header */

    auto seg_id = mtr->read_uint64(space_header + FSP_SEG_ID);

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

        Buf_pool::Request req{
          .m_rw_latch = RW_X_LATCH,
          .m_page_id = {space_id, page_no},
          .m_mode = BUF_GET,
          .m_file = __FILE__,
          .m_line = __LINE__,
          .m_mtr = mtr
        };

        block = m_buf_pool->get(req, nullptr);

        header = byte_offset + block->get_frame();

        mlog_write_ulint(header - byte_offset + FIL_PAGE_TYPE, FIL_PAGE_TYPE_SYS, MLOG_2BYTES, mtr);
      } else {

        free_seg_inode(space_id, inode, mtr);

        if (!reserved) {

          m_fil->space_release_free_extents(space_id, n_reserved);
        }

        return nullptr;
      }
    }

    mlog_write_ulint(header + FSEG_HDR_OFFSET, page_offset(inode), MLOG_2BYTES, mtr);

    mlog_write_ulint(header + FSEG_HDR_PAGE_NO, page_get_page_no(page_align(inode)), MLOG_4BYTES, mtr);

    mlog_write_ulint(header + FSEG_HDR_SPACE, space_id, MLOG_4BYTES, mtr);
  }

  if (!reserved) {

    m_fil->space_release_free_extents(space_id, n_reserved);
  }

  return block;
}

Buf_block *FSP::fseg_create(space_id_t space, page_no_t page, ulint byte_offset, mtr_t *mtr) noexcept {
  return fseg_create_general(space, page, byte_offset, false, mtr);
}

ulint FSP::fseg_n_reserved_pages(fseg_header_t *header, ulint *used, mtr_t *mtr) noexcept {
  const auto space = page_get_space_id(page_align(header));
  auto latch = m_fil->space_get_latch(space);

  ut_ad(!mutex_own(&m_buf_pool->m_trx_sys->m_mutex) || mtr->memo_contains(latch, MTR_MEMO_X_LOCK));

  mtr_x_lock(latch, mtr);

  auto inode = fseg_inode_get(header, space, mtr);

  return fseg_n_reserved_pages_low(inode, used, mtr);
}

page_no_t FSP::fseg_alloc_free_page_general(
  fseg_header_t *seg_header, page_no_t hint, byte direction, bool reserved, mtr_t *mtr
) noexcept {
  space_id_t space = page_get_space_id(page_align(seg_header));
  auto latch = m_fil->space_get_latch(space);

  ut_ad(!mutex_own(&m_buf_pool->m_trx_sys->m_mutex) || mtr->memo_contains(latch, MTR_MEMO_X_LOCK));

  mtr_x_lock(latch, mtr);

  auto inode = fseg_inode_get(seg_header, space, mtr);

  ulint n_reserved{};

  if (!reserved) {

    const auto success = reserve_free_extents(&n_reserved, space, 2, FSP_NORMAL, mtr);

    if (!success) {
      return FIL_NULL;
    }
  }

  const auto page_no = fseg_alloc_free_page_low(space, inode, hint, direction, mtr);

  if (!reserved) {
    m_fil->space_release_free_extents(space, n_reserved);
  }

  return page_no;
}

page_no_t FSP::fseg_alloc_free_page(fseg_header_t *seg_header, page_no_t hint, byte direction, mtr_t *mtr) noexcept {
  return fseg_alloc_free_page_general(seg_header, hint, direction, false, mtr);
}

bool FSP::reserve_free_extents(ulint *n_reserved, space_id_t space, ulint n_ext, ulint alloc_type, mtr_t *mtr) noexcept {
  *n_reserved = n_ext;

  auto latch = m_fil->space_get_latch(space);

  ut_ad(!mutex_own(&m_buf_pool->m_trx_sys->m_mutex) || mtr->memo_contains(latch, MTR_MEMO_X_LOCK));

  mtr_x_lock(latch, mtr);

  auto space_header = get_space_header(space, mtr);

  auto extend = [this](space_id_t space, fsp_header_t *space_header, mtr_t *mtr) -> auto {
    ulint n_pages_added{};
    auto success = try_extend_data_file(&n_pages_added, space, space_header, mtr);

    return success && n_pages_added > 0;
  };

  for (;;) {
    auto size = mtr->read_ulint(space_header + FSP_SIZE, MLOG_4BYTES);

    if (size < FSP_EXTENT_SIZE / 2) {
      /* Use different rules for small single-table tablespaces */
      *n_reserved = 0;
      return reserve_free_pages(space, space_header, size, mtr);
    }

    auto n_free_list_ext = flst_get_len(space_header + FSP_FREE, mtr);

    auto free_limit = mtr->read_ulint(space_header + FSP_FREE_LIMIT, MLOG_4BYTES);

    /* Below we play safe when counting free extents above the free limit:
    some of them will contain extent descriptor pages, and therefore
    will not be free extents */

    auto n_free_up = (size - free_limit) / FSP_EXTENT_SIZE;

    if (n_free_up > 0) {
      --n_free_up;
      n_free_up -= n_free_up / (UNIV_PAGE_SIZE / FSP_EXTENT_SIZE);
    }

    auto n_free = n_free_list_ext + n_free_up;

    if (alloc_type == FSP_NORMAL) {
      /* We reserve 1 extent + 0.5 % of the space size to undo logs
      and 1 extent + 0.5 % to cleaning operations; NOTE: this source
      code is duplicated in the function below! */

      auto reserve = 2 + ((size / FSP_EXTENT_SIZE) * 2) / 200;

      if (n_free <= reserve + n_ext) {
        if (!extend(space, space_header, mtr)) {
          return false;
        } else {
          continue;
        }
      }
    } else if (alloc_type == FSP_UNDO) {
      /* We reserve 0.5 % of the space size to cleaning operations */

      auto reserve = 1 + ((size / FSP_EXTENT_SIZE) * 1) / 200;

      if (n_free <= reserve + n_ext) {
        if (!extend(space, space_header, mtr)) {
          return false;
        } else {
          continue;
        }
      }

    } else {
      ut_a(alloc_type == FSP_CLEANING);
    }

    {
      auto success = m_fil->space_reserve_free_extents(space, n_free, n_ext);

      if (success) {
        return true;
      }
    }

    if (!extend(space, space_header, mtr)) {
      return false;
    }
  }

  return true;
}

uint64_t FSP::get_available_space_in_free_extents(space_id_t space) noexcept {
  mtr_t mtr;

  ut_ad(!mutex_own(&m_buf_pool->m_trx_sys->m_mutex));

  mtr.start();

  auto latch = m_fil->space_get_latch(space);

  mtr_x_lock(latch, &mtr);

  auto space_header = get_space_header(space, &mtr);

  auto size = mtr.read_ulint(space_header + FSP_SIZE, MLOG_4BYTES);

  auto n_free_list_ext = flst_get_len(space_header + FSP_FREE, &mtr);

  auto free_limit = mtr.read_ulint(space_header + FSP_FREE_LIMIT, MLOG_4BYTES);

  mtr.commit();

  if (size < FSP_EXTENT_SIZE) {
    /* This must be a single-table tablespace */
    ut_a(space != SYS_TABLESPACE);

    /* TODO: count free frag pages and return a value based on that */
    return 0;
  }

  /* Below we play safe when counting free extents above the free limit:
  some of them will contain extent descriptor pages, and therefore
  will not be free extents */

  auto n_free_up = (size - free_limit) / FSP_EXTENT_SIZE;

  if (n_free_up > 0) {
    --n_free_up;
    n_free_up -= n_free_up / (UNIV_PAGE_SIZE / FSP_EXTENT_SIZE);
  }

  auto n_free = n_free_list_ext + n_free_up;

  /* We reserve 1 extent + 0.5 % of the space size to undo logs
  and 1 extent + 0.5 % to cleaning operations; NOTE: this source
  code is duplicated in the function above! */

  auto reserve = 2 + ((size / FSP_EXTENT_SIZE) * 2) / 200;

  if (reserve > n_free) {
    return 0;
  } else {
    return uint64_t((n_free - reserve) * FSP_EXTENT_SIZE * (UNIV_PAGE_SIZE / 1024));
  }
}

void FSP::fseg_mark_page_used(fseg_inode_t *seg_inode, space_id_t space, page_no_t page, mtr_t *mtr) noexcept {
  ut_ad(!((page_offset(seg_inode) - FSEG_ARR_OFFSET) % FSEG_INODE_SIZE));
  ut_ad(mach_read_from_4(seg_inode + FSEG_MAGIC_N) == FSEG_MAGIC_N_VALUE);

  auto descr = xdes_get_descriptor(space, page, mtr);

  ut_ad(mtr->read_ulint(seg_inode + FSEG_ID, MLOG_4BYTES) == mtr->read_ulint(descr + XDES_ID, MLOG_4BYTES));

  if (xdes_is_free(descr, mtr)) {
    /* We move the extent from the free list to the
    NOT_FULL list */
    flst_remove(seg_inode + FSEG_FREE, descr + XDES_FLST_NODE, mtr);
    flst_add_last(seg_inode + FSEG_NOT_FULL, descr + XDES_FLST_NODE, mtr);
  }

  ut_ad(xdes_get_bit(descr, XDES_FREE_BIT, page % FSP_EXTENT_SIZE, mtr) == true);

  /* We mark the page as used */
  xdes_set_bit(descr, XDES_FREE_BIT, page % FSP_EXTENT_SIZE, false, mtr);

  auto not_full_n_used = mtr->read_ulint(seg_inode + FSEG_NOT_FULL_N_USED, MLOG_4BYTES);

  ++not_full_n_used;

  mlog_write_ulint(seg_inode + FSEG_NOT_FULL_N_USED, not_full_n_used, MLOG_4BYTES, mtr);

  if (xdes_is_full(descr, mtr)) {
    /* We move the extent from the NOT_FULL list to the FULL list */
    flst_remove(seg_inode + FSEG_NOT_FULL, descr + XDES_FLST_NODE, mtr);
    flst_add_last(seg_inode + FSEG_FULL, descr + XDES_FLST_NODE, mtr);

    mlog_write_ulint(seg_inode + FSEG_NOT_FULL_N_USED, not_full_n_used - FSP_EXTENT_SIZE, MLOG_4BYTES, mtr);
  }
}

void FSP::fseg_free_page(fseg_header_t *seg_header, space_id_t space, page_no_t page, mtr_t *mtr) noexcept {
  auto latch = m_fil->space_get_latch(space);

  ut_ad(!mutex_own(&m_buf_pool->m_trx_sys->m_mutex) || mtr->memo_contains(latch, MTR_MEMO_X_LOCK));

  mtr_x_lock(latch, mtr);

  const auto seg_inode = fseg_inode_get(seg_header, space, mtr);

  fseg_free_page_low(seg_inode, space, page, mtr);

  ut_d(m_buf_pool->set_file_page_was_freed(Page_id(space, page)));
}

bool FSP::fseg_free_step(fseg_header_t *header, mtr_t *mtr) noexcept {
  const auto space = page_get_space_id(page_align(header));
  auto header_page = page_get_page_no(page_align(header));
  auto latch = m_fil->space_get_latch(space);

  ut_ad(!mutex_own(&m_buf_pool->m_trx_sys->m_mutex) || mtr->memo_contains(latch, MTR_MEMO_X_LOCK));

  mtr_x_lock(latch, mtr);

  auto descr = xdes_get_descriptor(space, header_page, mtr);

  /* Check that the header resides on a page which has not been
  freed yet */

  ut_a(descr != nullptr);
  ut_a(xdes_get_bit(descr, XDES_FREE_BIT, header_page % FSP_EXTENT_SIZE, mtr) == false);

  const auto inode = fseg_inode_try_get(header, space, mtr);

  if (unlikely(inode == nullptr)) {

    log_err(std::format("double free of inode from {}:{}", space, header_page));

    return true;
  }

  descr = fseg_get_first_extent(inode, space, mtr);

  if (descr != nullptr) {
    /* Free the extent held by the segment */
    const auto page = xdes_get_offset(descr);

    fseg_free_extent(inode, space, page, mtr);

    return false;
  }

  /* Free a frag page */
  auto n = fseg_find_last_used_frag_page_slot(inode, mtr);

  if (n == ULINT_UNDEFINED) {
    /* Freeing completed: free the segment inode */
    free_seg_inode(space, inode, mtr);

    return (true);
  }

  fseg_free_page_low(inode, space, fseg_get_nth_frag_page_no(inode, n, mtr), mtr);

  n = fseg_find_last_used_frag_page_slot(inode, mtr);

  if (n == ULINT_UNDEFINED) {
    /* Freeing completed: free the segment inode */
    free_seg_inode(space, inode, mtr);

    return true;
  } else {
    return false;
  }
}

bool FSP::fseg_free_step_not_header(fseg_header_t *header, mtr_t *mtr) noexcept {
  const auto space = page_get_space_id(page_align(header));
  auto latch = m_fil->space_get_latch(space);

  ut_ad(!mutex_own(&m_buf_pool->m_trx_sys->m_mutex) || mtr->memo_contains(latch, MTR_MEMO_X_LOCK));

  mtr_x_lock(latch, mtr);

  auto inode = fseg_inode_get(header, space, mtr);
  auto descr = fseg_get_first_extent(inode, space, mtr);

  if (descr != nullptr) {
    /* Free the extent held by the segment */
    auto page = xdes_get_offset(descr);

    fseg_free_extent(inode, space, page, mtr);

    return false;
  }

  /* Free a frag page */

  const auto n = fseg_find_last_used_frag_page_slot(inode, mtr);

  if (n == ULINT_UNDEFINED) {
    ut_error;
  }

  const auto page_no = fseg_get_nth_frag_page_no(inode, n, mtr);

  if (page_no == page_get_page_no(page_align(header))) {

    return true;

  } else {

    fseg_free_page_low(inode, space, page_no, mtr);
    return false;
  }
}

#ifdef UNIV_DEBUG
bool FSP::fseg_validate(fseg_header_t *header, mtr_t *mtr) noexcept {
  auto space = page_get_space_id(page_align(header));

  mtr_x_lock(m_fil->space_get_latch(space), mtr);

  auto inode = fseg_inode_get(header, space, mtr);

  return fseg_validate_low(inode, mtr);
}
#endif /* UNIV_DEBUG */

void FSP::fseg_print(fseg_header_t *header, mtr_t *mtr) noexcept {
  auto space = page_get_space_id(page_align(header));

  mtr_x_lock(m_fil->space_get_latch(space), mtr);

  auto inode = fseg_inode_get(header, space, mtr);

  fseg_print_low(inode, mtr);
}

bool FSP::validate(space_id_t space) noexcept {
  mtr_t mtr;
  mtr_t mtr2;

  auto latch = m_fil->space_get_latch(space);

  /* Start first a mini-transaction mtr2 to lock out all other threads
  from the fsp system */
  mtr2.start();

  mtr_x_lock(latch, &mtr2);

  mtr.start();
  mtr_x_lock(latch, &mtr);

  auto header = get_space_header(space, &mtr);
  auto size = mtr.read_ulint(header + FSP_SIZE, MLOG_4BYTES);
  auto free_limit = mtr.read_ulint(header + FSP_FREE_LIMIT, MLOG_4BYTES);
  auto frag_n_used = mtr.read_ulint(header + FSP_FRAG_N_USED, MLOG_4BYTES);
  auto n_full_frag_pages = FSP_EXTENT_SIZE * flst_get_len(header + FSP_FULL_FRAG, &mtr);

  if (unlikely(free_limit > size)) {

    ut_a(space != 0);
    ut_a(size < FSP_EXTENT_SIZE);
  }

  flst_validate(header + FSP_FREE, &mtr);
  flst_validate(header + FSP_FREE_FRAG, &mtr);
  flst_validate(header + FSP_FULL_FRAG, &mtr);

  mtr.commit();

  /* Validate FSP_FREE list */
  mtr.start();

  mtr_x_lock(latch, &mtr);

  header = get_space_header(space, &mtr);

  auto node_addr = flst_get_first(header + FSP_FREE, &mtr);

  mtr.commit();

  ulint descr_count = 0;

  while (!m_fil->addr_is_null(node_addr)) {
    mtr.start();

    mtr_x_lock(latch, &mtr);

    ++descr_count;
    auto descr = xdes_lst_get_descriptor(space, node_addr, &mtr);

    ut_a(xdes_get_n_used(descr, &mtr) == 0);
    ut_a(xdes_get_state(descr, &mtr) == XDES_FREE);

    node_addr = flst_get_next_addr(descr + XDES_FLST_NODE, &mtr);

    mtr.commit();
  }

  /* Validate FSP_FREE_FRAG list */
  mtr.start();

  mtr_x_lock(latch, &mtr);

  header = get_space_header(space, &mtr);
  node_addr = flst_get_first(header + FSP_FREE_FRAG, &mtr);

  mtr.commit();

  ulint n_used{};

  while (!m_fil->addr_is_null(node_addr)) {
    mtr.start();

    mtr_x_lock(latch, &mtr);

    ++descr_count;
    auto descr = xdes_lst_get_descriptor(space, node_addr, &mtr);

    ut_a(xdes_get_n_used(descr, &mtr) > 0);
    ut_a(xdes_get_n_used(descr, &mtr) < FSP_EXTENT_SIZE);
    ut_a(xdes_get_state(descr, &mtr) == XDES_FREE_FRAG);

    n_used += xdes_get_n_used(descr, &mtr);
    node_addr = flst_get_next_addr(descr + XDES_FLST_NODE, &mtr);

    mtr.commit();
  }

  /* Validate FSP_FULL_FRAG list */
  mtr.start();
  mtr_x_lock(latch, &mtr);

  header = get_space_header(space, &mtr);
  node_addr = flst_get_first(header + FSP_FULL_FRAG, &mtr);

  mtr.commit();

  while (!m_fil->addr_is_null(node_addr)) {
    mtr.start();
    mtr_x_lock(latch, &mtr);

    ++descr_count;
    auto descr = xdes_lst_get_descriptor(space, node_addr, &mtr);

    ut_a(xdes_get_n_used(descr, &mtr) == FSP_EXTENT_SIZE);
    ut_a(xdes_get_state(descr, &mtr) == XDES_FULL_FRAG);

    node_addr = flst_get_next_addr(descr + XDES_FLST_NODE, &mtr);

    mtr.commit();
  }

  /* Validate segments */
  mtr.start();

  mtr_x_lock(latch, &mtr);

  header = get_space_header(space, &mtr);

  node_addr = flst_get_first(header + FSP_SEG_INODES_FULL, &mtr);

  auto seg_inode_len_full = flst_get_len(header + FSP_SEG_INODES_FULL, &mtr);

  mtr.commit();

  ulint n_used2{};

  while (!m_fil->addr_is_null(node_addr)) {

    ulint n{};
    Fil_addr next_node_addr;

    do {
      mtr.start();

      mtr_x_lock(latch, &mtr);

      auto seg_inode_page = fut_get_ptr(space, node_addr, RW_X_LATCH, &mtr) - FSEG_INODE_PAGE_NODE;

      auto seg_inode = fsp_seg_inode_page_get_nth_inode(seg_inode_page, n, &mtr);

      ut_a(mach_read_from_8(seg_inode + FSEG_ID) != 0);
      fseg_validate_low(seg_inode, &mtr);

      descr_count += flst_get_len(seg_inode + FSEG_FREE, &mtr);
      descr_count += flst_get_len(seg_inode + FSEG_FULL, &mtr);
      descr_count += flst_get_len(seg_inode + FSEG_NOT_FULL, &mtr);

      n_used2 += fseg_get_n_frag_pages(seg_inode, &mtr);

      next_node_addr = flst_get_next_addr(seg_inode_page + FSEG_INODE_PAGE_NODE, &mtr);

      mtr.commit();

    } while (++n < FSP_SEG_INODES_PER_PAGE);

    node_addr = next_node_addr;
  }

  mtr.start();

  mtr_x_lock(latch, &mtr);

  header = get_space_header(space, &mtr);

  node_addr = flst_get_first(header + FSP_SEG_INODES_FREE, &mtr);

  auto seg_inode_len_free = flst_get_len(header + FSP_SEG_INODES_FREE, &mtr);

  mtr.commit();

  while (!m_fil->addr_is_null(node_addr)) {

    ulint n{};
    Fil_addr next_node_addr;

    do {
      mtr.start();

      mtr_x_lock(latch, &mtr);

      auto seg_inode_page = fut_get_ptr(space, node_addr, RW_X_LATCH, &mtr) - FSEG_INODE_PAGE_NODE;

      auto seg_inode = fsp_seg_inode_page_get_nth_inode(seg_inode_page, n, &mtr);

      if (mach_read_from_8(seg_inode + FSEG_ID) != 0) {
        fseg_validate_low(seg_inode, &mtr);

        descr_count += flst_get_len(seg_inode + FSEG_FREE, &mtr);
        descr_count += flst_get_len(seg_inode + FSEG_FULL, &mtr);
        descr_count += flst_get_len(seg_inode + FSEG_NOT_FULL, &mtr);
        n_used2 += fseg_get_n_frag_pages(seg_inode, &mtr);
      }

      next_node_addr = flst_get_next_addr(seg_inode_page + FSEG_INODE_PAGE_NODE, &mtr);

      mtr.commit();
    } while (++n < FSP_SEG_INODES_PER_PAGE);

    node_addr = next_node_addr;
  }

  ut_a(descr_count * FSP_EXTENT_SIZE == free_limit);

  ut_a(
    n_used + n_full_frag_pages ==
    n_used2 + 2 * ((free_limit + (UNIV_PAGE_SIZE - 1)) / UNIV_PAGE_SIZE) + seg_inode_len_full + seg_inode_len_free
  );

  ut_a(frag_n_used == n_used);

  mtr2.commit();

  return true;
}

void FSP::print(space_id_t space) noexcept {
  auto latch = m_fil->space_get_latch(space);

  /* Start first a mini-transaction mtr2 to lock out all other threads
  from the fsp system */

  mtr_t mtr2;

  mtr2.start();

  mtr_x_lock(latch, &mtr2);

  mtr_t mtr;

  mtr.start();

  mtr_x_lock(latch, &mtr);

  auto header = get_space_header(space, &mtr);
  auto size = mtr.read_ulint(header + FSP_SIZE, MLOG_4BYTES);
  auto free_limit = mtr.read_ulint(header + FSP_FREE_LIMIT, MLOG_4BYTES);
  auto frag_n_used = mtr.read_ulint(header + FSP_FRAG_N_USED, MLOG_4BYTES);
  auto n_free = flst_get_len(header + FSP_FREE, &mtr);
  auto n_free_frag = flst_get_len(header + FSP_FREE_FRAG, &mtr);
  auto n_full_frag = flst_get_len(header + FSP_FULL_FRAG, &mtr);
  auto d_var = mtr.read_uint64(header + FSP_SEG_ID);

  auto seg_id_low = d_var;
  auto seg_id_high = d_var;

  log_info(std::format(
    "FILE SPACE INFO: id {}\n"
    "size {}, free limit {}, free extents {}\n"
    "not full frag extents {} : used pages {}, full frag extents {}\n"
    "first seg id not used {} {}",
    space,
    size,
    free_limit,
    n_free,
    n_free_frag,
    frag_n_used,
    n_full_frag,
    seg_id_high,
    seg_id_low
  ));

  mtr.commit();

  /* Print segments */

  mtr.start();

  mtr_x_lock(latch, &mtr);

  header = get_space_header(space, &mtr);

  auto node_addr = flst_get_first(header + FSP_SEG_INODES_FULL, &mtr);

  mtr.commit();

  ulint n_segs{};

  while (!m_fil->addr_is_null(node_addr)) {

    ulint n{};
    Fil_addr next_node_addr;

    do {

      mtr.start();

      mtr_x_lock(latch, &mtr);

      auto seg_inode_page = fut_get_ptr(space, node_addr, RW_X_LATCH, &mtr) - FSEG_INODE_PAGE_NODE;

      auto seg_inode = fsp_seg_inode_page_get_nth_inode(seg_inode_page, n, &mtr);
      ut_a(mach_read_from_8(seg_inode + FSEG_ID) != 0);

      fseg_print_low(seg_inode, &mtr);

      ++n_segs;

      next_node_addr = flst_get_next_addr(seg_inode_page + FSEG_INODE_PAGE_NODE, &mtr);

      mtr.commit();

    } while (++n < FSP_SEG_INODES_PER_PAGE);

    node_addr = next_node_addr;
  }

  mtr.start();

  mtr_x_lock(latch, &mtr);

  header = get_space_header(space, &mtr);

  node_addr = flst_get_first(header + FSP_SEG_INODES_FREE, &mtr);

  mtr.commit();

  while (!m_fil->addr_is_null(node_addr)) {

    ulint n{};
    Fil_addr next_node_addr;

    do {

      mtr.start();

      mtr_x_lock(latch, &mtr);

      auto seg_inode_page = fut_get_ptr(space, node_addr, RW_X_LATCH, &mtr) - FSEG_INODE_PAGE_NODE;

      auto seg_inode = fsp_seg_inode_page_get_nth_inode(seg_inode_page, n, &mtr);

      if (mach_read_from_8(seg_inode + FSEG_ID) != 0) {

        fseg_print_low(seg_inode, &mtr);
        ++n_segs;
      }

      next_node_addr = flst_get_next_addr(seg_inode_page + FSEG_INODE_PAGE_NODE, &mtr);

      mtr.commit();

      ++n;

    } while (n < FSP_SEG_INODES_PER_PAGE);

    node_addr = next_node_addr;
  }

  mtr2.commit();

  log_info("NUMBER of file segments: ", n_segs);
}

ulint FSP::get_size_low(page_t *page) noexcept {
  return mach_read_from_4(page + FSP_HEADER_OFFSET + FSP_SIZE);
}
