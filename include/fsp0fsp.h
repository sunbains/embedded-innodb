/****************************************************************************
Copyright (c) 1995, 2009, Innobase Oy. All Rights Reserved.
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

/** @file include/fsp0fsp.h
File space management

Created 12/18/1995 Heikki Tuuri
*******************************************************/

#pragma once

#include "innodb0types.h"

#include "fsp0types.h"
#include "fut0lst.h"
#include "mtr0mtr.h"
#include "page0types.h"
#include "ut0byte.h"

struct Log;
struct Fil;
struct Buf_pool;

struct FSP {
  /* The data structures in files are defined just as byte strings in C */

  /** EXTENT DESCRIPTOR
   * 
   * File extent descriptor data structure: contains bits to tell which pages in
   * the extent are free and which contain old tuple version to clean.
   */
  using xdes_t = byte;

  /** SPACE HEADER
   *
   * File space header data structure: this data structure is contained in the
   * first page of a space. The space for this header is reserved in every extent
   * descriptor page, but used only in the first.
   */
  using fsp_header_t = byte;

  /** FILE SEGMENT INODE
   *
   * Segment inode which is created for each segment in a tablespace. NOTE: in
   * purge we assume that a segment having only one currently used page can be
   * freed in a few steps, so that the freeing cannot fill the file buffer with
   * bufferfixed file pages.
   */
  using fseg_inode_t = byte;

  /** Consutructor.
   * 
   * @param[in] log             Log instance
   * @param[in] fil             Fil instance
   * @param[in] buf_pool        Buffer pool instance
   */
  FSP(Log *log, Fil *fil, Buf_pool *buf_pool) noexcept : m_log(log), m_fil(fil), m_buf_pool(buf_pool) {}

  /**
   * Create an instance of the FSP class.
   * 
   * @param[in] log             Log instance
   * @param[in] fil             Fil instance
   * @param[in] buf_pool        Buffer pool instance
   * 
   * @return Instance of the FSP class
   */
  [[nodiscard]] static FSP *create(Log *log, Fil *fil, Buf_pool *buf_pool) noexcept;

  /**
   * Destroy an instance of the FSP class.
   * 
   * @param[in,own] fsp  Instance to destroy
   */
  static void destroy(FSP *&fsp) noexcept;

  /**
   * Gets the current free limit of the system tablespace.  The free limit
   * means the place of the first page which has never been put to the
   * free list for allocation. The space above that address is initialized
   * to zero. Sets also the global variable log_fsp_current_free_limit.
   * 
   * @return	free limit in megabytes
   */
  [[nodiscard]] ulint init_system_space_free_limit() noexcept;

  /**
   * Gets the size of the system tablespace from the tablespace header. If
   * we do not have an auto-extending data file, this should be equal to
   * the size of the data files. If there is an auto-extending data file,
   * this can be smaller.
   * 
   * @return	size in pages
   */
  [[nodiscard]] ulint get_system_space_size() noexcept;

  /**
   * Reads the space id from the first page of a tablespace.
   * 
   * @param[in,out] page        Header page (page 0 in the tablespace).
   * 
   * @return	space id, ULINT UNDEFINED if error
   */
  [[nodiscard]] space_id_t get_space_id(const page_t *page) noexcept;

  /**
   * Reads the space flags from the first page of a tablespace.
   * 
   * @param[in page             Header page (page 0 in the tablespace).
   * 
   * @return	flags
   */
  [[nodiscard]] ulint get_flags(const page_t *page) noexcept;

  /**
   * Writes the space id and compressed page size to a tablespace header.
   * This function is used past the buffer pool when we in fil0fil.c create
   * a new single-table tablespace.
   * 
   * @param[in,out] page        First page in the tablespace
   * @param[in] space_id        Tablespace ID
   * @param[in] flags           Tablespace flags (FSP_SPACE_FLAFS):
                                0, or table->flags if newer than COMPACT.
   */
  void init_fields(page_t *page, space_id_t space_id, ulint flags) noexcept;

  /**
   * Initializes the space header of a new created space
   * 
   * @param[in] space           Tablespace ID
   * @param[in] size            Current size in block
   * @param[in,out] mtr         Mini-transaction covering the operation.
   */
  void header_init(space_id_t space, ulint size, mtr_t *mtr) noexcept;

  /**
   * Creates a new segment.
   * 
   * @param[in] space           Tablespace ID
   * @param[in] page,           page where the segment header is placed: if
   *                            this is != 0, the page must belong to another
   *                            segment, if this is 0, a new page will be
   *                            allocated and it will belong to the created
   *                            segment.
   * @param[in] byte_offset     Byte offset of the created segment header on
   *                            the page
   * @param[in,out] mtr         Mini-transaction covering the operation.
 . *
   * @return the block where the segment header is placed, x-latched, nullptr if
   *  could not create segment because of lack of space
   */
  Buf_block *fseg_create(space_id_t space, page_no_t page_no, ulint byte_offset, mtr_t *mtr) noexcept;

  /**
   * Creates a new segment.
   * 
   * @param[in] space           Tablespace ID
   * @param[in] page,           page where the segment header is placed: if
   *                            this is != 0, the page must belong to another
   *                            segment, if this is 0, a new page will be
   *                            allocated and it will belong to the created segment.     
   * @param[in] byte_offset     Byte offset of the created segment header on
   *                           the page
   * @param[in] reserved        true if the caller has already done the
   *                            reservation for the pages with
   *                            fsp_reserve_free_extents (at least 2 extents: one
   *                            for the inode and the other for the segment) then
   *                            there is no need to do the check for this individual
   *                            operation
   * @param[in,out] mtr         Mini-transaction covering the operation.
   * 
   * @return the block where the segment header is placed, x-latched, nullptr
   *  if could not create segment because of lack of space
  */
  [[nodiscard]] Buf_block *fseg_create_general(
    space_id_t space, page_no_t page_no, ulint byte_offset, bool has_done_reservation, mtr_t *mtr
  ) noexcept;

  /**
   * Calculates the number of pages reserved by a segment, and how many pages are
   * currently used.
   * 
   * @param[in] header - segment header
   * @param[out] used - number of pages used (not more than reserved)
   * @param[in] mtr - mtr handle
   * 
   * @return	number of reserved pages
   */
  ulint fseg_n_reserved_pages(fseg_header_t *header, ulint *used, mtr_t *mtr) noexcept;

  /**
   * Allocates a single free page from a segment. This function implements
   * the intelligent allocation strategy which tries to minimize
   * file space fragmentation.
   * 
   * @param[in] seg_header      segment header
   * @param[in] hint            hint of which page would be desirable
   * @param[in] direction       if the new page is needed because of an index
   *                            page split, and records are inserted there in
   *                            order, into which direction they go alphabetically:
   *                            FSP_DOWN, FSP_UP, FSP_NO_DIR
   * @param[in,out] mtr         mtr handle
   * 
   * @return	the allocated page offset FIL_NULL if no page could be allocated
   */
  [[nodiscard]] page_no_t fseg_alloc_free_page(fseg_header_t *seg_header, page_no_t hint, byte direction, mtr_t *mtr) noexcept;

  /**
   * Allocates a single free page from a segment. This function implements
   * the intelligent allocation strategy which tries to minimize file space
   * fragmentation.
   * 
   * @param[in] seg_header      segment header
   * @param[in] hint            hint of which page would be desirable
   * @param[in] direction       if the new page is needed because of an index
   *                            page split, and records are inserted there in
   *                            order, into which direction they go alphabetically:
   *                            FSP_DOWN, FSP_UP, FSP_NO_DIR
   * @param[in] reserved        true if the caller has already done the
   *                            reservation for the page with fsp_reserve_free_extents,
   *                            then there is no need to do the check for this individual
   *                            page
   * @param[in,out] mtr         mtr handle
   * 
   * @return	allocated page offset, FIL_NULL if no page could be allocated
   */
  [[nodiscard]] page_no_t fseg_alloc_free_page_general(
    fseg_header_t *seg_header, page_no_t hint, byte direction, bool reserved, mtr_t *mtr
  ) noexcept;

  /**
   * Reserves free pages from a tablespace. All mini-transactions which may
   * use several pages from the tablespace should call this function beforehand
   * and reserve enough free extents so that they certainly will be able
   * to do their operation, like a B-tree page split, fully. Reservations
   * must be released with function srv_fil->space_release_free_extents!
   * 
   * The alloc_type below has the following meaning: FSP_NORMAL means an
   * operation which will probably result in more space usage, like an
   * insert in a B-tree; FSP_UNDO means allocation to undo logs: if we are
   * deleting rows, then this allocation will in the long run result in
   * less space usage (after a purge); FSP_CLEANING means allocation done
   * in a physical record delete (like in a purge) or other cleaning operation
   * which will result in less space usage in the long run. We prefer the latter
   * two types of allocation: when space is scarce, FSP_NORMAL allocations
   * will not succeed, but the latter two allocations will succeed, if possible.
   * The purpose is to avoid dead end where the database is full but the
   * user cannot free any space because these freeing operations temporarily
   * reserve some space.
   *
   * Single-table tablespaces whose size is < 32 pages are a special case. In this
   * function we would liberally reserve several 64 page extents for every page
   * split or merge in a B-tree. But we do not want to waste disk space if the table
   * only occupies < 32 pages. That is why we apply different rules in that special
   * case, just ensuring that there are 3 free pages available.
   * 
   * @param[out] n_reserved      number of extents actually reserved; if we
   *                             return true and the tablespace size is < 64 pages,
   *                             then this can be 0, otherwise it is n_ext
   * @param[in] space            space id
   * @param[in] n_ext            number of extents to reserve
   * @param[in] alloc_type       FSP_NORMAL, FSP_UNDO, or FSP_CLEANING
   * @param[in,out] mtr          mtr handle
   * 
   * @return	true if we were able to make the reservation
   */
  [[nodiscard]] bool reserve_free_extents(ulint *n_reserved, space_id_t space, ulint n_ext, ulint alloc_type, mtr_t *mtr) noexcept;

  /**
   * This function should be used to get information on how much we still
   * will be able to insert new data to the database without running out the
   * tablespace. Only free extents are taken into account and we also subtract
   * the safety margin required by the above function reserve_free_extents.
   * 
   * @param[in] space            space id
   * 
   * @return	available space in KiB
   */
  [[nodiscard]] uint64_t get_available_space_in_free_extents(space_id_t space) noexcept;

  /**
   * Frees a single page of a segment.
   * 
   * @param[in] seg_header       segment header
   * @param[in] space            space id
   * @param[in] page_no          page offset
   * @param[in,out] mtr          mtr handle
   */
  void fseg_free_page(fseg_header_t *seg_header, space_id_t space, page_no_t page_no, mtr_t *mtr) noexcept;

  /**
   * Frees part of a segment. This function can be used to free a segment
   * by repeatedly calling this function in different mini-transactions.
   * Doing the freeing in a single mini-transaction might result in
   * too big a mini-transaction.
   * 
   * @param[in,own] header      segment header; NOTE: if the header resides on
   *                            the first page of the frag list of the segment,  
   *                            this pointer becomes obsolete after the last
   *                            freeing step
   * @param[in,out] mtr         mini-transaction handle
   * 
   * @return	true if freeing completed
   */
  [[nodiscard]] bool fseg_free_step(fseg_header_t *header, mtr_t *mtr) noexcept;

  /**
   * Frees part of a segment. Differs from fseg_free_step because this function
   * leaves the header page unfreed.
   * 
   * @param[in] header          segment header; NOTE: if the header resides on
   *                            the first page of the frag list of the segment.
   * @param[in,out] mtr         mini-transaction handle
   * 
   * @return	true if freeing completed, except the header page
   */
  [[nodiscard]] bool fseg_free_step_not_header(fseg_header_t *header, mtr_t *mtr) noexcept;

  /**
   * Parses a redo log record of a file page init.
   * 
   * @param[in] ptr              buffer
   * @param[in] end_ptr          buffer end
   * @param[in,out] block        block or nullptr
   * 
   * @return	end of log record or nullptr
   */
  [[nodiscard]] byte *parse_init_file_page(byte *ptr, byte *end_ptr, Buf_block *block) noexcept;

  /**
   * Validates the file space system and its segments.
   * 
   * @param[in] space            space id
   * 
   * @return	true if ok
   */
  [[nodiscard]] bool validate(space_id_t space) noexcept;

  /**
   * Prints info of a file space.
   * 
   * @param[in] space            space id
   */
  void print(space_id_t space) noexcept;

#ifdef UNIV_DEBUG
  /**
   * Validates a segment.
   * 
   * @param[in] header           segment header
   * @param[in,out] mtr          mtr
   * 
   * @return	true if ok
   */
  [[nodiscard]] bool fseg_validate(fseg_header_t *header, mtr_t *mtr) noexcept;
#endif /* UNIV_DEBUG */

  /**
   * Writes info of a segment.
   * 
   * @param[in] inode            segment inode
   * @param[in,out] mtr          mtr handle
   */
  void fseg_print(fseg_header_t *header, mtr_t *mtr) noexcept;

  /**
   * Checks if a page address is an extent descriptor page address.
   * 
   * @param[in] page_no             Page numnber to check.
   * 
   * @return	true if a descriptor page
   */
  [[nodiscard]] bool descr_page(page_no_t page_no) noexcept { return (page_no & (UNIV_PAGE_SIZE - 1)) == FSP_XDES_OFFSET; }

  /**
   * Reads the file space size stored in the header page.
   * 
   * @param[in,out] page             Header page (page 0 in the tablespace).
   * 
   * @return	tablespace size stored in the space header
   */
  [[nodiscard]] ulint get_size_low(page_t *page) noexcept;

 private:
  /**
   * Returns an extent to the free list of a space.
   * 
   * @param[in] space           Tablespace ID
   * @param[in] page            Page offset in the extent
   * @param[in,out] mtr         Mini-transaction covering the operation.
   */
  void free_extent(space_id_t space, ulint page, mtr_t *mtr) noexcept;

  /**
   * Frees an extent of a segment to the space free list.
   * 
   * @param[in,out] seg_inode   Segment inode
   * @param[in] space           Tablespace ID
   * @param[in] page            Page offset in the extent
   * @param[in,out] mtr         Mini-transaction covering the operation.
  */
  void fseg_free_extent(fseg_inode_t *seg_inode, space_id_t space, page_no_t page, mtr_t *mtr) noexcept;

  /**
   * Frees a single page of a segment.
   *
   * @param[in] seg_inode       Segment inode
   * @param[in] space           Tablespace id
   * @param[in] page            Page offset
   * @param[in] mtr             Mini-transaction handle
   */
  void fseg_free_page_low(fseg_inode_t *seg_inode, space_id_t space, ulint page, mtr_t *mtr) noexcept;

  /**
   * Gets a pointer to the space header and x-locks its page.
   *
   * @param[in] id              Tablespace id
   * @param[in,out] mtr         Mini-transaction handle
   *
   * @return pointer to the space header, page x-locked
   */
  inline fsp_header_t *get_space_header(space_id_t id, mtr_t *mtr) noexcept;

  /**
   * @brief Gets pointer to the extent descriptor if the file address
   * of the descriptor list node is known. The page where the extent descriptor
   * resides is x-locked.
   * 
   * @param[in] space           Tablespace id
   * @param[in] lst_node        File address of the list node contained in the descriptor
   * @param[in] mtr             Mini-transaction handle
   * 
   * @return pointer to the extent descriptor
   */
  inline xdes_t *xdes_lst_get_descriptor(space_id_t space, Fil_addr lst_node, mtr_t *mtr) noexcept;

  /**
   * @brief Gets pointer to the extent descriptor of a page. The page where the extent
   * descriptor resides is x-locked. If the page offset is equal to the free limit
   * of the space, adds new extents from above the free limit to the space free
   * list, if not free limit == space size. This adding is necessary to make the
   * descriptor defined, as they are uninitialized above the free limit.
   * 
   * @param[in,out] sp_header   Tablespace header, x-latched
   * @param[in] space           Tablespace id
   * @param[in] offset          page offset; if equal to the free limit, we try to
   *                            add new extents to the space free list
   * @param[in] mtr             Mini-transaction handle
   * 
   * @return pointer to the extent descriptor, nullptr if the page does not
   *  exist in the space or if the offset exceeds the free limit
   */
  inline xdes_t *xdes_get_descriptor_with_space_hdr(fsp_header_t *sp_header, space_id_t space, ulint offset, mtr_t *mtr) noexcept;

  /**
   * @brief Gets pointer to a the extent descriptor of a page. The page where the
   * extent descriptor resides is x-locked. If the page offset is equal to
   * the free limit of the space, adds new extents from above the free limit
   * to the space free list, if not free limit == space size. This adding
   * is necessary to make the descriptor defined, as they are uninitialized
   * above the free limit.
   * 
   * @param[in] space_id        Tablespace id
   * @param[in] page_no         Page offset; if equal to the free limit,
   *                            we try to add new extents to the space free list
   * @param[in] mtr             Mini-transaction handle
   * 
   * @return pointer to the extent descriptor, nullptr if the page does not
   *  exist in the space or if the offset exceeds the free limit
   */
  xdes_t *xdes_get_descriptor(space_id_t space_id, page_no_t page_no, mtr_t *mtr) noexcept;

  /**
   * Puts new extents to the free list if there are free extents above the free
   * limit. If an extent happens to contain an extent descriptor page, the extent
   * is put to the FSP_FREE_FRAG list with the page marked as used.
   * 
   * @param[in] init_space      true if this is a single-table tablespace and
   *                            we are only initing the tablespace's first
   *		                        descriptor page; then we do not allocate
  *		                          more extents
  * @param[in] space            Tablespace ID
  * @param[in,out] header       Tablespace header page
  * @param[in] mtr              Mini-transaction covering the operation. 
  */
  void fill_free_list(bool init_space, space_id_t space, fsp_header_t *header, mtr_t *mtr) noexcept;

  /**
   * Allocates a new free extent.
   *
   * @param[in] space           Tablespace id
   * @param[in] hint            hint of which extent would be desirable: any page offset
   *                            in the extent goes; the hint must not be > FSP_FREE_LIMIT
   * @param[in] mtr             Mini-transaction handle 
   *
   * @return extent descriptor, nullptr if cannot be allocated
   */
  xdes_t *alloc_free_extent(space_id_t space, ulint hint, mtr_t *mtr) noexcept;

  /**
   * Allocates a single free page from a space. The page is marked as used.
   *
   * @param[in] space           Tablespace id
   * @param[in] hint            hint of which page would be desirable
   * @param[in] mtr             Mini-transaction handle
   *
   * @return the page offset, FIL_NULL if no page could be allocated
   */
  ulint alloc_free_page(space_id_t space, page_no_t hint, mtr_t *mtr) noexcept;

  /**
   * Allocates a new file segment inode page.
   *
   * @param[in] space_header    Space header
   * @param[in,out] mtr         Mini-transaction handle
   *
   * @return true if could be allocated
   */
  bool alloc_seg_inode_page(fsp_header_t *space_header, mtr_t *mtr) noexcept;

  /**
   * Allocates a new file segment inode.
   *
   * @param[in] space_header    Space header
   * @param[in] mtr             Mini-transaction handle
   *
   * @return segment inode, or nullptr if not enough space
   */
  fseg_inode_t *alloc_seg_inode(fsp_header_t *space_header, mtr_t *mtr) noexcept;

  /**
   * Allocates a single free page from a segment. This function implements
   * the intelligent allocation strategy which tries to minimize file space
   * fragmentation.
   * 
   * @param[in] space           Tablespace ID where to allocate the page.
   * @param[in] seg_inode       Segment inode in the tablespace.
   * @param[in] hint            Hint of which page would be desirable
   * @param[in] direction       If the new page is needed because
   *                            of an index page split, and records are
   *                            inserted there in order, into which
   *                            direction they go alphabetically: FSP_DOWN,
   *                            FSP_UP, FSP_NO_DIR
   * @param[in,out] mtr         Mini-transaction covering the operation.
   * 
   * @return	the allocated page number, FIL_NULL if no page could be allocated
   */
  page_no_t fseg_alloc_free_page_low(
    space_id_t space, fseg_inode_t *seg_inode, page_no_t hint, byte direction, mtr_t *mtr
  ) noexcept;

  /**
   * Tries to extend a single-table tablespace so that a page would fit in the data file.
   *
   * @param[in] space           Tablespace ID
   * @param[in] page_no         Page number
   * @param[in] header          Tableespace header
   * @param[in] mtr             Mini-transaction handle
   *
   * @return true if success
   */
  bool try_extend_data_file_with_pages(space_id_t space, page_no_t page_no, fsp_header_t *header, mtr_t *mtr) noexcept;

  /**
   * Tries to extend the last data file of a tablespace if it is auto-extending.
   * 
   * @param[out] actual_increase Actual increase in pages, where we measure
   *                            the tablespace size from what the header field
   *                            says; it may be the actual file size rounded down
   *                            to megabyte
   * @param[in] space           Tablespace ID
   * @param[in] header          Tablespace header
   * @param[in,out] mtr         Mini-transaction handle
   * 
   * @return false if not auto-extending
   */
  bool try_extend_data_file(ulint *actual_increase, space_id_t space, fsp_header_t *header, mtr_t *mtr) noexcept;

  /**
   * Tries to fill the free list of a segment with consecutive free extents.
   * This happens if the segment is big enough to allow extents in the free list,
   * the free list is empty, and the extents can be allocated consecutively from
   * the hint onward.
   *
   * @param[in] inode             The segment inode.
   * @param[in] space             The space id.
   * @param[in] hint              The hint for the first extent to be added to the free list.
   * @param[in] mtr               The mini-transaction handle.
   */
  void fseg_fill_free_list(fseg_inode_t *inode, space_id_t space, page_no_t hint, mtr_t *mtr) noexcept;

  /**
   * Allocates a free extent for the segment: looks first in the free list of the
   * segment, then tries to allocate from the space free list. NOTE that the extent
   * returned still resides in the segment free list, it is not yet taken off it!
   *
   * @param[in] inode             The segment inode.
   * @param[in] space             Tablespace id.
   * @param[in] mtr               Mini-transaction handle.
   *
   * @return Allocated extent, still placed in the segment free list, nullptr if could not be allocated.
   */
  xdes_t *fseg_alloc_free_extent(fseg_inode_t *inode, space_id_t space, mtr_t *mtr) noexcept;

  /**
   * Frees a single page of a space.
   *
   * @param[in] space           Tablespace id
   * @param[in] page            Page offset
   * @param[in,out] mtr         Mini-transaction handle
   */
  void free_page(space_id_t space, page_no_t page_no, mtr_t *mtr) noexcept;

  /**
   * Frees a segment inode in the specified space.
   *
   * @param[in] space           The space id of the segment.
   * @param[in] inode           The segment inode to be freed.
   * @param[in] mtr             Mini-transaction handle.
   */
  void free_seg_inode(space_id_t space, fseg_inode_t *inode, mtr_t *mtr) noexcept;

  /**
   * Checks that we have at least 2 frag pages free in the first extent of a
   * single-table tablespace, and they are also physically initialized to the data
   * file. That is we have already extended the data file so that those pages are
   * inside the data file. If not, this function extends the tablespace with
   * pages.
   * 
   * @param[in] space_id          The space id of the tablespace, must be != SYS_TABLESPACE.
   * @param[in] space_header      The space header of the tablespace, x-latched.
   * @param[in] size              The size of the tablespace in pages, must be < FSP_EXTENT_SIZE / 2.
   * @param[in,out] mtr           The mini-transaction handle.
   * 
   * @return	true if there were >= 3 free pages, or we were able to extend
   */
  bool reserve_free_pages(space_id_t space, fsp_header_t *space_header, ulint size, mtr_t *mtr) noexcept;

  /**
   * Returns the first extent descriptor for a segment. We think of the extent
   * lists of the segment catenated in the order FSEG_FULL -> FSEG_NOT_FULL
   * -> FSEG_FREE.
   *
   * @param[in] inode             Segment inode
   * @param[in] space             Tablespace id
   * @param[in] mtr               Mini-transaction handle
   *
   * @return the first extent descriptor, or nullptr if none
   */
  xdes_t *fseg_get_first_extent(fseg_inode_t *inode, space_id_t space, mtr_t *mtr) noexcept;

  /**
   * Validates a segment.
   *
   * @param[in] inode             Segment inode
   * @param[in] mtr2              Mini-transaction handle
   *
   * @return true if validation is successful, false otherwise
   */
  bool fseg_validate_low(fseg_inode_t *inode, mtr_t *mtr2) noexcept;

  /**
   * Marks a page used. The page must reside within the extents of the given segment.
   * 
   * @param[in] seg_inode         Segment inode
   * @param[in] space             Space id
   * @param[in] page              Page number
   * @param[in,out] mtr           mini-transaction handle
   */
  void fseg_mark_page_used(fseg_inode_t *seg_inode, space_id_t space, page_no_t page, mtr_t *mtr) noexcept;

 public:
  /** Redo log to use. */
  Log *m_log{};

  /** File interface. */
  Fil *m_fil{};

  Buf_pool *m_buf_pool{};
};
