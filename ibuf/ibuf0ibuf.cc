/****************************************************************************
Copyright (c) 1997, 2010, Innobase Oy. All Rights Reserved.

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

/** @file ibuf/ibuf0ibuf.c
Insert buffer

Created 7/19/1997 Heikki Tuuri
*******************************************************/

#include "ibuf0ibuf.h"

/** Number of bits describing a single page */
constexpr ulint IBUF_BITS_PER_PAGE = 4;

static_assert(!(IBUF_BITS_PER_PAGE % 2),
              "error IBUF_BITS_PER_PAGE must be an even number!");

/** The start address for an insert buffer bitmap page bitmap */
#define IBUF_BITMAP PAGE_DATA

#ifdef UNIV_NONINL
#include "ibuf0ibuf.ic"
#endif

#include "btr0btr.h"
#include "btr0cur.h"
#include "btr0pcur.h"
#include "buf0buf.h"
#include "buf0rea.h"
#include "dict0boot.h"
#include "fil0fil.h"
#include "fsp0fsp.h"
#include "fut0lst.h"
#include "lock0lock.h"
#include "log0recv.h"
#include "que0que.h"
#include "rem0rec.h"
#include "sync0sync.h"
#include "thr0loc.h"
#include "trx0sys.h"

/*	STRUCTURE OF AN INSERT BUFFER RECORD

In versions < 4.1.x:

1. The first field is the page number.
2. The second field is an array which stores type info for each subsequent
   field. We store the information which affects the ordering of records, and
   also the physical storage size of an SQL nullptr value. E.g., for CHAR(10) it
   is 10 bytes.
3. Next we have the fields of the actual index record.

In versions >= 4.1.x:

Note that contary to what we planned in the 1990's, there will only be one
insert buffer tree, and that is in the system tablespace of InnoDB.

1. The first field is the space id.
2. The second field is a one-byte marker (0) which differentiates records from
   the < 4.1.x storage format.
3. The third field is the page number.
4. The fourth field contains the type info, where we have also added 2 bytes to
   store the charset. In the compressed table format of 5.0.x we must add more
   information here so that we can build a dummy 'index' struct which 5.0.x
   can use in the binary search on the index page in the ibuf merge phase.
5. The rest of the fields contain the fields of the actual index record.

In versions >= 5.0.3:

The first byte of the fourth field is an additional marker (0) if the record
is in the compact format.  The presence of this marker can be detected by
looking at the length of the field modulo DATA_NEW_ORDER_NULL_TYPE_BUF_SIZE.

The high-order bit of the character set field in the type info is the
"nullable" flag for the field. */

/*	PREVENTING DEADLOCKS IN THE INSERT BUFFER SYSTEM

If an OS thread performs any operation that brings in disk pages from
non-system tablespaces into the buffer pool, or creates such a page there,
then the operation may have as a side effect an insert buffer index tree
compression. Thus, the tree latch of the insert buffer tree may be acquired
in the x-mode, and also the file space latch of the system tablespace may
be acquired in the x-mode.

Also, an insert to an index in a non-system tablespace can have the same
effect. How do we know this cannot lead to a deadlock of OS threads? There
is a problem with the i\o-handler threads: they break the latching order
because they own x-latches to pages which are on a lower level than the
insert buffer tree latch, its page latches, and the tablespace latch an
insert buffer operation can reserve.

The solution is the following: Let all the tree and page latches connected
with the insert buffer be later in the latching order than the fsp latch and
fsp page latches.

Insert buffer pages must be such that the insert buffer is never invoked
when these pages are accessed as this would result in a recursion violating
the latching order. We let a special i/o-handler thread take care of i/o to
the insert buffer pages and the ibuf bitmap pages, as well as the fsp bitmap
pages and the first inode page, which contains the inode of the ibuf tree: let
us call all these ibuf pages. To prevent deadlocks, we do not let a read-ahead
access both non-ibuf and ibuf pages.

Then an i/o-handler for the insert buffer never needs to access recursively the
insert buffer tree and thus obeys the latching order. On the other hand, other
i/o-handlers for other tablespaces may require access to the insert buffer,
but because all kinds of latches they need to access there are later in the
latching order, no violation of the latching order occurs in this case,
either.

A problem is how to grow and contract an insert buffer tree. As it is later
in the latching order than the fsp management, we have to reserve the fsp
latch first, before adding or removing pages from the insert buffer tree.
We let the insert buffer tree have its own file space management: a free
list of pages linked to the tree root. To prevent recursive using of the
insert buffer when adding pages to the tree, we must first load these pages
to memory, obtaining a latch on them, and only after that add them to the
free list of the insert buffer tree. More difficult is removing of pages
from the free list. If there is an excess of pages in the free list of the
ibuf tree, they might be needed if some thread reserves the fsp latch,
intending to allocate more file space. So we do the following: if a thread
reserves the fsp latch, we check the writer count field of the latch. If
this field has value 1, it means that the thread did not own the latch
before entering the fsp system, and the mtr of the thread contains no
modifications to the fsp pages. Now we are free to reserve the ibuf latch,
and check if there is an excess of pages in the free list. We can then, in a
separate mini-transaction, take them out of the free list and free them to
the fsp system.

To avoid deadlocks in the ibuf system, we divide file pages into three levels:

(1) non-ibuf pages,
(2) ibuf tree pages and the pages in the ibuf tree free list, and
(3) ibuf bitmap pages.

No OS thread is allowed to access higher level pages if it has latches to
lower level pages; even if the thread owns a B-tree latch it must not access
the B-tree non-leaf pages if it has latches on lower level pages. Read-ahead
is only allowed for level 1 and 2 pages. Dedicated i/o-handler threads handle
exclusively level 1 i/o. A dedicated i/o handler thread handles exclusively
level 2 i/o. However, if an OS thread does the i/o handling for itself, i.e.,
it uses synchronous aio, it can access any pages, as long as it obeys the
access order rules. */

/** Buffer pool size per the maximum insert buffer size */
#define IBUF_POOL_SIZE_PER_MAX_SIZE 2

/** Table name for the insert buffer. */
#define IBUF_TABLE_NAME "SYS_IBUF_TABLE"

/** Operations that can currently be buffered. */
ibuf_use_t ibuf_use = IBUF_USE_INSERT;

/** The insert buffer control structure */
ibuf_t *ibuf = nullptr;

/** Counter for ibuf_should_try() */
ulint ibuf_flush_count = 0;

#ifdef UNIV_IBUF_COUNT_DEBUG

/** Number of tablespaces in the ibuf_counts array */
constexpr ulint IBUF_COUNT_N_SPACES = 4;

/** Number of pages within each tablespace in the ibuf_counts array */
constepr ulint IBUF_COUNT_N_PAGES = 130000;

/** Buffered entry counts for file pages, used in debugging */
static ulint ibuf_counts[IBUF_COUNT_N_SPACES][IBUF_COUNT_N_PAGES];

/** Checks that the indexes to ibuf_counts[][] are within limits.
@param[in] space_id             Space ID.
@param[in] page_jo              Page number. */
static void ibuf_count_check(ulint space_id, ulint page_no) {
  if (space_id < IBUF_COUNT_N_SPACES && page_no < IBUF_COUNT_N_PAGES) {
    return;
  }

  ib_logger(ib_stream,
            "InnoDB: UNIV_IBUF_COUNT_DEBUG limits space_id and page_no\n"
            "InnoDB: and breaks crash recovery.\n"
            "InnoDB: space_id=%lu, should be 0<=space_id<%lu\n"
            "InnoDB: page_no=%lu, should be 0<=page_no<%lu\n",
            (ulint)space_id, (ulint)IBUF_COUNT_N_SPACES, (ulint)page_no,
            (ulint)IBUF_COUNT_N_PAGES);
  ut_error;
}
#endif

/** @name Offsets to the per-page bits in the insert buffer bitmap */
/* @{ */

/** Bits indicating the amount of free space */
constexpr ulint IBUF_BITMAP_FREE = 0;

/** true if there are buffered changes for the page */
constexpr ulint IBUF_BITMAP_BUFFERED = 2;

/** true if page is a part of the ibuf tree, excluding
the root page, or is in the free list of the ibuf */
constexpr ulint IBUF_BITMAP_IBUF = 3;
/* @} */

/** The mutex used to block pessimistic inserts to ibuf trees */
static mutex_t ibuf_pessimistic_insert_mutex;

/** The mutex protecting the insert buffer structs */
static mutex_t ibuf_mutex;

/** The mutex protecting the insert buffer bitmaps */
static mutex_t ibuf_bitmap_mutex;

/** The area in pages from which contract looks for page numbers for merge */
constexpr ulint IBUF_MERGE_AREA = 8;

/** Inside the merge area, pages which have at most 1 per this number less
buffered entries compared to maximum volume that can buffered for a single
page are merged along with the page whose buffer became full */
constexpr ulint IBUF_MERGE_THRESHOLD = 4;

/** In ibuf_contract at most this number of pages is read to memory in one
batch, in order to merge the entries for them in the insert buffer */
constexpr auto IBUF_MAX_N_PAGES_MERGED = IBUF_MERGE_AREA;

/** If the combined size of the ibuf trees exceeds ibuf->max_size by this
many pages, we start to contract it in connection to inserts there, using
non-synchronous contract */
constexpr ulint IBUF_CONTRACT_ON_INSERT_NON_SYNC = 0;

/** If the combined size of the ibuf trees exceeds ibuf->max_size by this
many pages, we start to contract it in connection to inserts there, using
synchronous contract */
constexpr ulint IBUF_CONTRACT_ON_INSERT_SYNC = 5;

/** If the combined size of the ibuf trees exceeds ibuf->max_size by
this many pages, we start to contract it synchronous contract, but do
not insert */
constexpr ulint IBUF_CONTRACT_DO_NOT_INSERT = 10;

/* TODO: how to cope with drop table if there are records in the insert
buffer for the indexes of the table? Is there actually any problem,
because ibuf merge is done to a page when it is read in, and it is
still physically like the index page even if the index would have been
dropped! So, there seems to be no problem. */

void ibuf_var_init() {
  ibuf = nullptr;
  ibuf_flush_count = 0;

#ifdef UNIV_IBUF_COUNT_DEBUG
  memset(ibuf_counts, 0x0, sizeof(ibuf_counts));
#endif /* UNIV_IBUF_COUNT_DEBUG */

  memset(&ibuf_pessimistic_insert_mutex, 0x0,
         sizeof(ibuf_pessimistic_insert_mutex));

  memset(&ibuf_mutex, 0x0, sizeof(ibuf_mutex));
  memset(&ibuf_bitmap_mutex, 0x0, sizeof(ibuf_bitmap_mutex));
}

/** Sets the flag in the current OS thread local storage denoting that it is
inside an insert buffer routine. */
static void ibuf_enter() {
  bool *ptr;

  ptr = thr_local_get_in_ibuf_field();

  ut_ad(*ptr == false);

  *ptr = true;
}

/** Sets the flag in the current OS thread local storage denoting that it is
exiting an insert buffer routine. */
static void ibuf_exit(void) {
  bool *ptr;

  ptr = thr_local_get_in_ibuf_field();

  ut_ad(*ptr == true);

  *ptr = false;
}

/** Returns true if the current OS thread is performing an insert buffer
routine.

For instance, a read-ahead of non-ibuf pages is forbidden by threads
that are executing an insert buffer routine.
@return true if inside an insert buffer routine */

bool ibuf_inside() { return (*thr_local_get_in_ibuf_field()); }

/** Gets the ibuf header page and x-latches it.
@param[in,out] mtr              Mini-transaction.
@return	insert buffer header page */
static page_t *ibuf_header_page_get(mtr_t *mtr) {
  ut_ad(!ibuf_inside());

  auto block =
      buf_page_get(IBUF_SPACE_ID, 0, FSP_IBUF_HEADER_PAGE_NO, RW_X_LATCH, mtr);

  buf_block_dbg_add_level(block, SYNC_IBUF_HEADER);

  return buf_block_get_frame(block);
}

/** Gets the root page and x-latches it.
@param[in,out] mtr              Mini-transaction.
@return	insert buffer tree root page */
static page_t *ibuf_tree_root_get(mtr_t *mtr) {
  ut_ad(ibuf_inside());

  mtr_x_lock(dict_index_get_lock(ibuf->index), mtr);

  auto block = buf_page_get(IBUF_SPACE_ID, 0, FSP_IBUF_TREE_ROOT_PAGE_NO,
                            RW_X_LATCH, mtr);

  buf_block_dbg_add_level(block, SYNC_TREE_NODE);

  return buf_block_get_frame(block);
}

#ifdef UNIV_IBUF_COUNT_DEBUG
ulint ibuf_count_get(ulint space, ulint page_no) {
  ibuf_count_check(space, page_no);

  return ibuf_counts[space][page_no];
}

/** Sets the ibuf count for a given page */
static void ibuf_count_set(ulint space, ulint page_no, ulint val) {
  ibuf_count_check(space, page_no);

  ut_a(val < UNIV_PAGE_SIZE);

  ibuf_counts[space][page_no] = val;
}
#endif /* UNIV_IBUF_COUNT_DEBUG */

void ibuf_close() {
  mutex_free(&ibuf_pessimistic_insert_mutex);

  memset(&ibuf_pessimistic_insert_mutex, 0x0,
         sizeof(ibuf_pessimistic_insert_mutex));

  mutex_free(&ibuf_mutex);

  memset(&ibuf_mutex, 0x0, sizeof(ibuf_mutex));

  mutex_free(&ibuf_bitmap_mutex);

  memset(&ibuf_bitmap_mutex, 0x0, sizeof(ibuf_mutex));

  mem_free(ibuf);

  ibuf = nullptr;
}

/** Updates the size information of the ibuf, assuming the segment
size has not changed.
@param[in,out] root             IBUF tree root.
@param[in,out] mtr              Mini-transaction. */
static void ibuf_size_update(const page_t *root, mtr_t *mtr) {
  ut_ad(mutex_own(&ibuf_mutex));

  ibuf->free_list_len =
      flst_get_len(root + PAGE_HEADER + PAGE_BTR_IBUF_FREE_LIST, mtr);

  ibuf->height = 1 + btr_page_get_level(root, mtr);

  /* the '1 +' is the ibuf header page */
  ibuf->size = ibuf->seg_size - (1 + ibuf->free_list_len);

  ibuf->empty = page_get_n_recs(root) == 0;
}

void ibuf_init_at_db_start() {
  page_t *root;
  mtr_t mtr;
  dict_table_t *table;
  mem_heap_t *heap;
  dict_index_t *index;
  ulint n_used;
  page_t *header_page;
  ulint error;

  ibuf = (ibuf_t *)mem_alloc(sizeof(ibuf_t));

  memset(ibuf, 0, sizeof(*ibuf));

  /* Note that also a pessimistic delete can sometimes make a B-tree
  grow in size, as the references on the upper levels of the tree can
  change */

  ibuf->max_size =
      buf_pool_get_curr_size() / UNIV_PAGE_SIZE / IBUF_POOL_SIZE_PER_MAX_SIZE;

  mutex_create(&ibuf_pessimistic_insert_mutex, SYNC_IBUF_PESS_INSERT_MUTEX);

  mutex_create(&ibuf_mutex, SYNC_IBUF_MUTEX);

  mutex_create(&ibuf_bitmap_mutex, SYNC_IBUF_BITMAP_MUTEX);

  mtr_start(&mtr);

  mutex_enter(&ibuf_mutex);

  mtr_x_lock(fil_space_get_latch(IBUF_SPACE_ID), &mtr);

  header_page = ibuf_header_page_get(&mtr);

  fseg_n_reserved_pages(header_page + IBUF_HEADER + IBUF_TREE_SEG_HEADER,
                        &n_used, &mtr);
  ibuf_enter();

  ut_ad(n_used >= 2);

  ibuf->seg_size = n_used;

  {
    buf_block_t *block;

    block = buf_page_get(IBUF_SPACE_ID, 0, FSP_IBUF_TREE_ROOT_PAGE_NO,
                         RW_X_LATCH, &mtr);
    buf_block_dbg_add_level(block, SYNC_TREE_NODE);

    root = buf_block_get_frame(block);
  }

  ibuf_size_update(root, &mtr);
  mutex_exit(&ibuf_mutex);

  mtr_commit(&mtr);

  ibuf_exit();

  heap = mem_heap_create(450);

  /* Use old-style record format for the insert buffer. */
  table = dict_mem_table_create(IBUF_TABLE_NAME, IBUF_SPACE_ID, 1, 0);

  dict_mem_table_add_col(table, heap, "DUMMY_COLUMN", DATA_BINARY, 0, 0);

  table->id = ut_dulint_add(DICT_IBUF_ID_MIN, IBUF_SPACE_ID);

  dict_table_add_to_cache(table, heap);
  mem_heap_free(heap);

  index = dict_mem_index_create(IBUF_TABLE_NAME, "CLUST_IND", IBUF_SPACE_ID,
                                DICT_CLUSTERED | DICT_UNIVERSAL | DICT_IBUF, 1);

  dict_mem_index_add_field(index, "DUMMY_COLUMN", 0);

  index->id = ut_dulint_add(DICT_IBUF_ID_MIN, IBUF_SPACE_ID);

  error =
      dict_index_add_to_cache(table, index, FSP_IBUF_TREE_ROOT_PAGE_NO, false);
  ut_a(error == DB_SUCCESS);

  ibuf->index = dict_table_get_first_index(table);
}

void ibuf_bitmap_page_init(buf_block_t *block, mtr_t *mtr) {
  auto page = buf_block_get_frame(block);

  fil_page_set_type(page, FIL_PAGE_IBUF_BITMAP);

  /* Write all zeros to the bitmap */

  auto byte_offset = UT_BITS_IN_BYTES(UNIV_PAGE_SIZE * IBUF_BITS_PER_PAGE);

  memset(page + IBUF_BITMAP, 0, byte_offset);

  /* The remaining area (up to the page trailer) is uninitialized. */

  mlog_write_initial_log_record(page, MLOG_IBUF_BITMAP_INIT, mtr);
}

byte *ibuf_parse_bitmap_init(byte *ptr, byte *, buf_block_t *block,
                             mtr_t *mtr) {
  ut_ad(ptr);

  if (block != nullptr) {
    ibuf_bitmap_page_init(block, mtr);
  }

  return ptr;
}

/** Gets the desired bits for a given page from a bitmap page.
@return	value of bits */
static ulint
ibuf_bitmap_page_get_bits(const page_t *page, /*!< in: bitmap page */
                          ulint page_no,      /*!< in: page whose bits to get */
                          ulint bit,          /*!< in: IBUF_BITMAP_FREE,
                                              IBUF_BITMAP_BUFFERED, ... */
                          mtr_t *mtr __attribute__((unused)))
/*!< in: mtr containing an
x-latch to the bitmap page */
{
  ulint byte_offset;
  ulint bit_offset;
  ulint map_byte;
  ulint value;

  ut_ad(bit < IBUF_BITS_PER_PAGE);

  static_assert(!(IBUF_BITS_PER_PAGE % 2), "error IBUF_BITS_PER_PAGE % 2 != 0");

  ut_ad(mtr_memo_contains_page(mtr, page, MTR_MEMO_PAGE_X_FIX));

  bit_offset = (page_no % UNIV_PAGE_SIZE) * IBUF_BITS_PER_PAGE + bit;

  byte_offset = bit_offset / 8;
  bit_offset = bit_offset % 8;

  ut_ad(byte_offset + IBUF_BITMAP < UNIV_PAGE_SIZE);

  map_byte = mach_read_from_1(page + IBUF_BITMAP + byte_offset);

  value = ut_bit_get_nth(map_byte, bit_offset);

  if (bit == IBUF_BITMAP_FREE) {
    ut_ad(bit_offset + 1 < 8);

    value = value * 2 + ut_bit_get_nth(map_byte, bit_offset + 1);
  }

  return (value);
}

/** Sets the desired bit for a given page in a bitmap page. */
static void ibuf_bitmap_page_set_bits(
    page_t *page,  /*!< in: bitmap page */
    ulint page_no, /*!< in: page whose bits to set */
    ulint bit,     /*!< in: IBUF_BITMAP_FREE, IBUF_BITMAP_BUFFERED, ... */
    ulint val,     /*!< in: value to set */
    mtr_t *mtr)    /*!< in: mtr containing an x-latch to the bitmap page */
{
  ulint byte_offset;
  ulint bit_offset;
  ulint map_byte;

  ut_ad(bit < IBUF_BITS_PER_PAGE);

  static_assert(!(IBUF_BITS_PER_PAGE % 2), "error IBUF_BITS_PER_PAGE % 2 != 0");

  ut_ad(mtr_memo_contains_page(mtr, page, MTR_MEMO_PAGE_X_FIX));

#ifdef UNIV_IBUF_COUNT_DEBUG
  ut_a((bit != IBUF_BITMAP_BUFFERED) || (val != false) ||
       (0 == ibuf_count_get(page_get_space_id(page), page_no)));
#endif /* UNIV_IBUF_COUNT_DEBUG */

  bit_offset = (page_no % UNIV_PAGE_SIZE) * IBUF_BITS_PER_PAGE + bit;

  byte_offset = bit_offset / 8;
  bit_offset = bit_offset % 8;

  ut_ad(byte_offset + IBUF_BITMAP < UNIV_PAGE_SIZE);

  map_byte = mach_read_from_1(page + IBUF_BITMAP + byte_offset);

  if (bit == IBUF_BITMAP_FREE) {
    ut_ad(bit_offset + 1 < 8);
    ut_ad(val <= 3);

    map_byte = ut_bit_set_nth(map_byte, bit_offset, val / 2);
    map_byte = ut_bit_set_nth(map_byte, bit_offset + 1, val % 2);
  } else {
    ut_ad(val <= 1);
    map_byte = ut_bit_set_nth(map_byte, bit_offset, val);
  }

  mlog_write_ulint(page + IBUF_BITMAP + byte_offset, map_byte, MLOG_1BYTE, mtr);
}

/** Calculates the bitmap page number for a given page number.
@return	the bitmap page number where the file page is mapped */
static ulint
ibuf_bitmap_page_no_calc(ulint,
                         ulint page_no) /*!< in: tablespace page number */
{
  return (FSP_IBUF_BITMAP_OFFSET + (page_no & ~(UNIV_PAGE_SIZE - 1)));
}

/** Gets the ibuf bitmap page where the bits describing a given file page are
stored.
@return bitmap page where the file page is mapped, that is, the bitmap
page containing the descriptor bits for the file page; the bitmap page
is x-latched */
static page_t *ibuf_bitmap_get_map_page_func(
    ulint space,      /*!< in: space id of the file page */
    ulint page_no,    /*!< in: page number of the file page */
    const char *file, /*!< in: file name */
    ulint line,       /*!< in: line where called */
    mtr_t *mtr)       /*!< in: mtr */
{
  auto block = buf_page_get_gen(space, 0, ibuf_bitmap_page_no_calc(0, page_no),
                                RW_X_LATCH, nullptr, BUF_GET, file, line, mtr);

  buf_block_dbg_add_level(block, SYNC_IBUF_BITMAP);

  return buf_block_get_frame(block);
}

/** Gets the ibuf bitmap page where the bits describing a given file page are
stored.
@return bitmap page where the file page is mapped, that is, the bitmap
page containing the descriptor bits for the file page; the bitmap page
is x-latched
@param space	in: space id of the file page
@param page_no	in: page number of the file page
@param mtr	in: mini-transaction */
#define ibuf_bitmap_get_map_page(space, page_no, mtr)                \
  ibuf_bitmap_get_map_page_func(space, page_no, __FILE__, __LINE__, mtr)

/** Sets the free bits of the page in the ibuf bitmap. This is done in a
separate mini-transaction, hence this operation does not restrict further work
to only ibuf bitmap operations, which would result if the latch to the bitmap
page were kept. */
static void ibuf_set_free_bits_low(
    const buf_block_t *block, /*!< in: index page; free bits are set if
                                     the index is non-clustered and page
                                     level is 0 */
    ulint val,                       /*!< in: value to set: < 4 */
    mtr_t *mtr)                      /*!< in/out: mtr */
{
  page_t *bitmap_page;
  ulint space;
  ulint page_no;

  if (!page_is_leaf(buf_block_get_frame(block))) {

    return;
  }

  space = buf_block_get_space(block);
  page_no = buf_block_get_page_no(block);
  bitmap_page = ibuf_bitmap_get_map_page(space, page_no, mtr);

#ifdef UNIV_IBUF_DEBUG
  ut_a(val <= ibuf_index_page_calc_free(0, block));
#endif /* UNIV_IBUF_DEBUG */

  ibuf_bitmap_page_set_bits(bitmap_page, page_no, IBUF_BITMAP_FREE, val, mtr);
}

/** Sets the free bit of the page in the ibuf bitmap. This is done in a separate
mini-transaction, hence this operation does not restrict further work to only
ibuf bitmap operations, which would result if the latch to the bitmap page
were kept. */

void ibuf_set_free_bits_func(
    buf_block_t *block, /*!< in: index page of a non-clustered index;
                        free bit is reset if page level is 0 */
#ifdef UNIV_IBUF_DEBUG
    ulint max_val, /*!< in: ULINT_UNDEFINED or a maximum
                   value which the bits must have before
                   setting; this is for debugging */
#endif             /* UNIV_IBUF_DEBUG */
    ulint val)     /*!< in: value to set: < 4 */
{
  mtr_t mtr;
  page_t *page;
  page_t *bitmap_page;
  ulint space;
  ulint page_no;

  page = buf_block_get_frame(block);

  if (!page_is_leaf(page)) {

    return;
  }

  mtr_start(&mtr);

  space = buf_block_get_space(block);
  page_no = buf_block_get_page_no(block);
  bitmap_page = ibuf_bitmap_get_map_page(space, page_no, &mtr);

#ifdef UNIV_IBUF_DEBUG
  if (max_val != ULINT_UNDEFINED) {
    auto old_val =
        ibuf_bitmap_page_get_bits(bitmap_page, page_no, IBUF_BITMAP_FREE, &mtr);
    ut_a(old_val <= max_val);
  }

  ut_a(val <= ibuf_index_page_calc_free(0, block));
#endif /* UNIV_IBUF_DEBUG */

  ibuf_bitmap_page_set_bits(bitmap_page, page_no, IBUF_BITMAP_FREE, val, &mtr);

  mtr_commit(&mtr);
}

/** Resets the free bits of the page in the ibuf bitmap. This is done in a
separate mini-transaction, hence this operation does not restrict
further work to only ibuf bitmap operations, which would result if the
latch to the bitmap page were kept.  NOTE: The free bits in the insert
buffer bitmap must never exceed the free space on a page.  It is safe
to decrement or reset the bits in the bitmap in a mini-transaction
that is committed before the mini-transaction that affects the free
space. */

void ibuf_reset_free_bits(
    buf_block_t *block) /*!< in: index page; free bits are set to 0
                        if the index is a non-clustered
                        non-unique, and page level is 0 */
{
  ibuf_set_free_bits(block, 0, ULINT_UNDEFINED);
}

/** Updates the free bits for an uncompressed page to reflect the present
state.  Does this in the mtr given, which means that the latching
order rules virtually prevent any further operations for this OS
thread until mtr is committed.  NOTE: The free bits in the insert
buffer bitmap must never exceed the free space on a page.  It is safe
to set the free bits in the same mini-transaction that updated the
page. */

void ibuf_update_free_bits_low(const buf_block_t *block, /*!< in: index page */
                               ulint max_ins_size,       /*!< in: value of
                                                         maximum insert size
                                                         with reorganize before
                                                         the latest operation
                                                         performed to the page */
                               mtr_t *mtr)               /*!< in/out: mtr */
{

  auto before = ibuf_index_page_calc_free_bits(0, max_ins_size);
  auto after = ibuf_index_page_calc_free(0, block);

  /* This approach cannot be used on compressed pages, since the
  computed value of "before" often does not match the current
  state of the bitmap.  This is because the free space may
  increase or decrease when a compressed page is reorganized. */
  if (before != after) {
    ibuf_set_free_bits_low(block, after, mtr);
  }
}

/** Returns true if the page is one of the fixed address ibuf pages.
@return	true if a fixed address ibuf i/o page */
static bool ibuf_fixed_addr_page(ulint space, ulint page_no) {
  return (space == IBUF_SPACE_ID && page_no == IBUF_TREE_ROOT_PAGE_NO) ||
         ibuf_bitmap_page(page_no);
}

bool ibuf_page(ulint space, ulint page_no, mtr_t *mtr) {
  bool ret;
  mtr_t local_mtr;
  page_t *bitmap_page;

  ut_ad(!recv_no_ibuf_operations);

  if (ibuf_fixed_addr_page(space, page_no)) {

    return (true);
  } else if (space != IBUF_SPACE_ID) {

    return (false);
  }

  ut_ad(fil_space_get_type(IBUF_SPACE_ID) == FIL_TABLESPACE);

  if (mtr == nullptr) {
    mtr = &local_mtr;
    mtr_start(mtr);
  }

  bitmap_page = ibuf_bitmap_get_map_page(space, page_no, mtr);

  ret = ibuf_bitmap_page_get_bits(bitmap_page, page_no, IBUF_BITMAP_IBUF, mtr);

  if (mtr == &local_mtr) {
    mtr_commit(mtr);
  }

  return (ret);
}

/** Returns the page number field of an ibuf record.
@return	page number */
static ulint ibuf_rec_get_page_no(const rec_t *rec) /*!< in: ibuf record */
{
  const byte *field;
  ulint len;

  ut_ad(ibuf_inside());
  ut_ad(rec_get_n_fields_old(rec) > 2);

  field = rec_get_nth_field_old(rec, 1, &len);

  if (len == 1) {
    /* This is of the >= 4.1.x record format */
    ut_a(trx_sys_multiple_tablespace_format);

    field = rec_get_nth_field_old(rec, 2, &len);
  } else {
    ut_a(trx_doublewrite_must_reset_space_ids);
    ut_a(!trx_sys_multiple_tablespace_format);

    field = rec_get_nth_field_old(rec, 0, &len);
  }

  ut_a(len == 4);

  return (mach_read_from_4(field));
}

/** Returns the space id field of an ibuf record. For < 4.1.x format records
returns 0.
@return	space id */
static ulint ibuf_rec_get_space(const rec_t *rec) /*!< in: ibuf record */
{
  const byte *field;
  ulint len;

  ut_ad(ibuf_inside());
  ut_ad(rec_get_n_fields_old(rec) > 2);

  field = rec_get_nth_field_old(rec, 1, &len);

  if (len == 1) {
    /* This is of the >= 4.1.x record format */

    ut_a(trx_sys_multiple_tablespace_format);
    field = rec_get_nth_field_old(rec, 0, &len);
    ut_a(len == 4);

    return (mach_read_from_4(field));
  }

  ut_a(trx_doublewrite_must_reset_space_ids);
  ut_a(!trx_sys_multiple_tablespace_format);

  return (0);
}

/** Creates a dummy index for inserting a record to a non-clustered index.
@param[in] n                    Number of fields.
@param[in] comp                 true if compressed record
@return	dummy index */
static dict_index_t *ibuf_dummy_index_create(ulint n, bool comp) {
  auto table = dict_mem_table_create("IBUF_DUMMY", DICT_HDR_SPACE, n,
                                     comp ? DICT_TF_COMPACT : 0);
  auto index =
      dict_mem_index_create("IBUF_DUMMY", "IBUF_DUMMY", DICT_HDR_SPACE, 0, n);

  index->table = table;

  /* avoid ut_ad(index->cached) in dict_index_get_n_unique_in_tree */
  index->cached = true;

  return (index);
}

/** Add a column to the dummy index
@param[in,out] index           Dummy index instance.
@param[in] type                Meta data.
@param[in] len                 Length of the column. */
static void ibuf_dummy_index_add_col(dict_index_t *index, const dtype_t *type,
                                     ulint len) {
  ulint i = index->table->n_def;

  dict_mem_table_add_col(index->table, nullptr, nullptr, dtype_get_mtype(type),
                         dtype_get_prtype(type), dtype_get_len(type));
  dict_index_add_col(index, index->table,
                     dict_table_get_nth_col(index->table, i), len);
}

/** Deallocates a dummy index for inserting a record to a non-clustered index.
@param[in,own] index            Index to free.  */
static void ibuf_dummy_index_free(dict_index_t *index) {
  dict_table_t *table = index->table;

  dict_mem_index_free(index);
  dict_mem_table_free(table);
}

/** Builds the entry to insert into a non-clustered index when we have the
corresponding record in an ibuf index.

NOTE that as we copy pointers to fields in ibuf_rec, the caller must
hold a latch to the ibuf_rec page as long as the entry is used!

@return own: entry to insert to a non-clustered index */
static dtuple_t *ibuf_build_entry_pre_4_1_x(
    const rec_t *ibuf_rec, /*!< in: record in an insert buffer */
    mem_heap_t *heap,      /*!< in: heap where built */
    dict_index_t **pindex) /*!< out, own: dummy index that
                           describes the entry */
{
  ulint i;
  ulint len;
  const byte *types;
  dtuple_t *tuple;
  ulint n_fields;

  ut_a(trx_doublewrite_must_reset_space_ids);
  ut_a(!trx_sys_multiple_tablespace_format);

  n_fields = rec_get_n_fields_old(ibuf_rec) - 2;
  tuple = dtuple_create(heap, n_fields);
  types = rec_get_nth_field_old(ibuf_rec, 1, &len);

  ut_a(len == n_fields * DATA_ORDER_NULL_TYPE_BUF_SIZE);

  for (i = 0; i < n_fields; i++) {
    const byte *data;
    dfield_t *field;

    field = dtuple_get_nth_field(tuple, i);

    data = rec_get_nth_field_old(ibuf_rec, i + 2, &len);

    dfield_set_data(field, data, len);

    dtype_read_for_order_and_null_size(
        dfield_get_type(field), types + i * DATA_ORDER_NULL_TYPE_BUF_SIZE);
  }

  *pindex = ibuf_dummy_index_create(n_fields, false);

  return (tuple);
}

/** Builds the entry to insert into a non-clustered index when we have the
corresponding record in an ibuf index.

NOTE that as we copy pointers to fields in ibuf_rec, the caller must
hold a latch to the ibuf_rec page as long as the entry is used!

@return own: entry to insert to a non-clustered index */
static dtuple_t *ibuf_build_entry_from_ibuf_rec(
    const rec_t *ibuf_rec, /*!< in: record in an insert buffer */
    mem_heap_t *heap,      /*!< in: heap where built */
    dict_index_t **pindex) /*!< out, own: dummy index that
                           describes the entry */
{
  dtuple_t *tuple;
  dfield_t *field;
  ulint n_fields;
  const byte *types;
  const byte *data;
  ulint len;
  ulint i;
  dict_index_t *index;

  data = rec_get_nth_field_old(ibuf_rec, 1, &len);

  if (len > 1) {
    /* This a < 4.1.x format record */

    return (ibuf_build_entry_pre_4_1_x(ibuf_rec, heap, pindex));
  }

  /* This a >= 4.1.x format record */

  ut_a(trx_sys_multiple_tablespace_format);
  ut_a(*data == 0);
  ut_a(rec_get_n_fields_old(ibuf_rec) > 4);

  n_fields = rec_get_n_fields_old(ibuf_rec) - 4;

  tuple = dtuple_create(heap, n_fields);

  types = rec_get_nth_field_old(ibuf_rec, 3, &len);

  ut_a(len % DATA_NEW_ORDER_NULL_TYPE_BUF_SIZE <= 1);
  index = ibuf_dummy_index_create(n_fields,
                                  len % DATA_NEW_ORDER_NULL_TYPE_BUF_SIZE);

  if (len % DATA_NEW_ORDER_NULL_TYPE_BUF_SIZE) {
    /* compact record format */
    len--;
    ut_a(*types == 0);
    types++;
  }

  ut_a(len == n_fields * DATA_NEW_ORDER_NULL_TYPE_BUF_SIZE);

  for (i = 0; i < n_fields; i++) {
    field = dtuple_get_nth_field(tuple, i);

    data = rec_get_nth_field_old(ibuf_rec, i + 4, &len);

    dfield_set_data(field, data, len);

    dtype_new_read_for_order_and_null_size(
        dfield_get_type(field), types + i * DATA_NEW_ORDER_NULL_TYPE_BUF_SIZE);

    ibuf_dummy_index_add_col(index, dfield_get_type(field), len);
  }

  ut_d(dict_table_add_system_columns(index->table, index->table->heap));

  *pindex = index;

  return (tuple);
}

/** Returns the space taken by a stored non-clustered index entry if converted
to an index record.
@return size of index record in bytes + an upper limit of the space
taken in the page directory */
static ulint ibuf_rec_get_volume(const rec_t *ibuf_rec) /*!< in: ibuf record */
{
  dtype_t dtype;
  bool new_format = false;
  ulint data_size = 0;
  ulint n_fields;
  const byte *types;
  const byte *data;
  ulint len;
  ulint i;
  ulint comp;

  ut_ad(ibuf_inside());
  ut_ad(rec_get_n_fields_old(ibuf_rec) > 2);

  data = rec_get_nth_field_old(ibuf_rec, 1, &len);

  if (len > 1) {
    /* < 4.1.x format record */

    ut_a(trx_doublewrite_must_reset_space_ids);
    ut_a(!trx_sys_multiple_tablespace_format);

    n_fields = rec_get_n_fields_old(ibuf_rec) - 2;

    types = rec_get_nth_field_old(ibuf_rec, 1, &len);

    ut_ad(len == n_fields * DATA_ORDER_NULL_TYPE_BUF_SIZE);
    comp = 0;
  } else {
    /* >= 4.1.x format record */

    ut_a(trx_sys_multiple_tablespace_format);
    ut_a(*data == 0);

    types = rec_get_nth_field_old(ibuf_rec, 3, &len);

    comp = len % DATA_NEW_ORDER_NULL_TYPE_BUF_SIZE;

    ut_a(comp <= 1);
    if (comp) {
      /* compact record format */
      ulint volume;
      dict_index_t *dummy_index;
      mem_heap_t *heap = mem_heap_create(500);
      dtuple_t *entry =
          ibuf_build_entry_from_ibuf_rec(ibuf_rec, heap, &dummy_index);
      volume = rec_get_converted_size(dummy_index, entry, 0);
      ibuf_dummy_index_free(dummy_index);
      mem_heap_free(heap);
      return (volume + page_dir_calc_reserved_space(1));
    }

    n_fields = rec_get_n_fields_old(ibuf_rec) - 4;

    new_format = true;
  }

  for (i = 0; i < n_fields; i++) {
    if (new_format) {
      data = rec_get_nth_field_old(ibuf_rec, i + 4, &len);

      dtype_new_read_for_order_and_null_size(
          &dtype, types + i * DATA_NEW_ORDER_NULL_TYPE_BUF_SIZE);
    } else {
      data = rec_get_nth_field_old(ibuf_rec, i + 2, &len);

      dtype_read_for_order_and_null_size(
          &dtype, types + i * DATA_ORDER_NULL_TYPE_BUF_SIZE);
    }

    if (len == UNIV_SQL_NULL) {
      data_size += dtype_get_sql_null_size(&dtype, comp);
    } else {
      data_size += len;
    }
  }

  return (data_size + rec_get_converted_extra_size(data_size, n_fields, 0) +
          page_dir_calc_reserved_space(1));
}

/** Builds the tuple to insert to an ibuf tree when we have an entry for a
non-clustered index.

NOTE that the original entry must be kept because we copy pointers to
its fields.

@return	own: entry to insert into an ibuf index tree */
static dtuple_t *ibuf_entry_build(
    dict_index_t *index,   /*!< in: non-clustered index */
    const dtuple_t *entry, /*!< in: entry for a non-clustered index */
    ulint space,           /*!< in: space id */
    ulint page_no,         /*!< in: index page number where entry should
                           be inserted */
    mem_heap_t *heap)      /*!< in: heap into which to build */
{
  dtuple_t *tuple;
  dfield_t *field;
  const dfield_t *entry_field;
  ulint n_fields;
  ulint i;

  /* Starting from 4.1.x, we have to build a tuple whose
  (1) first field is the space id,
  (2) the second field a single marker byte (0) to tell that this
  is a new format record,
  (3) the third contains the page number, and
  (4) the fourth contains the relevent type information of each data
  field; the length of this field % DATA_NEW_ORDER_NULL_TYPE_BUF_SIZE is
  (a) 0 for b-trees in the old format, and
  (b) 1 for b-trees in the compact format, the first byte of the field
  being the marker (0);
  (5) and the rest of the fields are copied from entry. All fields
  in the tuple are ordered like the type binary in our insert buffer
  tree. */

  n_fields = dtuple_get_n_fields(entry);

  tuple = dtuple_create(heap, n_fields + 4);

  /* Store the space id in tuple */

  field = dtuple_get_nth_field(tuple, 0);

  auto buf = (byte *)mem_heap_alloc(heap, 4);

  mach_write_to_4(buf, space);

  dfield_set_data(field, buf, 4);

  /* Store the marker byte field in tuple */

  field = dtuple_get_nth_field(tuple, 1);

  buf = (byte *)mem_heap_alloc(heap, 1);

  /* We set the marker byte zero */

  mach_write_to_1(buf, 0);

  dfield_set_data(field, buf, 1);

  /* Store the page number in tuple */

  field = dtuple_get_nth_field(tuple, 2);

  buf = (byte *)mem_heap_alloc(heap, 4);

  mach_write_to_4(buf, page_no);

  dfield_set_data(field, buf, 4);

  /* Store the type info in buf2, and add the fields from entry to tuple */
  auto buf2 = (byte *)mem_heap_alloc(
      heap, n_fields * DATA_NEW_ORDER_NULL_TYPE_BUF_SIZE +
                dict_table_is_comp(index->table));

  if (dict_table_is_comp(index->table)) {
    *buf2++ = 0; /* write the compact format indicator */
  }
  for (i = 0; i < n_fields; i++) {
    ulint fixed_len;
    const dict_field_t *ifield;

    /* We add 4 below because we have the 4 extra fields at the
    start of an ibuf record */

    field = dtuple_get_nth_field(tuple, i + 4);
    entry_field = dtuple_get_nth_field(entry, i);
    dfield_copy(field, entry_field);

    ifield = dict_index_get_nth_field(index, i);
    /* Prefix index columns of fixed-length columns are of
    fixed length.  However, in the function call below,
    dfield_get_type(entry_field) contains the fixed length
    of the column in the clustered index.  Replace it with
    the fixed length of the secondary index column. */
    fixed_len = ifield->fixed_len;

#ifdef UNIV_DEBUG
    if (fixed_len) {
      /* dict_index_add_col() should guarantee these */
      ut_ad(fixed_len <= (ulint)dfield_get_type(entry_field)->len);
      if (ifield->prefix_len) {
        ut_ad(ifield->prefix_len == fixed_len);
      } else {
        ut_ad(fixed_len == (ulint)dfield_get_type(entry_field)->len);
      }
    }
#endif /* UNIV_DEBUG */

    dtype_new_store_for_order_and_null_size(
        buf2 + i * DATA_NEW_ORDER_NULL_TYPE_BUF_SIZE,
        dfield_get_type(entry_field), fixed_len);
  }

  /* Store the type info in buf2 to field 3 of tuple */

  field = dtuple_get_nth_field(tuple, 3);

  if (dict_table_is_comp(index->table)) {
    buf2--;
  }

  dfield_set_data(field, buf2,
                  n_fields * DATA_NEW_ORDER_NULL_TYPE_BUF_SIZE +
                      dict_table_is_comp(index->table));
  /* Set all the types in the new tuple binary */

  dtuple_set_types_binary(tuple, n_fields + 4);

  return (tuple);
}

/** Builds a search tuple used to search buffered inserts for an index page.
This is for < 4.1.x format records
@return	own: search tuple */
static dtuple_t *
ibuf_search_tuple_build(ulint space,      /*!< in: space id */
                        ulint page_no,    /*!< in: index page number */
                        mem_heap_t *heap) /*!< in: heap into which to build */
{
  dtuple_t *tuple;
  dfield_t *field;
  byte *buf;

  ut_a(space == 0);
  ut_a(trx_doublewrite_must_reset_space_ids);
  ut_a(!trx_sys_multiple_tablespace_format);

  tuple = dtuple_create(heap, 1);

  /* Store the page number in tuple */

  field = dtuple_get_nth_field(tuple, 0);

  buf = mem_heap_alloc(heap, 4);

  mach_write_to_4(buf, page_no);

  dfield_set_data(field, buf, 4);

  dtuple_set_types_binary(tuple, 1);

  return (tuple);
}

/** Builds a search tuple used to search buffered inserts for an index page.
This is for >= 4.1.x format records.
@return	own: search tuple */
static dtuple_t *ibuf_new_search_tuple_build(
    ulint space,      /*!< in: space id */
    ulint page_no,    /*!< in: index page number */
    mem_heap_t *heap) /*!< in: heap into which to build */
{
  dtuple_t *tuple;
  dfield_t *field;
  byte *buf;

  ut_a(trx_sys_multiple_tablespace_format);

  tuple = dtuple_create(heap, 3);

  /* Store the space id in tuple */

  field = dtuple_get_nth_field(tuple, 0);

  buf = mem_heap_alloc(heap, 4);

  mach_write_to_4(buf, space);

  dfield_set_data(field, buf, 4);

  /* Store the new format record marker byte */

  field = dtuple_get_nth_field(tuple, 1);

  buf = mem_heap_alloc(heap, 1);

  mach_write_to_1(buf, 0);

  dfield_set_data(field, buf, 1);

  /* Store the page number in tuple */

  field = dtuple_get_nth_field(tuple, 2);

  buf = mem_heap_alloc(heap, 4);

  mach_write_to_4(buf, page_no);

  dfield_set_data(field, buf, 4);

  dtuple_set_types_binary(tuple, 3);

  return (tuple);
}

/** Checks if there are enough pages in the free list of the ibuf tree that we
dare to start a pessimistic insert to the insert buffer.
@return	true if enough free pages in list */
static bool ibuf_data_enough_free_for_insert(void) {
  ut_ad(mutex_own(&ibuf_mutex));

  /* We want a big margin of free pages, because a B-tree can sometimes
  grow in size also if records are deleted from it, as the node pointers
  can change, and we must make sure that we are able to delete the
  inserts buffered for pages that we read to the buffer pool, without
  any risk of running out of free space in the insert buffer. */

  return (ibuf->free_list_len >= (ibuf->size / 2) + 3 * ibuf->height);
}

/** Checks if there are enough pages in the free list of the ibuf tree that we
should remove them and free to the file space management.
@return	true if enough free pages in list */
static bool ibuf_data_too_much_free(void) {
  ut_ad(mutex_own(&ibuf_mutex));

  return (ibuf->free_list_len >= 3 + (ibuf->size / 2) + 3 * ibuf->height);
}

/** Allocates a new page from the ibuf file segment and adds it to the free
list.
@return	DB_SUCCESS, or DB_STRONG_FAIL if no space left */
static ulint ibuf_add_free_page(void) {
  mtr_t mtr;
  page_t *header_page;
  ulint page_no;
  page_t *page;
  page_t *root;
  page_t *bitmap_page;

  mtr_start(&mtr);

  /* Acquire the fsp latch before the ibuf header, obeying the latching
  order */
  mtr_x_lock(fil_space_get_latch(IBUF_SPACE_ID), &mtr);

  header_page = ibuf_header_page_get(&mtr);

  /* Allocate a new page: NOTE that if the page has been a part of a
  non-clustered index which has subsequently been dropped, then the
  page may have buffered inserts in the insert buffer, and these
  should be deleted from there. These get deleted when the page
  allocation creates the page in buffer. Thus the call below may end
  up calling the insert buffer routines and, as we yet have no latches
  to insert buffer tree pages, these routines can run without a risk
  of a deadlock. This is the reason why we created a special ibuf
  header page apart from the ibuf tree. */

  page_no = fseg_alloc_free_page(
      header_page + IBUF_HEADER + IBUF_TREE_SEG_HEADER, 0, FSP_UP, &mtr);

  if (page_no == FIL_NULL) {
    mtr_commit(&mtr);

    return (DB_STRONG_FAIL);
  }

  {
    buf_block_t *block;

    block = buf_page_get(IBUF_SPACE_ID, 0, page_no, RW_X_LATCH, &mtr);

    buf_block_dbg_add_level(block, SYNC_TREE_NODE_NEW);

    page = buf_block_get_frame(block);
  }

  ibuf_enter();

  mutex_enter(&ibuf_mutex);

  root = ibuf_tree_root_get(&mtr);

  /* Add the page to the free list and update the ibuf size data */

  flst_add_last(root + PAGE_HEADER + PAGE_BTR_IBUF_FREE_LIST,
                page + PAGE_HEADER + PAGE_BTR_IBUF_FREE_LIST_NODE, &mtr);

  mlog_write_ulint(page + FIL_PAGE_TYPE, FIL_PAGE_IBUF_FREE_LIST, MLOG_2BYTES,
                   &mtr);

  ibuf->seg_size++;
  ibuf->free_list_len++;

  /* Set the bit indicating that this page is now an ibuf tree page
  (level 2 page) */

  bitmap_page = ibuf_bitmap_get_map_page(IBUF_SPACE_ID, page_no, &mtr);

  ibuf_bitmap_page_set_bits(bitmap_page, page_no, IBUF_BITMAP_IBUF, true, &mtr);

  mtr_commit(&mtr);

  mutex_exit(&ibuf_mutex);

  ibuf_exit();

  return (DB_SUCCESS);
}

/** Removes a page from the free list and frees it to the fsp system. */
static void ibuf_remove_free_page(void) {
  mtr_t mtr;
  mtr_t mtr2;
  page_t *header_page;
  ulint page_no;
  page_t *page;
  page_t *root;
  page_t *bitmap_page;

  mtr_start(&mtr);

  /* Acquire the fsp latch before the ibuf header, obeying the latching
  order */
  mtr_x_lock(fil_space_get_latch(IBUF_SPACE_ID), &mtr);

  header_page = ibuf_header_page_get(&mtr);

  /* Prevent pessimistic inserts to insert buffer trees for a while */
  mutex_enter(&ibuf_pessimistic_insert_mutex);

  ibuf_enter();

  mutex_enter(&ibuf_mutex);

  if (!ibuf_data_too_much_free()) {

    mutex_exit(&ibuf_mutex);

    ibuf_exit();

    mutex_exit(&ibuf_pessimistic_insert_mutex);

    mtr_commit(&mtr);

    return;
  }

  mtr_start(&mtr2);

  root = ibuf_tree_root_get(&mtr2);

  page_no =
      flst_get_last(root + PAGE_HEADER + PAGE_BTR_IBUF_FREE_LIST, &mtr2).page;

  /* NOTE that we must release the latch on the ibuf tree root
  because in fseg_free_page we access level 1 pages, and the root
  is a level 2 page. */

  mtr_commit(&mtr2);
  mutex_exit(&ibuf_mutex);

  ibuf_exit();

  /* Since pessimistic inserts were prevented, we know that the
  page is still in the free list. NOTE that also deletes may take
  pages from the free list, but they take them from the start, and
  the free list was so long that they cannot have taken the last
  page from it. */

  fseg_free_page(header_page + IBUF_HEADER + IBUF_TREE_SEG_HEADER,
                 IBUF_SPACE_ID, page_no, &mtr);

#ifdef UNIV_DEBUG_FILE_ACCESSES
  buf_page_reset_file_page_was_freed(IBUF_SPACE_ID, page_no);
#endif

  ibuf_enter();

  mutex_enter(&ibuf_mutex);

  root = ibuf_tree_root_get(&mtr);

  ut_ad(page_no ==
        flst_get_last(root + PAGE_HEADER + PAGE_BTR_IBUF_FREE_LIST, &mtr).page);

  {
    buf_block_t *block;

    block = buf_page_get(IBUF_SPACE_ID, 0, page_no, RW_X_LATCH, &mtr);

    buf_block_dbg_add_level(block, SYNC_TREE_NODE);

    page = buf_block_get_frame(block);
  }

  /* Remove the page from the free list and update the ibuf size data */

  flst_remove(root + PAGE_HEADER + PAGE_BTR_IBUF_FREE_LIST,
              page + PAGE_HEADER + PAGE_BTR_IBUF_FREE_LIST_NODE, &mtr);

  ibuf->seg_size--;
  ibuf->free_list_len--;

  mutex_exit(&ibuf_pessimistic_insert_mutex);

  /* Set the bit indicating that this page is no more an ibuf tree page
  (level 2 page) */

  bitmap_page = ibuf_bitmap_get_map_page(IBUF_SPACE_ID, page_no, &mtr);

  ibuf_bitmap_page_set_bits(bitmap_page, page_no, IBUF_BITMAP_IBUF, false,
                            &mtr);

#ifdef UNIV_DEBUG_FILE_ACCESSES
  buf_page_set_file_page_was_freed(IBUF_SPACE_ID, page_no);
#endif
  mtr_commit(&mtr);

  mutex_exit(&ibuf_mutex);

  ibuf_exit();
}

/** Frees excess pages from the ibuf free list. This function is called when an
OS thread calls fsp services to allocate a new file segment, or a new page to a
file segment, and the thread did not own the fsp latch before this call. */

void ibuf_free_excess_pages(void) {
  ulint i;

#ifdef UNIV_SYNC_DEBUG
  ut_ad(rw_lock_own(fil_space_get_latch(IBUF_SPACE_ID), RW_LOCK_EX));
#endif /* UNIV_SYNC_DEBUG */

  ut_ad(rw_lock_get_x_lock_count(fil_space_get_latch(IBUF_SPACE_ID)) == 1);

  ut_ad(!ibuf_inside());

  /* NOTE: We require that the thread did not own the latch before,
  because then we know that we can obey the correct latching order
  for ibuf latches */

  if (!ibuf) {
    /* Not yet initialized; not sure if this is possible, but
    does no harm to check for it. */

    return;
  }

  /* Free at most a few pages at a time, so that we do not delay the
  requested service too much */

  for (i = 0; i < 4; i++) {

    mutex_enter(&ibuf_mutex);

    if (!ibuf_data_too_much_free()) {

      mutex_exit(&ibuf_mutex);

      return;
    }

    mutex_exit(&ibuf_mutex);

    ibuf_remove_free_page();
  }
}

/** Reads page numbers from a leaf in an ibuf tree.
@return a lower limit for the combined volume of records which will be
merged */
static ulint ibuf_get_merge_page_nos(
    bool contract,           /*!< in: true if this function is called to
                             contract the tree, false if this is called
                             when a single page becomes full and we look
                             if it pays to read also nearby pages */
    rec_t *rec,              /*!< in: record from which we read up and down
                             in the chain of records */
    ulint *space_ids,        /*!< in/out: space id's of the pages */
    int64_t *space_versions, /*!< in/out: tablespace version
                         timestamps; used to prevent reading in old
                         pages after DISCARD + IMPORT tablespace */
    ulint *page_nos,         /*!< in/out: buffer for at least
                            IBUF_MAX_N_PAGES_MERGED many page numbers;
                            the page numbers are in an ascending order */
    ulint *n_stored)         /*!< out: number of page numbers stored to
                            page_nos in this function */
{
  ulint prev_page_no;
  ulint prev_space_id;
  ulint first_page_no;
  ulint first_space_id;
  ulint rec_page_no;
  ulint rec_space_id;
  ulint sum_volumes;
  ulint volume_for_page;
  ulint rec_volume;
  ulint limit;
  ulint n_pages;

  *n_stored = 0;

  limit = ut_min(IBUF_MAX_N_PAGES_MERGED, buf_pool->curr_size / 4);

  if (page_rec_is_supremum(rec)) {

    rec = page_rec_get_prev(rec);
  }

  if (page_rec_is_infimum(rec)) {

    rec = page_rec_get_next(rec);
  }

  if (page_rec_is_supremum(rec)) {

    return (0);
  }

  first_page_no = ibuf_rec_get_page_no(rec);
  first_space_id = ibuf_rec_get_space(rec);
  n_pages = 0;
  prev_page_no = 0;
  prev_space_id = 0;

  /* Go backwards from the first rec until we reach the border of the
  'merge area', or the page start or the limit of storeable pages is
  reached */

  while (!page_rec_is_infimum(rec) && likely(n_pages < limit)) {

    rec_page_no = ibuf_rec_get_page_no(rec);
    rec_space_id = ibuf_rec_get_space(rec);

    if (rec_space_id != first_space_id ||
        (rec_page_no / IBUF_MERGE_AREA) != (first_page_no / IBUF_MERGE_AREA)) {

      break;
    }

    if (rec_page_no != prev_page_no || rec_space_id != prev_space_id) {
      n_pages++;
    }

    prev_page_no = rec_page_no;
    prev_space_id = rec_space_id;

    rec = page_rec_get_prev(rec);
  }

  rec = page_rec_get_next(rec);

  /* At the loop start there is no prev page; we mark this with a pair
  of space id, page no (0, 0) for which there can never be entries in
  the insert buffer */

  prev_page_no = 0;
  prev_space_id = 0;
  sum_volumes = 0;
  volume_for_page = 0;

  while (*n_stored < limit) {
    if (page_rec_is_supremum(rec)) {
      /* When no more records available, mark this with
      another 'impossible' pair of space id, page no */
      rec_page_no = 1;
      rec_space_id = 0;
    } else {
      rec_page_no = ibuf_rec_get_page_no(rec);
      rec_space_id = ibuf_rec_get_space(rec);
      ut_ad(rec_page_no > IBUF_TREE_ROOT_PAGE_NO);
    }

#ifdef UNIV_IBUF_DEBUG
    ut_a(*n_stored < IBUF_MAX_N_PAGES_MERGED);
#endif
    if ((rec_space_id != prev_space_id || rec_page_no != prev_page_no) &&
        (prev_space_id != 0 || prev_page_no != 0)) {

      if ((prev_page_no == first_page_no && prev_space_id == first_space_id) ||
          contract ||
          (volume_for_page > ((IBUF_MERGE_THRESHOLD - 1) * 4 * UNIV_PAGE_SIZE /
                              IBUF_PAGE_SIZE_PER_FREE_SPACE) /
                                 IBUF_MERGE_THRESHOLD)) {

        space_ids[*n_stored] = prev_space_id;
        space_versions[*n_stored] = fil_space_get_version(prev_space_id);
        page_nos[*n_stored] = prev_page_no;

        (*n_stored)++;

        sum_volumes += volume_for_page;
      }

      if (rec_space_id != first_space_id ||
          rec_page_no / IBUF_MERGE_AREA != first_page_no / IBUF_MERGE_AREA) {

        break;
      }

      volume_for_page = 0;
    }

    if (rec_page_no == 1 && rec_space_id == 0) {
      /* Supremum record */

      break;
    }

    rec_volume = ibuf_rec_get_volume(rec);

    volume_for_page += rec_volume;

    prev_page_no = rec_page_no;
    prev_space_id = rec_space_id;

    rec = page_rec_get_next(rec);
  }

#ifdef UNIV_IBUF_DEBUG
  ut_a(*n_stored <= IBUF_MAX_N_PAGES_MERGED);
#endif
#if 0
	ib_logger(ib_stream, "Ibuf merge batch %lu pages %lu volume\n",
		*n_stored, sum_volumes);
#endif
  return (sum_volumes);
}

/** Contracts insert buffer trees by reading pages to the buffer pool.
@return a lower limit for the combined size in bytes of entries which
will be merged from ibuf trees to the pages read, 0 if ibuf is
empty */
static ulint
ibuf_contract_ext(ulint *n_pages, /*!< out: number of pages to which merged */
                  bool sync) /*!< in: true if the caller wants to wait for the
                              issued read with the highest tablespace address
                              to complete */
{
  btr_pcur_t pcur;
  ulint page_nos[IBUF_MAX_N_PAGES_MERGED];
  ulint space_ids[IBUF_MAX_N_PAGES_MERGED];
  int64_t space_versions[IBUF_MAX_N_PAGES_MERGED];
  ulint n_stored;
  ulint sum_sizes;
  mtr_t mtr;

  *n_pages = 0;
  ut_ad(!ibuf_inside());

  mutex_enter(&ibuf_mutex);

  if (ibuf->empty) {
  ibuf_is_empty:
    mutex_exit(&ibuf_mutex);

    return (0);
  }

  mtr_start(&mtr);

  ibuf_enter();

  /* Open a cursor to a randomly chosen leaf of the tree, at a random
  position within the leaf */

  btr_pcur_open_at_rnd_pos(ibuf->index, BTR_SEARCH_LEAF, &pcur, &mtr);

  if (page_get_n_recs(btr_pcur_get_page(&pcur)) == 0) {
    /* When the ibuf tree is emptied completely, the last record
    is removed using an optimistic delete and ibuf_size_update
    is not called, causing ibuf->empty to remain false. If we do
    not reset it to true here then database shutdown will hang
    in the loop in ibuf_contract_for_n_pages. */

    ibuf->empty = true;

    ibuf_exit();

    mtr_commit(&mtr);
    btr_pcur_close(&pcur);

    goto ibuf_is_empty;
  }

  mutex_exit(&ibuf_mutex);

  sum_sizes = ibuf_get_merge_page_nos(true, btr_pcur_get_rec(&pcur), space_ids,
                                      space_versions, page_nos, &n_stored);
#if 0 /* defined UNIV_IBUF_DEBUG */
	ib_logger(ib_stream, "Ibuf contract sync %lu pages %lu volume %lu\n",
		sync, n_stored, sum_sizes);
#endif
  ibuf_exit();

  mtr_commit(&mtr);
  btr_pcur_close(&pcur);

  buf_read_ibuf_merge_pages(sync, space_ids, space_versions, page_nos,
                            n_stored);
  *n_pages = n_stored;

  return (sum_sizes + 1);
}

/** Contracts insert buffer trees by reading pages to the buffer pool.
@return a lower limit for the combined size in bytes of entries which
will be merged from ibuf trees to the pages read, 0 if ibuf is
empty */

ulint ibuf_contract(bool sync) /*!< in: true if the caller wants to wait for
                                the issued read with the highest tablespace
                                address to complete */
{
  ulint n_pages;

  return (ibuf_contract_ext(&n_pages, sync));
}

/** Contracts insert buffer trees by reading pages to the buffer pool.
@return a lower limit for the combined size in bytes of entries which
will be merged from ibuf trees to the pages read, 0 if ibuf is
empty */

ulint ibuf_contract_for_n_pages(
    bool sync,     /*!< in: true if the caller wants to wait for the
                    issued read with the highest tablespace address
                    to complete */
    ulint n_pages) /*!< in: try to read at least this many pages to
                   the buffer pool and merge the ibuf contents to
                   them */
{
  ulint sum_bytes = 0;
  ulint sum_pages = 0;
  ulint n_bytes;
  ulint n_pag2;

  while (sum_pages < n_pages) {
    n_bytes = ibuf_contract_ext(&n_pag2, sync);

    if (n_bytes == 0) {
      return (sum_bytes);
    }

    sum_bytes += n_bytes;
    sum_pages += n_pag2;
  }

  return (sum_bytes);
}

/** Contract insert buffer trees after insert if they are too big. */
static void
ibuf_contract_after_insert(ulint entry_size) /*!< in: size of a record which was
                                             inserted into an ibuf tree */
{
  bool sync;
  ulint sum_sizes;
  ulint size;

  mutex_enter(&ibuf_mutex);

  if (ibuf->size < ibuf->max_size + IBUF_CONTRACT_ON_INSERT_NON_SYNC) {
    mutex_exit(&ibuf_mutex);

    return;
  }

  sync = false;

  if (ibuf->size >= ibuf->max_size + IBUF_CONTRACT_ON_INSERT_SYNC) {

    sync = true;
  }

  mutex_exit(&ibuf_mutex);

  /* Contract at least entry_size many bytes */
  sum_sizes = 0;
  size = 1;

  while ((size > 0) && (sum_sizes < entry_size)) {

    size = ibuf_contract(sync);
    sum_sizes += size;
  }
}

/** Gets an upper limit for the combined size of entries buffered in the insert
buffer for a given page.
@return upper limit for the volume of buffered inserts for the index
page, in bytes; UNIV_PAGE_SIZE, if the entries for the index page span
several pages in the insert buffer */
static ulint ibuf_get_volume_buffered(
    btr_pcur_t *pcur, /*!< in: pcur positioned at a place in an
                      insert buffer tree where we would insert an
                      entry for the index page whose number is
                      page_no, latch mode has to be BTR_MODIFY_PREV
                      or BTR_MODIFY_TREE */
    ulint space,      /*!< in: space id */
    ulint page_no,    /*!< in: page number of an index page */
    mtr_t *mtr)       /*!< in: mtr */
{
  ulint volume;
  rec_t *rec;
  page_t *page;
  ulint prev_page_no;
  page_t *prev_page;
  ulint next_page_no;
  page_t *next_page;

  ut_a(trx_sys_multiple_tablespace_format);

  ut_ad((pcur->latch_mode == BTR_MODIFY_PREV) ||
        (pcur->latch_mode == BTR_MODIFY_TREE));

  /* Count the volume of records earlier in the alphabetical order than
  pcur */

  volume = 0;

  rec = btr_pcur_get_rec(pcur);
  page = page_align(rec);

  if (page_rec_is_supremum(rec)) {
    rec = page_rec_get_prev(rec);
  }

  for (;;) {
    if (page_rec_is_infimum(rec)) {

      break;
    }

    if (page_no != ibuf_rec_get_page_no(rec) ||
        space != ibuf_rec_get_space(rec)) {

      goto count_later;
    }

    volume += ibuf_rec_get_volume(rec);

    rec = page_rec_get_prev(rec);
  }

  /* Look at the previous page */

  prev_page_no = btr_page_get_prev(page, mtr);

  if (prev_page_no == FIL_NULL) {

    goto count_later;
  }

  {
    buf_block_t *block;

    block = buf_page_get(IBUF_SPACE_ID, 0, prev_page_no, RW_X_LATCH, mtr);

    buf_block_dbg_add_level(block, SYNC_TREE_NODE);

    prev_page = buf_block_get_frame(block);
  }

#ifdef UNIV_BTR_DEBUG
  ut_a(btr_page_get_next(prev_page, mtr) == page_get_page_no(page));
#endif /* UNIV_BTR_DEBUG */

  rec = page_get_supremum_rec(prev_page);
  rec = page_rec_get_prev(rec);

  for (;;) {
    if (page_rec_is_infimum(rec)) {

      /* We cannot go to yet a previous page, because we
      do not have the x-latch on it, and cannot acquire one
      because of the latching order: we have to give up */

      return (UNIV_PAGE_SIZE);
    }

    if (page_no != ibuf_rec_get_page_no(rec) ||
        space != ibuf_rec_get_space(rec)) {

      goto count_later;
    }

    volume += ibuf_rec_get_volume(rec);

    rec = page_rec_get_prev(rec);
  }

count_later:
  rec = btr_pcur_get_rec(pcur);

  if (!page_rec_is_supremum(rec)) {
    rec = page_rec_get_next(rec);
  }

  for (;;) {
    if (page_rec_is_supremum(rec)) {

      break;
    }

    if (page_no != ibuf_rec_get_page_no(rec) ||
        space != ibuf_rec_get_space(rec)) {

      return (volume);
    }

    volume += ibuf_rec_get_volume(rec);

    rec = page_rec_get_next(rec);
  }

  /* Look at the next page */

  next_page_no = btr_page_get_next(page, mtr);

  if (next_page_no == FIL_NULL) {

    return (volume);
  }

  {
    buf_block_t *block;

    block = buf_page_get(IBUF_SPACE_ID, 0, next_page_no, RW_X_LATCH, mtr);

    buf_block_dbg_add_level(block, SYNC_TREE_NODE);

    next_page = buf_block_get_frame(block);
  }

#ifdef UNIV_BTR_DEBUG
  ut_a(btr_page_get_prev(next_page, mtr) == page_get_page_no(page));
#endif /* UNIV_BTR_DEBUG */

  rec = page_get_infimum_rec(next_page);
  rec = page_rec_get_next(rec);

  for (;;) {
    if (page_rec_is_supremum(rec)) {

      /* We give up */

      return (UNIV_PAGE_SIZE);
    }

    if (page_no != ibuf_rec_get_page_no(rec) ||
        space != ibuf_rec_get_space(rec)) {

      return (volume);
    }

    volume += ibuf_rec_get_volume(rec);

    rec = page_rec_get_next(rec);
  }
}

/** Reads the biggest tablespace id from the high end of the insert buffer
tree and updates the counter in fil_system. */

void ibuf_update_max_tablespace_id(void) {
  ulint max_space_id;
  const rec_t *rec;
  const byte *field;
  ulint len;
  btr_pcur_t pcur;
  mtr_t mtr;

  ut_a(!dict_table_is_comp(ibuf->index->table));

  ibuf_enter();

  mtr_start(&mtr);

  btr_pcur_open_at_index_side(false, ibuf->index, BTR_SEARCH_LEAF, &pcur, true,
                              &mtr);

  btr_pcur_move_to_prev(&pcur, &mtr);

  if (btr_pcur_is_before_first_on_page(&pcur)) {
    /* The tree is empty */

    max_space_id = 0;
  } else {
    rec = btr_pcur_get_rec(&pcur);

    field = rec_get_nth_field_old(rec, 0, &len);

    ut_a(len == 4);

    max_space_id = mach_read_from_4(field);
  }

  mtr_commit(&mtr);
  ibuf_exit();

  /* printf("Maximum space id in insert buffer %lu\n", max_space_id); */

  fil_set_max_space_id_if_bigger(max_space_id);
}

/** Makes an index insert to the insert buffer, instead of directly to the disk
page, if this is possible.
@return	DB_SUCCESS, DB_FAIL, DB_STRONG_FAIL */
static ulint
ibuf_insert_low(ulint mode, /*!< in: BTR_MODIFY_PREV or BTR_MODIFY_TREE */
                const dtuple_t *entry, /*!< in: index entry to insert */
                ulint entry_size,
                /*!< in: rec_get_converted_size(index, entry) */
                dict_index_t *index, /*!< in: index where to insert; must not be
                                     unique or clustered */
                ulint space,         /*!< in: space id where to insert */
                ulint,          /*!< in: compressed page size in bytes, or 0 */
                ulint page_no,  /*!< in: page number where to insert */
                que_thr_t *thr) /*!< in: query thread */
{
  big_rec_t *dummy_big_rec;
  btr_pcur_t pcur;
  btr_cur_t *cursor;
  dtuple_t *ibuf_entry;
  mem_heap_t *heap;
  ulint buffered;
  rec_t *ins_rec;
  bool old_bit_value;
  page_t *bitmap_page;
  page_t *root;
  ulint err;
  bool do_merge;
  ulint space_ids[IBUF_MAX_N_PAGES_MERGED];
  int64_t space_versions[IBUF_MAX_N_PAGES_MERGED];
  ulint page_nos[IBUF_MAX_N_PAGES_MERGED];
  ulint n_stored;
  ulint bits;
  mtr_t mtr;
  mtr_t bitmap_mtr;

  ut_a(!dict_index_is_clust(index));
  ut_ad(dtuple_check_typed(entry));
  ut_ad(ut_is_2pow(0));

  ut_a(trx_sys_multiple_tablespace_format);

  do_merge = false;

  mutex_enter(&ibuf_mutex);

  if (ibuf->size >= ibuf->max_size + IBUF_CONTRACT_DO_NOT_INSERT) {
    /* Insert buffer is now too big, contract it but do not try
    to insert */

    mutex_exit(&ibuf_mutex);

#ifdef UNIV_IBUF_DEBUG
    ib_logger(ib_stream, "Ibuf too big\n");
#endif
    /* Use synchronous contract (== true) */
    ibuf_contract(true);

    return (DB_STRONG_FAIL);
  }

  mutex_exit(&ibuf_mutex);

  if (mode == BTR_MODIFY_TREE) {
    mutex_enter(&ibuf_pessimistic_insert_mutex);

    ibuf_enter();

    mutex_enter(&ibuf_mutex);

    while (!ibuf_data_enough_free_for_insert()) {

      mutex_exit(&ibuf_mutex);

      ibuf_exit();

      mutex_exit(&ibuf_pessimistic_insert_mutex);

      err = ibuf_add_free_page();

      if (err == DB_STRONG_FAIL) {

        return (err);
      }

      mutex_enter(&ibuf_pessimistic_insert_mutex);

      ibuf_enter();

      mutex_enter(&ibuf_mutex);
    }
  } else {
    ibuf_enter();
  }

  heap = mem_heap_create(512);

  /* Build the entry which contains the space id and the page number as
  the first fields and the type information for other fields, and which
  will be inserted to the insert buffer. */

  ibuf_entry = ibuf_entry_build(index, entry, space, page_no, heap);

  /* Open a cursor to the insert buffer tree to calculate if we can add
  the new entry to it without exceeding the free space limit for the
  page. */

  mtr_start(&mtr);

  btr_pcur_open(ibuf->index, ibuf_entry, PAGE_CUR_LE, mode, &pcur, &mtr);

  /* Find out the volume of already buffered inserts for the same index
  page */
  buffered = ibuf_get_volume_buffered(&pcur, space, page_no, &mtr);

#ifdef UNIV_IBUF_COUNT_DEBUG
  ut_a((buffered == 0) || ibuf_count_get(space, page_no));
#endif
  mtr_start(&bitmap_mtr);

  bitmap_page = ibuf_bitmap_get_map_page(space, page_no, &bitmap_mtr);

  /* We check if the index page is suitable for buffered entries */

  if (buf_page_peek(space, page_no) ||
      lock_rec_expl_exist_on_page(space, page_no)) {
    err = DB_STRONG_FAIL;

    mtr_commit(&bitmap_mtr);

    goto function_exit;
  }

  bits = ibuf_bitmap_page_get_bits(bitmap_page, page_no, IBUF_BITMAP_FREE,
                                   &bitmap_mtr);

  if (buffered + entry_size + page_dir_calc_reserved_space(1) >
      ibuf_index_page_calc_free_from_bits(bits)) {
    mtr_commit(&bitmap_mtr);

    /* It may not fit */
    err = DB_STRONG_FAIL;

    do_merge = true;

    ibuf_get_merge_page_nos(false, btr_pcur_get_rec(&pcur), space_ids,
                            space_versions, page_nos, &n_stored);
    goto function_exit;
  }

  /* Set the bitmap bit denoting that the insert buffer contains
  buffered entries for this index page, if the bit is not set yet */

  old_bit_value = ibuf_bitmap_page_get_bits(bitmap_page, page_no,
                                            IBUF_BITMAP_BUFFERED, &bitmap_mtr);

  if (!old_bit_value) {
    ibuf_bitmap_page_set_bits(bitmap_page, page_no, IBUF_BITMAP_BUFFERED, true,
                              &bitmap_mtr);
  }

  mtr_commit(&bitmap_mtr);

  cursor = btr_pcur_get_btr_cur(&pcur);

  if (mode == BTR_MODIFY_PREV) {

    err = btr_cur_optimistic_insert(BTR_NO_LOCKING_FLAG, cursor, ibuf_entry,
                                    &ins_rec, &dummy_big_rec, 0, thr, &mtr);

    if (err == DB_SUCCESS) {
      /* Update the page max trx id field */
      page_update_max_trx_id(btr_cur_get_block(cursor), thr_get_trx(thr)->id, &mtr);
    }
  } else {
    ut_ad(mode == BTR_MODIFY_TREE);

    /* We acquire an x-latch to the root page before the insert,
    because a pessimistic insert releases the tree x-latch,
    which would cause the x-latching of the root after that to
    break the latching order. */

    root = ibuf_tree_root_get(&mtr);

    err = btr_cur_pessimistic_insert(BTR_NO_LOCKING_FLAG | BTR_NO_UNDO_LOG_FLAG,
                                     cursor, ibuf_entry, &ins_rec,
                                     &dummy_big_rec, 0, thr, &mtr);
    if (err == DB_SUCCESS) {
      /* Update the page max trx id field */
      page_update_max_trx_id(btr_cur_get_block(cursor), thr_get_trx(thr)->id, &mtr);
    }

    ibuf_size_update(root, &mtr);
  }

function_exit:
#ifdef UNIV_IBUF_COUNT_DEBUG
  if (err == DB_SUCCESS) {
    ib_logger(ib_stream,
              "Incrementing ibuf count of space %lu page %lu\n"
              "from %lu by 1\n",
              space, page_no, ibuf_count_get(space, page_no));

    ibuf_count_set(space, page_no, ibuf_count_get(space, page_no) + 1);
  }
#endif
  if (mode == BTR_MODIFY_TREE) {

    mutex_exit(&ibuf_mutex);
    mutex_exit(&ibuf_pessimistic_insert_mutex);
  }

  mtr_commit(&mtr);
  btr_pcur_close(&pcur);
  ibuf_exit();

  mem_heap_free(heap);

  if (err == DB_SUCCESS) {
    mutex_enter(&ibuf_mutex);

    ibuf->empty = false;
    ibuf->n_inserts++;

    mutex_exit(&ibuf_mutex);

    if (mode == BTR_MODIFY_TREE) {
      ibuf_contract_after_insert(entry_size);
    }
  }

  if (do_merge) {
#ifdef UNIV_IBUF_DEBUG
    ut_a(n_stored <= IBUF_MAX_N_PAGES_MERGED);
#endif
    buf_read_ibuf_merge_pages(false, space_ids, space_versions, page_nos,
                              n_stored);
  }

  return (err);
}

bool ibuf_insert(const dtuple_t *entry, dict_index_t *index, ulint space, 
                 ulint page_no, que_thr_t *thr) {
  ulint err;
  ulint entry_size;

  ut_a(trx_sys_multiple_tablespace_format);
  ut_ad(dtuple_check_typed(entry));

  ut_a(!dict_index_is_clust(index));

  switch (expect(ibuf_use, IBUF_USE_INSERT)) {
  case IBUF_USE_NONE:
    return (false);
  case IBUF_USE_INSERT:
    goto do_insert;
  case IBUF_USE_COUNT:
    break;
  }

  ut_error; /* unknown value of ibuf_use */

do_insert:
  entry_size = rec_get_converted_size(index, entry, 0);

  if (entry_size >=
      (page_get_free_space_of_empty(dict_table_is_comp(index->table)) / 2)) {
    return (false);
  }

  err = ibuf_insert_low(BTR_MODIFY_PREV, entry, entry_size, index, space, 0,
                        page_no, thr);
  if (err == DB_FAIL) {
    err = ibuf_insert_low(BTR_MODIFY_TREE, entry, entry_size, index, space, 0,
                          page_no, thr);
  }

  if (err == DB_SUCCESS) {
#ifdef UNIV_IBUF_DEBUG
    /* ib_logger(ib_stream,
            "Ibuf insert for page no %lu of index %s\n",
            page_no, index->name); */
#endif
    return (true);

  } else {
    ut_a(err == DB_STRONG_FAIL);

    return (false);
  }
}

/** During merge, inserts to an index page a secondary index entry extracted
from the insert buffer. */
static void ibuf_insert_to_index_page(
    dtuple_t *entry,     /*!< in: buffered entry to insert */
    buf_block_t *block,  /*!< in/out: index page where the
                         buffered entry  should be placed */
    dict_index_t *index, /*!< in: record descriptor */
    mtr_t *mtr)          /*!< in: mtr */
{
  page_cur_t page_cur;
  ulint low_match;
  page_t *page = buf_block_get_frame(block);
  rec_t *rec;
  page_t *bitmap_page;
  ulint old_bits;

  ut_ad(ibuf_inside());
  ut_ad(dtuple_check_typed(entry));

  if (unlikely(dict_table_is_comp(index->table) !=
               (bool)!!page_is_comp(page))) {
    ib_logger(ib_stream, "InnoDB: Trying to insert a record from"
                         " the insert buffer to an index page\n"
                         "InnoDB: but the 'compact' flag does not match!\n");
    goto dump;
  }

  rec = page_rec_get_next(page_get_infimum_rec(page));

  if (unlikely(rec_get_n_fields(rec, index) != dtuple_get_n_fields(entry))) {
    ib_logger(ib_stream, "InnoDB: Trying to insert a record from"
                         " the insert buffer to an index page\n"
                         "InnoDB: but the number of fields does not match!\n");
  dump:
    buf_page_print(page, 0);

    dtuple_print(ib_stream, entry);

    ib_logger(ib_stream, "InnoDB: The table where where"
                         " this index record belongs\n"
                         "InnoDB: is now probably corrupt."
                         " Please run CHECK TABLE on\n"
                         "InnoDB: your tables.\n"
                         "InnoDB: Submit a detailed bug report, check the "
                         "InnoDB website for details");

    return;
  }

  low_match = page_cur_search(block, index, entry, PAGE_CUR_LE, &page_cur);

  if (low_match == dtuple_get_n_fields(entry)) {
    rec = page_cur_get_rec(&page_cur);

    btr_cur_del_unmark_for_ibuf(rec, mtr);
  } else {
    rec = page_cur_tuple_insert(&page_cur, entry, index, 0, mtr);

    if (likely(rec != nullptr)) {
      return;
    }

    /* If the record did not fit, reorganize */

    btr_page_reorganize(block, index, mtr);
    page_cur_search(block, index, entry, PAGE_CUR_LE, &page_cur);

    /* This time the record must fit */
    if (unlikely(!page_cur_tuple_insert(&page_cur, entry, index, 0, mtr))) {
      ulint space;
      ulint page_no;

      ut_print_timestamp(ib_stream);

      ib_logger(ib_stream,
                "  InnoDB: Error: Insert buffer insert"
                " fails; page free %lu,"
                " dtuple size %lu\n",
                (ulong)page_get_max_insert_size(page, 1),
                (ulong)rec_get_converted_size(index, entry, 0));
      ib_logger(ib_stream, "InnoDB: Cannot insert index record ");
      dtuple_print(ib_stream, entry);
      ib_logger(ib_stream, "\nInnoDB: The table where"
                           " this index record belongs\n"
                           "InnoDB: is now probably corrupt."
                           " Please run CHECK TABLE on\n"
                           "InnoDB: that table.\n");

      space = page_get_space_id(page);
      page_no = page_get_page_no(page);

      bitmap_page = ibuf_bitmap_get_map_page(space, page_no, mtr);
      old_bits = ibuf_bitmap_page_get_bits(bitmap_page, page_no,
                                           IBUF_BITMAP_FREE, mtr);

      ib_logger(ib_stream, "InnoDB: space %lu, page %lu, bitmap bits %lu\n",
                (ulong)space, (ulong)page_no, (ulong)old_bits);

      ib_logger(ib_stream, "InnoDB: Submit a detailed bug report, check"
                           "the InnoDB website for details");
    }
  }
}

/** Deletes from ibuf the record on which pcur is positioned. If we have to
resort to a pessimistic delete, this function commits mtr and closes
the cursor.
@return	true if mtr was committed and pcur closed in this operation */
static bool
ibuf_delete_rec(ulint space,      /*!< in: space id */
                ulint page_no,    /*!< in: index page number where the record
                                  should belong */
                btr_pcur_t *pcur, /*!< in: pcur positioned on the record to
                                  delete, having latch mode BTR_MODIFY_LEAF */
                const dtuple_t *search_tuple,
                /*!< in: search tuple for entries of page_no */
                mtr_t *mtr) /*!< in: mtr */
{
  bool success;
  page_t *root;
  db_err err;

  ut_ad(ibuf_inside());
  ut_ad(page_rec_is_user_rec(btr_pcur_get_rec(pcur)));
  ut_ad(ibuf_rec_get_page_no(btr_pcur_get_rec(pcur)) == page_no);
  ut_ad(ibuf_rec_get_space(btr_pcur_get_rec(pcur)) == space);

  success = btr_cur_optimistic_delete(btr_pcur_get_btr_cur(pcur), mtr);

  if (success) {
#ifdef UNIV_IBUF_COUNT_DEBUG
    ib_logger(ib_stream,
              "Decrementing ibuf count of space %lu page %lu\n"
              "from %lu by 1\n",
              space, page_no, ibuf_count_get(space, page_no));
    ibuf_count_set(space, page_no, ibuf_count_get(space, page_no) - 1);
#endif /* UNIV_IBUF_COUNT_DEBUG */
    return (false);
  }

  ut_ad(page_rec_is_user_rec(btr_pcur_get_rec(pcur)));
  ut_ad(ibuf_rec_get_page_no(btr_pcur_get_rec(pcur)) == page_no);
  ut_ad(ibuf_rec_get_space(btr_pcur_get_rec(pcur)) == space);

  /* We have to resort to a pessimistic delete from ibuf */
  btr_pcur_store_position(pcur, mtr);

  btr_pcur_commit_specify_mtr(pcur, mtr);

  mutex_enter(&ibuf_mutex);

  mtr_start(mtr);

  success = btr_pcur_restore_position(BTR_MODIFY_TREE, pcur, mtr);

  if (!success) {
    if (fil_space_get_flags(space) == ULINT_UNDEFINED) {
      /* The tablespace has been dropped.  It is possible
      that another thread has deleted the insert buffer
      entry.  Do not complain. */
      goto commit_and_exit;
    }

    ib_logger(ib_stream,
              "InnoDB: ERROR: Submit the output to InnoDB."
              "Check the InnoDB website for details.\n"
              "InnoDB: ibuf cursor restoration fails!\n"
              "InnoDB: ibuf record inserted to page %lu\n",
              (ulong)page_no);

    rec_print_old(ib_stream, btr_pcur_get_rec(pcur));
    rec_print_old(ib_stream, pcur->old_rec);
    dtuple_print(ib_stream, search_tuple);

    rec_print_old(ib_stream, page_rec_get_next(btr_pcur_get_rec(pcur)));

    btr_pcur_commit_specify_mtr(pcur, mtr);

    ib_logger(ib_stream, "InnoDB: Validating insert buffer tree:\n");
    if (!btr_validate_index(ibuf->index, nullptr)) {
      ut_error;
    }

    ib_logger(ib_stream, "InnoDB: ibuf tree ok\n");

    goto func_exit;
  }

  root = ibuf_tree_root_get(mtr);

  btr_cur_pessimistic_delete(&err, true, btr_pcur_get_btr_cur(pcur), RB_NONE,
                             mtr);
  ut_a(err == DB_SUCCESS);

#ifdef UNIV_IBUF_COUNT_DEBUG
  ibuf_count_set(space, page_no, ibuf_count_get(space, page_no) - 1);
#endif /* UNIV_IBUF_COUNT_DEBUG */
  ibuf_size_update(root, mtr);

commit_and_exit:
  btr_pcur_commit_specify_mtr(pcur, mtr);

func_exit:
  btr_pcur_close(pcur);

  mutex_exit(&ibuf_mutex);

  return (true);
}

void ibuf_merge_or_delete_for_page(buf_block_t *block, ulint space,
                                   ulint page_no,
                                   bool update_ibuf_bitmap) {
  btr_pcur_t pcur;
  dtuple_t *search_tuple;
  ulint n_inserts;
#ifdef UNIV_IBUF_DEBUG
  ulint volume;
#endif /* UNIV_IBUF_DEBUG */
  bool tablespace_being_deleted = false;
  bool corruption_noticed = false;
  mtr_t mtr;

  ut_ad(block == nullptr || buf_block_get_space(block) == space);
  ut_ad(block == nullptr || buf_block_get_page_no(block) == page_no);

  if (srv_force_recovery >= IB_RECOVERY_NO_IBUF_MERGE ||
      trx_sys_hdr_page(space, page_no)) {
    return;
  }

  if (ibuf_fixed_addr_page(space, page_no) || fsp_descr_page(page_no)) {
    return;
  }

  if (likely(update_ibuf_bitmap)) {
    if (ibuf_fixed_addr_page(space, page_no) || fsp_descr_page(page_no)) {
      return;
    }

    /* If the following returns false, we get the counter
    incremented, and must decrement it when we leave this
    function. When the counter is > 0, that prevents tablespace
    from being dropped. */

    tablespace_being_deleted = fil_inc_pending_ibuf_merges(space);

    if (unlikely(tablespace_being_deleted)) {
      /* Do not try to read the bitmap page from space;
      just delete the ibuf records for the page */

      block = nullptr;
      update_ibuf_bitmap = false;
    } else {
      page_t *bitmap_page;

      mtr_start(&mtr);

      bitmap_page = ibuf_bitmap_get_map_page(space, page_no, &mtr);

      if (!ibuf_bitmap_page_get_bits(bitmap_page, page_no, IBUF_BITMAP_BUFFERED,
                                     &mtr)) {
        /* No inserts buffered for this page */
        mtr_commit(&mtr);

        if (!tablespace_being_deleted) {
          fil_decr_pending_ibuf_merges(space);
        }

        return;
      }
      mtr_commit(&mtr);
    }
  } else if (block != nullptr && (ibuf_fixed_addr_page(space, page_no) ||
                                  fsp_descr_page(page_no))) {

    return;
  }

  ibuf_enter();

  auto heap = mem_heap_create(512);

  if (!trx_sys_multiple_tablespace_format) {
    ut_a(trx_doublewrite_must_reset_space_ids);
    search_tuple = ibuf_search_tuple_build(space, page_no, heap);
  } else {
    search_tuple = ibuf_new_search_tuple_build(space, page_no, heap);
  }

  if (block != nullptr) {
    /* Move the ownership of the x-latch on the page to this OS
    thread, so that we can acquire a second x-latch on it. This
    is needed for the insert operations to the index page to pass
    the debug checks. */

    rw_lock_x_lock_move_ownership(&(block->lock));

    if (unlikely(fil_page_get_type(block->frame) != FIL_PAGE_INDEX) ||
        unlikely(!page_is_leaf(block->frame))) {

      page_t *bitmap_page;

      corruption_noticed = true;

      ut_print_timestamp(ib_stream);

      mtr_start(&mtr);

      ib_logger(ib_stream, "  InnoDB: Dump of the ibuf bitmap page:\n");

      bitmap_page = ibuf_bitmap_get_map_page(space, page_no, &mtr);
      buf_page_print(bitmap_page, 0);

      mtr_commit(&mtr);

      ib_logger(ib_stream, "\nInnoDB: Dump of the page:\n");

      buf_page_print(block->frame, 0);

      ib_logger(ib_stream,
                "InnoDB: Error: corruption in the tablespace."
                " Bitmap shows insert\n"
                "InnoDB: buffer records to page n:o %lu"
                " though the page\n"
                "InnoDB: type is %lu, which is"
                " not an index leaf page!\n"
                "InnoDB: We try to resolve the problem"
                " by skipping the insert buffer\n"
                "InnoDB: merge for this page."
                " Please run CHECK TABLE on your tables\n"
                "InnoDB: to determine if they are corrupt"
                " after this.\n\n"
                "InnoDB: Please submit a detailed bug report, "
                "check InnoDB website for details",
                (ulong)page_no, (ulong)fil_page_get_type(block->frame));
    }
  }

  n_inserts = 0;
#ifdef UNIV_IBUF_DEBUG
  volume = 0;
#endif /* UNIV_IBUF_DEBUG */
loop:
  mtr_start(&mtr);

  if (block != nullptr) {
    auto success = buf_page_get_known_nowait(RW_X_LATCH, block, BUF_KEEP_OLD, __FILE__, __LINE__, &mtr);

    ut_a(success);

    buf_block_dbg_add_level(block, SYNC_TREE_NODE);
  }

  /* Position pcur in the insert buffer at the first entry for this index page */
  btr_pcur_open_on_user_rec(ibuf->index, search_tuple, PAGE_CUR_GE, BTR_MODIFY_LEAF, &pcur, &mtr);

  if (!btr_pcur_is_on_user_rec(&pcur)) {
    ut_ad(btr_pcur_is_after_last_in_tree(&pcur, &mtr));

    goto reset_bit;
  }

  for (;;) {
    ut_ad(btr_pcur_is_on_user_rec(&pcur));

    auto rec = btr_pcur_get_rec(&pcur);

    /* Check if the entry is for this index page */
    if (ibuf_rec_get_page_no(rec) != page_no || ibuf_rec_get_space(rec) != space) {

      if (block != nullptr) {
        page_header_reset_last_insert(block->frame, &mtr);
      }

      goto reset_bit;
    }

    if (unlikely(corruption_noticed)) {
      ib_logger(ib_stream, "InnoDB: Discarding record\n ");
      rec_print_old(ib_stream, rec);
      ib_logger(ib_stream, "\nInnoDB: from the insert buffer!\n\n");
    } else if (block != nullptr) {
      /* Now we have at pcur a record which should be
      inserted to the index page; NOTE that the call below
      copies pointers to fields in rec, and we must
      keep the latch to the rec page until the
      insertion is finished! */
      dtuple_t *entry;
      trx_id_t max_trx_id;
      dict_index_t *dummy_index;

      max_trx_id = page_get_max_trx_id(page_align(rec));
      page_update_max_trx_id(block, max_trx_id, &mtr);

      entry = ibuf_build_entry_from_ibuf_rec(rec, heap, &dummy_index);
#ifdef UNIV_IBUF_DEBUG
      volume += rec_get_converted_size(dummy_index, entry, 0) +
                page_dir_calc_reserved_space(1);
      ut_a(volume <= 4 * UNIV_PAGE_SIZE / IBUF_PAGE_SIZE_PER_FREE_SPACE);
#endif /* UNIV_IBUF_DEBUG */
      ibuf_insert_to_index_page(entry, block, dummy_index, &mtr);
      ibuf_dummy_index_free(dummy_index);
    }

    ++n_inserts;

    /* Delete the record from ibuf */
    if (ibuf_delete_rec(space, page_no, &pcur, search_tuple, &mtr)) {
      /* Deletion was pessimistic and mtr was committed:
      we start from the beginning again */

      goto loop;
    } else if (btr_pcur_is_after_last_on_page(&pcur)) {
      mtr_commit(&mtr);
      btr_pcur_close(&pcur);

      goto loop;
    }
  }

reset_bit:
#ifdef UNIV_IBUF_COUNT_DEBUG
  if (ibuf_count_get(space, page_no) > 0) {
    /* btr_print_tree(ibuf_data->index->tree, 100);
    ibuf_print(); */
  }
#endif /* UNIV_IBUF_COUNT_DEBUG */
  if (likely(update_ibuf_bitmap)) {
    page_t *bitmap_page;

    bitmap_page = ibuf_bitmap_get_map_page(space, page_no, &mtr);

    ibuf_bitmap_page_set_bits(bitmap_page, page_no, IBUF_BITMAP_BUFFERED, false,
                              &mtr);

    if (block != nullptr) {
      ulint old_bits = ibuf_bitmap_page_get_bits(bitmap_page, page_no,
                                                 IBUF_BITMAP_FREE, &mtr);
      ulint new_bits = ibuf_index_page_calc_free(0, block);

      if (old_bits != new_bits) {
        ibuf_bitmap_page_set_bits(bitmap_page, page_no, IBUF_BITMAP_FREE,
                                  new_bits, &mtr);
      }
    }
  }

  mtr_commit(&mtr);
  btr_pcur_close(&pcur);
  mem_heap_free(heap);

  /* Protect our statistics keeping from race conditions */
  mutex_enter(&ibuf_mutex);

  ibuf->n_merges++;
  ibuf->n_merged_recs += n_inserts;

  mutex_exit(&ibuf_mutex);

  if (update_ibuf_bitmap && !tablespace_being_deleted) {

    fil_decr_pending_ibuf_merges(space);
  }

  ibuf_exit();

#ifdef UNIV_IBUF_COUNT_DEBUG
  ut_a(ibuf_count_get(space, page_no) == 0);
#endif /* UNIV_IBUF_COUNT_DEBUG */
}

void ibuf_delete_for_discarded_space(ulint space) {
  mem_heap_t *heap;
  btr_pcur_t pcur;
  dtuple_t *search_tuple;
  rec_t *ibuf_rec;
  ulint page_no;
  bool closed;
  ulint n_inserts;
  mtr_t mtr;

  heap = mem_heap_create(512);

  /* Use page number 0 to build the search tuple so that we get the
  cursor positioned at the first entry for this space id */

  search_tuple = ibuf_new_search_tuple_build(space, 0, heap);

  n_inserts = 0;
loop:
  ibuf_enter();

  mtr_start(&mtr);

  /* Position pcur in the insert buffer at the first entry for the
  space */
  btr_pcur_open_on_user_rec(ibuf->index, search_tuple, PAGE_CUR_GE,
                            BTR_MODIFY_LEAF, &pcur, &mtr);

  if (!btr_pcur_is_on_user_rec(&pcur)) {
    ut_ad(btr_pcur_is_after_last_in_tree(&pcur, &mtr));

    goto leave_loop;
  }

  for (;;) {
    ut_ad(btr_pcur_is_on_user_rec(&pcur));

    ibuf_rec = btr_pcur_get_rec(&pcur);

    /* Check if the entry is for this space */
    if (ibuf_rec_get_space(ibuf_rec) != space) {

      goto leave_loop;
    }

    page_no = ibuf_rec_get_page_no(ibuf_rec);

    n_inserts++;

    /* Delete the record from ibuf */
    closed = ibuf_delete_rec(space, page_no, &pcur, search_tuple, &mtr);
    if (closed) {
      /* Deletion was pessimistic and mtr was committed:
      we start from the beginning again */

      ibuf_exit();

      goto loop;
    }

    if (btr_pcur_is_after_last_on_page(&pcur)) {
      mtr_commit(&mtr);
      btr_pcur_close(&pcur);

      ibuf_exit();

      goto loop;
    }
  }

leave_loop:
  mtr_commit(&mtr);
  btr_pcur_close(&pcur);

  /* Protect our statistics keeping from race conditions */
  mutex_enter(&ibuf_mutex);

  ibuf->n_merges++;
  ibuf->n_merged_recs += n_inserts;

  mutex_exit(&ibuf_mutex);

  ibuf_exit();

  mem_heap_free(heap);
}

/** Looks if the insert buffer is empty.
@return	true if empty */

bool ibuf_is_empty(void) {
  bool is_empty;
  const page_t *root;
  mtr_t mtr;

  ibuf_enter();

  mutex_enter(&ibuf_mutex);

  mtr_start(&mtr);

  root = ibuf_tree_root_get(&mtr);

  if (page_get_n_recs(root) == 0) {

    is_empty = true;

    if (ibuf->empty == false) {
      ib_logger(ib_stream, "InnoDB: Warning: insert buffer tree is empty"
                           " but the data struct does not\n"
                           "InnoDB: know it. This condition is legal"
                           " if the master thread has not yet\n"
                           "InnoDB: run to completion.\n");
    }
  } else {
    ut_a(ibuf->empty == false);

    is_empty = false;
  }

  mtr_commit(&mtr);

  mutex_exit(&ibuf_mutex);

  ibuf_exit();

  return (is_empty);
}

/** Prints info of ibuf. */

void ibuf_print(ib_stream_t ib_stream) /*!< in: stream where to print */
{
#ifdef UNIV_IBUF_COUNT_DEBUG
  ulint i;
  ulint j;
#endif

  mutex_enter(&ibuf_mutex);

  ib_logger(ib_stream,
            "Ibuf: size %lu, free list len %lu, seg size %lu,\n"
            "%lu inserts, %lu merged recs, %lu merges\n",
            (ulong)ibuf->size, (ulong)ibuf->free_list_len,
            (ulong)ibuf->seg_size, (ulong)ibuf->n_inserts,
            (ulong)ibuf->n_merged_recs, (ulong)ibuf->n_merges);
#ifdef UNIV_IBUF_COUNT_DEBUG
  for (i = 0; i < IBUF_COUNT_N_SPACES; i++) {
    for (j = 0; j < IBUF_COUNT_N_PAGES; j++) {
      ulint count = ibuf_count_get(i, j);

      if (count > 0) {
        ib_logger(ib_stream,
                  "Ibuf count for space/page %lu/%lu"
                  " is %lu\n",
                  (ulong)i, (ulong)j, (ulong)count);
      }
    }
  }
#endif /* UNIV_IBUF_COUNT_DEBUG */

  mutex_exit(&ibuf_mutex);
}

void ibuf_update_free_bits_for_two_pages_low(buf_block_t *block1, buf_block_t *block2, mtr_t *mtr) {
  /* As we have to x-latch two random bitmap pages, we have to acquire
  the bitmap mutex to prevent a deadlock with a similar operation
  performed by another OS thread. */

  mutex_enter(&ibuf_bitmap_mutex);

  auto state = ibuf_index_page_calc_free(0, block1);

  ibuf_set_free_bits_low(block1, state, mtr);

  state = ibuf_index_page_calc_free(0, block2);

  ibuf_set_free_bits_low(block2, state, mtr);

  mutex_exit(&ibuf_bitmap_mutex);
}
