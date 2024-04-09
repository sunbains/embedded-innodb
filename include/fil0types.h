#pragma once

#include "innodb0types.h"

#include "hash0hash.h"
#include "sync0rw.h"
#include "ut0lst.h"

using fil_faddr_t = byte;

/** File space address
An address stored in a file page is a string of bytes */
struct fil_addr_t {
  /** page number within a space */
  page_no_t m_page_no;

  /** byte offset within the page */
  uint32_t m_boffset;
};

/** Initial size of a single-table tablespace in pages */
constexpr uint FIL_IBD_FILE_INITIAL_SIZE = 4;

/** 'null' (undefined) page offset in the context of file spaces */
constexpr auto FIL_NULL = ULINT32_UNDEFINED;

/** Space address data type; this is intended to be used when
addresses accurate to a byte are stored in file pages. If the page part
of the address is FIL_NULL, the address is considered undefined. */

/** First in address is the page offset */
constexpr ulint FIL_ADDR_PAGE = 0;

/** Then comes 2-byte byte offset within page*/
constexpr ulint FIL_ADDR_BYTE = 4;

/** Address size is 6 bytes */
constexpr ulint FIL_ADDR_SIZE = 6;

/** The null file address */
extern const fil_addr_t fil_addr_null;

/** The byte offsets on a file page for various variables @{ */

/** Checksum of the page */
constexpr ulint FIL_PAGE_SPACE_OR_CHKSUM = 0;

/** Page offset inside space */
constexpr ulint FIL_PAGE_OFFSET = 4;

/** If there is a 'natural' predecessor of the page, its offset.
Otherwise FIL_NULL. This field is not set on BLOB pages, which
are stored as a singly-linked list.  See also FIL_PAGE_NEXT. */
constexpr ulint FIL_PAGE_PREV = 8;

/** If there is a 'natural' successor of the page, its offset.
Otherwise FIL_NULL.  B-tree index pages (FIL_PAGE_TYPE contains
FIL_PAGE_INDEX) on the same PAGE_LEVEL are maintained as a doubly
linked list via FIL_PAGE_PREV and FIL_PAGE_NEXT in the collation
order of the smallest user record on each page. */
constexpr ulint FIL_PAGE_NEXT = 12;

/** LSN of the end of the newest modification log record to the page */
constexpr ulint FIL_PAGE_LSN = 16;

/** File page type: FIL_PAGE_INDEX,..., 2 bytes. */
constexpr ulint FIL_PAGE_TYPE = 24;

/** This is only defined for the first page in a system tablespace
data file (ibdata*, not *.ibd): the file has been flushed to disk at least up to
this lsn */
constexpr ulint FIL_PAGE_FILE_FLUSH_LSN = 26;

/** Starting from 4.1.x this contains the space id of the page */
constexpr ulint FIL_PAGE_SPACE_ID = 34;

/** start of the data on the page */
constexpr ulint FIL_PAGE_DATA = 38;

/* @} */
/** File page trailer @{ */

/** the low 4 bytes of this are used to store the page checksum, the last 4
bytes should be identical to the last 4 bytes of FIL_PAGE_LSN */
constexpr ulint FIL_PAGE_END_LSN_OLD_CHKSUM = 8;

/** Size of the page trailer */
constexpr ulint FIL_PAGE_DATA_END = 8;

/* @} */

/** File page types (values of FIL_PAGE_TYPE) @{ */

/** B-tree node */
constexpr ulint FIL_PAGE_INDEX = 17855;

/** Undo log page */
constexpr ulint FIL_PAGE_UNDO_LOG = 2;

/** Index node */
constexpr ulint FIL_PAGE_INODE = 3;

/* File page types introduced in InnoDB 5.1.7 */
/** Freshly allocated page */
constexpr ulint FIL_PAGE_TYPE_ALLOCATED = 0;

/** System page */
constexpr ulint FIL_PAGE_TYPE_SYS = 6;

/** Transaction system data */
constexpr ulint FIL_PAGE_TYPE_TRX_SYS = 7;

/** File space header */
constexpr ulint FIL_PAGE_TYPE_FSP_HDR = 8;

/** Extent descriptor page */
constexpr ulint FIL_PAGE_TYPE_XDES = 9;

/** Uncompressed BLOB page */
constexpr ulint FIL_PAGE_TYPE_BLOB = 10;

/** First compressed BLOB page */
constexpr ulint FIL_PAGE_TYPE_ZBLOB = 11;

/** Subsequent compressed BLOB page */
constexpr ulint FIL_PAGE_TYPE_ZBLOB2 = 12;

/* @} */

enum Fil_type {
/** Tablespace */
FIL_TABLESPACE = 501,

/** Redo log */
FIL_LOG = 502
};

/** Value of fil_space_t::magic_n */
constexpr uint32_t FIL_SPACE_MAGIC_N = 89472;

/** Value of fil_node_t::magic_n */
constexpr uint32_t FIL_NODE_MAGIC_N = 89389;

struct fil_space_t;

/** File node of a tablespace or the log data space */
struct fil_node_t {
  /** backpointer to the space where this node belongs */
  fil_space_t *m_space;

  /** path to the file */
  char *m_file_name;

  /** true if file open */
  bool open;

  /** OS handle to the file, if file open */
  os_file_t m_fh;

  /** true if the 'file' is actually a raw device or a raw
  disk partition */
  bool m_is_raw_disk;

  /** size of the file in database pages, 0 if not known yet;
  the possible last incomplete megabyte may be ignored if space == 0 */
  page_no_t m_size_in_pages;

  /** count of pending i/o's on this file; closing of the file is not
  allowed if this is > 0 */
  uint32_t m_n_pending;

  /** count of pending flushes on this file; closing of the file
  is not allowed if this is > 0 */
  uint32_t m_n_pending_flushes;

  /** when we write to the file we increment this by one */
  int64_t m_modification_counter;

  /** Up to what modification_counter value we have flushed the
  modifications to disk */
  int64_t m_flush_counter;

  /** Link field for the file chain */
  UT_LIST_NODE_T(fil_node_t) m_chain;

  /** Link field for the LRU list */
  UT_LIST_NODE_T(fil_node_t) m_LRU;

  /** FIL_NODE_MAGIC_N */
  uint32_t m_magic_n;
};

/** Tablespace or log data space: let us call them by a common name space */
struct fil_space_t {
  /** space name = the path to the first file in it */
  char *m_name;

  /** space id */
  space_id_t m_id;

  /** in DISCARD/IMPORT this timestamp is used to check if
  we should ignore an insert buffer merge request for a page
  because it actually was for the previous incarnation of the space */
  int64_t m_tablespace_version;

  /** this is set to true at database startup if the space corresponds
  to a table in the InnoDB data dictionary; so we can print a warning
  of orphaned tablespaces */
  bool m_mark;

  /** true if we want to rename the .ibd file of tablespace and want to
  stop temporarily posting of new i/o requests on the file */
  bool m_stop_ios;

  /** this is set to true when we start deleting a single-table tablespace
  and its file; when this flag is set no further i/o or flush requests can
  be placed on this space, though there may be such requests still being
  processed on this space */
  bool m_is_being_deleted;

  /** FIL_TABLESPACE or FIL_LOG */
  Fil_type m_type;

  /** base node for the file chain */
  UT_LIST_BASE_NODE_T(fil_node_t, m_chain) m_chain;

  /** space size in pages; 0 if a single-table tablespace whose size we do
  not know yet; last incomplete megabytes in data files may be ignored if
  space == 0 */
  page_no_t m_size_in_pages;

  /** File format, or 0 */
  uint32_t m_flags;

  /** number of reserved free extents for ongoing operations like B-tree
  page split */
  uint32_t m_n_reserved_extents;

  /** this is positive when flushing the tablespace to disk; dropping of
  the tablespace is forbidden if this is positive */
  uint32_t m_n_pending_flushes;

  /** Hash chain node */
  hash_node_t m_hash;

  /** hash chain the name_hash table */
  hash_node_t m_name_hash;

  /** latch protecting the file space storage allocation */
  rw_lock_t m_latch;

  /** list of spaces with at least one unflushed file we have written to */
  UT_LIST_NODE_T(fil_space_t) m_unflushed_spaces;

  /** true if this space is currently in unflushed_spaces */
  bool m_is_in_unflushed_spaces;

  /** list of all spaces */
  UT_LIST_NODE_T(fil_space_t) m_space_list;

  /** FIL_SPACE_MAGIC_N */
  uint32_t m_magic_n;
};
