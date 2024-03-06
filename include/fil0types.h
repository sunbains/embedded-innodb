#pragma once

#include "innodb0types.h"

struct fil_space_t;
using fil_faddr_t = byte;

/** File space address
An address stored in a file page is a string of bytes */
struct fil_addr_t {
  /** page number within a space */
  page_no_t page;

  /** byte offset within the page */
  ulint boffset;
};

/** The number of fsyncs done to the log */
extern ulint fil_n_log_flushes;

/** Number of pending redo log flushes */
extern ulint fil_n_pending_log_flushes;

/** Number of pending tablespace flushes */
extern ulint fil_n_pending_tablespace_flushes;

/** When program is run, the default directory "." is the current datadir,
but in ibbackup we must set it explicitly; the path must NOT contain the
trailing '/' or '' */
extern const char *fil_path_to_client_datadir;

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

/** Space types @{ */
/** tablespace */
constexpr ulint FIL_TABLESPACE = 501;

/** redo log */
constexpr ulint FIL_LOG = 502;
/* @} */
