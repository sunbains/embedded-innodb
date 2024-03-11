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

/** @file fil/fil0fil.c
The tablespace memory cache

Created 10/25/1995 Heikki Tuuri
*******************************************************/

#include "fil0fil.h"

#include "buf0buf.h"
#include "buf0flu.h"
#include "buf0lru.h"
#include "dict0dict.h"
#include "fsp0fsp.h"
#include "hash0hash.h"
#include "log0recv.h"
#include "mach0data.h"
#include "mem0mem.h"
#include "mtr0log.h"
#include "mtr0mtr.h"
#include "os0file.h"
#include "os0sync.h"
#include "page0page.h"
#include "srv0srv.h"
#include "srv0start.h"
#include "sync0sync.h"

/*
                IMPLEMENTATION OF THE TABLESPACE MEMORY CACHE
                =============================================

The tablespace cache is responsible for providing fast read/write access to
tablespaces and logs of the database. File creation and deletion is done
in other modules which know more of the logic of the operation, however.

A tablespace consists of a chain of files. The size of the files does not
have to be divisible by the database block size, because we may just leave
the last incomplete block unused. When a new file is appended to the
tablespace, the maximum size of the file is also specified. At the moment,
we think that it is best to extend the file to its maximum size already at
the creation of the file, because then we can avoid dynamically extending
the file when more space is needed for the tablespace.

A block's position in the tablespace is specified with a 32-bit unsigned
integer. The files in the chain are thought to be catenated, and the block
corresponding to an address n is the nth block in the catenated file (where
the first block is named the 0th block, and the incomplete block fragments
at the end of files are not taken into account). A tablespace can be extended
by appending a new file at the end of the chain.

Our tablespace concept is similar to the one of Oracle.

To acquire more speed in disk transfers, a technique called disk striping is
sometimes used. This means that logical block addresses are divided in a
round-robin fashion across several disks. Windows NT supports disk striping,
so there we do not need to support it in the database. Disk striping is
implemented in hardware in RAID disks. We conclude that it is not necessary
to implement it in the database. Oracle 7 does not support disk striping,
either.

Another trick used at some database sites is replacing tablespace files by
raw disks, that is, the whole physical disk drive, or a partition of it, is
opened as a single file, and it is accessed through byte offsets calculated
from the start of the disk or the partition. This is recommended in some
books on database tuning to achieve more speed in i/o. Using raw disk
certainly prevents the OS from fragmenting disk space, but it is not clear
if it really adds speed. We measured on the Pentium 100 MHz + NT + NTFS file
system + EIDE Conner disk only a negligible difference in speed when reading
from a file, versus reading from a raw disk.

To have fast access to a tablespace or a log file, we put the data structures
to a hash table. Each tablespace and log file is given an unique 32-bit
identifier.

Some operating systems do not support many open files at the same time,
though NT seems to tolerate at least 900 open files. Therefore, we put the
open files in an LRU-list. If we need to open another file, we may close the
file at the end of the LRU-list. When an i/o-operation is pending on a file,
the file cannot be closed. We take the file nodes with pending i/o-operations
out of the LRU-list and keep a count of pending operations. When an operation
completes, we decrement the count and return the file node to the LRU-list if
the count drops to zero. */

/** The number of fsyncs done to the log */
ulint fil_n_log_flushes = 0;

/** Number of pending redo log flushes */
ulint fil_n_pending_log_flushes = 0;
/** Number of pending tablespace flushes */
ulint fil_n_pending_tablespace_flushes = 0;

/** The null file address */
const fil_addr_t fil_addr_null = {FIL_NULL, 0};

/** File node of a tablespace or the log data space */
struct fil_node_t {
  /** backpointer to the space where this node belongs */
  fil_space_t *space;

  /** path to the file */
  char *name;

  /** true if file open */
  bool open;

  /** OS handle to the file, if file open */
  os_file_t handle;

  /** true if the 'file' is actually a raw device or a raw
  disk partition */
  bool is_raw_disk;

  /** size of the file in database pages, 0 if not known yet;
  the possible last incomplete megabyte may be ignored if space == 0 */
  ulint size;

  /** count of pending i/o's on this file; closing of the file is not
  allowed if this is > 0 */
  ulint n_pending;

  /** count of pending flushes on this file; closing of the file
  is not allowed if this is > 0 */
  ulint n_pending_flushes;

  /** when we write to the file we increment this by one */
  int64_t modification_counter;

  /** Up to what modification_counter value we have flushed the
  modifications to disk */
  int64_t flush_counter;

  /** Link field for the file chain */
  UT_LIST_NODE_T(fil_node_t) chain;

  /** Link field for the LRU list */
  UT_LIST_NODE_T(fil_node_t) LRU;

  /** FIL_NODE_MAGIC_N */
  ulint magic_n;
};

/** Value of fil_node_t::magic_n */
constexpr ulint FIL_NODE_MAGIC_N = 89389;

/** Tablespace or log data space: let us call them by a common name space */
struct fil_space_t {
  /** space name = the path to the first file in it */
  char *name;

  /** space id */
  ulint id;

  /** in DISCARD/IMPORT this timestamp is used to check if
  we should ignore an insert buffer merge request for a page
  because it actually was for the previous incarnation of the space */
  int64_t tablespace_version;

  /** this is set to true at database startup if the space corresponds
  to a table in the InnoDB data dictionary; so we can print a warning
  of orphaned tablespaces */
  bool mark;

  /** true if we want to rename the .ibd file of tablespace and want to
  stop temporarily posting of new i/o requests on the file */
  bool stop_ios;

  /** this is set to true when we start deleting a single-table tablespace
  and its file; when this flag is set no further i/o or flush requests can
  be placed on this space, though there may be such requests still being
  processed on this space */
  bool is_being_deleted;

  /** FIL_TABLESPACE, FIL_LOG, or FIL_ARCH_LOG */
  ulint purpose;

  /** base node for the file chain */
  UT_LIST_BASE_NODE_T(fil_node_t, chain) chain;

  /** space size in pages; 0 if a single-table tablespace whose size we do
  not know yet; last incomplete megabytes in data files may be ignored if
  space == 0 */
  ulint size;

  /** compressed page size and file format, or 0 */
  ulint flags;

  /** number of reserved free extents for ongoing operations like B-tree
  page split */
  ulint n_reserved_extents;

  /** this is positive when flushing the tablespace to disk; dropping of
  the tablespace is forbidden if this is positive */
  ulint n_pending_flushes;

  /** hash chain node */
  hash_node_t hash;

  /** hash chain the name_hash table */
  hash_node_t name_hash;

  /** latch protecting the file space storage allocation */
  rw_lock_t latch;

  /** list of spaces with at least one unflushed file we have written to */
  UT_LIST_NODE_T(fil_space_t) unflushed_spaces;

  /** true if this space is currently in unflushed_spaces */
  bool is_in_unflushed_spaces;

  /** list of all spaces */
  UT_LIST_NODE_T(fil_space_t) space_list;

  /** FIL_SPACE_MAGIC_N */
  ulint magic_n;
};

/** Value of fil_space_t::magic_n */
constexpr ulint FIL_SPACE_MAGIC_N = 89472;

/** The tablespace memory cache */
struct fil_system_t;

/** The tablespace memory cache; also the totality of logs (the log
data space) is stored here; below we talk about tablespaces, but also
the ib_logfiles form a 'space' and it is handled here */
struct fil_system_t {
  /** The mutex protecting the cache */
  mutex_t mutex;

  /** The hash table of spaces in the system; they are hashed on the space id */
  hash_table_t *spaces;

  /** hash table based on the space name */
  hash_table_t *name_hash;

  /** base node for the list of those tablespaces whose files contain unflushed
  writes; those spaces have at least one file node where modification_counter
  > flush_counter */
  UT_LIST_BASE_NODE_T(fil_space_t, unflushed_spaces) unflushed_spaces;

  /** number of files currently open */
  ulint n_open;

  /** n_open is not allowed to exceed this */
  ulint max_n_open;

  /** when we write to a file we increment this by one */
  int64_t modification_counter;

  /** maximum space id in the existing tables, or assigned during the time the
  server has been up; at an InnoDB startup we scan the data dictionary and set
  here the maximum of the space id's of the tables there */
  ulint max_assigned_id;

  /** A counter which is incremented for every space object memory creation;
  every space mem object gets a 'timestamp' from this; in DISCARD/ IMPORT
  this is used to check if we should ignore an insert buffer merge request */
  int64_t tablespace_version;

  /** List of all file spaces */
  UT_LIST_BASE_NODE_T(fil_space_t, space_list) space_list;
};

/** The tablespace memory cache. This variable is nullptr before the module is
initialized. */
static fil_system_t *fil_system = nullptr;

/** Frees a space object from the tablespace memory cache. Closes the files in
the chain but does not delete them. There must not be any pending i/o's or
flushes on the files.
@return	true if success */
static bool fil_space_free(
  ulint id, /** in: space id */
  bool own_mutex
); /** in: true if own
                                             fil_system->mutex */

/** Remove extraneous '.' && '\' && '/' characters from the prefix.
Note: Currently it will not handle paths like: ../a/b.
@return	pointer to normalized path */
static const char *fil_normalize_path(const char *ptr) /** in: path to normalize */
{
  if (*ptr == '.' && *(ptr + 1) == SRV_PATH_SEPARATOR) {

    /* Skip ./ */
    ptr += 2;

    /* Skip any subsequent slashes. */
    while (*ptr == SRV_PATH_SEPARATOR) {
      ++ptr;
    }
  }

  return ptr;
}

/** Compare two table names. It will compare the two table names using the
canonical names. e.g., ./a/b == a/b. TODO: /path/to/a/b == a/b if both
/path/to/a/b and a/b refer to the same file.
@return	 = 0 if name1 == name2 < 0 if name1 < name2 > 0 if name1 > name2 */
static int fil_tablename_compare(
  const char *name1, /** in: table name to compare */
  const char *name2
) /** in: table name to compare */
{
  name1 = fil_normalize_path(name1);
  name2 = fil_normalize_path(name2);

  return strcmp(name1, name2);
}

/** NOTE: you must call fil_mutex_enter_and_prepare_for_io() first!

Prepares a file node for i/o. Opens the file if it is closed. Updates the
pending i/o's field in the node and the system appropriately. Takes the node
off the LRU list if it is in the LRU list. The caller must hold the fil_sys
mutex. */
static void fil_node_prepare_for_io(
  fil_node_t *node,     /** in: file node */
  fil_system_t *system, /** in: tablespace memory cache */
  fil_space_t *space
); /** in: space */

/** Updates the data structures when an i/o operation finishes. Updates the
pending i/o's field in the node appropriately. */
static void fil_node_complete_io(
  fil_node_t *node,     /** in: file node */
  fil_system_t *system, /** in: tablespace memory cache */
  ulint type
); /** in: OS_FILE_WRITE or OS_FILE_READ; marks
                                  the node as modified if
                                  type == OS_FILE_WRITE */

/** Checks if a single-table tablespace for a given table name exists in the
tablespace memory cache.
@return	space id, ULINT_UNDEFINED if not found */
static ulint fil_get_space_id_for_table(const char *name); /** in: table name in the standard
                       'databasename/tablename' format */

/** Frees a space object from the tablespace memory cache. Closes the files in
the chain but does not delete them. There must not be any pending i/o's or
flushes on the files. */
static bool fil_space_free(
  /* out: true if success */
  ulint id, /* in: space id */
  bool own_mutex
); /* in: true if own system->mutex */

void fil_var_init() {
  fil_system = nullptr;
  fil_n_log_flushes = 0;
  fil_n_pending_log_flushes = 0;
  fil_n_pending_tablespace_flushes = 0;
}

/** Reads data from a space to a buffer. Remember that the possible incomplete
blocks at the end of file are ignored: they are not taken into account when
calculating the byte offset within a space.
@return DB_SUCCESS, or DB_TABLESPACE_DELETED if we are trying to do
i/o on a tablespace which does not exist */
static ulint fil_read(
  bool sync,                 /** in: true if synchronous aio is desired */
  ulint space_id,            /** in: space id */
  ulint, ulint block_offset, /** in: offset in number of blocks */
  ulint byte_offset,         /** in: remainder of offset in bytes; in aio
                            this must be divisible by the OS block size */
  ulint len,                 /** in: how many bytes to read; this must not
                            cross a file boundary; in aio this must be a
                            block size multiple */
  void *buf,                 /** in/out: buffer where to store data read;
                            in aio this must be appropriately aligned */
  void *message
) /** in: message for aio handler if non-sync
                            aio used, else ignored */
{
  return fil_io(OS_FILE_READ, sync, space_id, block_offset, byte_offset, len, buf, message);
}

/** Writes data to a space from a buffer. Remember that the possible incomplete
blocks at the end of file are ignored: they are not taken into account when
calculating the byte offset within a space.
@return DB_SUCCESS, or DB_TABLESPACE_DELETED if we are trying to do
i/o on a tablespace which does not exist */
static db_err fil_write(
  bool sync,                 /** in: true if synchronous aio is desired */
  ulint space_id,            /** in: space id */
  ulint, ulint block_offset, /** in: offset in number of blocks */
  ulint byte_offset,         /** in: remainder of offset in bytes; in aio
                             this must be divisible by the OS block size */
  ulint len,                 /** in: how many bytes to write; this must
                             not cross a file boundary; in aio this must
                             be a block size multiple */
  void *buf,                 /** in: buffer from which to write; in aio
                             this must be appropriately aligned */
  void *message
) /** in: message for aio handler if non-sync
                             aio used, else ignored */
{
  return fil_io(OS_FILE_WRITE, sync, space_id, block_offset, byte_offset, len, buf, message);
}

/** Returns the table space by a given id, nullptr if not found. */
static fil_space_t *fil_space_get_by_id(ulint id) /** in: space id */
{
  fil_space_t *space;

  ut_ad(mutex_own(&fil_system->mutex));

  HASH_SEARCH(hash, fil_system->spaces, id, fil_space_t *, space, ut_ad(space->magic_n == FIL_SPACE_MAGIC_N), space->id == id);

  return space;
}

/** Returns the table space by a given name, nullptr if not found. */
static fil_space_t *fil_space_get_by_name(const char *name) /** in: space name */
{
  fil_space_t *space;
  ulint fold;

  ut_ad(mutex_own(&fil_system->mutex));

  fold = ut_fold_string(name);

  HASH_SEARCH(
    name_hash,
    fil_system->name_hash,
    fold,
    fil_space_t *,
    space,
    ut_ad(space->magic_n == FIL_SPACE_MAGIC_N),
    !fil_tablename_compare(name, space->name)
  );

  return space;
}

int64_t fil_space_get_version(ulint id) {
  fil_space_t *space;
  int64_t version = -1;

  ut_ad(fil_system);

  mutex_enter(&fil_system->mutex);

  space = fil_space_get_by_id(id);

  if (space) {
    version = space->tablespace_version;
  }

  mutex_exit(&fil_system->mutex);

  return version;
}

rw_lock_t *fil_space_get_latch(ulint id) {
  ut_ad(fil_system != nullptr);

  mutex_enter(&fil_system->mutex);

  auto space = fil_space_get_by_id(id);
  ut_a(space != nullptr);

  mutex_exit(&fil_system->mutex);

  return &space->latch;
}

ulint fil_space_get_type(ulint id) {
  fil_space_t *space;

  ut_ad(fil_system);

  mutex_enter(&fil_system->mutex);

  space = fil_space_get_by_id(id);

  ut_a(space);

  mutex_exit(&fil_system->mutex);

  return space->purpose;
}

/** Checks if all the file nodes in a space are flushed. The caller must hold
the fil_system mutex.
@return	true if all are flushed */
static bool fil_space_is_flushed(fil_space_t *space) /** in: space */
{
  fil_node_t *node;

  ut_ad(mutex_own(&fil_system->mutex));

  node = UT_LIST_GET_FIRST(space->chain);

  while (node) {
    if (node->modification_counter > node->flush_counter) {

      return false;
    }

    node = UT_LIST_GET_NEXT(chain, node);
  }

  return true;
}

void fil_node_create(const char *name, ulint size, ulint id, bool is_raw) {
  fil_space_t *space;

  ut_a(fil_system);
  ut_a(name);

  mutex_enter(&fil_system->mutex);

  auto node = (fil_node_t *)mem_alloc(sizeof(fil_node_t));

  node->name = mem_strdup(name);
  node->open = false;

  ut_a(!is_raw || srv_start_raw_disk_in_use);

  node->is_raw_disk = is_raw;
  node->size = size;
  node->magic_n = FIL_NODE_MAGIC_N;
  node->n_pending = 0;
  node->n_pending_flushes = 0;

  node->modification_counter = 0;
  node->flush_counter = 0;

  space = fil_space_get_by_id(id);

  if (!space) {
    ut_print_timestamp(ib_stream);
    ib_logger(
      ib_stream,
      "  Error: Could not find tablespace %lu for\n"
      "file ",
      (ulong)id
    );
    ut_print_filename(ib_stream, name);
    ib_logger(ib_stream, " in the tablespace memory cache.\n");
    mem_free(node->name);

    mem_free(node);

    mutex_exit(&fil_system->mutex);

    return;
  }

  space->size += size;

  node->space = space;

  UT_LIST_ADD_LAST(space->chain, node);

  if (id < SRV_LOG_SPACE_FIRST_ID && fil_system->max_assigned_id < id) {

    fil_system->max_assigned_id = id;
  }

  mutex_exit(&fil_system->mutex);
}

/** Opens a the file of a node of a tablespace. The caller must own the
fil_system mutex. */
static void fil_node_open_file(
  fil_node_t *node,     /** in: file node */
  fil_system_t *system, /** in: tablespace memory cache */
  fil_space_t *space
) /** in: space */
{
  ulint size_low;
  ulint size_high;
  bool ret;
  bool success;
  byte *page;
  ulint space_id;
  ulint flags;

  ut_ad(mutex_own(&(system->mutex)));
  ut_a(node->n_pending == 0);
  ut_a(node->open == false);

  if (node->size == 0) {
    /* It must be a single-table tablespace and we do not know the
    size of the file yet. First we open the file in the normal
    mode, no async I/O here, for simplicity. Then do some checks,
    and close the file again.
    NOTE that we could not use the simple file read function
    os_file_read() in Windows to read from a file opened for
    async I/O! */

    node->handle = os_file_create_simple_no_error_handling(node->name, OS_FILE_OPEN, OS_FILE_READ_ONLY, &success);
    if (!success) {
      /* The following call prints an error message */
      os_file_get_last_error(true);

      ut_print_timestamp(ib_stream);

      ib_logger(
        ib_stream,
        "  Fatal error: cannot open %s\n."
        "Have you deleted .ibd files"
        " under a running server?\n",
        node->name
      );
      ut_a(0);
    }

    os_file_get_size(node->handle, &size_low, &size_high);

    ut_a(space->id != 0);
    ut_a(space->purpose != FIL_LOG);

    auto size_bytes = (((off_t)size_high) << 32) + (off_t)size_low;

    if (size_bytes < off_t(FIL_IBD_FILE_INITIAL_SIZE * UNIV_PAGE_SIZE)) {
      ib_logger(
        ib_stream,
        "Error: the size of single-table"
        " tablespace file %s\n"
        "is only %lu %lu,"
        " should be at least %lu!\n",
        node->name,
        (ulong)size_high,
        (ulong)size_low,
        (ulong)(FIL_IBD_FILE_INITIAL_SIZE * UNIV_PAGE_SIZE)
      );

      ut_a(0);
    }

    /* Read the first page of the tablespace */

    auto buf2 = (byte *)ut_malloc(2 * UNIV_PAGE_SIZE);
    /* Align the memory for file i/o if we might have O_DIRECT set */
    page = (page_t *)ut_align(buf2, UNIV_PAGE_SIZE);

    success = os_file_read(node->handle, page, 0, 0, UNIV_PAGE_SIZE);
    space_id = fsp_header_get_space_id(page);
    flags = fsp_header_get_flags(page);

    ut_free(buf2);

    /* Close the file now that we have read the space id from it */

    os_file_close(node->handle);

    if (unlikely(space_id != space->id)) {
      ib_logger(
        ib_stream,
        "Error: tablespace id is %lu"
        " in the data dictionary\n"
        "but in file %s it is %lu!\n",
        space->id,
        node->name,
        space_id
      );

      ut_error;
    }

    if (unlikely(space_id == ULINT_UNDEFINED || space_id == 0)) {
      ib_logger(
        ib_stream,
        "Error: tablespace id %lu"
        " in file %s is not sensible\n",
        (ulong)space_id,
        node->name
      );

      ut_error;
    }

    if (unlikely(space->flags != flags)) {
      ib_logger(
        ib_stream,
        "Error: table flags are %lx"
        " in the data dictionary\n"
        "but the flags in file %s are %lx!\n",
        space->flags,
        node->name,
        flags
      );

      ut_error;
    }

    if (size_bytes >= 1024 * 1024) {
      /* Truncate the size to whole megabytes. */
      size_bytes = ut_2pow_round(size_bytes, 1024 * 1024);
    }

    node->size = (ulint)(size_bytes / UNIV_PAGE_SIZE);

    space->size += node->size;
  }

  /* printf("Opening file %s\n", node->name); */

  /* Open the file for reading and writing, in Windows normally in the
  unbuffered async I/O mode, though global variables may make
  os_file_create() to fall back to the normal file I/O mode. */

  if (space->purpose == FIL_LOG) {
    node->handle = os_file_create(node->name, OS_FILE_OPEN, OS_FILE_AIO, OS_LOG_FILE, &ret);
  } else if (node->is_raw_disk) {
    node->handle = os_file_create(node->name, OS_FILE_OPEN_RAW, OS_FILE_AIO, OS_DATA_FILE, &ret);
  } else {
    node->handle = os_file_create(node->name, OS_FILE_OPEN, OS_FILE_AIO, OS_DATA_FILE, &ret);
  }

  ut_a(ret);

  node->open = true;

  system->n_open++;

}

/** Closes a file. */
static void fil_node_close_file(
  fil_node_t *node, /** in: file node */
  fil_system_t *system
) /** in: tablespace memory cache */
{
  bool ret;

  ut_ad(node && system);
  ut_ad(mutex_own(&(system->mutex)));
  ut_a(node->open);
  ut_a(node->n_pending == 0);
  ut_a(node->n_pending_flushes == 0);
  ut_a(node->modification_counter == node->flush_counter);

  ret = os_file_close(node->handle);
  ut_a(ret);

  /* printf("Closing file %s\n", node->name); */

  node->open = false;
  ut_a(system->n_open > 0);
  system->n_open--;
}

/** Reserves the fil_system mutex and tries to make sure we can open at least
one file while holding it. This should be called before calling
fil_node_prepare_for_io(), because that function may need to open a file. */
static void fil_mutex_enter_and_prepare_for_io(ulint space_id) /** in: space id */
{
  fil_space_t *space;
  ulint count = 0;
  ulint count2 = 0;

retry:
  mutex_enter(&fil_system->mutex);

  if (space_id == 0 || space_id >= SRV_LOG_SPACE_FIRST_ID) {
    /* We keep log files and system tablespace files always open;
    this is important in preventing deadlocks in this module, as
    a page read completion often performs another read from the
    insert buffer. The insert buffer is in tablespace 0, and we
    cannot end up waiting in this function. */

    return;
  }

  if (fil_system->n_open < fil_system->max_n_open) {

    return;
  }

  space = fil_space_get_by_id(space_id);

  if (space != nullptr && space->stop_ios) {
    /* We are going to do a rename file and want to stop new i/o's
    for a while */

    if (count2 > 20000) {
      ib_logger(ib_stream, "Warning: tablespace ");
      ut_print_filename(ib_stream, space->name);
      ib_logger(ib_stream, " has i/o ops stopped for a long time %lu\n", (ulong)count2);
    }

    mutex_exit(&fil_system->mutex);

    os_thread_sleep(20000);

    count2++;

    goto retry;
  }

  /* If the file is already open, no need to do anything; if the space
  does not exist, we handle the situation in the function which called
  this function */

  if (!space || UT_LIST_GET_FIRST(space->chain)->open) {

    return;
  }

  if (fil_system->n_open < fil_system->max_n_open) {
    /* Ok */

    return;
  }

  if (count >= 2) {
    ut_print_timestamp(ib_stream);
    ib_logger(
      ib_stream,
      "  Warning: too many (%lu) files stay open"
      " while the maximum\n"
      "allowed value would be %lu.\n"
      "You may need to raise the value of"
      " max_files_open\n",
      (ulong)fil_system->n_open,
      (ulong)fil_system->max_n_open
    );

    return;
  }

  mutex_exit(&fil_system->mutex);

  /* Wake the i/o-handler threads to make sure pending i/o's are performed */
  os_aio_simulated_wake_handler_threads();

  os_thread_sleep(20000);

  /* Flush tablespaces so that we can close modified files in the LRU list */

  fil_flush_file_spaces(FIL_TABLESPACE);

  count++;

  goto retry;
}

/** Frees a file node object from a tablespace memory cache. */
static void fil_node_free(
  fil_node_t *node,     /** in, own: file node */
  fil_system_t *system, /** in: tablespace memory cache */
  fil_space_t *space
) /** in: space where the file node is chained */
{
  ut_ad(node && system && space);
  ut_ad(mutex_own(&(system->mutex)));
  ut_a(node->magic_n == FIL_NODE_MAGIC_N);
  ut_a(node->n_pending == 0);

  if (node->open) {
    /* We fool the assertion in fil_node_close_file() to think
    there are no unflushed modifications in the file */

    node->modification_counter = node->flush_counter;

    if (space->is_in_unflushed_spaces && fil_space_is_flushed(space)) {

      space->is_in_unflushed_spaces = false;

      UT_LIST_REMOVE(system->unflushed_spaces, space);
    }

    fil_node_close_file(node, system);
  }

  space->size -= node->size;

  UT_LIST_REMOVE(space->chain, node);

  mem_free(node->name);
  mem_free(node);
}

bool fil_space_create(const char *name, ulint id, ulint flags, ulint purpose) {
  fil_space_t *space;

  /* The tablespace flags (FSP_SPACE_FLAGS) should be 0 for
  ROW_FORMAT=COMPACT
  ((table->flags & ~(~0UL << DICT_TF_BITS)) == DICT_TF_COMPACT) and
  ROW_FORMAT=REDUNDANT (table->flags == 0).  For any other
  format, the tablespace flags should equal
  (table->flags & ~(~0UL << DICT_TF_BITS)). */
  ut_a(flags != DICT_TF_COMPACT);
  ut_a(!(flags & (~0UL << DICT_TF_BITS)));

try_again:
  /*printf(
  "Adding tablespace %lu of name %s, purpose %lu\n", id, name,
  purpose);*/

  ut_a(fil_system);
  ut_a(name);

  mutex_enter(&fil_system->mutex);

  space = fil_space_get_by_name(name);

  if (likely_null(space)) {
    ulint namesake_id;

    ut_print_timestamp(ib_stream);
    ib_logger(
      ib_stream,
      "  Warning: trying to init to the"
      " tablespace memory cache\n"
      "a tablespace %lu of name ",
      (ulong)id
    );
    ut_print_filename(ib_stream, name);
    ib_logger(
      ib_stream,
      ",\n"
      "but a tablespace %lu of the same name\n"
      "already exists in the"
      " tablespace memory cache!\n",
      (ulong)space->id
    );

    if (id == 0 || purpose != FIL_TABLESPACE) {

      mutex_exit(&fil_system->mutex);

      return false;
    }

    ib_logger(
      ib_stream,
      "We assume that InnoDB did a crash recovery,"
      " and you had\n"
      "an .ibd file for which the table"
      " did not exist in the\n"
      "InnoDB internal data dictionary in the"
      " ibdata files.\n"
      "We assume that you later removed the"
      " .ibd file,\n"
      "and are now trying to recreate the table."
      " We now remove the\n"
      "conflicting tablespace object"
      " from the memory cache and try\n"
      "the init again.\n"
    );

    namesake_id = space->id;

    mutex_exit(&fil_system->mutex);

    fil_space_free(namesake_id, false);

    goto try_again;
  }

  space = fil_space_get_by_id(id);

  if (likely_null(space)) {
    ib_logger(
      ib_stream,
      "Error: trying to add tablespace %lu"
      " of name ",
      (ulong)id
    );
    ut_print_filename(ib_stream, name);
    ib_logger(
      ib_stream,
      "\n"
      "to the tablespace memory cache,"
      " but tablespace\n"
      "%lu of name ",
      (ulong)space->id
    );
    ut_print_filename(ib_stream, space->name);
    ib_logger(
      ib_stream,
      " already exists in the tablespace\n"
      "memory cache!\n"
    );

    mutex_exit(&fil_system->mutex);

    return false;
  }

  space = (fil_space_t *)mem_alloc(sizeof(fil_space_t));

  space->name = mem_strdup(name);
  space->id = id;

  fil_system->tablespace_version++;
  space->tablespace_version = fil_system->tablespace_version;
  space->mark = false;

  if (purpose == FIL_TABLESPACE && id > fil_system->max_assigned_id) {
    fil_system->max_assigned_id = id;
  }

  space->stop_ios = false;
  space->is_being_deleted = false;
  space->purpose = purpose;
  space->size = 0;
  space->flags = flags;

  space->n_reserved_extents = 0;

  space->n_pending_flushes = 0;

  UT_LIST_INIT(space->chain);
  space->magic_n = FIL_SPACE_MAGIC_N;

  rw_lock_create(&space->latch, SYNC_FSP);

  HASH_INSERT(fil_space_t, hash, fil_system->spaces, id, space);

  HASH_INSERT(fil_space_t, name_hash, fil_system->name_hash, ut_fold_string(name), space);
  space->is_in_unflushed_spaces = false;

  UT_LIST_ADD_LAST(fil_system->space_list, space);

  mutex_exit(&fil_system->mutex);

  return true;
}

/** Assigns a new space id for a new single-table tablespace. This works simply
by incrementing the global counter. If 4 billion id's is not enough, we may need
to recycle id's.
@return	new tablespace id; ULINT_UNDEFINED if could not assign an id */
static ulint fil_assign_new_space_id(void) {
  ulint id;

  mutex_enter(&fil_system->mutex);

  fil_system->max_assigned_id++;

  id = fil_system->max_assigned_id;

  if (id > (SRV_LOG_SPACE_FIRST_ID / 2) && (id % 1000000UL == 0)) {
    ut_print_timestamp(ib_stream);
    ib_logger(
      ib_stream,
      "Warning: you are running out of new"
      " single-table tablespace id's.\n"
      "Current counter is %lu and it"
      " must not exceed %lu!\n"
      "To reset the counter to zero"
      " you have to dump all your tables and\n"
      "recreate the whole InnoDB installation.\n",
      (ulong)id,
      (ulong)SRV_LOG_SPACE_FIRST_ID
    );
  }

  if (id >= SRV_LOG_SPACE_FIRST_ID) {
    ut_print_timestamp(ib_stream);
    ib_logger(
      ib_stream,
      "You have run out of single-table"
      " tablespace id's!\n"
      "Current counter is %lu.\n"
      "To reset the counter to zero you"
      " have to dump all your tables and\n"
      "recreate the whole InnoDB installation.\n",
      (ulong)id
    );
    fil_system->max_assigned_id--;

    id = ULINT_UNDEFINED;
  }

  mutex_exit(&fil_system->mutex);

  return id;
}

/** Frees a space object from the tablespace memory cache. Closes the files in
the chain but does not delete them. There must not be any pending i/o's or
flushes on the files.
@return	true if success */
static bool fil_space_free(
  /* out: true if success */
  ulint id, /* in: space id */
  bool own_mutex
) /* in: true if own system->mutex */
{
  fil_space_t *space;
  fil_node_t *fil_node;
  fil_space_t *fil_namespace;

  if (!own_mutex) {
    mutex_enter(&fil_system->mutex);
  }

  space = fil_space_get_by_id(id);

  if (!space) {
    ut_print_timestamp(ib_stream);
    ib_logger(
      ib_stream,
      "  Error: trying to remove tablespace %lu"
      " from the cache but\n"
      "it is not there.\n",
      (ulong)id
    );

    mutex_exit(&fil_system->mutex);

    return false;
  }

  HASH_DELETE(fil_space_t, hash, fil_system->spaces, id, space);

  fil_namespace = fil_space_get_by_name(space->name);
  ut_a(fil_namespace);
  ut_a(space == fil_namespace);

  HASH_DELETE(fil_space_t, name_hash, fil_system->name_hash, ut_fold_string(space->name), space);

  if (space->is_in_unflushed_spaces) {
    space->is_in_unflushed_spaces = false;

    UT_LIST_REMOVE(fil_system->unflushed_spaces, space);
  }

  UT_LIST_REMOVE(fil_system->space_list, space);

  ut_a(space->magic_n == FIL_SPACE_MAGIC_N);
  ut_a(0 == space->n_pending_flushes);

  fil_node = UT_LIST_GET_FIRST(space->chain);

  while (fil_node != nullptr) {
    fil_node_free(fil_node, fil_system, space);

    fil_node = UT_LIST_GET_FIRST(space->chain);
  }

  ut_a(0 == UT_LIST_GET_LEN(space->chain));

  if (!own_mutex) {
    mutex_exit(&fil_system->mutex);
  }

  rw_lock_free(&(space->latch));

  mem_free(space->name);
  mem_free(space);

  return true;
}

ulint fil_space_get_size(ulint id) {
  ut_ad(fil_system);

  fil_mutex_enter_and_prepare_for_io(id);

  auto space = fil_space_get_by_id(id);

  if (space == nullptr) {
    mutex_exit(&fil_system->mutex);

    return 0;
  }

  if (space->size == 0 && space->purpose == FIL_TABLESPACE) {
    ut_a(id != 0);

    ut_a(1 == UT_LIST_GET_LEN(space->chain));

    auto node = UT_LIST_GET_FIRST(space->chain);

    /* It must be a single-table tablespace and we have not opened
    the file yet; the following calls will open it and update the
    size fields */

    fil_node_prepare_for_io(node, fil_system, space);
    fil_node_complete_io(node, fil_system, OS_FILE_READ);
  }

  auto size = space->size;

  mutex_exit(&fil_system->mutex);

  return size;
}

ulint fil_space_get_flags(ulint id) {

  ut_ad(fil_system != nullptr);

  if (id == 0) {
    return 0;
  }

  fil_mutex_enter_and_prepare_for_io(id);

  auto space = fil_space_get_by_id(id);

  if (space == nullptr) {
    mutex_exit(&fil_system->mutex);

    return ULINT_UNDEFINED;
  }

  if (space->size == 0 && space->purpose == FIL_TABLESPACE) {
    ut_a(id != 0);

    ut_a(1 == UT_LIST_GET_LEN(space->chain));

    auto node = UT_LIST_GET_FIRST(space->chain);

    /* It must be a single-table tablespace and we have not opened
    the file yet; the following calls will open it and update the
    size fields */

    fil_node_prepare_for_io(node, fil_system, space);
    fil_node_complete_io(node, fil_system, OS_FILE_READ);
  }

  auto flags = space->flags;

  mutex_exit(&fil_system->mutex);

  return flags;
}

bool fil_check_adress_in_tablespace(ulint id, ulint page_no) {
  return fil_space_get_size(id) > page_no;
}

void fil_init(ulint hash_size, ulint max_n_open) {
  ut_a(fil_system == nullptr);

  ut_a(hash_size > 0);
  ut_a(max_n_open > 0);

  fil_system = (fil_system_t *)mem_alloc(sizeof(fil_system_t));

  mutex_create(&fil_system->mutex, SYNC_ANY_LATCH);

  fil_system->spaces = hash_create(hash_size);
  fil_system->name_hash = hash_create(hash_size);

  fil_system->n_open = 0;
  fil_system->max_n_open = max_n_open;

  fil_system->modification_counter = 0;
  fil_system->max_assigned_id = 0;

  fil_system->tablespace_version = 0;

  UT_LIST_INIT(fil_system->unflushed_spaces);
  UT_LIST_INIT(fil_system->space_list);
}

void fil_open_log_and_system_tablespace_files() {
  fil_space_t *space;
  fil_node_t *node;

  mutex_enter(&fil_system->mutex);

  space = UT_LIST_GET_FIRST(fil_system->space_list);

  while (space != nullptr) {
    if (space->purpose != FIL_TABLESPACE || space->id == 0) {
      node = UT_LIST_GET_FIRST(space->chain);

      while (node != nullptr) {
        if (!node->open) {
          fil_node_open_file(node, fil_system, space);
        }
        if (fil_system->max_n_open < 10 + fil_system->n_open) {
          ib_logger(
            ib_stream,
            "Warning: you must"
            " raise the value of"
            " max_open_files!\n"
            " Remember that"
            " InnoDB keeps all log files"
            " and all system\n"
            "tablespace files open"
            " for the whole time server is"
            " running, and\n"
            "needs to open also"
            " some .ibd files if the"
            " file-per-table storage\n"
            "model is used."
            " Current open files %lu,"
            " max allowed"
            " open files %lu.\n",
            (ulong)fil_system->n_open,
            (ulong)fil_system->max_n_open
          );
        }
        node = UT_LIST_GET_NEXT(chain, node);
      }
    }
    space = UT_LIST_GET_NEXT(space_list, space);
  }

  mutex_exit(&fil_system->mutex);
}

void fil_close_all_files() {
  fil_space_t *space;
  fil_node_t *node;

  /* If we decide to abort before this module has been initialized
  then we simply ignore the request. */
  if (fil_system == nullptr) {
    return;
  }

  mutex_enter(&fil_system->mutex);

  space = UT_LIST_GET_FIRST(fil_system->space_list);

  while (space != nullptr) {
    fil_space_t *prev_space = space;

    node = UT_LIST_GET_FIRST(space->chain);

    while (node != nullptr) {
      if (node->open) {
        fil_node_close_file(node, fil_system);
      }
      node = UT_LIST_GET_NEXT(chain, node);
    }
    space = UT_LIST_GET_NEXT(space_list, space);
    fil_space_free(prev_space->id, true);
  }

  mutex_exit(&fil_system->mutex);
}

void fil_set_max_space_id_if_bigger(ulint max_id) {
  if (max_id >= SRV_LOG_SPACE_FIRST_ID) {
    ib_logger(
      ib_stream,
      "Fatal error: max tablespace id"
      " is too high, %lu\n",
      (ulong)max_id
    );
    ut_error;
  }

  mutex_enter(&fil_system->mutex);

  if (fil_system->max_assigned_id < max_id) {

    fil_system->max_assigned_id = max_id;
  }

  mutex_exit(&fil_system->mutex);
}

/** Writes the flushed lsn and the latest archived log number to the page header
of the first page of a data file of the system tablespace (space 0),
which is uncompressed. */
static db_err fil_write_lsn_and_arch_no_to_file(
  ulint sum_of_sizes, /** in: combined size of previous files
                        in space, in database pages */
  lsn_t lsn
) /** in: lsn to write */
{
  auto buf1 = (byte *)mem_alloc(2 * UNIV_PAGE_SIZE);
  auto buf = (byte *)ut_align(buf1, UNIV_PAGE_SIZE);

  fil_read(true, 0, SYS_TABLESPACE, sum_of_sizes, 0, UNIV_PAGE_SIZE, buf, nullptr);

  mach_write_to_8(buf + FIL_PAGE_FILE_FLUSH_LSN, lsn);

  fil_write(true, 0, SYS_TABLESPACE, sum_of_sizes, 0, UNIV_PAGE_SIZE, buf, nullptr);

  mem_free(buf1);

  return DB_SUCCESS;
}

db_err fil_write_flushed_lsn_to_data_files(lsn_t lsn) {
  fil_node_t *node;
  ulint sum_of_sizes;

  mutex_enter(&fil_system->mutex);

  auto space = UT_LIST_GET_FIRST(fil_system->space_list);

  while (space) {
    /* We only write the lsn to all existing data files which have
    been open during the lifetime of the server process; they are
    represented by the space objects in the tablespace memory
    cache. Note that all data files in the system tablespace 0 are
    always open. */

    if (space->purpose == FIL_TABLESPACE && space->id == 0) {
      sum_of_sizes = 0;

      node = UT_LIST_GET_FIRST(space->chain);
      while (node) {
        mutex_exit(&fil_system->mutex);

        auto err = fil_write_lsn_and_arch_no_to_file(sum_of_sizes, lsn);

        if (err != DB_SUCCESS) {

          return err;
        }

        mutex_enter(&fil_system->mutex);

        sum_of_sizes += node->size;
        node = UT_LIST_GET_NEXT(chain, node);
      }
    }
    space = UT_LIST_GET_NEXT(space_list, space);
  }

  mutex_exit(&fil_system->mutex);

  return DB_SUCCESS;
}

void fil_read_flushed_lsn_and_arch_log_no(
  os_file_t data_file, bool one_read_already, lsn_t *min_flushed_lsn, /** in/out: */
  lsn_t *max_flushed_lsn
) /** in/out: */
{
  uint64_t flushed_lsn;
  auto buf2 = (byte *)ut_malloc(2 * UNIV_PAGE_SIZE);

  /* Align the memory for a possible read from a raw device */
  auto buf = (byte *)ut_align(buf2, UNIV_PAGE_SIZE);

  os_file_read(data_file, buf, 0, 0, UNIV_PAGE_SIZE);

  flushed_lsn = mach_read_from_8(buf + FIL_PAGE_FILE_FLUSH_LSN);

  ut_free(buf2);

  if (!one_read_already) {
    *min_flushed_lsn = flushed_lsn;
    *max_flushed_lsn = flushed_lsn;
    return;
  }

  if (*min_flushed_lsn > flushed_lsn) {
    *min_flushed_lsn = flushed_lsn;
  }
  if (*max_flushed_lsn < flushed_lsn) {
    *max_flushed_lsn = flushed_lsn;
  }
}

/** Creates the database directory for a table if it does not exist yet. */
static void fil_create_directory_for_tablename(const char *name) /** in: name in the standard
                      'databasename/tablename' format */
{
  const char *namend;

  auto len = strlen(srv_data_home);
  ut_a(len > 0);

  namend = strchr(name, '/');
  ut_a(namend);

  auto path = (char *)mem_alloc(len + (namend - name) + 2);

  strncpy(path, srv_data_home, len);
  ut_a(path[len - 1] == SRV_PATH_SEPARATOR);

  strncpy(path + len, name, namend - name);

  ut_a(os_file_create_directory(path, false));
  mem_free(path);
}

/** Writes a log record about an .ibd file create/rename/delete. */
static void fil_op_write_log(
  ulint type,           /** in: MLOG_FILE_CREATE,
                                       MLOG_FILE_CREATE2,
                                       MLOG_FILE_DELETE, or
                                       MLOG_FILE_RENAME */
  ulint space_id,       /** in: space id */
  ulint log_flags,      /** in: redo log flags (stored
                                       in the page number field) */
  ulint flags,          /** in: compressed page size
                                       and file format
                                       if type==MLOG_FILE_CREATE2, or 0 */
  const char *name,     /** in: table name in the familiar
                                       'databasename/tablename' format, or
                                       the file path in the case of
                                       MLOG_FILE_DELETE */
  const char *new_name, /** in: if type is MLOG_FILE_RENAME,
                                       the new table name in the
                                       'databasename/tablename' format */
  mtr_t *mtr
) /** in: mini-transaction handle */
{
  byte *log_ptr;
  ulint len;

  log_ptr = mlog_open(mtr, 11 + 2 + 1);

  if (!log_ptr) {
    /* Logging in mtr is switched off during crash recovery:
    in that case mlog_open returns nullptr */
    return;
  }

  log_ptr = mlog_write_initial_log_record_for_file_op(type, space_id, log_flags, log_ptr, mtr);

  if (type == MLOG_FILE_CREATE2) {
    mach_write_to_4(log_ptr, flags);
    log_ptr += 4;
  }
  /* Let us store the strings as null-terminated for easier readability
  and handling */

  len = strlen(name) + 1;

  mach_write_to_2(log_ptr, len);
  log_ptr += 2;
  mlog_close(mtr, log_ptr);

  mlog_catenate_string(mtr, (byte *)name, len);

  if (type == MLOG_FILE_RENAME) {
    len = strlen(new_name) + 1;
    log_ptr = mlog_open(mtr, 2 + len);
    ut_a(log_ptr);
    mach_write_to_2(log_ptr, len);
    log_ptr += 2;
    mlog_close(mtr, log_ptr);

    mlog_catenate_string(mtr, (byte *)new_name, len);
  }
}

byte *fil_op_log_parse_or_replay(byte *ptr, byte *end_ptr, ulint type, ulint space_id, ulint log_flags) {
  ulint name_len;
  ulint new_name_len;
  const char *name;
  const char *new_name = nullptr;
  ulint flags = 0;

  if (type == MLOG_FILE_CREATE2) {
    if (end_ptr < ptr + 4) {

      return nullptr;
    }

    flags = mach_read_from_4(ptr);
    ptr += 4;
  }

  if (end_ptr < ptr + 2) {

    return nullptr;
  }

  name_len = mach_read_from_2(ptr);

  ptr += 2;

  if (end_ptr < ptr + name_len) {

    return nullptr;
  }

  name = (const char *)ptr;

  ptr += name_len;

  if (type == MLOG_FILE_RENAME) {
    if (end_ptr < ptr + 2) {

      return nullptr;
    }

    new_name_len = mach_read_from_2(ptr);

    ptr += 2;

    if (end_ptr < ptr + new_name_len) {

      return nullptr;
    }

    new_name = (const char *)ptr;

    ptr += new_name_len;
  }

  /* We managed to parse a full log record body */
  if (!space_id) {

    return ptr;
  }

  /* Let us try to perform the file operation, if sensible. Note that
  ibbackup has at this stage already read in all space id info to the
  fil0fil.c data structures.

  NOTE that our algorithm is not guaranteed to work correctly if there
  were renames of tables during the backup. See ibbackup code for more
  on the problem. */

  switch (type) {
    case MLOG_FILE_DELETE:
      if (fil_tablespace_exists_in_mem(space_id)) {
        ut_a(fil_delete_tablespace(space_id));
      }

      break;

    case MLOG_FILE_RENAME:
      /* We do the rename based on space id, not old file name;
    this should guarantee that after the log replay each .ibd file
    has the correct name for the latest log sequence number; the
    proof is left as an exercise :) */

      if (fil_tablespace_exists_in_mem(space_id)) {
        /* Create the database directory for the new name, if
      it does not exist yet */
        fil_create_directory_for_tablename(new_name);

        /* Rename the table if there is not yet a tablespace
      with the same name */

        if (fil_get_space_id_for_table(new_name) == ULINT_UNDEFINED) {
          /* We do not care of the old name, that is
        why we pass nullptr as the first argument */
          if (!fil_rename_tablespace(nullptr, space_id, new_name)) {
            ut_error;
          }
        }
      }

      break;

    case MLOG_FILE_CREATE:
    case MLOG_FILE_CREATE2:
      if (fil_tablespace_exists_in_mem(space_id)) {
        /* Do nothing */
      } else if (fil_get_space_id_for_table(name) != ULINT_UNDEFINED) {
        /* Do nothing */
      } else if (log_flags & MLOG_FILE_FLAG_TEMP) {
        /* Temporary table, do nothing */
      } else {
        /* Create the database directory for name, if it does
      not exist yet */
        fil_create_directory_for_tablename(name);

        if (fil_create_new_single_table_tablespace(&space_id, name, false, flags, FIL_IBD_FILE_INITIAL_SIZE) != DB_SUCCESS) {
          ut_error;
        }
      }

      break;

    default:
      ut_error;
  }

  return ptr;
}

bool fil_delete_tablespace(ulint id) {
  bool success;
  fil_space_t *space;
  fil_node_t *node;
  char *path;

  ut_a(id != 0);

  ulint count{};

  for (;;) {
    mutex_enter(&fil_system->mutex);

    space = fil_space_get_by_id(id);

    if (space == nullptr) {
      ut_print_timestamp(ib_stream);
      ib_logger(
        ib_stream,
        "  Error: cannot delete tablespace %lu\n"
        "because it is not found in the"
        " tablespace memory cache.\n",
        (ulong)id
      );

      mutex_exit(&fil_system->mutex);

      return false;
    }

    space->is_being_deleted = true;

    ut_a(UT_LIST_GET_LEN(space->chain) == 1);
    node = UT_LIST_GET_FIRST(space->chain);

    if (space->n_pending_flushes == 0 && node->n_pending == 0) {
      break;
    }

    if (count > 1000) {
      ut_print_timestamp(ib_stream);
      ib_logger(
        ib_stream,
        "  Warning: trying to"
        " delete tablespace "
      );
      ut_print_filename(ib_stream, space->name);
      ib_logger(
        ib_stream,
        ",\n"
        "but there are %lu flushes"
        " and %lu pending i/o's on it\n"
        "Loop %lu.\n",
        (ulong)space->n_pending_flushes,
        (ulong)node->n_pending,
        (ulong)count
      );
    }

    mutex_exit(&fil_system->mutex);

    os_thread_sleep(20000);

    ++count;
  }

  path = mem_strdup(space->name);

  mutex_exit(&fil_system->mutex);

  /* Invalidate in the buffer pool all pages belonging to the tablespace.
  Since we have set space->is_being_deleted = true, readahead can no longer
  read more pages of this tablespace to the buffer pool. Thus we can clean
  the tablespace out of the buffer pool completely and permanently. The flag
  is_being_deleted also prevents fil_flush() from being applied to this
  tablespace. */

  buf_LRU_invalidate_tablespace(id);

  success = fil_space_free(id, false);

  if (success) {
    success = os_file_delete(path);

    if (!success) {
      success = os_file_delete_if_exists(path);
    }
  }

  if (success) {
    /* Write a log record about the deletion of the .ibd
    file, so that ibbackup can replay it in the
    --apply-log phase. We use a dummy mtr and the familiar
    log write mechanism. */
    mtr_t mtr;

    /* When replaying the operation in ibbackup, do not try
    to write any log record */
    mtr_start(&mtr);

    fil_op_write_log(MLOG_FILE_DELETE, id, 0, 0, path, nullptr, &mtr);
    mtr_commit(&mtr);
    mem_free(path);

    return true;
  }

  mem_free(path);

  return false;
}

bool fil_discard_tablespace(ulint id) {
  bool success;

  success = fil_delete_tablespace(id);

  if (!success) {
    ib_logger(
      ib_stream,
      "Warning: cannot delete tablespace %lu"
      " in DISCARD TABLESPACE.\n"
      "But let us remove the"
      " insert buffer entries for this tablespace.\n",
      (ulong)id
    );
  }

  return success;
}

/** Renames the memory cache structures of a single-table tablespace.
@return	true if success */
static bool fil_rename_tablespace_in_mem(
  fil_space_t *space, /** in: tablespace memory object */
  fil_node_t *node,   /** in: file node of that tablespace */
  const char *path
) /** in: new name */
{
  fil_space_t *space2;
  const char *old_name = space->name;

  ut_ad(mutex_own(&fil_system->mutex));

  space2 = fil_space_get_by_name(old_name);
  if (space != space2) {
    ib_logger(ib_stream, "Error: cannot find ");
    ut_print_filename(ib_stream, old_name);
    ib_logger(ib_stream, " in tablespace memory cache\n");

    return false;
  }

  space2 = fil_space_get_by_name(path);
  if (space2 != nullptr) {
    ib_logger(ib_stream, "Error: ");
    ut_print_filename(ib_stream, path);
    ib_logger(ib_stream, " is already in tablespace memory cache\n");

    return false;
  }

  HASH_DELETE(fil_space_t, name_hash, fil_system->name_hash, ut_fold_string(space->name), space);
  mem_free(space->name);
  mem_free(node->name);

  space->name = mem_strdup(path);
  node->name = mem_strdup(path);

  HASH_INSERT(fil_space_t, name_hash, fil_system->name_hash, ut_fold_string(path), space);
  return true;
}

/** Allocates a file name for a single-table tablespace. The string must be
freed by caller with mem_free().
@return	own: file name */
static char *fil_make_ibd_name(
  const char *name, /** in: table name or a dir path of a
                                    TEMPORARY table */
  bool is_temp
) /** in: true if it is a dir path */
{
  ulint namelen = strlen(name);
  auto dirlen = strlen(srv_data_home);
  auto sz = dirlen + namelen + sizeof("/.ibd");
  auto filename = (char *)mem_alloc(sz);

  ut_snprintf(filename, sz, "%s%s.ibd", fil_normalize_path(srv_data_home), name);

  return filename;
}

bool fil_rename_tablespace(const char *old_name, ulint id, const char *new_name) {
  bool success;
  fil_space_t *space;
  fil_node_t *node;
  ulint count = 0;
  char *path;
  bool old_name_was_specified = true;
  char *old_path;

  ut_a(id != 0);

  if (old_name == nullptr) {
    old_name = "(name not specified)";
    old_name_was_specified = false;
  }
retry:
  count++;

  if (count > 1000) {
    ut_print_timestamp(ib_stream);
    ib_logger(ib_stream, "  Warning: problems renaming ");
    ut_print_filename(ib_stream, old_name);
    ib_logger(ib_stream, " to ");
    ut_print_filename(ib_stream, new_name);
    ib_logger(ib_stream, ", %lu iterations\n", (ulong)count);
  }

  mutex_enter(&fil_system->mutex);

  space = fil_space_get_by_id(id);

  if (space == nullptr) {
    ib_logger(
      ib_stream,
      "Error: cannot find space id %lu"
      " in the tablespace memory cache\n"
      "though the table ",
      (ulong)id
    );
    ut_print_filename(ib_stream, old_name);
    ib_logger(ib_stream, " in a rename operation should have that id\n");
    mutex_exit(&fil_system->mutex);

    return false;
  }

  if (count > 25000) {
    space->stop_ios = false;
    mutex_exit(&fil_system->mutex);

    return false;
  }

  /* We temporarily close the .ibd file because we do not trust that
  operating systems can rename an open file. For the closing we have to
  wait until there are no pending i/o's or flushes on the file. */

  space->stop_ios = true;

  ut_a(UT_LIST_GET_LEN(space->chain) == 1);
  node = UT_LIST_GET_FIRST(space->chain);

  if (node->n_pending > 0 || node->n_pending_flushes > 0) {
    /* There are pending i/o's or flushes, sleep for a while and
    retry */

    mutex_exit(&fil_system->mutex);

    os_thread_sleep(20000);

    goto retry;

  } else if (node->modification_counter > node->flush_counter) {
    /* Flush the space */

    mutex_exit(&fil_system->mutex);

    os_thread_sleep(20000);

    fil_flush(id);

    goto retry;

  } else if (node->open) {
    /* Close the file */

    fil_node_close_file(node, fil_system);
  }

  /* Check that the old name in the space is right */

  if (old_name_was_specified) {
    old_path = fil_make_ibd_name(old_name, false);

    ut_a(fil_tablename_compare(space->name, old_path) == 0);
    ut_a(fil_tablename_compare(node->name, old_path) == 0);
  } else {
    old_path = mem_strdup(space->name);
  }

  /* Rename the tablespace and the node in the memory cache */
  path = fil_make_ibd_name(new_name, false);
  success = fil_rename_tablespace_in_mem(space, node, path);

  if (success) {
    success = os_file_rename(old_path, path);

    if (!success) {
      /* We have to revert the changes we made
      to the tablespace memory cache */

      ut_a(fil_rename_tablespace_in_mem(space, node, old_path));
    }
  }

  mem_free(path);
  mem_free(old_path);

  space->stop_ios = false;

  mutex_exit(&fil_system->mutex);

  if (success) {
    mtr_t mtr;

    mtr_start(&mtr);

    fil_op_write_log(MLOG_FILE_RENAME, id, 0, 0, old_name, new_name, &mtr);
    mtr_commit(&mtr);
  }

  return success;
}

db_err fil_create_new_single_table_tablespace(ulint *space_id, const char *tablename, bool is_temp, ulint flags, ulint size) {
  os_file_t file;
  bool ret;
  ulint err;
  bool success;
  char *path;

  ut_a(size >= FIL_IBD_FILE_INITIAL_SIZE);
  /* The tablespace flags (FSP_SPACE_FLAGS) should be 0 for
  ROW_FORMAT=COMPACT
  ((table->flags & ~(~0UL << DICT_TF_BITS)) == DICT_TF_COMPACT) and
  ROW_FORMAT=REDUNDANT (table->flags == 0).  For any other
  format, the tablespace flags should equal
  (table->flags & ~(~0UL << DICT_TF_BITS)). */
  ut_a(flags != DICT_TF_COMPACT);
  ut_a(!(flags & (~0UL << DICT_TF_BITS)));

  path = fil_make_ibd_name(tablename, is_temp);

  file = os_file_create(path, OS_FILE_CREATE, OS_FILE_NORMAL, OS_DATA_FILE, &ret);
  if (ret == false) {
    ut_print_timestamp(ib_stream);
    ib_logger(ib_stream, "  Error creating file ");
    ut_print_filename(ib_stream, path);
    ib_logger(ib_stream, ".\n");

    /* The following call will print an error message */

    err = os_file_get_last_error(true);

    if (err == OS_FILE_ALREADY_EXISTS) {
      ib_logger(
        ib_stream,
        "The file already exists though"
        " the corresponding table did not\n"
        "exist in the InnoDB data dictionary."
        " Have you moved InnoDB\n"
        ".ibd files around without using the"
        " SQL commands\n"
        "DISCARD TABLESPACE and"
        " IMPORT TABLESPACE, or did\n"
        "the server crash in the middle of"
        " CREATE TABLE? You can\n"
        "resolve the problem by"
        " removing the file "
      );
      ut_print_filename(ib_stream, path);
      ib_logger(
        ib_stream,
        "\n"
        "under the 'datadir' of the server.\n"
      );

      mem_free(path);
      return DB_TABLESPACE_ALREADY_EXISTS;
    }

    if (err == OS_FILE_DISK_FULL) {

      mem_free(path);
      return DB_OUT_OF_FILE_SPACE;
    }

    mem_free(path);
    return DB_ERROR;
  }

  auto buf2 = (byte *)ut_malloc(3 * UNIV_PAGE_SIZE);
  /* Align the memory for file i/o if we might have O_DIRECT set */
  auto page = (byte *)ut_align(buf2, UNIV_PAGE_SIZE);

  ret = os_file_set_size(path, file, size * UNIV_PAGE_SIZE, 0);

  if (!ret) {
    ut_free(buf2);
    os_file_close(file);
    os_file_delete(path);

    mem_free(path);
    return DB_OUT_OF_FILE_SPACE;
  }

  if (*space_id == 0) {
    *space_id = fil_assign_new_space_id();
  }

  /* printf("Creating tablespace %s id %lu\n", path, *space_id); */

  if (*space_id == ULINT_UNDEFINED) {
    ut_free(buf2);
  error_exit:
    os_file_close(file);
  error_exit2:
    os_file_delete(path);

    mem_free(path);
    return DB_ERROR;
  }

  /* We have to write the space id to the file immediately and flush the
  file to disk. This is because in crash recovery we must be aware what
  tablespaces exist and what are their space id's, so that we can apply
  the log records to the right file. It may take quite a while until
  buffer pool flush algorithms write anything to the file and flush it to
  disk. If we would not write here anything, the file would be filled
  with zeros from the call of os_file_set_size(), until a buffer pool
  flush would write to it. */

  memset(page, '\0', UNIV_PAGE_SIZE);

  fsp_header_init_fields(page, *space_id, flags);
  mach_write_to_4(page + FIL_PAGE_SPACE_ID, *space_id);

  buf_flush_init_for_writing(page, 0);
  ret = os_file_write(path, file, page, 0, 0, UNIV_PAGE_SIZE);

  ut_free(buf2);

  if (ret == 0) {
    ib_logger(
      ib_stream,
      "Error: could not write the first page"
      " to tablespace "
    );
    ut_print_filename(ib_stream, path);
    ib_logger(ib_stream, "\n");
    goto error_exit;
  }

  ret = os_file_flush(file);

  if (!ret) {
    ib_logger(ib_stream, "Error: file flush of tablespace ");
    ut_print_filename(ib_stream, path);
    ib_logger(ib_stream, " failed\n");
    goto error_exit;
  }

  os_file_close(file);

  if (*space_id == ULINT_UNDEFINED) {
    goto error_exit2;
  }

  success = fil_space_create(path, *space_id, flags, FIL_TABLESPACE);

  if (!success) {
    goto error_exit2;
  }

  fil_node_create(path, size, *space_id, false);

  {
    mtr_t mtr;

    mtr_start(&mtr);

    fil_op_write_log(
      flags ? MLOG_FILE_CREATE2 : MLOG_FILE_CREATE, *space_id, is_temp ? MLOG_FILE_FLAG_TEMP : 0, flags, tablename, nullptr, &mtr
    );

    mtr_commit(&mtr);
  }

  mem_free(path);
  return DB_SUCCESS;
}

bool fil_reset_too_high_lsns(const char *name, uint64_t current_lsn) {
  os_file_t file;
  char *filepath;
  uint64_t flush_lsn;
  ulint space_id;
  int64_t file_size;
  int64_t offset;
  bool success;

  filepath = fil_make_ibd_name(name, false);

  file = os_file_create_simple_no_error_handling(filepath, OS_FILE_OPEN, OS_FILE_READ_WRITE, &success);
  if (!success) {
    /* The following call prints an error message */
    os_file_get_last_error(true);

    ut_print_timestamp(ib_stream);

    ib_logger(
      ib_stream,
      "  Error: trying to open a table,"
      " but could not\n"
      "open the tablespace file "
    );
    ut_print_filename(ib_stream, filepath);
    ib_logger(ib_stream, "!\n");
    mem_free(filepath);

    return false;
  }

  /* Read the first page of the tablespace */

  auto buf2 = (byte *)ut_malloc(3 * UNIV_PAGE_SIZE);
  /* Align the memory for file i/o if we might have O_DIRECT set */
  auto page = (byte *)ut_align(buf2, UNIV_PAGE_SIZE);

  success = os_file_read(file, page, 0, 0, UNIV_PAGE_SIZE);

  if (!success) {
    goto func_exit;
  }

  /* We have to read the file flush lsn from the header of the file */

  flush_lsn = mach_read_from_8(page + FIL_PAGE_FILE_FLUSH_LSN);

  if (current_lsn >= flush_lsn) {
    /* Ok */
    success = true;

    goto func_exit;
  }

  space_id = fsp_header_get_space_id(page);

  ut_print_timestamp(ib_stream);
  ib_logger(
    ib_stream,
    "  Flush lsn in the tablespace file %lu"
    " to be imported\n"
    "is %lu, which exceeds current"
    " system lsn %lu.\n"
    "We reset the lsn's in the file ",
    space_id,
    flush_lsn,
    current_lsn
  );
  ut_print_filename(ib_stream, filepath);
  ib_logger(ib_stream, ".\n");

  /* Loop through all the pages in the tablespace and reset the lsn and
  the page checksum if necessary */

  file_size = os_file_get_size_as_iblonglong(file);

  for (offset = 0; offset < file_size; offset += UNIV_PAGE_SIZE) {

    success = os_file_read(file, page, (ulint)(offset & 0xFFFFFFFFUL), (ulint)(offset >> 32), UNIV_PAGE_SIZE);

    if (!success) {

      goto func_exit;
    }

    if (mach_read_from_8(page + FIL_PAGE_LSN) > current_lsn) {
      /* We have to reset the lsn */

      buf_flush_init_for_writing(page, current_lsn);
      success = os_file_write(filepath, file, page, (ulint)(offset & 0xFFFFFFFFUL), (ulint)(offset >> 32), UNIV_PAGE_SIZE);

      if (!success) {

        goto func_exit;
      }
    }
  }

  success = os_file_flush(file);

  if (!success) {

    goto func_exit;
  }

  /* We now update the flush_lsn stamp at the start of the file */
  success = os_file_read(file, page, 0, 0, UNIV_PAGE_SIZE);

  if (!success) {

    goto func_exit;
  }

  mach_write_to_8(page + FIL_PAGE_FILE_FLUSH_LSN, current_lsn);

  success = os_file_write(filepath, file, page, 0, 0, UNIV_PAGE_SIZE);

  if (!success) {

    goto func_exit;
  }
  success = os_file_flush(file);
func_exit:
  os_file_close(file);
  ut_free(buf2);
  mem_free(filepath);

  return success;
}

bool fil_open_single_table_tablespace(bool check_space_id, ulint id, ulint flags, const char *name) {
  os_file_t file;
  char *filepath;
  bool success;
  ulint space_id;
  ulint space_flags;

  filepath = fil_make_ibd_name(name, false);

  /* The tablespace flags (FSP_SPACE_FLAGS) should be 0 for
  ROW_FORMAT=COMPACT
  ((table->flags & ~(~0UL << DICT_TF_BITS)) == DICT_TF_COMPACT) and
  ROW_FORMAT=REDUNDANT (table->flags == 0).  For any other
  format, the tablespace flags should equal
  (table->flags & ~(~0UL << DICT_TF_BITS)). */
  ut_a(flags != DICT_TF_COMPACT);
  ut_a(!(flags & (~0UL << DICT_TF_BITS)));

  file = os_file_create_simple_no_error_handling(filepath, OS_FILE_OPEN, OS_FILE_READ_ONLY, &success);
  if (!success) {
    /* The following call prints an error message */
    os_file_get_last_error(true);

    ut_print_timestamp(ib_stream);

    ib_logger(
      ib_stream,
      "  Error: trying to open a table,"
      " but could not\n"
      "open the tablespace file "
    );
    ut_print_filename(ib_stream, filepath);
    ib_logger(
      ib_stream,
      "!\n"
      "Have you moved InnoDB .ibd files around"
      " without using the\n"
      "commands DISCARD TABLESPACE and"
      " IMPORT TABLESPACE?\n"
      "It is also possible that this is"
      " a temporary table ...,\n"
      "and the server removed the .ibd file for this.\n"
      "Please refer to\n"
      "the InnoDB website for details\n"
      "for how to resolve the issue.\n"
    );

    mem_free(filepath);

    return false;
  }

  if (!check_space_id) {
    space_id = id;
  } else {
    /* Read the first page of the tablespace */

    auto buf2 = (byte *)ut_malloc(2 * UNIV_PAGE_SIZE);
    /* Align the memory for file i/o if we might have O_DIRECT set */
    auto page = (byte *)ut_align(buf2, UNIV_PAGE_SIZE);

    success = os_file_read(file, page, 0, 0, UNIV_PAGE_SIZE);

    /* We have to read the tablespace id and flags from the file. */

    space_id = fsp_header_get_space_id(page);
    space_flags = fsp_header_get_flags(page);

    ut_free(buf2);

    if (unlikely(space_id != id || space_flags != (flags & ~(~0UL << DICT_TF_BITS)))) {
      ut_print_timestamp(ib_stream);

      ib_logger(ib_stream, "  Error: tablespace id and flags in file ");
      ut_print_filename(ib_stream, filepath);
      ib_logger(
        ib_stream,
        " are %lu and %lu, but in the InnoDB\n"
        "data dictionary they are %lu and %lu.\n"
        "Have you moved InnoDB .ibd files"
        " around without using the\n"
        "commands DISCARD TABLESPACE and"
        " IMPORT TABLESPACE?\n"
        "Please refer to\n"
        "the InnoDB website for details\n"
        "for how to resolve the issue.\n",
        (ulong)space_id,
        (ulong)space_flags,
        (ulong)id,
        (ulong)flags
      );

      os_file_close(file);
      mem_free(filepath);

      return false;
    }
  }

  if (fil_space_create(filepath, space_id, flags, FIL_TABLESPACE)) {
    /* We do not measure the size of the file, that is why we pass the 0 below
     */

    fil_node_create(filepath, 0, space_id, false);
  }

  os_file_close(file);
  mem_free(filepath);

  return success;
}

/** Opens an .ibd file and adds the associated single-table tablespace to the
InnoDB fil0fil.c data structures. */
static void fil_load_single_table_tablespace(
  ib_recovery_t recovery, /** in: recovery flag */
  const char *dbname,     /** in: database name */
  const char *filename
) /** in: file name (not a path),
                            including the .ibd extension */
{
  os_file_t file;
  bool success;
  ulint space_id;
  ulint flags;
  ulint size_low;
  ulint size_high;
  ulint len;
  const char *ptr;
  ulint dbname_len;
  char dir[OS_FILE_MAX_PATH];

  strcpy(dir, srv_data_home);

  ptr = fil_normalize_path(dir);

  len = strlen(dbname) + strlen(filename) + strlen(dir) + 3;
  auto filepath = (char *)mem_alloc(len);

  dbname_len = strlen(dbname);

  if (strlen(ptr) > 0) {
    ut_snprintf(filepath, len, "%s%s/%s", ptr, dbname, filename);
  } else if (dbname_len == 0 || dbname[dbname_len - 1] == SRV_PATH_SEPARATOR) {

    ut_snprintf(filepath, len, "%s%s", dbname, filename);
  } else {
    ut_snprintf(filepath, len, "%s/%s", dbname, filename);
  }

  file = os_file_create_simple_no_error_handling(filepath, OS_FILE_OPEN, OS_FILE_READ_ONLY, &success);
  if (!success) {
    /* The following call prints an error message */
    os_file_get_last_error(true);

    ib_logger(
      ib_stream,
      "Error: could not open single-table tablespace"
      " file\n"
      "%s!\n"
      "We do not continue the crash recovery,"
      " because the table may become\n"
      "corrupt if we cannot apply the log records"
      " in the InnoDB log to it.\n"
      "To fix the problem and start InnoDB:\n"
      "1) If there is a permission problem"
      " in the file and InnoDB cannot\n"
      "open the file, you should"
      " modify the permissions.\n"
      "2) If the table is not needed, or you can"
      " restore it from a backup,\n"
      "then you can remove the .ibd file,"
      " and InnoDB will do a normal\n"
      "crash recovery and ignore that table.\n"
      "3) If the file system or the"
      " disk is broken, and you cannot remove\n"
      "the .ibd file, you can set"
      " force_recovery != IB_RECOVERY_DEFAULT \n"
      "and force InnoDB to continue crash"
      " recovery here.\n",
      filepath
    );

    mem_free(filepath);

    if (recovery != IB_RECOVERY_DEFAULT) {
      ib_logger(
        ib_stream,
        "force_recovery"
        " was set to %d. Continuing crash recovery\n"
        "even though we cannot access"
        " the .ibd file of this table.\n",
        (int)recovery
      );
      return;
    }

    log_fatal("Cannot access .ibd file: ", filepath);
  }

  success = os_file_get_size(file, &size_low, &size_high);

  if (!success) {
    /* The following call prints an error message */
    os_file_get_last_error(true);

    ib_logger(
      ib_stream,
      "Error: could not measure the size"
      " of single-table tablespace file\n"
      "%s!\n"
      "We do not continue crash recovery,"
      " because the table will become\n"
      "corrupt if we cannot apply the log records"
      " in the InnoDB log to it.\n"
      "To fix the problem and start the server:\n"
      "1) If there is a permission problem"
      " in the file and the server cannot\n"
      "access the file, you should"
      " modify the permissions.\n"
      "2) If the table is not needed,"
      " or you can restore it from a backup,\n"
      "then you can remove the .ibd file,"
      " and InnoDB will do a normal\n"
      "crash recovery and ignore that table.\n"
      "3) If the file system or the disk is broken,"
      " and you cannot remove\n"
      "the .ibd file, you can set"
      " force_recovery != IB_RECOVERY_DEFAULT\n"
      "and force InnoDB to continue"
      " crash recovery here.\n",
      filepath
    );

    os_file_close(file);
    mem_free(filepath);

    if (recovery != IB_RECOVERY_DEFAULT) {
      ib_logger(
        ib_stream,
        "force_recovery"
        " was set to %d. Continuing crash recovery\n"
        "even though we cannot access"
        " the .ibd file of this table.\n",
        (int)recovery
      );
      return;
    }

    log_fatal("Could not measure size of ", filepath);
  }

  /* TODO: What to do in other cases where we cannot access an .ibd
  file during a crash recovery? */

  /* Every .ibd file is created >= 4 pages in size. Smaller files
  cannot be ok. */

  auto size = (((off_t)size_high) << 32) + (off_t)size_low;
  if (size < off_t(FIL_IBD_FILE_INITIAL_SIZE * UNIV_PAGE_SIZE)) {
    ib_logger(
      ib_stream,
      "Error: the size of single-table tablespace"
      " file %s\n"
      "is only %lu %lu, should be at least %lu!",
      filepath,
      (ulong)size_high,
      (ulong)size_low,
      (ulong)(4 * UNIV_PAGE_SIZE)
    );
    os_file_close(file);
    mem_free(filepath);

    return;
  }
  /* Read the first page of the tablespace if the size big enough */

  auto buf2 = (byte *)ut_malloc(2 * UNIV_PAGE_SIZE);

  /* Align the memory for file i/o if we might have O_DIRECT set */
  auto page = (byte *)ut_align(buf2, UNIV_PAGE_SIZE);

  if (size >= off_t(FIL_IBD_FILE_INITIAL_SIZE * UNIV_PAGE_SIZE)) {
    success = os_file_read(file, page, 0, 0, UNIV_PAGE_SIZE);

    /* We have to read the tablespace id from the file */

    space_id = fsp_header_get_space_id(page);
    flags = fsp_header_get_flags(page);
  } else {
    space_id = ULINT_UNDEFINED;
    flags = 0;
  }

  if (space_id == ULINT_UNDEFINED || space_id == 0) {
    ib_logger(
      ib_stream,
      "Error: tablespace id %lu in file %s"
      " is not sensible\n",
      (ulong)space_id,
      filepath
    );
    goto func_exit;
  }
  success = fil_space_create(filepath, space_id, flags, FIL_TABLESPACE);

  if (!success) {

    if (srv_force_recovery > 0) {
      ib_logger(
        ib_stream,
        "innodb_force_recovery"
        " was set to %lu. Continuing crash recovery\n"
        "even though the tablespace creation"
        " of this table failed.\n",
        (ulong)srv_force_recovery
      );
      goto func_exit;
    }

    log_fatal("During recovery");
  }

  /* We do not use the size information we have about the file, because
  the rounding formula for extents and pages is somewhat complex; we
  let fil_node_open() do that task. */

  fil_node_create(filepath, 0, space_id, false);
func_exit:
  os_file_close(file);
  ut_free(buf2);
  mem_free(filepath);
}

/** A fault-tolerant function that tries to read the next file name in the
directory. We retry 100 times if os_file_readdir_next_file() returns -1. The
idea is to read as much good data as we can and jump over bad data.
@return 0 if ok, -1 if error even after the retries, 1 if at the end
of the directory */
static int fil_file_readdir_next_file(
  db_err *err,         /** out: this is set to DB_ERROR if an error
                         was encountered, otherwise not changed */
  const char *dirname, /** in: directory name or path */
  os_file_dir_t dir,   /** in: directory stream */
  os_file_stat_t *info
) /** in/out: buffer where the info is returned */
{
  ulint i;
  int ret;

  for (i = 0; i < 100; i++) {
    ret = os_file_readdir_next_file(dirname, dir, info);

    if (ret != -1) {

      return ret;
    }

    ib_logger(
      ib_stream,
      "Error: os_file_readdir_next_file()"
      " returned -1 in\n"
      "directory %s\n"
      "Crash recovery may have failed"
      " for some .ibd files!\n",
      dirname
    );

    *err = DB_ERROR;
  }

  return -1;
}

db_err fil_load_single_table_tablespaces(ib_recovery_t recovery) {
  int ret;
  ulint dbpath_len = 100;
  os_file_dir_t dbdir;
  os_file_stat_t dbinfo;
  os_file_stat_t fileinfo;
  db_err err = DB_SUCCESS;
  char home[OS_FILE_MAX_PATH];

  /* The datadir of the server is always the default directory. */

  strcpy(home, srv_data_home);

  auto dir = os_file_opendir(home, true);

  if (dir == nullptr) {

    return DB_ERROR;
  }

  auto dbpath = (char *)mem_alloc(dbpath_len);

  /* Scan all directories under the datadir. They are the database
  directories of the server. */

  ret = fil_file_readdir_next_file(&err, home, dir, &dbinfo);
  while (ret == 0) {
    ulint len;
    /* printf("Looking at %s in datadir\n", dbinfo.name); */

    if (dbinfo.type == REGULAR_FILE || dbinfo.type == OS_FILE_TYPE_UNKNOWN) {

      goto next_datadir_item;
    }

    /* We found a symlink or a directory; try opening it to see
    if a symlink is a directory */

    len = strlen(home) + strlen(dbinfo.name) + 2;

    if (len > dbpath_len) {
      dbpath_len = len;

      if (dbpath) {
        mem_free(dbpath);
      }

      dbpath = (char *)mem_alloc(dbpath_len);
    }

    len = strlen(home);
    ut_a(home[len - 1] == SRV_PATH_SEPARATOR);

    ut_snprintf(dbpath, dbpath_len, "%s%s", home, dbinfo.name);

    dbdir = os_file_opendir(dbpath, false);

    if (dbdir != nullptr) {
      /* printf("Opened dir %s\n", dbinfo.name); */

      /* We found a database directory; loop through it,
      looking for possible .ibd files in it */

      ret = fil_file_readdir_next_file(&err, dbpath, dbdir, &fileinfo);

      while (ret == 0) {
        /* printf(
        "     Looking at file %s\n", fileinfo.name); */

        if (fileinfo.type == DIRECTORY) {

          goto next_file_item;
        }

        len = strlen(fileinfo.name);

        /* We found a symlink or a file */
        if (len > 4 && 0 == strcmp(fileinfo.name + len - 4, ".ibd")) {
          /* The name ends in .ibd; try opening
          the file */
          fil_load_single_table_tablespace(recovery, dbinfo.name, fileinfo.name);
        }
      next_file_item:
        ret = fil_file_readdir_next_file(&err, dbpath, dbdir, &fileinfo);
      }

      if (0 != os_file_closedir(dbdir)) {
        ib_logger(
          ib_stream,
          "Warning: could not"
          " close database directory "
        );
        ut_print_filename(ib_stream, dbpath);
        ib_logger(ib_stream, "\n");

        err = DB_ERROR;
      }
    }

  next_datadir_item:
    ret = fil_file_readdir_next_file(&err, home, dir, &dbinfo);
  }

  mem_free(dbpath);

  if (0 != os_file_closedir(dir)) {
    ib_logger(ib_stream, "Error: could not close datadir\n");

    return DB_ERROR;
  }

  return err;
}

void fil_print_orphaned_tablespaces(void) {
  mutex_enter(&fil_system->mutex);

  auto space = UT_LIST_GET_FIRST(fil_system->space_list);

  while (space) {
    if (space->purpose == FIL_TABLESPACE && space->id != 0 && !space->mark) {
      ib_logger(ib_stream, "Warning: tablespace ");
      ut_print_filename(ib_stream, space->name);
      ib_logger(
        ib_stream,
        " of id %lu has no matching table in\n"
        "the InnoDB data dictionary.\n",
        (ulong)space->id
      );
    }

    space = UT_LIST_GET_NEXT(space_list, space);
  }

  mutex_exit(&fil_system->mutex);
}

bool fil_tablespace_deleted_or_being_deleted_in_mem(ulint id, int64_t version) {
  fil_space_t *space;

  ut_ad(fil_system);

  mutex_enter(&fil_system->mutex);

  space = fil_space_get_by_id(id);

  if (space == nullptr || space->is_being_deleted) {
    mutex_exit(&fil_system->mutex);

    return true;
  }

  if (version != ((int64_t)-1) && space->tablespace_version != version) {
    mutex_exit(&fil_system->mutex);

    return true;
  }

  mutex_exit(&fil_system->mutex);

  return false;
}

bool fil_tablespace_exists_in_mem(ulint id) {
  fil_space_t *space;

  ut_ad(fil_system);

  mutex_enter(&fil_system->mutex);

  space = fil_space_get_by_id(id);

  mutex_exit(&fil_system->mutex);

  return space != nullptr;
}

bool fil_space_for_table_exists_in_mem(
  ulint id, const char *name, bool is_temp, bool mark_space, bool print_error_if_does_not_exist
) {
  fil_space_t *fil_namespace;
  fil_space_t *space;
  char *path;

  ut_ad(fil_system);

  mutex_enter(&fil_system->mutex);

  path = fil_make_ibd_name(name, is_temp);

  /* Look if there is a space with the same id */

  space = fil_space_get_by_id(id);

  /* Look if there is a space with the same name; the name is the
  directory path from the datadir to the file */

  fil_namespace = fil_space_get_by_name(path);
  if (space && space == fil_namespace) {
    /* Found */

    if (mark_space) {
      space->mark = true;
    }

    mem_free(path);
    mutex_exit(&fil_system->mutex);

    return true;
  }

  if (!print_error_if_does_not_exist) {

    mem_free(path);
    mutex_exit(&fil_system->mutex);

    return false;
  }

  if (space == nullptr) {
    if (fil_namespace == nullptr) {
      ut_print_timestamp(ib_stream);
      ib_logger(ib_stream, "  Error: table ");
      ut_print_filename(ib_stream, name);
      ib_logger(
        ib_stream,
        "\n"
        "in InnoDB data dictionary"
        " has tablespace id %lu,\n"
        "but tablespace with that id"
        " or name does not exist. Have\n"
        "you deleted or moved .ibd files?\n",
        (ulong)id
      );
    } else {
      ut_print_timestamp(ib_stream);
      ib_logger(ib_stream, "  Error: table ");
      ut_print_filename(ib_stream, name);
      ib_logger(
        ib_stream,
        "\n"
        "in InnoDB data dictionary has"
        " tablespace id %lu,\n"
        "but a tablespace with that id"
        " does not exist. There is\n"
        "a tablespace of name %s and id %lu,"
        " though. Have\n"
        "you deleted or moved .ibd files?\n",
        (ulong)id,
        fil_namespace->name,
        (ulong)fil_namespace->id
      );
    }
  error_exit:
    ib_logger(
      ib_stream,
      "Please refer to\n"
      "the InnoDB website for details\n"
      "for how to resolve the issue.\n"
    );

    mem_free(path);
    mutex_exit(&fil_system->mutex);

    return false;
  }

  if (0 != fil_tablename_compare(space->name, path)) {
    ut_print_timestamp(ib_stream);
    ib_logger(ib_stream, "  Error: table ");
    ut_print_filename(ib_stream, name);
    ib_logger(
      ib_stream,
      "\n"
      "in InnoDB data dictionary has"
      " tablespace id %lu,\n"
      "but the tablespace with that id"
      " has name %s.\n"
      "Have you deleted or moved .ibd files?\n",
      (ulong)id,
      space->name
    );

    if (fil_namespace != nullptr) {
      ib_logger(ib_stream, "There is a tablespace with the right name\n");
      ut_print_filename(ib_stream, fil_namespace->name);
      ib_logger(ib_stream, ", but its id is %lu.\n", (ulong)fil_namespace->id);
    }

    goto error_exit;
  }

  mem_free(path);
  mutex_exit(&fil_system->mutex);

  return false;
}

/** Checks if a single-table tablespace for a given table name exists in the
tablespace memory cache.
@return	space id, ULINT_UNDEFINED if not found */
static ulint fil_get_space_id_for_table(const char *name) /** in: table name in the standard
                                             'databasename/tablename' format */
{
  fil_space_t *fil_namespace;
  ulint id = ULINT_UNDEFINED;
  char *path;

  ut_ad(fil_system);

  mutex_enter(&fil_system->mutex);

  path = fil_make_ibd_name(name, false);

  /* Look if there is a space with the same name; the name is the
  directory path to the file */

  fil_namespace = fil_space_get_by_name(path);

  if (fil_namespace) {
    id = fil_namespace->id;
  }

  mem_free(path);

  mutex_exit(&fil_system->mutex);

  return id;
}

bool fil_extend_space_to_desired_size(ulint *actual_size, ulint space_id, ulint size_after_extend) {
  fil_node_t *node;
  fil_space_t *space;
  ulint buf_size;
  ulint start_page_no;
  ulint file_start_page_no;
  ulint offset_high;
  ulint offset_low;
  ulint page_size;
  bool success = true;

  fil_mutex_enter_and_prepare_for_io(space_id);

  space = fil_space_get_by_id(space_id);
  ut_a(space);

  if (space->size >= size_after_extend) {
    /* Space already big enough */

    *actual_size = space->size;

    mutex_exit(&fil_system->mutex);

    return true;
  }

  page_size = UNIV_PAGE_SIZE;

  node = UT_LIST_GET_LAST(space->chain);

  fil_node_prepare_for_io(node, fil_system, space);

  start_page_no = space->size;
  file_start_page_no = space->size - node->size;

  /* Extend at most 64 pages at a time */
  buf_size = ut_min(64, size_after_extend - start_page_no) * page_size;
  auto buf2 = (byte *)mem_alloc(buf_size + page_size);
  auto buf = (byte *)ut_align(buf2, page_size);

  memset(buf, 0, buf_size);

  while (start_page_no < size_after_extend) {
    ulint n_pages = ut_min(buf_size / page_size, size_after_extend - start_page_no);

    offset_high = (start_page_no - file_start_page_no) / (4096 * ((1024 * 1024) / page_size));

    offset_low = ((start_page_no - file_start_page_no) % (4096 * ((1024 * 1024) / page_size))) * page_size;

    success = os_aio(
      OS_FILE_WRITE, OS_AIO_SYNC, node->name, node->handle, buf, offset_low, offset_high, page_size * n_pages, nullptr, nullptr
    );

    if (success) {
      node->size += n_pages;
      space->size += n_pages;

      os_has_said_disk_full = false;
    } else {
      /* Let us measure the size of the file to determine how much we were able
       * to extend it */

      n_pages = ((ulint)(os_file_get_size_as_iblonglong(node->handle) / page_size)) - node->size;

      node->size += n_pages;
      space->size += n_pages;

      break;
    }

    start_page_no += n_pages;
  }

  mem_free(buf2);

  fil_node_complete_io(node, fil_system, OS_FILE_WRITE);

  *actual_size = space->size;

  if (space_id == 0) {
    ulint pages_per_mb = (1024 * 1024) / page_size;

    /* Keep the last data file size info up to date, rounded to
    full megabytes */

    srv_data_file_sizes[srv_n_data_files - 1] = (node->size / pages_per_mb) * pages_per_mb;
  }

  mutex_exit(&fil_system->mutex);

  fil_flush(space_id);

  return success;
}

bool fil_space_reserve_free_extents(ulint id, ulint n_free_now, ulint n_to_reserve) {
  fil_space_t *space;
  bool success;

  ut_ad(fil_system);

  mutex_enter(&fil_system->mutex);

  space = fil_space_get_by_id(id);

  ut_a(space);

  if (space->n_reserved_extents + n_to_reserve > n_free_now) {
    success = false;
  } else {
    space->n_reserved_extents += n_to_reserve;
    success = true;
  }

  mutex_exit(&fil_system->mutex);

  return success;
}

void fil_space_release_free_extents(ulint id, ulint n_reserved) {
  fil_space_t *space;

  ut_ad(fil_system);

  mutex_enter(&fil_system->mutex);

  space = fil_space_get_by_id(id);

  ut_a(space);
  ut_a(space->n_reserved_extents >= n_reserved);

  space->n_reserved_extents -= n_reserved;

  mutex_exit(&fil_system->mutex);
}

ulint fil_space_get_n_reserved_extents(ulint id) {
  fil_space_t *space;
  ulint n;

  ut_ad(fil_system);

  mutex_enter(&fil_system->mutex);

  space = fil_space_get_by_id(id);

  ut_a(space);

  n = space->n_reserved_extents;

  mutex_exit(&fil_system->mutex);

  return n;
}

/** NOTE: you must call fil_mutex_enter_and_prepare_for_io() first!

Prepares a file node for i/o. Opens the file if it is closed. Updates the
pending i/o's field in the node and the system appropriately. Takes the node
off the LRU list if it is in the LRU list. The caller must hold the fil_sys
mutex. */
static void fil_node_prepare_for_io(
  fil_node_t *node,     /** in: file node */
  fil_system_t *system, /** in: tablespace memory cache */
  fil_space_t *space
) /** in: space */
{
  ut_ad(node && system && space);
  ut_ad(mutex_own(&(system->mutex)));

  if (system->n_open > system->max_n_open + 5) {
    ut_print_timestamp(ib_stream);
    ib_logger(
      ib_stream,
      "  Warning: open files %lu"
      " exceeds the limit %lu\n",
      (ulong)system->n_open,
      (ulong)system->max_n_open
    );
  }

  if (node->open == false) {
    /* File is closed: open it */
    ut_a(node->n_pending == 0);

    fil_node_open_file(node, system, space);
  }

  node->n_pending++;
}

/** Updates the data structures when an i/o operation finishes. Updates the
pending i/o's field in the node appropriately. */
static void fil_node_complete_io(
  fil_node_t *node,     /** in: file node */
  fil_system_t *system, /** in: tablespace memory cache */
  ulint type
) /** in: OS_FILE_WRITE or OS_FILE_READ; marks
                                 the node as modified if
                                 type == OS_FILE_WRITE */
{
  ut_ad(node);
  ut_ad(system);
  ut_ad(mutex_own(&(system->mutex)));

  ut_a(node->n_pending > 0);

  node->n_pending--;

  if (type == OS_FILE_WRITE) {
    system->modification_counter++;
    node->modification_counter = system->modification_counter;

    if (!node->space->is_in_unflushed_spaces) {

      node->space->is_in_unflushed_spaces = true;
      UT_LIST_ADD_FIRST(system->unflushed_spaces, node->space);
    }
  }
}

/** Report information about an invalid page access. */
static void fil_report_invalid_page_access(
  ulint block_offset,     /** in: block offset */
  ulint space_id,         /** in: space id */
  const char *space_name, /** in: space name */
  ulint byte_offset,      /** in: byte offset */
  ulint len,              /** in: I/O length */
  ulint type
) /** in: I/O type */
{
  ib_logger(
    ib_stream,
    "Error: trying to access page number %lu"
    " in space %lu,\n"
    "space name %s,\n"
    "which is outside the tablespace bounds.\n"
    "Byte offset %lu, len %lu, i/o type %lu.\n"
    "If you get this error at server startup,"
    " please check that\n"
    "your config file matches the ibdata files"
    " that you have in the\n"
    "server.\n",
    (ulong)block_offset,
    (ulong)space_id,
    space_name,
    (ulong)byte_offset,
    (ulong)len,
    (ulong)type
  );
}

db_err fil_io(
  ulint type, bool sync, space_id_t space_id, ulint block_offset, ulint byte_offset, ulint len, void *buf, void *message
) {
  bool is_log = (type & OS_FILE_LOG) > 0;
  type = type & ~OS_FILE_LOG;

  auto wake_later = type & OS_AIO_SIMULATED_WAKE_LATER;
  type = type & ~OS_AIO_SIMULATED_WAKE_LATER;

  ut_ad(len > 0);
  ut_ad(buf != nullptr);
  ut_ad(byte_offset < UNIV_PAGE_SIZE);

  static_assert((1 << UNIV_PAGE_SIZE_SHIFT) == UNIV_PAGE_SIZE, "error (1 << UNIV_PAGE_SIZE_SHIFT) != UNIV_PAGE_SIZE");

  ut_ad(fil_validate());

  ulint mode;

  if (sync) {
    mode = OS_AIO_SYNC;
  } else if (is_log) {
    mode = OS_AIO_LOG;
  } else {
    mode = OS_AIO_NORMAL;
  }

  if (type == OS_FILE_READ) {
    srv_data_read += len;
  } else if (type == OS_FILE_WRITE) {
    srv_data_written += len;
  }

  /* Reserve the fil_system mutex and make sure that we can open at
  least one file while holding it, if the file is not already open */

  fil_mutex_enter_and_prepare_for_io(space_id);

  auto space = fil_space_get_by_id(space_id);

  if (!space) {
    mutex_exit(&fil_system->mutex);

    ut_print_timestamp(ib_stream);
    ib_logger(
      ib_stream,
      "  Error: trying to do i/o"
      " to a tablespace which does not exist.\n"
      "i/o type %lu, space id %lu,"
      " page no. %lu, i/o length %lu bytes\n",
      (ulong)type,
      (ulong)space_id,
      (ulong)block_offset,
      (ulong)len
    );

    return DB_TABLESPACE_DELETED;
  }

  ut_ad(is_log == (space->purpose == FIL_LOG));

  auto node = UT_LIST_GET_FIRST(space->chain);

  for (;;) {
    if (unlikely(node == nullptr)) {
      fil_report_invalid_page_access(block_offset, space_id, space->name, byte_offset, len, type);

      ut_error;
    }

    if (space->id != 0 && node->size == 0) {
      /* We do not know the size of a single-table tablespace
      before we open the file */

      break;
    }

    if (node->size > block_offset) {
      /* Found! */
      break;
    } else {
      block_offset -= node->size;
      node = UT_LIST_GET_NEXT(chain, node);
    }
  }

  /* Open file if closed */
  fil_node_prepare_for_io(node, fil_system, space);

  /* Check that at least the start offset is within the bounds of a
  single-table tablespace */
  if (unlikely(node->size <= block_offset) && space->id != 0 && space->purpose == FIL_TABLESPACE) {

    fil_report_invalid_page_access(block_offset, space_id, space->name, byte_offset, len, type);

    ut_error;
  }

  /* Now we have made the changes in the data structures of fil_system */
  mutex_exit(&fil_system->mutex);

  /* Calculate the low 32 bits and the high 32 bits of the file offset */

  auto offset_high = (block_offset >> (32 - UNIV_PAGE_SIZE_SHIFT));
  auto offset_low = ((block_offset << UNIV_PAGE_SIZE_SHIFT) & 0xFFFFFFFFUL) + byte_offset;

  ut_a(node->size - block_offset >= ((byte_offset + len + (UNIV_PAGE_SIZE - 1)) / UNIV_PAGE_SIZE));

  /* Do aio */

  ut_a(byte_offset % IB_FILE_BLOCK_SIZE == 0);
  ut_a((len % IB_FILE_BLOCK_SIZE) == 0);

  /* Queue the aio request */
  auto ret = os_aio(type, mode | wake_later, node->name, node->handle, buf, offset_low, offset_high, len, node, message);
  ut_a(ret);

  if (mode == OS_AIO_SYNC) {
    /* The i/o operation is already completed when we return from
    os_aio: */

    mutex_enter(&fil_system->mutex);

    fil_node_complete_io(node, fil_system, type);

    mutex_exit(&fil_system->mutex);

    ut_ad(fil_validate());
  }

  return DB_SUCCESS;
}

void fil_aio_wait(ulint segment) {
  bool ret;
  fil_node_t *fil_node;
  void *message;
  ulint type;

  ut_ad(fil_validate());

  if (os_aio_use_native_aio) {
    os_set_io_thread_op_info(segment, "native aio handle");
    ret = 0; /* Eliminate compiler warning */
    ut_error;
  } else {
    os_set_io_thread_op_info(segment, "simulated aio handle");

    ret = os_aio_simulated_handle(segment, &fil_node, &message, &type);
  }

  ut_a(ret);

  os_set_io_thread_op_info(segment, "complete io for fil node");

  mutex_enter(&fil_system->mutex);

  fil_node_complete_io(fil_node, fil_system, type);

  mutex_exit(&fil_system->mutex);

  ut_ad(fil_validate());

  /* Do the i/o handling */
  /* IMPORTANT: since i/o handling for reads will read also the insert
  buffer in tablespace 0, you have to be very careful not to introduce
  deadlocks in the i/o system. We keep tablespace 0 data files always
  open, and use a special i/o thread to serve insert buffer requests. */

  if (fil_node->space->purpose == FIL_TABLESPACE) {
    os_set_io_thread_op_info(segment, "complete io for buf page");
    buf_page_io_complete((buf_page_t *)message);
  } else {
    os_set_io_thread_op_info(segment, "complete io for log");
    log_io_complete((log_group_t *)message);
  }
}

void fil_flush(ulint space_id) {
  fil_space_t *space;
  fil_node_t *node;
  os_file_t file;
  int64_t old_mod_counter;

  mutex_enter(&fil_system->mutex);

  space = fil_space_get_by_id(space_id);

  if (!space || space->is_being_deleted) {
    mutex_exit(&fil_system->mutex);

    return;
  }

  space->n_pending_flushes++; /** prevent dropping of the space while
                              we are flushing */
  node = UT_LIST_GET_FIRST(space->chain);

  while (node) {
    if (node->modification_counter > node->flush_counter) {
      ut_a(node->open);

      /* We want to flush the changes at least up to
      old_mod_counter */
      old_mod_counter = node->modification_counter;

      if (space->purpose == FIL_TABLESPACE) {
        fil_n_pending_tablespace_flushes++;
      } else {
        fil_n_pending_log_flushes++;
        fil_n_log_flushes++;
      }
    retry:
      if (node->n_pending_flushes > 0) {
        /* We want to avoid calling os_file_flush() on
        the file twice at the same time, because we do
        not know what bugs OS's may contain in file
        i/o; sleep for a while */

        mutex_exit(&fil_system->mutex);

        os_thread_sleep(20000);

        mutex_enter(&fil_system->mutex);

        if (node->flush_counter >= old_mod_counter) {

          goto skip_flush;
        }

        goto retry;
      }

      ut_a(node->open);
      file = node->handle;
      node->n_pending_flushes++;

      mutex_exit(&fil_system->mutex);

      /* ib_logger(ib_stream, "Flushing to file %s\n",
      node->name); */

      os_file_flush(file);

      mutex_enter(&fil_system->mutex);

      node->n_pending_flushes--;
    skip_flush:
      if (node->flush_counter < old_mod_counter) {
        node->flush_counter = old_mod_counter;

        if (space->is_in_unflushed_spaces && fil_space_is_flushed(space)) {

          space->is_in_unflushed_spaces = false;

          UT_LIST_REMOVE(fil_system->unflushed_spaces, space);
        }
      }

      if (space->purpose == FIL_TABLESPACE) {
        fil_n_pending_tablespace_flushes--;
      } else {
        fil_n_pending_log_flushes--;
      }
    }

    node = UT_LIST_GET_NEXT(chain, node);
  }

  space->n_pending_flushes--;

  mutex_exit(&fil_system->mutex);
}

void fil_flush_file_spaces(ulint purpose) {
  fil_space_t *space;

  mutex_enter(&fil_system->mutex);

  auto n_space_ids = UT_LIST_GET_LEN(fil_system->unflushed_spaces);
  if (n_space_ids == 0) {

    mutex_exit(&fil_system->mutex);
    return;
  }

  /* Assemble a list of space ids to flush.  Previously, we
  traversed fil_system->unflushed_spaces and called UT_LIST_GET_NEXT()
  on a space that was just removed from the list by fil_flush().
  Thus, the space could be dropped and the memory overwritten. */
  auto space_ids = (ulint *)mem_alloc(n_space_ids * sizeof(ulint));

  ulint i = 0;

  for (space = UT_LIST_GET_FIRST(fil_system->unflushed_spaces); space; space = UT_LIST_GET_NEXT(unflushed_spaces, space)) {

    if (space->purpose == purpose && !space->is_being_deleted) {

      ut_ad(i < n_space_ids);
      space_ids[i++] = space->id;
    }
  }

  n_space_ids = i;

  mutex_exit(&fil_system->mutex);

  /* Flush the spaces.  It will not hurt to call fil_flush() on
  a non-existing space id. */
  for (i = 0; i < n_space_ids; i++) {

    fil_flush(space_ids[i]);
  }

  mem_free(space_ids);
}

bool fil_validate() {
  fil_node_t *fil_node;
  ulint n_open = 0;
  ulint i;

  mutex_enter(&fil_system->mutex);

  /* Look for spaces in the hash table */

  for (i = 0; i < hash_get_n_cells(fil_system->spaces); i++) {

    auto space = (fil_space_t *)HASH_GET_FIRST(fil_system->spaces, i);

    while (space != nullptr) {
      auto check = [](const fil_node_t *node) {
        ut_a(node->open || !node->n_pending);
      };

      ut_list_validate(space->chain, check);

      fil_node = UT_LIST_GET_FIRST(space->chain);

      while (fil_node != nullptr) {
        if (fil_node->n_pending > 0) {
          ut_a(fil_node->open);
        }

        if (fil_node->open) {
          n_open++;
        }
        fil_node = UT_LIST_GET_NEXT(chain, fil_node);
      }
      space = (fil_space_t *)HASH_GET_NEXT(hash, space);
    }
  }

  ut_a(fil_system->n_open == n_open);

  while (fil_node != nullptr) {
    ut_a(fil_node->n_pending == 0);
    ut_a(fil_node->open);
    ut_a(fil_node->space->purpose == FIL_TABLESPACE);
    ut_a(fil_node->space->id != 0);
  }

  mutex_exit(&fil_system->mutex);

  return true;
}

bool fil_addr_is_null(fil_addr_t addr) {
  return addr.page == FIL_NULL;
}

ulint fil_page_get_prev(const byte *page) {
  return mach_read_from_4(page + FIL_PAGE_PREV);
}

ulint fil_page_get_next(const byte *page) {
  return mach_read_from_4(page + FIL_PAGE_NEXT);
}

void fil_page_set_type(byte *page, ulint type) {
  mach_write_to_2(page + FIL_PAGE_TYPE, type);
}

ulint fil_page_get_type(const byte *page) {
  return mach_read_from_2(page + FIL_PAGE_TYPE);
}

void fil_close() {
  ulint i;
  fil_system_t *system = fil_system;

  /* This can happen if we abort during the startup phase. */
  if (system == nullptr) {
    return;
  }

  mutex_free(&system->mutex);
  memset(&system->mutex, 0x0, sizeof(system->mutex));

  /* Free the hash elements. We don't remove them from the table
  because we are going to destroy the table anyway. */
  for (i = 0; i < hash_get_n_cells(system->spaces); i++) {
    auto space = (fil_space_t *)HASH_GET_FIRST(system->spaces, i);

    while (space) {
      fil_space_t *prev_space = space;

      space = (fil_space_t *)HASH_GET_NEXT(hash, prev_space);
      ut_a(prev_space->magic_n == FIL_SPACE_MAGIC_N);
      mem_free(prev_space);
    }
  }
  hash_table_free(system->spaces);

  /* The elements in this hash table are the same in system->spaces,
  therefore no need to free the individual elements. */
  hash_table_free(system->name_hash);

  ut_a(UT_LIST_GET_LEN(system->unflushed_spaces) == 0);
  ut_a(UT_LIST_GET_LEN(system->space_list) == 0);

  mem_free(system);

  fil_system = nullptr;
}

bool fil_rmdir(const char *dbname) {
  bool success = false;
  char dir[OS_FILE_MAX_PATH];

  ut_snprintf(dir, sizeof(dir), "%s%s", srv_data_home, dbname);

  if (rmdir(dbname) != 0) {
    ib_logger(ib_stream, "Error removing directory: %s\n", dbname);
  } else {
    success = true;
  }
  return success;
}

bool fil_mkdir(const char *dbname) {
  char dir[OS_FILE_MAX_PATH];

  ut_snprintf(dir, sizeof(dir), "%s%s", srv_data_home, dbname);

  /* If exists (false) then don't return error. */
  return os_file_create_directory(dir, false);
}
