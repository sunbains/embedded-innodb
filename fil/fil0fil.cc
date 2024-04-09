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

#include <filesystem>

#include "buf0buf.h"
#include "buf0flu.h"
#include "buf0lru.h"
#include "dict0dict.h"
#include "fil0fil.h"
#include "fsp0fsp.h"
#include "hash0hash.h"
#include "log0recv.h"
#include "mach0data.h"
#include "mem0mem.h"
#include "mtr0log.h"
#include "mtr0mtr.h"
#include "os0aio.h"
#include "os0file.h"
#include "os0sync.h"
#include "page0page.h"
#include "srv0srv.h"
#include "srv0start.h"
#include "sync0sync.h"
#include "ut0logger.h"

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

extern AIO *srv_aio;

/** The number of fsyncs done to the log */
ulint fil_n_log_flushes = 0;

/** Number of pending redo log flushes */
ulint fil_n_pending_log_flushes = 0;

/** Number of pending tablespace flushes */
ulint fil_n_pending_tablespace_flushes = 0;

/** The null file address */
const fil_addr_t fil_addr_null = {FIL_NULL, 0};

/** The tablespace memory cache */
struct fil_system_t;

/** The tablespace memory cache; also the totality of logs (the log
data space) is stored here; below we talk about tablespaces, but also
the ib_logfiles form a 'space' and it is handled here */
struct fil_system_t {
  /** The mutex protecting the cache */
  mutex_t m_mutex;

  /** The hash table of spaces in the system; they are hashed on the space id */
  hash_table_t *m_spaces;

  /** hash table based on the space name */
  hash_table_t *m_name_hash;

  /** Base node for the list of those tablespaces whose files contain unflushed
  writes; those spaces have at least one file node where m_modification_counter
  > m_flush_counter */
  UT_LIST_BASE_NODE_T(fil_space_t, m_unflushed_spaces) m_unflushed_spaces;

  /** Number of files currently open */
  ulint m_n_open;

  /** n_open is not allowed to exceed this */
  ulint m_max_n_open;

  /** when we write to a file we increment this by one */
  int64_t m_modification_counter;

  /** Maximum space id in the existing tables, or assigned during the time the
  server has been up; at an InnoDB startup we scan the data dictionary and set
  here the maximum of the space id's of the tables there */
  ulint m_max_assigned_id;

  /** A counter which is incremented for every space object memory creation;
  every space mem object gets a 'timestamp' from this; in DISCARD/ IMPORT
  this is used to check if we should ignore an insert buffer merge request */
  int64_t m_tablespace_version;

  /** List of all file spaces */
  UT_LIST_BASE_NODE_T(fil_space_t, m_space_list) m_space_list;
};

/** The tablespace memory cache. This variable is nullptr before the module is
initialized. */
static fil_system_t *fil_system = nullptr;

/**
 * @brief Frees a space object from the tablespace memory cache. Closes the files in
 * the chain but does not delete them. There must not be any pending i/o's or
 * flushes on the files.
 * 
 * @param id - in: space id
 * @param own_mutex - in: true if own fil_system->mutex
 * @return true if success
 */
static bool fil_space_free(space_id_t id, bool own_mutex);

/**
 * @brief Remove extraneous '.' && '\' && '/' characters from the prefix.
 * 
 * Note: Currently it will not handle paths like: ../a/b.
 * 
 * @param ptr - in: path to normalize
 * @return pointer to normalized path
 */
static const char *fil_normalize_path(const char *ptr) {
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

/**
 * @brief Compare two table names.
 * 
 * It will compare the two table names using the canonical names.
 * e.g., ./a/b == a/b. TODO: /path/to/a/b == a/b if both /path/to/a/b and a/b refer to the same file.
 * 
 * @param name1 - in: table name to compare
 * @param name2 - in: table name to compare
 * 
 * @return = 0 if name1 == name2 < 0 if name1 < name2 > 0 if name1 > name2
 */
static int fil_tablename_compare(const char *name1, const char *name2) {
  name1 = fil_normalize_path(name1);
  name2 = fil_normalize_path(name2);

  return strcmp(name1, name2);
}

/**
 * @brief Prepares a file node for i/o. Opens the file if it is closed. Updates the
 * pending i/o's field in the node and the system appropriately. Takes the node
 * off the LRU list if it is in the LRU list. The caller must hold the fil_sys
 * mutex.
 * 
 * @param node - in: file node
 * @param system - in: tablespace memory cache
 * @param space - in: space
 */
static void fil_node_prepare_for_io(fil_node_t *node, fil_system_t *system, fil_space_t *space);

/**
 * @brief Updates the data structures when an i/o operation finishes.
 * 
 * @param node - file node
 * @param system - tablespace memory cache
 * @param io_request - IO_request::Write or IO_request::Read; marks the node as
 *  modified if type == IO_request::Write
 */
static void fil_node_complete_io(fil_node_t *node, fil_system_t *system, IO_request io_request);

/**
 * @brief Checks if a single-table tablespace for a given table name
 * exists in the tablespace memory cache.
 * 
 * @param name - table name in the standard 'databasename/tablename' format
 * @return space id, ULINT_UNDEFINED if not found
 */
static ulint fil_get_space_id_for_table(const char *name);

/**
 * @brief Frees a space object from the tablespace memory cache. Closes the files in
 * the chain but does not delete them. There must not be any pending i/o's or
 * flushes on the files.
 * 
 * @param id - in: space id
 * @param own_mutex - in: true if own system->mutex
 * @return true if success
 */
static bool fil_space_free(space_id_t id, bool own_mutex);

void fil_var_init() {
  fil_system = nullptr;
  fil_n_log_flushes = 0;
  fil_n_pending_log_flushes = 0;
  fil_n_pending_tablespace_flushes = 0;
}

/**
 * @brief Returns the table space by a given id, nullptr if not found.
 * 
 * @param id space id
 * @return fil_space_t* table space, nullptr if not found
 */
static fil_space_t *fil_space_get_by_id(space_id_t id) {
  fil_space_t *space;

  ut_ad(mutex_own(&fil_system->m_mutex));

  HASH_SEARCH(
    m_hash, fil_system->m_spaces, id, fil_space_t *, space, ut_ad(space->m_magic_n == FIL_SPACE_MAGIC_N), space->m_id == id
  );

  return space;
}

/**
 * @brief Returns the table space by a given name, nullptr if not found.
 * 
 * @param name in: space name
 * @return fil_space_t* table space, nullptr if not found
 */
static fil_space_t *fil_space_get_by_name(const char *name) {
  fil_space_t *space;

  ut_ad(mutex_own(&fil_system->m_mutex));

  auto fold = ut_fold_string(name);

  HASH_SEARCH(
    m_name_hash,
    fil_system->m_name_hash,
    fold,
    fil_space_t *,
    space,
    ut_ad(space->m_magic_n == FIL_SPACE_MAGIC_N),
    !fil_tablename_compare(name, space->m_name)
  );

  return space;
}

int64_t fil_space_get_version(space_id_t id) {
  int64_t version = -1;

  ut_ad(fil_system);

  mutex_enter(&fil_system->m_mutex);

  auto space = fil_space_get_by_id(id);

  if (space) {
    version = space->m_tablespace_version;
  }

  mutex_exit(&fil_system->m_mutex);

  return version;
}

rw_lock_t *fil_space_get_latch(space_id_t id) {
  ut_ad(fil_system != nullptr);

  mutex_enter(&fil_system->m_mutex);

  auto space = fil_space_get_by_id(id);
  ut_a(space != nullptr);

  mutex_exit(&fil_system->m_mutex);

  return &space->m_latch;
}

ulint fil_space_get_type(space_id_t id) {
  fil_space_t *space;

  ut_ad(fil_system);

  mutex_enter(&fil_system->m_mutex);

  space = fil_space_get_by_id(id);

  ut_a(space);

  mutex_exit(&fil_system->m_mutex);

  return space->m_type;
}

/**
 * @brief Checks if all the file nodes in a space are flushed.
 * 
 * @param space in: space
 * @return true if all are flushed
 */
static bool fil_space_is_flushed(fil_space_t *space) {
  ut_ad(mutex_own(&fil_system->m_mutex));

  auto node = UT_LIST_GET_FIRST(space->m_chain);

  while (node != nullptr) {
    if (node->m_modification_counter > node->m_flush_counter) {

      return false;
    }

    node = UT_LIST_GET_NEXT(m_chain, node);
  }

  return true;
}

void fil_node_create(const char *name, ulint size, space_id_t id, bool is_raw) {
  fil_space_t *space;

  mutex_enter(&fil_system->m_mutex);

  auto node = (fil_node_t *)mem_alloc(sizeof(fil_node_t));

  node->m_file_name = mem_strdup(name);
  node->open = false;

  ut_a(!is_raw || srv_start_raw_disk_in_use);

  node->m_is_raw_disk = is_raw;
  node->m_size_in_pages = size;
  node->m_magic_n = FIL_NODE_MAGIC_N;
  node->m_n_pending = 0;
  node->m_n_pending_flushes = 0;

  node->m_modification_counter = 0;
  node->m_flush_counter = 0;

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
    mem_free(node->m_file_name);

    mem_free(node);

    mutex_exit(&fil_system->m_mutex);

    return;
  }

  space->m_size_in_pages += size;

  node->m_space = space;

  UT_LIST_ADD_LAST(space->m_chain, node);

  if (id < SRV_LOG_SPACE_FIRST_ID && fil_system->m_max_assigned_id < id) {

    fil_system->m_max_assigned_id = id;
  }

  mutex_exit(&fil_system->m_mutex);
}

/**
 * Opens a the file of a node of a tablespace.
 * The caller must own the fil_system mutex.
 *
 * @param node The file node.
 * @param system The tablespace memory cache.
 * @param space The space.
 */
static void fil_node_open_file(fil_node_t *node, fil_system_t *system, fil_space_t *space) {
  off_t size;
  bool success;

  ut_ad(mutex_own(&(system->m_mutex)));
  ut_a(node->m_n_pending == 0);
  ut_a(node->open == false);

  if (node->m_size_in_pages == 0) {
    /* It must be a single-table tablespace and we do not know the
    size of the file yet. First we open the file in the normal
    mode, no async I/O here, for simplicity. Then do some checks,
    and close the file again.
    NOTE that we could not use the simple file read function
    os_file_read() in Windows to read from a file opened for
    async I/O! */

    node->m_fh = os_file_create_simple_no_error_handling(node->m_file_name, OS_FILE_OPEN, OS_FILE_READ_ONLY, &success);

    if (!success) {
      /* The following call prints an error message */
      os_file_get_last_error(true);

      ut_print_timestamp(ib_stream);

      ib_logger(
        ib_stream,
        "  Fatal error: cannot open %s\n."
        "Have you deleted .ibd files"
        " under a running server?\n",
        node->m_file_name
      );
      ut_a(0);
    }

    os_file_get_size(node->m_fh, &size);

    ut_a(space->m_id != 0);
    ut_a(space->m_type != FIL_LOG);

    auto size_bytes = size;

    if (size_bytes < off_t(FIL_IBD_FILE_INITIAL_SIZE * UNIV_PAGE_SIZE)) {
      log_fatal(std::format(
        "The size of single-table tablespace file {} is only {}, should be at least {}!",
        node->m_file_name,
        size,
        (FIL_IBD_FILE_INITIAL_SIZE * UNIV_PAGE_SIZE)
      ));
    }

    /* Read the first page of the tablespace */
    auto buf2 = (byte *)ut_new(2 * UNIV_PAGE_SIZE);

    /* Align the memory for file i/o if we might have O_DIRECT set */
    auto page = reinterpret_cast<page_t *>(ut_align(buf2, UNIV_PAGE_SIZE));

    success = os_file_read(node->m_fh, page, UNIV_PAGE_SIZE, 0);

    auto flags = fsp_header_get_flags(page);
    auto space_id = fsp_header_get_space_id(page);

    ut_delete(buf2);

    /* Close the file now that we have read the space id from it */

    os_file_close(node->m_fh);

    if (unlikely(space_id != space->m_id)) {
      log_fatal(
        std::format("Tablespace id is {} in the data dictionary but in file {} it is {}!", space->m_id, node->m_file_name, space_id)
      );
    }

    if (unlikely(space_id == ULINT_UNDEFINED || space_id == 0)) {
      log_fatal(std::format("Tablespace id {} in file {} is not sensible", space_id, node->m_file_name));
    }

    if (unlikely(space->m_flags != flags)) {
      log_fatal(std::format(
        "Table flags are {} in the data dictionary but the flags in file {} are {:x}!", space->m_flags, node->m_file_name, flags
      ));
    }

    if (size_bytes >= 1024 * 1024) {
      /* Truncate the size to whole megabytes. */
      size_bytes = ut_2pow_round(size_bytes, 1024 * 1024);
    }

    node->m_size_in_pages = (ulint)(size_bytes / UNIV_PAGE_SIZE);

    space->m_size_in_pages += node->m_size_in_pages;
  }

  /* printf("Opening file %s\n", node->name); */

  /* Open the file for reading and writing, in Windows normally in the
  unbuffered async I/O mode, though global variables may make
  os_file_create() to fall back to the normal file I/O mode. */

  bool ret;

  if (space->m_type == FIL_LOG) {
    node->m_fh = os_file_create(node->m_file_name, OS_FILE_OPEN, OS_FILE_AIO, OS_LOG_FILE, &ret);
  } else if (node->m_is_raw_disk) {
    node->m_fh = os_file_create(node->m_file_name, OS_FILE_OPEN_RAW, OS_FILE_AIO, OS_DATA_FILE, &ret);
  } else {
    node->m_fh = os_file_create(node->m_file_name, OS_FILE_OPEN, OS_FILE_AIO, OS_DATA_FILE, &ret);
  }

  ut_a(ret);

  node->open = true;

  system->m_n_open++;
}

/**
 * @brief Closes a file.
 *
 * @param node in: file node
 * @param system in: tablespace memory cache
 */
static void fil_node_close_file(fil_node_t *node, fil_system_t *system) {
  bool ret;

  ut_ad(node && system);
  ut_ad(mutex_own(&(system->m_mutex)));
  ut_a(node->open);
  ut_a(node->m_n_pending == 0);
  ut_a(node->m_n_pending_flushes == 0);
  ut_a(node->m_modification_counter == node->m_flush_counter);

  ret = os_file_close(node->m_fh);
  ut_a(ret);

  /* printf("Closing file %s\n", node->name); */

  node->open = false;
  ut_a(system->m_n_open > 0);
  system->m_n_open--;
}

/**
 * @brief Reserves the fil_system mutex and tries to make sure we can open at least
 * one file while holding it. This should be called before calling
 * fil_node_prepare_for_io(), because that function may need to open a file.
 *
 * @param space_id in: space id
 */
static void fil_mutex_enter_and_prepare_for_io(space_id_t space_id) {
  fil_space_t *space;
  ulint count = 0;
  ulint count2 = 0;

retry:
  mutex_enter(&fil_system->m_mutex);

  if (space_id == 0 || space_id >= SRV_LOG_SPACE_FIRST_ID) {
    /* We keep log files and system tablespace files always open;
    this is important in preventing deadlocks in this module, as
    a page read completion often performs another read from the
    insert buffer. The insert buffer is in tablespace 0, and we
    cannot end up waiting in this function. */

    return;
  }

  if (fil_system->m_n_open < fil_system->m_max_n_open) {

    return;
  }

  space = fil_space_get_by_id(space_id);

  if (space != nullptr && space->m_stop_ios) {
    /* We are going to do a rename file and want to stop new i/o's
    for a while */

    if (count2 > 20000) {
      ib_logger(ib_stream, "Warning: tablespace ");
      ut_print_filename(ib_stream, space->m_name);
      ib_logger(ib_stream, " has i/o ops stopped for a long time %lu\n", (ulong)count2);
    }

    mutex_exit(&fil_system->m_mutex);

    os_thread_sleep(20000);

    count2++;

    goto retry;
  }

  /* If the file is already open, no need to do anything; if the space
  does not exist, we handle the situation in the function which called
  this function */

  if (!space || UT_LIST_GET_FIRST(space->m_chain)->open) {

    return;
  }

  if (fil_system->m_n_open < fil_system->m_max_n_open) {
    /* Ok */

    return;
  }

  if (count >= 2) {
    ib_logger(
      ib_stream,
      "Too many (%lu) files stay open while the maximum\n"
      " allowed value would be %lu. You may need to raise the value of"
      " max_files_open\n",
      (ulong)fil_system->m_n_open,
      (ulong)fil_system->m_max_n_open
    );

    return;
  }

  mutex_exit(&fil_system->m_mutex);

  os_thread_sleep(20000);

  /* Flush tablespaces so that we can close modified files in the LRU list */

  fil_flush_file_spaces(FIL_TABLESPACE);

  count++;

  goto retry;
}

/**
 * @brief Frees a file node object from a tablespace memory cache.
 *
 * @param node in, own: file node
 * @param system in: tablespace memory cache
 * @param space in: space where the file node is chained
 */
static void fil_node_free(fil_node_t *node, fil_system_t *system, fil_space_t *space) {
  ut_ad(node && system && space);
  ut_ad(mutex_own(&(system->m_mutex)));
  ut_a(node->m_magic_n == FIL_NODE_MAGIC_N);
  ut_a(node->m_n_pending == 0);

  if (node->open) {
    /* We fool the assertion in fil_node_close_file() to think
    there are no unflushed modifications in the file */

    node->m_modification_counter = node->m_flush_counter;

    if (space->m_is_in_unflushed_spaces && fil_space_is_flushed(space)) {

      space->m_is_in_unflushed_spaces = false;

      UT_LIST_REMOVE(system->m_unflushed_spaces, space);
    }

    fil_node_close_file(node, system);
  }

  space->m_size_in_pages -= node->m_size_in_pages;

  UT_LIST_REMOVE(space->m_chain, node);

  mem_free(node->m_file_name);
  mem_free(node);
}

bool fil_space_create(const char *name, space_id_t id, ulint flags, Fil_type fil_type) {
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
  ut_a(fil_system);
  ut_a(name);

  mutex_enter(&fil_system->m_mutex);

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
      (ulong)space->m_id
    );

    if (id == 0 || fil_type != FIL_TABLESPACE) {

      mutex_exit(&fil_system->m_mutex);

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

    namesake_id = space->m_id;

    mutex_exit(&fil_system->m_mutex);

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
      (ulong)space->m_id
    );
    ut_print_filename(ib_stream, space->m_name);
    ib_logger(
      ib_stream,
      " already exists in the tablespace\n"
      "memory cache!\n"
    );

    mutex_exit(&fil_system->m_mutex);

    return false;
  }

  space = (fil_space_t *)mem_alloc(sizeof(fil_space_t));

  space->m_name = mem_strdup(name);
  space->m_id = id;

  fil_system->m_tablespace_version++;
  space->m_tablespace_version = fil_system->m_tablespace_version;
  space->m_mark = false;

  if (fil_type == FIL_TABLESPACE && id > fil_system->m_max_assigned_id) {
    fil_system->m_max_assigned_id = id;
  }

  space->m_stop_ios = false;
  space->m_is_being_deleted = false;
  space->m_type = fil_type;
  space->m_size_in_pages = 0;
  space->m_flags = flags;

  space->m_n_reserved_extents = 0;

  space->m_n_pending_flushes = 0;

  UT_LIST_INIT(space->m_chain);
  space->m_magic_n = FIL_SPACE_MAGIC_N;

  rw_lock_create(&space->m_latch, SYNC_FSP);

  HASH_INSERT(fil_space_t, m_hash, fil_system->m_spaces, id, space);

  HASH_INSERT(fil_space_t, m_name_hash, fil_system->m_name_hash, ut_fold_string(name), space);

  space->m_is_in_unflushed_spaces = false;

  UT_LIST_ADD_LAST(fil_system->m_space_list, space);

  mutex_exit(&fil_system->m_mutex);

  return true;
}

/** Assigns a new space id for a new single-table tablespace. This works simply
by incrementing the global counter. If 4 billion id's is not enough, we may need
to recycle id's.
@return	new tablespace id; ULINT_UNDEFINED if could not assign an id */
static ulint fil_assign_new_space_id() {
  space_id_t id;

  mutex_enter(&fil_system->m_mutex);

  fil_system->m_max_assigned_id++;

  id = fil_system->m_max_assigned_id;

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
    fil_system->m_max_assigned_id--;

    id = ULINT_UNDEFINED;
  }

  mutex_exit(&fil_system->m_mutex);

  return id;
}

/**
 * @brief Frees a space object from the tablespace memory cache. Closes the files in
 * the chain but does not delete them. There must not be any pending i/o's or
 * flushes on the files.
 *
 * @param id out: true if success
 * @param own_mutex in: true if own system->mutex
 *
 * @return true if success
 */
static bool fil_space_free(space_id_t id, bool own_mutex) {
  fil_space_t *space;
  fil_node_t *fil_node;
  fil_space_t *fil_namespace;

  if (!own_mutex) {
    mutex_enter(&fil_system->m_mutex);
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

    mutex_exit(&fil_system->m_mutex);

    return false;
  }

  HASH_DELETE(fil_space_t, m_hash, fil_system->m_spaces, id, space);

  fil_namespace = fil_space_get_by_name(space->m_name);
  ut_a(fil_namespace);
  ut_a(space == fil_namespace);

  HASH_DELETE(fil_space_t, m_name_hash, fil_system->m_name_hash, ut_fold_string(space->m_name), space);

  if (space->m_is_in_unflushed_spaces) {
    space->m_is_in_unflushed_spaces = false;

    UT_LIST_REMOVE(fil_system->m_unflushed_spaces, space);
  }

  UT_LIST_REMOVE(fil_system->m_space_list, space);

  ut_a(space->m_magic_n == FIL_SPACE_MAGIC_N);
  ut_a(0 == space->m_n_pending_flushes);

  fil_node = UT_LIST_GET_FIRST(space->m_chain);

  while (fil_node != nullptr) {
    fil_node_free(fil_node, fil_system, space);

    fil_node = UT_LIST_GET_FIRST(space->m_chain);
  }

  ut_a(0 == UT_LIST_GET_LEN(space->m_chain));

  if (!own_mutex) {
    mutex_exit(&fil_system->m_mutex);
  }

  rw_lock_free(&(space->m_latch));

  mem_free(space->m_name);
  mem_free(space);

  return true;
}

ulint fil_space_get_size(space_id_t id) {
  ut_ad(fil_system);

  fil_mutex_enter_and_prepare_for_io(id);

  auto space = fil_space_get_by_id(id);

  if (space == nullptr) {
    mutex_exit(&fil_system->m_mutex);

    return 0;
  }

  if (space->m_size_in_pages == 0 && space->m_type == FIL_TABLESPACE) {
    ut_a(id != 0);

    ut_a(1 == UT_LIST_GET_LEN(space->m_chain));

    auto node = UT_LIST_GET_FIRST(space->m_chain);

    /* It must be a single-table tablespace and we have not opened
    the file yet; the following calls will open it and update the
    size fields */

    fil_node_prepare_for_io(node, fil_system, space);
    fil_node_complete_io(node, fil_system, IO_request::Sync_read);
  }

  auto size = space->m_size_in_pages;

  mutex_exit(&fil_system->m_mutex);

  return size;
}

ulint fil_space_get_flags(space_id_t id) {

  ut_ad(fil_system != nullptr);

  if (id == 0) {
    return 0;
  }

  fil_mutex_enter_and_prepare_for_io(id);

  auto space = fil_space_get_by_id(id);

  if (space == nullptr) {
    mutex_exit(&fil_system->m_mutex);

    return ULINT_UNDEFINED;
  }

  if (space->m_size_in_pages == 0 && space->m_type == FIL_TABLESPACE) {
    ut_a(id != 0);

    ut_a(1 == UT_LIST_GET_LEN(space->m_chain));

    auto node = UT_LIST_GET_FIRST(space->m_chain);

    /* It must be a single-table tablespace and we have not opened
    the file yet; the following calls will open it and update the
    size fields */

    fil_node_prepare_for_io(node, fil_system, space);
    fil_node_complete_io(node, fil_system, IO_request::Sync_read);
  }

  auto flags = space->m_flags;

  mutex_exit(&fil_system->m_mutex);

  return flags;
}

bool fil_check_adress_in_tablespace(space_id_t id, page_no_t page_no) {
  return fil_space_get_size(id) > page_no;
}

void fil_open(ulint max_n_open) {
  ut_a(fil_system == nullptr);

  ut_a(max_n_open > 0);

  fil_system = reinterpret_cast<fil_system_t *>(mem_alloc(sizeof(fil_system_t)));

  mutex_create(&fil_system->m_mutex, IF_DEBUG("fil_sys_mutex", ) IF_SYNC_DEBUG(SYNC_ANY_LATCH, ) Source_location{});

  fil_system->m_spaces = hash_create(max_n_open);
  fil_system->m_name_hash = hash_create(max_n_open);

  fil_system->m_n_open = 0;
  fil_system->m_max_n_open = max_n_open;

  fil_system->m_modification_counter = 0;
  fil_system->m_max_assigned_id = 0;

  fil_system->m_tablespace_version = 0;

  UT_LIST_INIT(fil_system->m_unflushed_spaces);
  UT_LIST_INIT(fil_system->m_space_list);
}

void fil_open_log_and_system_tablespace_files() {
  fil_space_t *space;
  fil_node_t *node;

  mutex_enter(&fil_system->m_mutex);

  space = UT_LIST_GET_FIRST(fil_system->m_space_list);

  while (space != nullptr) {
    if (space->m_type != FIL_TABLESPACE || space->m_id == 0) {
      node = UT_LIST_GET_FIRST(space->m_chain);

      while (node != nullptr) {
        if (!node->open) {
          fil_node_open_file(node, fil_system, space);
        }
        if (fil_system->m_max_n_open < 10 + fil_system->m_n_open) {
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
            (ulong)fil_system->m_n_open,
            (ulong)fil_system->m_max_n_open
          );
        }
        node = UT_LIST_GET_NEXT(m_chain, node);
      }
    }
    space = UT_LIST_GET_NEXT(m_space_list, space);
  }

  mutex_exit(&fil_system->m_mutex);
}

void fil_close_all_files() {
  fil_space_t *space;
  fil_node_t *node;

  /* If we decide to abort before this module has been initialized
  then we simply ignore the request. */
  if (fil_system == nullptr) {
    return;
  }

  mutex_enter(&fil_system->m_mutex);

  space = UT_LIST_GET_FIRST(fil_system->m_space_list);

  while (space != nullptr) {
    fil_space_t *prev_space = space;

    node = UT_LIST_GET_FIRST(space->m_chain);

    while (node != nullptr) {
      if (node->open) {
        fil_node_close_file(node, fil_system);
      }
      node = UT_LIST_GET_NEXT(m_chain, node);
    }
    space = UT_LIST_GET_NEXT(m_space_list, space);
    fil_space_free(prev_space->m_id, true);
  }

  mutex_exit(&fil_system->m_mutex);
}

void fil_set_max_space_id_if_bigger(ulint max_id) {
  if (max_id >= SRV_LOG_SPACE_FIRST_ID) {
    ib_logger(ib_stream, "Fatal error: max tablespace id is too high, %lu\n", (ulong)max_id);
    ut_error;
  }

  mutex_enter(&fil_system->m_mutex);

  if (fil_system->m_max_assigned_id < max_id) {

    fil_system->m_max_assigned_id = max_id;
  }

  mutex_exit(&fil_system->m_mutex);
}

/**
 * @brief Writes the flushed lsn and the latest archived log number to the page header
 * of the first page of a data file of the system tablespace (space 0),
 * which is uncompressed.
 *
 * @param sum_of_sizes in: combined size of previous files in space, in database pages
 * @param lsn in: lsn to write
 *
 * @return db_err
 */
static db_err fil_write_lsn_and_arch_no_to_file(ulint sum_of_sizes, lsn_t lsn) {
  auto buf1 = (byte *)mem_alloc(2 * UNIV_PAGE_SIZE);
  auto buf = (byte *)ut_align(buf1, UNIV_PAGE_SIZE);

  fil_io(IO_request::Sync_read, false, SYS_TABLESPACE, sum_of_sizes, 0, UNIV_PAGE_SIZE, buf, nullptr);

  mach_write_to_8(buf + FIL_PAGE_FILE_FLUSH_LSN, lsn);

  fil_io(IO_request::Sync_read, false, SYS_TABLESPACE, sum_of_sizes, 0, UNIV_PAGE_SIZE, buf, nullptr);

  mem_free(buf1);

  return DB_SUCCESS;
}

db_err fil_write_flushed_lsn_to_data_files(lsn_t lsn) {
  fil_node_t *node;
  ulint sum_of_sizes;

  mutex_enter(&fil_system->m_mutex);

  auto space = UT_LIST_GET_FIRST(fil_system->m_space_list);

  while (space) {
    /* We only write the lsn to all existing data files which have
    been open during the lifetime of the server process; they are
    represented by the space objects in the tablespace memory
    cache. Note that all data files in the system tablespace 0 are
    always open. */

    if (space->m_type == FIL_TABLESPACE && space->m_id == 0) {
      sum_of_sizes = 0;

      node = UT_LIST_GET_FIRST(space->m_chain);
      while (node) {
        mutex_exit(&fil_system->m_mutex);

        auto err = fil_write_lsn_and_arch_no_to_file(sum_of_sizes, lsn);

        if (err != DB_SUCCESS) {

          return err;
        }

        mutex_enter(&fil_system->m_mutex);

        sum_of_sizes += node->m_size_in_pages;
        node = UT_LIST_GET_NEXT(m_chain, node);
      }
    }
    space = UT_LIST_GET_NEXT(m_space_list, space);
  }

  mutex_exit(&fil_system->m_mutex);

  return DB_SUCCESS;
}

void fil_read_flushed_lsn_and_arch_log_no(
  os_file_t data_file, bool one_read_already, lsn_t *min_flushed_lsn, lsn_t *max_flushed_lsn
) {
  uint64_t flushed_lsn;
  auto buf2 = (byte *)ut_new(2 * UNIV_PAGE_SIZE);

  /* Align the memory for a possible read from a raw device */
  auto buf = (byte *)ut_align(buf2, UNIV_PAGE_SIZE);

  os_file_read(data_file, buf, UNIV_PAGE_SIZE, 0);

  flushed_lsn = mach_read_from_8(buf + FIL_PAGE_FILE_FLUSH_LSN);

  ut_delete(buf2);

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

/**
 * @brief Creates the database directory for a table if it does not exist yet.
 *
 * @param name in: name in the standard 'databasename/tablename' format
 */
static void fil_create_directory_for_tablename(const char *name) {
  auto len = strlen(srv_data_home);
  ut_a(len > 0);

  auto namend = strchr(name, '/');
  ut_a(namend);

  auto path = (char *)mem_alloc(len + (namend - name) + 2);

  strncpy(path, srv_data_home, len);
  ut_a(path[len - 1] == SRV_PATH_SEPARATOR);

  strncpy(path + len, name, namend - name);

  ut_a(os_file_create_directory(path, false));
  mem_free(path);
}

/**
 * @brief Writes a log record about an .ibd file create/rename/delete.
 *
 * @param type      in: MLOG_FILE_CREATE, MLOG_FILE_CREATE2, MLOG_FILE_DELETE, or MLOG_FILE_RENAME
 * @param space_id  in: space id
 * @param log_flags in: redo log flags (stored in the page number field)
 * @param flags     in: compressed page size and file format if type==MLOG_FILE_CREATE2, or 0
 * @param name      in: table name in the familiar 'databasename/tablename' format, or the file path in the case of MLOG_FILE_DELETE
 * @param new_name  in: if type is MLOG_FILE_RENAME, the new table name in the 'databasename/tablename' format
 * @param mtr       in: mini-transaction handle
 */
static void fil_op_write_log(
  ulint type, space_id_t space_id, ulint log_flags, ulint flags, const char *name, const char *new_name, mtr_t *mtr
) {
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

byte *fil_op_log_parse_or_replay(byte *ptr, byte *end_ptr, ulint type, space_id_t space_id, ulint log_flags) {
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

bool fil_delete_tablespace(space_id_t id) {
  bool success;
  fil_space_t *space;
  fil_node_t *node;
  char *path;

  ut_a(id != 0);

  ulint count{};

  for (;;) {
    mutex_enter(&fil_system->m_mutex);

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

      mutex_exit(&fil_system->m_mutex);

      return false;
    }

    space->m_is_being_deleted = true;

    ut_a(UT_LIST_GET_LEN(space->m_chain) == 1);
    node = UT_LIST_GET_FIRST(space->m_chain);

    if (space->m_n_pending_flushes == 0 && node->m_n_pending == 0) {
      break;
    }

    if (count > 1000) {
      ut_print_timestamp(ib_stream);
      ib_logger(ib_stream, "Trying to delete tablespace ");
      ut_print_filename(ib_stream, space->m_name);
      ib_logger(
        ib_stream,
        ",\n"
        "but there are %lu flushes and %lu pending i/o's on it"
        " Loop %lu.\n",
        (ulong)space->m_n_pending_flushes,
        (ulong)node->m_n_pending,
        (ulong)count
      );
    }

    mutex_exit(&fil_system->m_mutex);

    os_thread_sleep(20000);

    ++count;
  }

  path = mem_strdup(space->m_name);

  mutex_exit(&fil_system->m_mutex);

  /* Invalidate in the buffer pool all pages belonging to the tablespace.
  Since we have set space->is_being_deleted = true, readahead can no longer
  read more pages of this tablespace to the buffer pool. Thus we can clean
  the tablespace out of the buffer pool completely and permanently. The flag
  is_being_deleted also prevents fil_flush() from being applied to this
  tablespace. */

  srv_buf_pool->m_LRU->invalidate_tablespace(id);

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

bool fil_discard_tablespace(space_id_t id) {
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
  const char *old_name = space->m_name;

  ut_ad(mutex_own(&fil_system->m_mutex));

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

  HASH_DELETE(fil_space_t, m_name_hash, fil_system->m_name_hash, ut_fold_string(space->m_name), space);
  mem_free(space->m_name);
  mem_free(node->m_file_name);

  space->m_name = mem_strdup(path);
  node->m_file_name = mem_strdup(path);

  HASH_INSERT(fil_space_t, m_name_hash, fil_system->m_name_hash, ut_fold_string(path), space);
  return true;
}

/**
 * @brief Allocates a file name for a single-table tablespace.
 * The string must be freed by caller with mem_free().
 * 
 * @param name The table name or a dir path of a TEMPORARY table.
 * @param is_temp True if it is a dir path.
 * @return char* The allocated file name.
 */
static char *fil_make_ibd_name(const char *name, bool is_temp) {
  ulint namelen = strlen(name);
  auto dirlen = strlen(srv_data_home);
  auto sz = dirlen + namelen + sizeof("/.ibd");
  auto filename = (char *)mem_alloc(sz);

  ut_snprintf(filename, sz, "%s%s.ibd", fil_normalize_path(srv_data_home), name);

  return filename;
}

bool fil_rename_tablespace(const char *old_name, space_id_t id, const char *new_name) {
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

  mutex_enter(&fil_system->m_mutex);

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
    mutex_exit(&fil_system->m_mutex);

    return false;
  }

  if (count > 25000) {
    space->m_stop_ios = false;
    mutex_exit(&fil_system->m_mutex);

    return false;
  }

  /* We temporarily close the .ibd file because we do not trust that
  operating systems can rename an open file. For the closing we have to
  wait until there are no pending i/o's or flushes on the file. */

  space->m_stop_ios = true;

  ut_a(UT_LIST_GET_LEN(space->m_chain) == 1);
  node = UT_LIST_GET_FIRST(space->m_chain);

  if (node->m_n_pending > 0 || node->m_n_pending_flushes > 0) {
    /* There are pending i/o's or flushes, sleep for a while and
    retry */

    mutex_exit(&fil_system->m_mutex);

    os_thread_sleep(20000);

    goto retry;

  } else if (node->m_modification_counter > node->m_flush_counter) {
    /* Flush the space */

    mutex_exit(&fil_system->m_mutex);

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

    ut_a(fil_tablename_compare(space->m_name, old_path) == 0);
    ut_a(fil_tablename_compare(node->m_file_name, old_path) == 0);
  } else {
    old_path = mem_strdup(space->m_name);
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

  space->m_stop_ios = false;

  mutex_exit(&fil_system->m_mutex);

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

  auto buf2 = (byte *)ut_new(3 * UNIV_PAGE_SIZE);
  /* Align the memory for file i/o if we might have O_DIRECT set */
  auto page = (byte *)ut_align(buf2, UNIV_PAGE_SIZE);

  ret = os_file_set_size(path, file, size * UNIV_PAGE_SIZE, 0);

  if (!ret) {
    ut_delete(buf2);
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
    ut_delete(buf2);
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

  srv_buf_pool->m_flusher->init_for_writing(page, 0);
  ret = os_file_write(path, file, page, UNIV_PAGE_SIZE, 0);

  ut_delete(buf2);

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

bool fil_open_single_table_tablespace(bool check_space_id, space_id_t id, ulint flags, const char *name) {
  os_file_t file;
  char *filepath;
  bool success;
  space_id_t space_id;
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
      "Have you moved InnoDB .ibd files around without using the commands DISCARD TABLESPACE and"
      " IMPORT TABLESPACE? It is also possible that this is a temporary table ..., and the server"
      " removed the .ibd file for this."
    );

    mem_free(filepath);

    return false;
  }

  if (!check_space_id) {
    space_id = id;
  } else {
    /* Read the first page of the tablespace */

    auto buf2 = (byte *)ut_new(2 * UNIV_PAGE_SIZE);
    /* Align the memory for file i/o if we might have O_DIRECT set */
    auto page = (byte *)ut_align(buf2, UNIV_PAGE_SIZE);

    success = os_file_read(file, page, UNIV_PAGE_SIZE, 0);

    /* We have to read the tablespace id and flags from the file. */

    space_id = fsp_header_get_space_id(page);
    space_flags = fsp_header_get_flags(page);

    ut_delete(buf2);

    if (unlikely(space_id != id || space_flags != (flags & ~(~0UL << DICT_TF_BITS)))) {
      ut_print_timestamp(ib_stream);

      ib_logger(ib_stream, "  Error: tablespace id and flags in file ");
      ut_print_filename(ib_stream, filepath);
      ib_logger(
        ib_stream,
        " are %lu and %lu, but in the InnoDB data dictionary they are %lu and %lu. Have you moved InnoDB .ibd files"
        " around without using the commands DISCARD TABLESPACE andIMPORT TABLESPACE?",
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

/**
 * @param recovery recovery flag
 * @param dbname database (or directory) name
 * @param filename file name (not a path), including the .ibd extension
 */
static void fil_load_single_table_tablespace(ib_recovery_t recovery, const char *dbname, const char *filename) {
  char dir[OS_FILE_MAX_PATH];

  strcpy(dir, srv_data_home);

  auto ptr = fil_normalize_path(dir);
  auto len = strlen(dbname) + strlen(filename) + strlen(dir) + 3;
  auto filepath = (char *)mem_alloc(len);

  auto dbname_len = strlen(dbname);

  if (strlen(ptr) > 0) {
    ut_snprintf(filepath, len, "%s%s/%s", ptr, dbname, filename);
  } else if (dbname_len == 0 || dbname[dbname_len - 1] == SRV_PATH_SEPARATOR) {

    ut_snprintf(filepath, len, "%s%s", dbname, filename);
  } else {
    ut_snprintf(filepath, len, "%s/%s", dbname, filename);
  }

  bool success;

  auto file = os_file_create_simple_no_error_handling(filepath, OS_FILE_OPEN, OS_FILE_READ_ONLY, &success);

  if (!success) {
    /* The following call prints an error message */
    os_file_get_last_error(true);

    ib_logger(
      ib_stream,
      "Error: could not open single-table tablespace file %s!"
      " We do not continue the crash recovery, because the table may become"
      " corrupt if we cannot apply the log records in the InnoDB log to it."
      " To fix the problem and start InnoDB: \n"
      "   1) If there is a permission problem in the file and InnoDB cannot"
      " open the file, you should"
      " modify the permissions.\n"
      "   2) If the table is not needed, or you can restore it from a backup,"
      " then you can remove the .ibd file, and InnoDB will do a normal crash"
      " recovery and ignore that table.\n"
      "   3) If the file system or the disk is broken, and you cannot remove"
      " the .ibd file, you can set force_recovery != IB_RECOVERY_DEFAULT"
      " and force InnoDB to continue crash recovery here.\n",
      filepath
    );

    mem_free(filepath);

    if (recovery != IB_RECOVERY_DEFAULT) {
      ib_logger(
        ib_stream,
        "force_recovery was set to %d. Continuing crash recovery even though"
        " we cannot access the .ibd file of this table.\n",
        (int)recovery
      );
      return;
    }

    log_fatal("Cannot access .ibd file: ", filepath);
  }

  off_t size{};

  success = os_file_get_size(file, &size);

  if (!success) {
    /* The following call prints an error message */
    os_file_get_last_error(true);

    ib_logger(
      ib_stream,
      "Could not measure the size of single-table tablespace file %s!"
      " We do not continue crash recovery, because the table will become"
      " corrupt if we cannot apply the log recordsin the InnoDB log to it."
      " To fix the problem and start the server:\n"
      "    1) If there is a permission problem in the file and the server cannot"
      " access the file, you should modify the permissions.\n"
      "    2) If the table is not needed, or you can restore it from a backup,"
      " then you can remove the .ibd file, and InnoDB will do a normal"
      " crash recovery and ignore that table.\n"
      "    3) If the file system or the disk is broken, and you cannot remove"
      " the .ibd file, you can set force_recovery != IB_RECOVERY_DEFAULT"
      " and force InnoDB to continue crash recovery here.\n",
      filepath
    );

    os_file_close(file);
    mem_free(filepath);

    if (recovery != IB_RECOVERY_DEFAULT) {

      ib_logger(
        ib_stream,
        "force_recovery was set to %d. Continuing crash recovery"
        " even though we cannot access the .ibd file of this table.\n",
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

  if (size < off_t(FIL_IBD_FILE_INITIAL_SIZE * UNIV_PAGE_SIZE)) {
    ib_logger(
      ib_stream,
      "Error: the size of single-table tablespace file %s"
      " is only %lu, should be at least %lu!",
      filepath,
      (ulong)size,
      (ulong)(4 * UNIV_PAGE_SIZE)
    );
    os_file_close(file);
    mem_free(filepath);

    return;
  }

  /* Read the first page of the tablespace if the size big enough */

  auto buf2 = (byte *)ut_new(2 * UNIV_PAGE_SIZE);

  /* Align the memory for file i/o if we might have O_DIRECT set */
  auto page = (byte *)ut_align(buf2, UNIV_PAGE_SIZE);

  ulint flags{};
  space_id_t space_id;
  if (size >= off_t(FIL_IBD_FILE_INITIAL_SIZE * UNIV_PAGE_SIZE)) {
    success = os_file_read(file, page, UNIV_PAGE_SIZE, 0);

    /* We have to read the tablespace id from the file */

    space_id = fsp_header_get_space_id(page);
    flags = fsp_header_get_flags(page);
  } else {
    space_id = ULINT_UNDEFINED;
    flags = 0;
  }

  if (space_id == ULINT_UNDEFINED || space_id == 0) {

    ib_logger(ib_stream, "Tablespace id %lu in file %s is not sensible\n", (ulong)space_id, filepath);

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
  ut_delete(buf2);
  mem_free(filepath);
}

static db_err fil_scan_and_load_tablespaces(const std::string &dir, ib_recovery_t recovery, ulint max_depth, ulint depth) {
  namespace fs = std::filesystem;

  if (depth >= max_depth) {
    return DB_SUCCESS;
  }

  /* The datadir of the server is always the default directory. */
  fs::path path(dir);
  auto path_name = path.filename().string();

  auto check_directory = [path_name](const fs::path &path) -> db_err {
    if (fs::status(path).type() != fs::file_type::directory) {
      log_err("The datadir is not a directory: ", path_name);
      return DB_ERROR;
    }

    switch (fs::status(path).permissions()) {
      case fs::perms::owner_all:
        break;
      case fs::perms::owner_read:
      case fs::perms::owner_write:
        log_err("The datadir is not readable and writable: ", path_name);
        return DB_ERROR;
      default:
        log_err("The datadir is not writable: ", path_name);
        return DB_ERROR;
    }

    return DB_SUCCESS;
  };

  if (check_directory(path) != DB_SUCCESS) {
    return DB_ERROR;
  }

  const std::string ext = ".ibd";

  for (auto it{fs::directory_iterator(path)}; it != fs::directory_iterator(); ++it) {
    auto filename = it->path().filename().string();

    if (it->is_directory()) {
      if (check_directory(it->path()) != DB_SUCCESS) {
        return DB_ERROR;
      }

      /* Recursively load tablespaces. */
      auto err = fil_scan_and_load_tablespaces(filename, recovery, max_depth, depth + 1);

      if (err != DB_SUCCESS) {
        log_err("Failed to scan and load tablespaces in directory: ", filename);
      }

    } else if (it->is_regular_file() && it->path().filename().has_extension() &&
               it->path().filename().extension().string() == ext) {

      fil_load_single_table_tablespace(recovery, path_name.c_str(), filename.c_str());
    }
  }

  return DB_SUCCESS;
}

db_err fil_load_single_table_tablespaces(const std::string &dir, ib_recovery_t recovery, ulint max_depth) {
  return fil_scan_and_load_tablespaces(dir, recovery, max_depth, 0);
}

void fil_print_orphaned_tablespaces() {
  mutex_enter(&fil_system->m_mutex);

  auto space = UT_LIST_GET_FIRST(fil_system->m_space_list);

  while (space) {
    if (space->m_type == FIL_TABLESPACE && space->m_id != 0 && !space->m_mark) {
      ib_logger(ib_stream, "Warning: tablespace ");
      ut_print_filename(ib_stream, space->m_name);
      ib_logger(
        ib_stream,
        " of id %lu has no matching table in\n"
        "the InnoDB data dictionary.\n",
        (ulong)space->m_id
      );
    }

    space = UT_LIST_GET_NEXT(m_space_list, space);
  }

  mutex_exit(&fil_system->m_mutex);
}

bool fil_tablespace_deleted_or_being_deleted_in_mem(space_id_t id, int64_t version) {
  fil_space_t *space;

  ut_ad(fil_system);

  mutex_enter(&fil_system->m_mutex);

  space = fil_space_get_by_id(id);

  if (space == nullptr || space->m_is_being_deleted) {
    mutex_exit(&fil_system->m_mutex);

    return true;
  }

  if (version != ((int64_t)-1) && space->m_tablespace_version != version) {
    mutex_exit(&fil_system->m_mutex);

    return true;
  }

  mutex_exit(&fil_system->m_mutex);

  return false;
}

bool fil_tablespace_exists_in_mem(space_id_t id) {
  fil_space_t *space;

  ut_ad(fil_system);

  mutex_enter(&fil_system->m_mutex);

  space = fil_space_get_by_id(id);

  mutex_exit(&fil_system->m_mutex);

  return space != nullptr;
}

bool fil_space_for_table_exists_in_mem(
  space_id_t id, const char *name, bool is_temp, bool mark_space, bool print_error_if_does_not_exist
) {
  fil_space_t *fil_namespace;
  fil_space_t *space;
  char *path;

  ut_ad(fil_system);

  mutex_enter(&fil_system->m_mutex);

  path = fil_make_ibd_name(name, is_temp);

  /* Look if there is a space with the same id */

  space = fil_space_get_by_id(id);

  /* Look if there is a space with the same name; the name is the
  directory path from the datadir to the file */

  fil_namespace = fil_space_get_by_name(path);
  if (space && space == fil_namespace) {
    /* Found */

    if (mark_space) {
      space->m_mark = true;
    }

    mem_free(path);
    mutex_exit(&fil_system->m_mutex);

    return true;
  }

  if (!print_error_if_does_not_exist) {

    mem_free(path);
    mutex_exit(&fil_system->m_mutex);

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
        fil_namespace->m_name,
        (ulong)fil_namespace->m_id
      );
    }
  error_exit:
    ib_logger(ib_stream, "Please refer to the Embdedded InnoDB GitHub repository for details for how to resolve the issue.");

    mem_free(path);
    mutex_exit(&fil_system->m_mutex);

    return false;
  }

  if (0 != fil_tablename_compare(space->m_name, path)) {
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
      space->m_name
    );

    if (fil_namespace != nullptr) {
      ib_logger(ib_stream, "There is a tablespace with the right name\n");
      ut_print_filename(ib_stream, fil_namespace->m_name);
      ib_logger(ib_stream, ", but its id is %lu.\n", (ulong)fil_namespace->m_id);
    }

    goto error_exit;
  }

  mem_free(path);
  mutex_exit(&fil_system->m_mutex);

  return false;
}

/**
 * @brief Checks if a single-table tablespace for a given table name exists in the tablespace memory cache.
 * 
 * @param name table name in the standard 'databasename/tablename' format
 * @return space id, ULINT_UNDEFINED if not found
 */
static ulint fil_get_space_id_for_table(const char *name) {
  char *path;
  fil_space_t *fil_namespace;
  space_id_t id = ULINT_UNDEFINED;

  ut_ad(fil_system);

  mutex_enter(&fil_system->m_mutex);

  path = fil_make_ibd_name(name, false);

  /* Look if there is a space with the same name; the name is the
  directory path to the file */

  fil_namespace = fil_space_get_by_name(path);

  if (fil_namespace) {
    id = fil_namespace->m_id;
  }

  mem_free(path);

  mutex_exit(&fil_system->m_mutex);

  return id;
}

bool fil_extend_space_to_desired_size(ulint *actual_size, space_id_t space_id, ulint size_after_extend) {
  fil_mutex_enter_and_prepare_for_io(space_id);

  auto space = fil_space_get_by_id(space_id);

  if (space->m_size_in_pages >= size_after_extend) {
    *actual_size = space->m_size_in_pages;

    mutex_exit(&fil_system->m_mutex);

    return true;
  }

  auto page_size = UNIV_PAGE_SIZE;

  auto node = UT_LIST_GET_LAST(space->m_chain);

  fil_node_prepare_for_io(node, fil_system, space);

  auto start_page_no = space->m_size_in_pages;
  /* Extend at most 64 pages at a time */
  auto buf_size = ut_min(64, size_after_extend - start_page_no) * page_size;
  auto buf2 = (byte *)mem_alloc(buf_size + page_size);
  auto buf = (byte *)ut_align(buf2, page_size);

  memset(buf, 0, buf_size);

  bool success{true};
  auto io_request = IO_request::Sync_write;

  while (start_page_no < size_after_extend) {
    const ulint n_pages = ut_min(buf_size / page_size, size_after_extend - start_page_no);
    const off_t off = off_t(start_page_no) * page_size;

    IO_ctx io_ctx = {.m_batch = false, .m_fil_node = node, .m_msg = nullptr, .m_io_request = io_request};

    success = srv_aio->submit(std::move(io_ctx), buf, page_size * n_pages, off);

    if (success) {
      node->m_size_in_pages += n_pages;
      space->m_size_in_pages += n_pages;

      os_has_said_disk_full = false;
    } else {
      /* Let us measure the size of the file to determine how much we were able
       * to extend it */

      auto n_pages = ((ulint)(os_file_get_size_as_iblonglong(node->m_fh) / page_size)) - node->m_size_in_pages;

      node->m_size_in_pages += n_pages;
      space->m_size_in_pages += n_pages;

      break;
    }

    start_page_no += n_pages;
  }

  mem_free(buf2);

  fil_node_complete_io(node, fil_system, io_request);

  *actual_size = space->m_size_in_pages;

  if (space_id == 0) {
    ulint pages_per_mb = (1024 * 1024) / page_size;

    /* Keep the last data file size info up to date, rounded to
    full megabytes */

    srv_data_file_sizes[srv_n_data_files - 1] = (node->m_size_in_pages / pages_per_mb) * pages_per_mb;
  }

  mutex_exit(&fil_system->m_mutex);

  fil_flush(space_id);

  return success;
}

bool fil_space_reserve_free_extents(space_id_t id, ulint n_free_now, ulint n_to_reserve) {
  bool success;
  fil_space_t *space;

  ut_ad(fil_system);

  mutex_enter(&fil_system->m_mutex);

  space = fil_space_get_by_id(id);

  ut_a(space);

  if (space->m_n_reserved_extents + n_to_reserve > n_free_now) {
    success = false;
  } else {
    space->m_n_reserved_extents += n_to_reserve;
    success = true;
  }

  mutex_exit(&fil_system->m_mutex);

  return success;
}

void fil_space_release_free_extents(space_id_t id, ulint n_reserved) {
  fil_space_t *space;

  ut_ad(fil_system);

  mutex_enter(&fil_system->m_mutex);

  space = fil_space_get_by_id(id);

  ut_a(space);
  ut_a(space->m_n_reserved_extents >= n_reserved);

  space->m_n_reserved_extents -= n_reserved;

  mutex_exit(&fil_system->m_mutex);
}

ulint fil_space_get_n_reserved_extents(space_id_t id) {
  fil_space_t *space;
  ulint n;

  ut_ad(fil_system);

  mutex_enter(&fil_system->m_mutex);

  space = fil_space_get_by_id(id);

  ut_a(space);

  n = space->m_n_reserved_extents;

  mutex_exit(&fil_system->m_mutex);

  return n;
}

/**
 * @brief Prepares a file node for i/o. Opens the file if it is closed. Updates the
 * pending i/o's field in the node and the system appropriately. Takes the node
 * off the LRU list if it is in the LRU list. The caller must hold the fil_sys
 * mutex.
 *
 * @param node in: file node
 * @param system in: tablespace memory cache
 * @param space in: space
 */
static void fil_node_prepare_for_io(fil_node_t *node, fil_system_t *system, fil_space_t *space) {
  ut_ad(node && system && space);
  ut_ad(mutex_own(&(system->m_mutex)));

  if (system->m_n_open > system->m_max_n_open + 5) {
    ut_print_timestamp(ib_stream);
    ib_logger(
      ib_stream,
      "  Warning: open files %lu"
      " exceeds the limit %lu\n",
      (ulong)system->m_n_open,
      (ulong)system->m_max_n_open
    );
  }

  if (node->open == false) {
    /* File is closed: open it */
    ut_a(node->m_n_pending == 0);

    fil_node_open_file(node, system, space);
  }

  node->m_n_pending++;
}

static void fil_node_complete_io(fil_node_t *node, fil_system_t *system, IO_request io_request) {
  ut_ad(node != nullptr);
  ut_ad(mutex_own(&system->m_mutex));

  ut_a(node->m_n_pending > 0);

  --node->m_n_pending;

  if (io_request == IO_request::Sync_write) {
    ++system->m_modification_counter;
    node->m_modification_counter = system->m_modification_counter;

    if (!node->m_space->m_is_in_unflushed_spaces) {

      node->m_space->m_is_in_unflushed_spaces = true;
      UT_LIST_ADD_FIRST(system->m_unflushed_spaces, node->m_space);
    }
  }
}

/**
 * @brief Report information about an invalid page access.
 *
 * @param block_offset   in: block offset
 * @param space_id       in: space id
 * @param space_name     in: space name
 * @param byte_offset    in: byte offset
 * @param len            in: I/O length
 * @param io_request     in: I/O request type
 */
[[noreturn]] static void fil_report_invalid_page_access(
  ulint block_offset, space_id_t space_id, const char *space_name, ulint byte_offset, ulint len, IO_request io_request
) {
  log_fatal(std::format(
    "Trying to access page number {} in space {}, space name {}, which is"
    " outside the tablespace bounds. Byte offset {}, len {}, i/o type {}."
    " If you get this error at server startup, please check that your config"
    " file matches the ibdata files that you have in the server.",
    block_offset,
    space_id,
    space_name,
    byte_offset,
    len,
    (int)io_request
  ));
}

db_err fil_io(
  IO_request io_request, bool batched, space_id_t space_id, page_no_t page_no, ulint byte_offset, ulint len, void *buf,
  void *message
) {
  ut_ad(len > 0);
  ut_ad(buf != nullptr);
  ut_ad(byte_offset < UNIV_PAGE_SIZE);

  static_assert((1 << UNIV_PAGE_SIZE_SHIFT) == UNIV_PAGE_SIZE, "error (1 << UNIV_PAGE_SIZE_SHIFT) != UNIV_PAGE_SIZE");

  ut_ad(fil_validate());

  bool is_sync_request{};

  switch (io_request) {
    case IO_request::None:
      ut_error;
      break;
    case IO_request::Sync_log_read:
      is_sync_request = true;
      // fallthrough
    case IO_request::Async_log_read:
      break;

    case IO_request::Sync_log_write:
      is_sync_request = true;
      // falthrough
    case IO_request::Async_log_write:
      break;

    case IO_request::Sync_read:
      is_sync_request = true;
      // falthrough
    case IO_request::Async_read:
      srv_data_read += len;
      break;

    case IO_request::Sync_write:
      is_sync_request = true;
      // falthrough
    case IO_request::Async_write:
      srv_data_written += len;
      break;
  }

  /* Reserve the fil_system mutex and make sure that we can open at
  least one file while holding it, if the file is not already open */

  fil_mutex_enter_and_prepare_for_io(space_id);

  auto space = fil_space_get_by_id(space_id);

  if (space == nullptr) {
    mutex_exit(&fil_system->m_mutex);

    log_err(std::format(
      "Trying to do i/o to a tablespace which does not exist."
      " i/o type {}, space id {}, page no. {}, i/o length {} bytes",
      (int)io_request,
      space_id,
      page_no,
      len
    ));

    return DB_TABLESPACE_DELETED;
  }

  auto fil_node = UT_LIST_GET_FIRST(space->m_chain);

  for (;;) {
    if (unlikely(fil_node == nullptr)) {
      fil_report_invalid_page_access(page_no, space_id, space->m_name, byte_offset, len, io_request);
    }

    if (space->m_id != SYS_TABLESPACE && fil_node->m_size_in_pages == 0) {
      /* We do not know the size of a single-table tablespace
      before we open the file */

      break;
    }

    if (fil_node->m_size_in_pages > page_no) {
      break;
    } else {
      page_no -= fil_node->m_size_in_pages;
      fil_node = UT_LIST_GET_NEXT(m_chain, fil_node);
    }
  }

  /* Open file if closed */
  fil_node_prepare_for_io(fil_node, fil_system, space);

  /* Check that at least the start offset is within the bounds of a
  single-table tablespace */
  if (fil_node->m_size_in_pages <= page_no && space->m_id != SYS_TABLESPACE && space->m_type == FIL_TABLESPACE) {

    fil_report_invalid_page_access(page_no, space_id, space->m_name, byte_offset, len, io_request);
  }

  /* Now we have made the changes in the data structures of fil_system */
  mutex_exit(&fil_system->m_mutex);

  /* Calculate the low 32 bits and the high 32 bits of the file offset */

  off_t off = (off_t(page_no) * off_t(UNIV_PAGE_SIZE)) + byte_offset;

  ut_a(fil_node->m_size_in_pages - page_no >= ((byte_offset + len + (UNIV_PAGE_SIZE - 1)) / UNIV_PAGE_SIZE));

  /* Do aio */

  ut_a(byte_offset % IB_FILE_BLOCK_SIZE == 0);
  ut_a((len % IB_FILE_BLOCK_SIZE) == 0);

  IO_ctx io_ctx = {.m_batch = batched, .m_fil_node = fil_node, .m_msg = message, .m_io_request = io_request};

  /* Queue the aio request */
  auto err = srv_aio->submit(std::move(io_ctx), buf, len, off);
  ut_a(err == DB_SUCCESS);

  if (is_sync_request) {
    /* The i/o operation is already completed when we return from os_aio: */

    mutex_enter(&fil_system->m_mutex);

    fil_node_complete_io(fil_node, fil_system, io_request);

    mutex_exit(&fil_system->m_mutex);

    ut_ad(fil_validate());
  }

  return DB_SUCCESS;
}

bool fil_aio_wait(ulint segment) {
  ut_ad(fil_validate());

  IO_ctx io_ctx{};

  auto err = srv_aio->reap(segment, io_ctx);

  if (io_ctx.is_shutdown()) {
    return false;
  }

  ut_a(io_ctx.m_ret > 0);
  ut_a(err == DB_SUCCESS);

  mutex_enter(&fil_system->m_mutex);

  fil_node_complete_io(io_ctx.m_fil_node, fil_system, io_ctx.m_io_request);

  mutex_exit(&fil_system->m_mutex);

  ut_ad(fil_validate());

  /* Do the i/o handling */
  /* IMPORTANT: since i/o handling for reads will read also the insert
  buffer in tablespace 0, you have to be very careful not to introduce
  deadlocks in the i/o system. We keep tablespace 0 data files always
  open, and use a special i/o thread to serve insert buffer requests. */

  if (io_ctx.m_fil_node->m_space->m_type == FIL_TABLESPACE) {
    srv_buf_pool->io_complete(reinterpret_cast<buf_page_t *>(io_ctx.m_msg));
  } else {
    log_io_complete(reinterpret_cast<log_group_t *>(io_ctx.m_msg));
  }

  return true;
}

void fil_flush(space_id_t space_id) {
  os_file_t file;
  int64_t old_mod_counter;

  mutex_enter(&fil_system->m_mutex);

  auto space = fil_space_get_by_id(space_id);

  if (!space || space->m_is_being_deleted) {
    mutex_exit(&fil_system->m_mutex);

    return;
  }

  /** prevent dropping of the space while we are flushing */
  space->m_n_pending_flushes++;

  auto node = UT_LIST_GET_FIRST(space->m_chain);

  while (node) {
    if (node->m_modification_counter > node->m_flush_counter) {
      ut_a(node->open);

      /* We want to flush the changes at least up to
      old_mod_counter */
      old_mod_counter = node->m_modification_counter;

      if (space->m_type == FIL_TABLESPACE) {
        fil_n_pending_tablespace_flushes++;
      } else {
        fil_n_pending_log_flushes++;
        fil_n_log_flushes++;
      }
    retry:
      if (node->m_n_pending_flushes > 0) {
        /* We want to avoid calling os_file_flush() on
        the file twice at the same time, because we do
        not know what bugs OS's may contain in file
        i/o; sleep for a while */

        mutex_exit(&fil_system->m_mutex);

        os_thread_sleep(20000);

        mutex_enter(&fil_system->m_mutex);

        if (node->m_flush_counter >= old_mod_counter) {

          goto skip_flush;
        }

        goto retry;
      }

      ut_a(node->open);
      file = node->m_fh;
      node->m_n_pending_flushes++;

      mutex_exit(&fil_system->m_mutex);

      /* ib_logger(ib_stream, "Flushing to file %s\n",
      node->name); */

      os_file_flush(file);

      mutex_enter(&fil_system->m_mutex);

      node->m_n_pending_flushes--;
    skip_flush:
      if (node->m_flush_counter < old_mod_counter) {
        node->m_flush_counter = old_mod_counter;

        if (space->m_is_in_unflushed_spaces && fil_space_is_flushed(space)) {

          space->m_is_in_unflushed_spaces = false;

          UT_LIST_REMOVE(fil_system->m_unflushed_spaces, space);
        }
      }

      if (space->m_type == FIL_TABLESPACE) {
        fil_n_pending_tablespace_flushes--;
      } else {
        fil_n_pending_log_flushes--;
      }
    }

    node = UT_LIST_GET_NEXT(m_chain, node);
  }

  space->m_n_pending_flushes--;

  mutex_exit(&fil_system->m_mutex);
}

void fil_flush_file_spaces(ulint purpose) {
  mutex_enter(&fil_system->m_mutex);

  auto n_space_ids = UT_LIST_GET_LEN(fil_system->m_unflushed_spaces);
  if (n_space_ids == 0) {

    mutex_exit(&fil_system->m_mutex);
    return;
  }

  /* Assemble a list of space ids to flush.  Previously, we
  traversed fil_system->unflushed_spaces and called UT_LIST_GET_NEXT()
  on a space that was just removed from the list by fil_flush().
  Thus, the space could be dropped and the memory overwritten. */
  auto space_ids = (ulint *)mem_alloc(n_space_ids * sizeof(ulint));

  ulint i = 0;

  for (auto space = UT_LIST_GET_FIRST(fil_system->m_unflushed_spaces); space != nullptr;
       space = UT_LIST_GET_NEXT(m_unflushed_spaces, space)) {

    if (space->m_type == purpose && !space->m_is_being_deleted) {

      ut_ad(i < n_space_ids);
      space_ids[i] = space->m_id;
      ++i;
    }
  }

  n_space_ids = i;

  mutex_exit(&fil_system->m_mutex);

  /* Flush the spaces.  It will not hurt to call fil_flush() on
  a non-existing space id. */
  for (ulint i = 0; i < n_space_ids; i++) {

    fil_flush(space_ids[i]);
  }

  mem_free(space_ids);
}

bool fil_validate() {
  fil_node_t *fil_node;
  ulint n_open = 0;
  ulint i;

  mutex_enter(&fil_system->m_mutex);

  /* Look for spaces in the hash table */

  for (i = 0; i < hash_get_n_cells(fil_system->m_spaces); i++) {

    auto space = (fil_space_t *)HASH_GET_FIRST(fil_system->m_spaces, i);

    while (space != nullptr) {
      auto check = [](const fil_node_t *node) {
        ut_a(node->open || !node->m_n_pending);
      };

      ut_list_validate(space->m_chain, check);

      fil_node = UT_LIST_GET_FIRST(space->m_chain);

      while (fil_node != nullptr) {
        if (fil_node->m_n_pending > 0) {
          ut_a(fil_node->open);
        }

        if (fil_node->open) {
          n_open++;
        }
        fil_node = UT_LIST_GET_NEXT(m_chain, fil_node);
      }
      space = (fil_space_t *)HASH_GET_NEXT(m_hash, space);
    }
  }

  ut_a(fil_system->m_n_open == n_open);

  while (fil_node != nullptr) {
    ut_a(fil_node->m_n_pending == 0);
    ut_a(fil_node->open);
    ut_a(fil_node->m_space->m_type == FIL_TABLESPACE);
    ut_a(fil_node->m_space->m_id != 0);
  }

  mutex_exit(&fil_system->m_mutex);

  return true;
}

bool fil_addr_is_null(fil_addr_t addr) {
  return addr.m_page_no == FIL_NULL;
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

  mutex_free(&system->m_mutex);

  /* Free the hash elements. We don't remove them from the table
  because we are going to destroy the table anyway. */
  for (i = 0; i < hash_get_n_cells(system->m_spaces); i++) {
    auto space = (fil_space_t *)HASH_GET_FIRST(system->m_spaces, i);

    while (space) {
      fil_space_t *prev_space = space;

      space = (fil_space_t *)HASH_GET_NEXT(m_hash, prev_space);
      ut_a(prev_space->m_magic_n == FIL_SPACE_MAGIC_N);
      mem_free(prev_space);
    }
  }
  hash_table_free(system->m_spaces);

  /* The elements in this hash table are the same in system->spaces,
  therefore no need to free the individual elements. */
  hash_table_free(system->m_name_hash);

  ut_a(UT_LIST_GET_LEN(system->m_unflushed_spaces) == 0);
  ut_a(UT_LIST_GET_LEN(system->m_space_list) == 0);

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
