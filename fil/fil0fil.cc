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

/** @file fil/fil0fil.c
The tablespace memory cache

Created 10/25/1995 Heikki Tuuri
*******************************************************/

#include <fcntl.h>
#include <algorithm>
#include <filesystem>

#include "buf0buf.h"
#include "buf0flu.h"
#include "buf0lru.h"
#include "dict0dict.h"
#include "fil0fil.h"
#include "fsp0fsp.h"
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

To have fast access to a tablespace, we put the data structures to a hash
table. Each tablespace is given an unique 32-bit identifier.

Some operating systems do not support many open files at the same time,
though NT seems to tolerate at least 900 open files. Therefore, we put the
open files in an LRU-list. If we need to open another file, we may close the
file at the end of the LRU-list. When an i/o-operation is pending on a file,
the file cannot be closed. We take the file nodes with pending i/o-operations
out of the LRU-list and keep a count of pending operations. When an operation
completes, we decrement the count and return the file node to the LRU-list if
the count drops to zero. */


/** The null file address */
constexpr FilAddr fil_addr_null = {FIL_NULL, 0};

Fil::Fil(System &sys) noexcept : m_sys(sys), m_file_per_table(sys.m_config.m_fil.m_file_per_table) {
  const auto &config = m_sys.m_config.m_fil;

  ut_a(config.m_max_n_open_files > 0);

  mutex_create(&m_mutex, IF_DEBUG("Fil::mutex", )  Current_location());

  m_max_n_open = config.m_max_n_open_files;

  UT_LIST_INIT(m_space_list);
  UT_LIST_INIT(m_unflushed_spaces);
}

Fil::~Fil() noexcept {
  for (auto [id, space] : m_space_by_id) {
    ut_a(space->m_magic_n == FIL_SPACE_MAGIC_N);

    for (auto node : space->m_chain) {
      ut_a(node->m_magic_n == FIL_NODE_MAGIC_N);
      mem_free(node->m_file_name);
      delete node;
    }

    space->m_latch.destroy();

    mem_free(space->m_name);
    mem_free(space);
  }

  ut_a(UT_LIST_GET_LEN(m_space_list) == 0);
  ut_a(UT_LIST_GET_LEN(m_unflushed_spaces) == 0);

  mutex_free(&m_mutex);
}

const char *Fil::normalize_path(const char *ptr) {
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

int Fil::tablename_compare(const char *name1, const char *name2) {
  name1 = normalize_path(name1);
  name2 = normalize_path(name2);

  return strcmp(name1, name2);
}

FilSpace *Fil::space_get_by_id(space_id_t id) {
  ut_ad(mutex_own(&m_mutex));

  auto it{m_space_by_id.find(id)};

  return it != m_space_by_id.end() ? it->second : nullptr;
}

FilSpace *Fil::space_get_by_name(const char *name) {
  ut_ad(mutex_own(&m_mutex));

  auto it{m_space_by_name.find(name)};

  return it != m_space_by_name.end() ? it->second : nullptr;
}

int64_t Fil::space_get_version(space_id_t id) {
  mutex_enter(&m_mutex);

  auto space = space_get_by_id(id);

  int64_t version;

  if (space != nullptr) {
    version = space->m_tablespace_version;
  } else {
    version = -1;
  }

  mutex_exit(&m_mutex);

  return version;
}

RwLatch *Fil::space_get_latch(space_id_t id) {
  mutex_enter(&m_mutex);

  auto space = space_get_by_id(id);

  mutex_exit(&m_mutex);

  return &space->m_latch;
}

bool Fil::space_is_flushed(FilSpace *space) {
  ut_ad(mutex_own(&m_mutex));

  for (const auto node : space->m_chain) {

    if (node->m_modification_counter > node->m_flush_counter) {
      return false;
    }
  }

  return true;
}

void Fil::node_create(const char *name, ulint size, space_id_t id, bool is_raw) {
  mutex_enter(&m_mutex);

  auto node = new FilNode{};

  node->m_file_name = mem_strdup(name);
  node->open = false;

  ut_a(!is_raw);

  node->m_is_raw_disk = is_raw;
  node->m_size_in_pages = size;
  node->m_magic_n = FIL_NODE_MAGIC_N;
  node->m_n_pending = 0;
  node->m_n_pending_flushes = 0;

  node->m_modification_counter = 0;
  node->m_flush_counter = 0;

  auto space = space_get_by_id(id);

  if (space == nullptr) {
    mem_free(node->m_file_name);

    log_err(std::format("Could not find tablespace {} for file in the tablespace memory cache.", id));

    delete node;

  } else {

    space->m_size_in_pages += size;

    node->m_space = space;

    UT_LIST_ADD_LAST(space->m_chain, node);
    ut_a(UT_LIST_GET_LEN(space->m_chain) == 1);

    if (id < SRV_SPACE_ID_LIMIT && m_max_assigned_id < id) {

      m_max_assigned_id = id;
    }
  }

  mutex_exit(&m_mutex);
}

void Fil::node_open_file(FilNode *node, FilSpace *space, bool startup) {
  ut_ad(mutex_own(&m_mutex));
  ut_a(node->m_n_pending == 0);
  ut_a(node->open == false);

  if (node->m_size_in_pages == 0) {
    /* It must be a single-table tablespace and we do not know the size of the file yet: open it
    for reading only, check it, and close it again. */
    auto file = os::File::open(node->m_file_name, os::Open::READ);

    if (!file) {
      log_fatal(std::format("Cannot open '{}': {}. Have you deleted .ibd files under a running server?", node->m_file_name, file.error().message()));
    }

    const auto measured = file->size();

    if (!measured) {
      log_fatal(std::format("Cannot measure the size of '{}': {}", node->m_file_name, measured.error().message()));
    }

    /* The system tablespace is opened at the start and closed at shutdown. */
    ut_a(space->m_id != SYS_TABLESPACE || startup);

    const auto size = *measured;
    auto size_bytes = size;

    if (size_bytes < off_t(FIL_IBD_FILE_INITIAL_SIZE * UNIV_PAGE_SIZE)) {
      log_fatal(std::format(
        "The size of single-table tablespace file {} is only {}, should be at least {}!",
        node->m_file_name,
        size,
        (FIL_IBD_FILE_INITIAL_SIZE * UNIV_PAGE_SIZE)
      ));
    }

    auto ptr = static_cast<byte *>(ut_new(2 * UNIV_PAGE_SIZE));
    auto page = reinterpret_cast<page_t *>(ut_align(ptr, UNIV_PAGE_SIZE));

    if (const auto err = file->read(page, UNIV_PAGE_SIZE, 0); err) {
      log_fatal(std::format("Cannot read the first page of '{}': {}", node->m_file_name, err.message()));
    }

    auto flags = m_sys.m_fsp->get_flags(page);
    auto space_id = m_sys.m_fsp->get_space_id(page);

    ut_delete(ptr);

    /* Read only: nothing a close can lose. */
    (void)file->close();

    if (space_id != space->m_id) {
      log_fatal(
        std::format("Tablespace id is {} in the data dictionary but in file {} it is {}!", space->m_id, node->m_file_name, space_id)
      );
    }

    if (!startup && (space_id == ULINT_UNDEFINED || space_id == SYS_TABLESPACE)) {
      log_fatal(std::format("Tablespace id {} in file {} is not sensible", space_id, node->m_file_name));
    }

    if (space->m_flags != flags) {
      log_fatal(std::format(
        "Table flags are {} in the data dictionary but the flags in file {} are {:x}!", space->m_flags, node->m_file_name, flags
      ));
    }

    if (size_bytes >= 1024 * 1024) {
      /* Truncate the size to whole megabytes. */
      size_bytes = ut_2pow_round(size_bytes, 1024 * 1024);
    }

    node->m_size_in_pages = ulint(size_bytes / UNIV_PAGE_SIZE);

    space->m_size_in_pages += node->m_size_in_pages;
  }

  /* For reading and writing, past the OS cache if the flush method says so. */
  auto file = os::File::open(node->m_file_name, os::Open::READ_WRITE, m_sys.m_config.m_fil.m_flush_method == SRV_UNIX_O_DIRECT);

  if (!file) {
    log_fatal(std::format("Cannot open '{}' for I/O: {}", node->m_file_name, file.error().message()));
  }

  node->m_file = std::move(*file);
  node->open = true;

  ++m_n_open;
}

void Fil::node_close_file(FilNode *node) {
  ut_ad(mutex_own(&m_mutex));
  ut_a(node->open);
  ut_a(node->m_n_pending == 0);
  ut_a(node->m_n_pending_flushes == 0);
  ut_a(node->m_modification_counter == node->m_flush_counter);

  /* Everything written is flushed, see the asserts: an error of close() is the file system's. */
  if (const auto err = node->m_file.close(); err) {
    log_fatal(std::format("Cannot close '{}': {}", node->m_file_name, err.message()));
  }

  node->open = false;
  ut_a(m_n_open > 0);

  --m_n_open;
}

void Fil::mutex_enter_and_prepare_for_io(space_id_t space_id) {
  ulint n_retries{};
  ulint n_stopped_ios{};

  for (;;) {
    mutex_enter(&m_mutex);

    if (space_id == SYS_TABLESPACE) {
      /* We keep system tablespace files always open;
      this is important in preventing deadlocks in this module, as
      a page read completion often performs another read from the
      insert buffer. The insert buffer is in tablespace 0, and we
      cannot end up waiting in this function. */

      return;
    }

    if (m_n_open < m_max_n_open) {

      return;
    }

    auto space = space_get_by_id(space_id);

    /* If the file is already open, no need to do anything; if the space
    does not exist, we handle the situation in the function which called
    this function */

    if (space == nullptr || UT_LIST_GET_FIRST(space->m_chain)->open) {

      return;

    } else if (space->m_stop_ios) {

      /* We are going to do a rename file and want to stop new i/o's
      for a while */

      if (n_stopped_ios > 20000) {
        log_warn(std::format("Tablespace {} has i/o ops stopped for a long time {}", space->m_name, n_stopped_ios));
      }

      ++n_stopped_ios;

    } else if (m_n_open < m_max_n_open) {
      /* Ok */

      return;

    } else if (n_retries >= 2) {

      log_warn(std::format(
        "Too many ({}u) files stay open while the maximum"
        " allowed value would be {}. You may need to raise the value of"
        " max_files_open",
        m_n_open,
        m_max_n_open
      ));

      return;
    }

    mutex_exit(&m_mutex);

    os_thread_sleep(20000);

    if (!space->m_stop_ios) {

      /* Flush tablespaces so that we can close modified files in the LRU list */
      flush_file_spaces();

      ++n_retries;
    }
  }
}

void Fil::node_free(FilNode *node, FilSpace *space) {
  ut_ad(mutex_own(&(m_mutex)));
  ut_a(node->m_magic_n == FIL_NODE_MAGIC_N);
  ut_a(node->m_n_pending == 0);

  if (node->open) {
    /* We fool the assertion in fil_node_close_file() to think
    there are no unflushed modifications in the file */

    node->m_modification_counter = node->m_flush_counter;

    if (space->m_is_in_unflushed_spaces && space_is_flushed(space)) {

      space->m_is_in_unflushed_spaces = false;

      UT_LIST_REMOVE(m_unflushed_spaces, space);
    }

    node_close_file(node);
  }

  space->m_size_in_pages -= node->m_size_in_pages;

  UT_LIST_REMOVE(space->m_chain, node);

  mem_free(node->m_file_name);
  delete node;
}

bool Fil::space_create(const char *name, space_id_t id, ulint flags) {
  FilSpace *space;

  /* FIXME: This is redundant now. */
  ut_a(flags == 0);

try_again:
  mutex_enter(&m_mutex);

  space = space_get_by_name(name);

  if (likely_null(space)) {
    ulint namesake_id;

    log_warn(std::format(
      "Trying to init to the tablespace memory cache a tablespace {} of name {}"
      " but a tablespace {} of the same name already exists in the"
      " tablespace memory cache!",
      name,
      id,
      space->m_id
    ));

    if (id == SYS_TABLESPACE) {

      mutex_exit(&m_mutex);

      return false;
    }

    log_warn(
      "We assume that InnoDB did a crash recovery, and you had"
      " an .ibd file for which the table did not exist in the"
      " InnoDB internal data dictionary in the ibdata files."
      " We assume that you later removed the .ibd file,and are"
      " now trying to recreate the table. We now remove the"
      " conflicting tablespace object from the memory cache and try"
      " the init again."
    );

    namesake_id = space->m_id;

    mutex_exit(&m_mutex);

    space_free(namesake_id, false);

    goto try_again;
  }

  space = space_get_by_id(id);

  if (likely_null(space)) {
    log_err(
      "Trying to add tablespace ",
      id,
      " of name ",
      name,
      " to the tablespace memory cache, but tablespace ",
      space->m_id,
      " of name ",
      space->m_name,
      " already exists in the tablespace memory cache!"
    );

    mutex_exit(&m_mutex);

    return false;
  }

  space = static_cast<FilSpace *>(mem_alloc(sizeof(FilSpace)));

  space->m_name = mem_strdup(name);
  space->m_id = id;

  ++m_tablespace_version;

  space->m_tablespace_version = m_tablespace_version;
  space->m_mark = false;

  if (id > m_max_assigned_id) {
    m_max_assigned_id = id;
  }

  space->m_stop_ios = false;
  space->m_is_being_deleted = false;
  space->m_is_being_extended = false;
  space->m_size_in_pages = 0;
  space->m_flags = flags;

  space->m_n_reserved_extents = 0;

  space->m_n_pending_flushes = 0;

  UT_LIST_INIT(space->m_chain);
  space->m_magic_n = FIL_SPACE_MAGIC_N;

  space->m_latch.create();

  {
    auto r{m_space_by_id.emplace(id, space)};
    ut_a(r.second);
  }

  {
    auto r = m_space_by_name.emplace(space->m_name, space);
    ut_a(r.second);
  }

  space->m_is_in_unflushed_spaces = false;

  UT_LIST_ADD_LAST(m_space_list, space);

  mutex_exit(&m_mutex);

  return true;
}

ulint Fil::assign_new_space_id() {
  mutex_enter(&m_mutex);

  ++m_max_assigned_id;

  auto id = m_max_assigned_id;

  if (id > (SRV_SPACE_ID_LIMIT / 2) && (id % 1000000UL) == 0) {
    log_warn(std::format(
      "You are running out of new single-table tablespace id's."
      " Current counter is {} and it must not exceed {}!"
      " To reset the counter to zero you have to dump all your tables and"
      " recreate the whole InnoDB installation.",
      id,
      SRV_SPACE_ID_LIMIT
    ));
  }

  if (id >= SRV_SPACE_ID_LIMIT) {
    log_warn(std::format(
      "You have run out of single-table tablespace id's!"
      " Current counter is {}. To reset the counter to zero you"
      " have to dump all your tables and recreate the whole InnoDB"
      " installation.",
      id
    ));

    --m_max_assigned_id;

    id = ULINT_UNDEFINED;
  }

  mutex_exit(&m_mutex);

  return id;
}

bool Fil::space_free(space_id_t id, bool own_mutex) {
  if (!own_mutex) {
    mutex_enter(&m_mutex);
  }

  auto space = space_get_by_id(id);

  if (space == nullptr) {
    log_err(std::format(
      "Trying to remove tablespace {} from the cache but"
      " it is not there.",
      id
    ));

    mutex_exit(&m_mutex);

    return false;
  }

  {
    auto r{m_space_by_id.erase(id)};
    ut_a(r == 1);
  }

  {
    auto found_space = space_get_by_name(space->m_name);
    ut_a(found_space != nullptr);
    ut_a(space == found_space);
  }

  {
    auto r{m_space_by_name.erase(space->m_name)};
    ut_a(r == 1);
  }

  if (space->m_is_in_unflushed_spaces) {
    space->m_is_in_unflushed_spaces = false;

    UT_LIST_REMOVE(m_unflushed_spaces, space);
  }

  UT_LIST_REMOVE(m_space_list, space);

  ut_a(space->m_magic_n == FIL_SPACE_MAGIC_N);
  ut_a(space->m_n_pending_flushes == 0);

  auto node = UT_LIST_GET_FIRST(space->m_chain);

  while (node != nullptr) {

    node_free(node, space);

    node = UT_LIST_GET_FIRST(space->m_chain);
  }

  ut_a(UT_LIST_GET_LEN(space->m_chain) == 0);

  if (!own_mutex) {
    mutex_exit(&m_mutex);
  }

  space->m_latch.destroy();

  mem_free(space->m_name);
  mem_free(space);

  return true;
}

ulint Fil::space_get_size(space_id_t id) {
  mutex_enter_and_prepare_for_io(id);

  auto space = space_get_by_id(id);

  if (space == nullptr) {
    mutex_exit(&m_mutex);

    return 0;
  }

  if (space->m_size_in_pages == 0) {
    ut_a(id != SYS_TABLESPACE);

    ut_a(UT_LIST_GET_LEN(space->m_chain) == 1);

    auto node = UT_LIST_GET_FIRST(space->m_chain);

    /* It must be a single-table tablespace and we have not opened
    the file yet; the following calls will open it and update the
    size fields */

    node_prepare_for_io(node, space);
    node_complete_io(node, IoRequest::Sync_read);
  }

  auto size = space->m_size_in_pages;

  mutex_exit(&m_mutex);

  return size;
}

ulint Fil::space_get_flags(space_id_t id) {

  if (id == SYS_TABLESPACE) {
    return 0;
  }

  mutex_enter_and_prepare_for_io(id);

  auto space = space_get_by_id(id);

  if (space == nullptr) {
    mutex_exit(&m_mutex);

    return ULINT_UNDEFINED;
  }

  if (space->m_size_in_pages == 0) {
    ut_a(id != SYS_TABLESPACE);

    ut_a(UT_LIST_GET_LEN(space->m_chain) == 1);

    auto node = UT_LIST_GET_FIRST(space->m_chain);

    /* It must be a single-table tablespace and we have not opened
    the file yet; the following calls will open it and update the
    size fields */

    node_prepare_for_io(node, space);
    node_complete_io(node, IoRequest::Sync_read);
  }

  auto flags = space->m_flags;

  mutex_exit(&m_mutex);

  return flags;
}

bool Fil::check_adress_in_tablespace(space_id_t id, page_no_t page_no) {
  return space_get_size(id) > page_no;
}

void Fil::open_system_tablespace() {
  mutex_enter(&m_mutex);

  for (auto space : m_space_list) {
    if (space->m_id == SYS_TABLESPACE) {
      ut_a(UT_LIST_GET_LEN(space->m_chain) == 1);

      auto node = UT_LIST_GET_FIRST(space->m_chain);

      if (!node->open) {
        node_open_file(node, space, true);
      }
    }
  }

  mutex_exit(&m_mutex);
}

void Fil::close_all_files() {
  mutex_enter(&m_mutex);

  auto space = UT_LIST_GET_FIRST(m_space_list);

  while (space != nullptr) {
    FilSpace *prev_space = space;

    ut_a(UT_LIST_GET_LEN(space->m_chain) == 1);
    auto node = UT_LIST_GET_FIRST(space->m_chain);

    if (node->open) {
      node_close_file(node);
    }

    space = UT_LIST_GET_NEXT(m_space_list, space);

    space_free(prev_space->m_id, true);
  }

  mutex_exit(&m_mutex);
}

void Fil::set_max_space_id_if_bigger(space_id_t max_id) {
  if (max_id >= SRV_SPACE_ID_LIMIT) {
    log_fatal("Max tablespace id is too high, ", max_id);
  }

  mutex_enter(&m_mutex);

  if (m_max_assigned_id < max_id) {

    m_max_assigned_id = max_id;
  }

  mutex_exit(&m_mutex);
}

DbErr Fil::write_lsn_and_arch_no_to_file(ulint sum_of_sizes, lsn_t lsn) {
  auto ptr = static_cast<byte *>(mem_alloc(2 * UNIV_PAGE_SIZE));
  auto buf = static_cast<byte *>(ut_align(ptr, UNIV_PAGE_SIZE));

  data_io(IoRequest::Sync_read, false, PageId(SYS_TABLESPACE, sum_of_sizes), 0, UNIV_PAGE_SIZE, buf, nullptr);

  mach_write_to_8(buf + FIL_PAGE_FILE_FLUSH_LSN, lsn);

  data_io(IoRequest::Sync_write, false, PageId(SYS_TABLESPACE, sum_of_sizes), 0, UNIV_PAGE_SIZE, buf, nullptr);

  mem_free(ptr);

  return DB_SUCCESS;
}

DbErr Fil::write_flushed_lsn_to_data_files(lsn_t lsn) {
  ulint sum_of_sizes;

  mutex_enter(&m_mutex);

  auto space = UT_LIST_GET_FIRST(m_space_list);

  while (space) {
    /* We only write the lsn to all existing data files which have
    been open during the lifetime of the server process; they are
    represented by the space objects in the tablespace memory
    cache. Note that all data files in the system tablespace 0 are
    always open. */

    if (space->m_id == 0) {
      sum_of_sizes = 0;

      auto node = UT_LIST_GET_FIRST(space->m_chain);

      while (node != nullptr) {
        mutex_exit(&m_mutex);

        auto err = write_lsn_and_arch_no_to_file(sum_of_sizes, lsn);

        if (err != DB_SUCCESS) {

          return err;
        }

        mutex_enter(&m_mutex);

        sum_of_sizes += node->m_size_in_pages;
        node = UT_LIST_GET_NEXT(m_chain, node);
      }
    }
    space = UT_LIST_GET_NEXT(m_space_list, space);
  }

  mutex_exit(&m_mutex);

  return DB_SUCCESS;
}

void Fil::read_flushed_lsn(const os::File &file, lsn_t &flushed_lsn) {
  auto ptr = static_cast<byte *>(ut_new(2 * UNIV_PAGE_SIZE));
  /* Align the memory for a possible read from a raw device */
  auto buf = static_cast<byte *>(ut_align(ptr, UNIV_PAGE_SIZE));

  if (const auto err = file.read(buf, UNIV_PAGE_SIZE, 0); err) {
    log_fatal("Cannot read the flushed LSN: ", err.message());
  }

  flushed_lsn = mach_read_from_8(buf + FIL_PAGE_FILE_FLUSH_LSN);

  ut_delete(ptr);
}

void Fil::create_directory_for_tablename(const char *name) {
  const auto &home = m_sys.m_config.m_fil.m_data_home;

  ut_a(!home.empty());

  const auto namend = strchr(name, '/');

  ut_a(namend != nullptr);

  const auto dir = std::filesystem::path(home) / std::string_view(name, size_t(namend - name));
  std::error_code ec;

  /* One that exists is no error. */
  (void)std::filesystem::create_directory(dir, ec);

  if (ec) {
    log_fatal(std::format("Cannot create the directory {}: {}", dir.string(), ec.message()));
  }
}

void Fil::op_write_log(
  MlogType type, space_id_t space_id, ulint log_flags, ulint flags, const char *name, const char *new_name, Mtr *mtr
) {
  auto log_ptr = mlog_open(mtr, 11 + 2 + 1);

  if (log_ptr == nullptr) {
    /* Logging in mtr is switched off during crash recovery:
    in that case mlog_open returns nullptr */
    return;
  }

  log_ptr = mlog_write_initial_log_record_for_file_op(type, space_id, log_flags, log_ptr, mtr);

  mach_write_to_4(log_ptr, flags);
  log_ptr += 4;

  /* Let us store the strings as null-terminated for easier readability
  and handling */

  auto len = strlen(name) + 1;

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

void Fil::extend_write_log(space_id_t space_id, page_no_t size, Mtr *mtr) noexcept {
  auto log_ptr = mlog_open(mtr, 11 + 4);

  if (log_ptr == nullptr) {
    /* Logging is off in recovery. */
    return;
  }

  log_ptr = mlog_write_initial_log_record_for_file_op(MLOG_FILE_EXTEND, space_id, 0, log_ptr, mtr);

  mach_write_to_4(log_ptr, size);
  log_ptr += 4;

  mlog_close(mtr, log_ptr);
}

void Fil::extend_replay(space_id_t space_id, page_no_t size) noexcept {
  mutex_enter(&m_mutex);

  const auto space = space_get_by_id(space_id);
  const bool known = space != nullptr && !space->m_is_being_deleted;

  mutex_exit(&m_mutex);

  if (!known) {
    return;
  }

  page_no_t actual{};

  if (!extend_space_to_desired_size(&actual, space_id, size)) {
    log_fatal(std::format("Cannot extend tablespace {} to {} pages in recovery", space_id, size));
  }
}

byte *Fil::op_log_parse_or_replay(byte *ptr, byte *end_ptr, ulint type, space_id_t space_id, ulint log_flags) {
  ulint name_len;
  ulint new_name_len;
  const char *name;
  const char *new_name = nullptr;
  ulint flags = 0;

  if (end_ptr < ptr + 4) {

    return nullptr;
  }

  flags = mach_read_from_4(ptr);
  ptr += 4;

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
      if (tablespace_exists_in_mem(space_id)) {
        ut_a(delete_tablespace(space_id));
      }

      break;

    case MLOG_FILE_RENAME:
      /* We do the rename based on space id, not old file name;
    this should guarantee that after the log replay each .ibd file
    has the correct name for the latest log sequence number; the
    proof is left as an exercise :) */

      if (tablespace_exists_in_mem(space_id)) {
        /* Create the database directory for the new name, if
      it does not exist yet */
        create_directory_for_tablename(new_name);

        /* Rename the table if there is not yet a tablespace
      with the same name */

        if (get_space_id_for_table(new_name) == ULINT_UNDEFINED) {
          /* We do not care of the old name, that is
        why we pass nullptr as the first argument */
          if (!rename_tablespace(nullptr, space_id, new_name)) {
            ut_error;
          }
        }
      }

      break;

    case MLOG_FILE_CREATE:
      if (tablespace_exists_in_mem(space_id)) {
        /* Do nothing */
      } else if (get_space_id_for_table(name) != ULINT_UNDEFINED) {
        /* Do nothing */
      } else if (log_flags & MLOG_FILE_FLAG_TEMP) {
        /* Temporary table, do nothing */
      } else {
        /* Create the database directory for name, if it does
      not exist yet */
        create_directory_for_tablename(name);

        if (create_new_single_table_tablespace(&space_id, name, false, flags, FIL_IBD_FILE_INITIAL_SIZE) != DB_SUCCESS) {
          ut_error;
        }
      }

      break;

    default:
      ut_error;
  }

  return ptr;
}

bool Fil::delete_tablespace(space_id_t id) {
  ut_a(id != SYS_TABLESPACE);

  auto delete_file = [this](space_id_t id, const char *name) -> bool {
    auto path = mem_strdup(name);

    mutex_exit(&m_mutex);

    /* Invalidate in the buffer pool all pages belonging to the tablespace.
    Since we have set space->is_being_deleted = true, readahead can no longer
    read more pages of this tablespace to the buffer pool. Thus we can clean
    the tablespace out of the buffer pool completely and permanently. The flag
    is_being_deleted also prevents Fil::flush() from being applied to this
    tablespace. */

    auto success = space_free(id, false);

    if (success) {
      Mtr mtr;

      /* Write a log record to replay during recovery. */
      mtr.start();

      op_write_log(MLOG_FILE_DELETE, id, 0, 0, path, nullptr, &mtr);

      mtr.commit(m_sys);

      /* A file that's gone already is no error. */
      std::error_code ec;

      (void)std::filesystem::remove(path, ec);

      success = !ec;

      if (ec) {
        log_err(std::format("Cannot delete {}: {}", path, ec.message()));
      }
    }

    if (!success) {
      log_err("Failed to delete file: ", path);
    }

    mem_free(path);

    return success;
  };

  ulint count{1};

  for (;;) {
    mutex_enter(&m_mutex);

    auto space = space_get_by_id(id);

    if (space == nullptr) {
      log_err(std::format(
        "Cannot delete tablespace {} because it is not found in the"
        " tablespace memory cache.",
        id
      ));

      mutex_exit(&m_mutex);

      return false;
    }

    space->m_is_being_deleted = true;

    ut_a(UT_LIST_GET_LEN(space->m_chain) == 1);

    auto node = UT_LIST_GET_FIRST(space->m_chain);

    if (space->m_n_pending_flushes == 0 && node->m_n_pending == 0) {
      return delete_file(id, space->m_name);
    }

    if (!(count % 1000)) {
      log_warn(std::format(
        "Trying to delete tablespace {}, but there are {} flushes and {}"
        " pending i/o's on it, loop count {}.",
        space->m_name,
        space->m_n_pending_flushes,
        node->m_n_pending,
        count
      ));
    }

    mutex_exit(&m_mutex);

    os_thread_sleep(20000);

    ++count;
  }
}

bool Fil::discard_tablespace(space_id_t id) {
  auto success = delete_tablespace(id);

  if (!success) {
    log_warn(std::format("Cannot delete tablespace {} in DISCARD TABLESPACE.", id));
  }

  return success;
}

bool Fil::rename_tablespace_in_mem(FilSpace *space, FilNode *node, const char *path) {
  auto old_name = space->m_name;

  ut_ad(mutex_own(&m_mutex));

  auto found_space = space_get_by_name(old_name);

  if (space != found_space) {
    log_err("Cannot find ", old_name, "in tablespace memory cache");

    return false;
  }

  found_space = space_get_by_name(path);

  if (found_space != nullptr) {
    log_err(path, " is already in tablespace memory cache");

    return false;
  }

  {
    auto r{m_space_by_name.erase(old_name)};
    ut_a(r == 1);
  }

  mem_free(space->m_name);
  mem_free(node->m_file_name);

  space->m_name = mem_strdup(path);
  node->m_file_name = mem_strdup(path);

  {
    auto r{m_space_by_name.emplace(space->m_name, space)};
    ut_a(r.second);
  }

  return true;
}

char *Fil::make_ibd_name(const char *name, bool is_temp) {
  ulint namelen = strlen(name);
  auto dirlen = m_sys.m_config.m_fil.m_data_home.size();
  auto sz = dirlen + namelen + sizeof("/.ibd");
  auto filename = static_cast<char *>(mem_alloc(sz));

  std::snprintf(filename, sz, "%s%s.ibd", normalize_path(m_sys.m_config.m_fil.m_data_home.c_str()), name);

  return filename;
}

bool Fil::rename_tablespace(const char *old_name, space_id_t id, const char *new_name) {
  bool success;
  FilSpace *space;
  FilNode *node;
  ulint count = 0;
  char *path;
  bool old_name_was_specified = true;
  char *old_path;

  ut_a(id != SYS_TABLESPACE);

  if (old_name == nullptr) {
    old_name = "(name not specified)";
    old_name_was_specified = false;
  }

retry:
  count++;

  if (count > 1000) {
    log_warn(std::format("Problems renaming {} to {}, {} iterations", old_name, new_name, count));
  }

  mutex_enter(&m_mutex);

  space = space_get_by_id(id);

  if (space == nullptr) {

    log_err(std::format(
      "Cannot find space id {} in the tablespace memory cache"
      " though the table {} in a rename operation should have that id",
      id,
      old_name
    ));

    mutex_exit(&m_mutex);

    return false;
  }

  if (count > 25000) {
    space->m_stop_ios = false;
    mutex_exit(&m_mutex);

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

    mutex_exit(&m_mutex);

    os_thread_sleep(20000);

    goto retry;

  } else if (node->m_modification_counter > node->m_flush_counter) {
    /* Flush the space */

    mutex_exit(&m_mutex);

    os_thread_sleep(20000);

    flush(id);

    goto retry;

  } else if (node->open) {
    /* Close the file */

    node_close_file(node);
  }

  /* Check that the old name in the space is right */

  if (old_name_was_specified) {
    old_path = make_ibd_name(old_name, false);

    ut_a(tablename_compare(space->m_name, old_path) == 0);
    ut_a(tablename_compare(node->m_file_name, old_path) == 0);
  } else {
    old_path = mem_strdup(space->m_name);
  }

  /* Rename the tablespace and the node in the memory cache */
  path = make_ibd_name(new_name, false);
  success = rename_tablespace_in_mem(space, node, path);

  if (success) {
    std::error_code ec;

    std::filesystem::rename(old_path, path, ec);

    success = !ec;

    if (ec) {
      log_err(std::format("Cannot rename {} to {}: {}", old_path, path, ec.message()));
    }

    if (!success) {
      /* We have to revert the changes we made
      to the tablespace memory cache */

      ut_a(rename_tablespace_in_mem(space, node, old_path));
    }
  }

  mem_free(path);
  mem_free(old_path);

  space->m_stop_ios = false;

  mutex_exit(&m_mutex);

  if (success) {
    Mtr mtr;

    mtr.start();

    op_write_log(MLOG_FILE_RENAME, id, 0, 0, old_name, new_name, &mtr);
    mtr.commit(m_sys);
  }

  return success;
}

DbErr Fil::create_new_single_table_tablespace(space_id_t *space_id, const char *tablename, bool is_temp, ulint flags, ulint size) {
  bool success;

  ut_a(flags == 0);
  ut_a(size >= FIL_IBD_FILE_INITIAL_SIZE);

  auto path = make_ibd_name(tablename, is_temp);
  auto opened = os::File::open(path, os::Open::CREATE, m_sys.m_config.m_fil.m_flush_method == SRV_UNIX_O_DIRECT);

  if (!opened) {
    const auto err = opened.error();

    log_err(std::format("Unable to create file {}: {}", path, err.message()));

    mem_free(path);

    if (err == std::errc::file_exists) {
      log_err(
        "The file already exists though the corresponding table did not"
        " exist in the InnoDB data dictionary. Have you moved InnoDB"
        " .ibd files around without using the SQL commands DISCARD TABLESPACE"
        " and IMPORT TABLESPACE, or did the server crash in the middle of"
        " CREATE TABLE? You can resolve the problem by removing the file"
        " under the 'datadir' of the server."
      );

      return DB_TABLESPACE_ALREADY_EXISTS;
    }

    return err == std::errc::no_space_on_device ? DB_OUT_OF_FILE_SPACE : DB_ERROR;
  }

  auto file = std::move(*opened);

  /* Until it's a tablespace of the cache, a file that failed goes again. */
  auto err_exit = [&file](char *path, DbErr err) -> DbErr {
    (void)file.close();

    std::error_code ec;

    (void)std::filesystem::remove(path, ec);

    mem_free(path);

    return err;
  };

  if (const auto err = file.allocate(0, off_t(size) * UNIV_PAGE_SIZE); err) {
    log_err(std::format("Unable to allocate {} pages to {}: {}", size, path, err.message()));

    return err_exit(path, err == std::errc::no_space_on_device ? DB_OUT_OF_FILE_SPACE : DB_ERROR);
  }

  if (*space_id == SYS_TABLESPACE) {
    *space_id = assign_new_space_id();
  }

  if (*space_id == ULINT_UNDEFINED) {
    return err_exit(path, DB_ERROR);
  }

  /* We have to write the space id to the file immediately and flush the
  file to disk. This is because in crash recovery we must be aware what
  tablespaces exist and what are their space id's, so that we can apply
  the log records to the right file. It may take quite a while until
  buffer pool flush algorithms write anything to the file and flush it to
  disk. If we would not write here anything, the file would be all zeros
  until a buffer pool flush would write to it. */

  auto ptr = static_cast<byte *>(ut_new(3 * UNIV_PAGE_SIZE));
  auto page = static_cast<byte *>(ut_align(ptr, UNIV_PAGE_SIZE));

  memset(page, '\0', UNIV_PAGE_SIZE);

  m_sys.m_fsp->init_fields(page, *space_id, flags);

  mach_write_to_4(page + FIL_PAGE_SPACE_ID, *space_id);

  m_sys.m_buf_pool->m_flusher->init_for_writing(page, 0);

  const auto write_err = file.write(page, UNIV_PAGE_SIZE, 0);

  ut_delete(ptr);

  if (write_err) {
    log_err(std::format("Could not write the first page to tablespace {}: {}", path, write_err.message()));

    return err_exit(path, DB_ERROR);
  }

  if (const auto err = file.sync(); err) {
    log_err(std::format("File flush of tablespace {} failed: {}", path, err.message()));

    return err_exit(path, DB_ERROR);
  }

  if (const auto err = file.close(); err) {
    log_err(std::format("Could not close tablespace {}: {}", path, err.message()));

    return err_exit(path, DB_ERROR);
  }

  success = space_create(path, *space_id, flags);

  if (!success) {
    return err_exit(path, DB_ERROR);
  }

  node_create(path, size, *space_id, false);

  {
    Mtr mtr;

    mtr.start();

    op_write_log(MLOG_FILE_CREATE, *space_id, is_temp ? MLOG_FILE_FLAG_TEMP : 0, flags, tablename, nullptr, &mtr);

    mtr.commit(m_sys);
  }

  mem_free(path);

  return DB_SUCCESS;
}

bool Fil::open_single_table_tablespace(bool check_space_id, space_id_t id, ulint flags, const char *name) {
  auto filepath = make_ibd_name(name, false);

  /* ROW_FORMAT=REDUNDANT (table->flags == 0).  For any other format, the tablespace flags
  should equal (table->flags & ~(~0UL << DICT_TF_BITS)). */
  ut_a(!(flags & (~0UL << DICT_TF_BITS)));

  auto file = os::File::open(filepath, os::Open::READ);

  if (!file) {
    log_err(std::format(
      "Trying to open a table, but could not open the tablespace file {}: {}!"
      " Have you moved InnoDB .ibd files around without using the commands DISCARD TABLESPACE and"
      " IMPORT TABLESPACE? It is also possible that this is a temporary table ..., and the server"
      " removed the .ibd file for this.",
      filepath,
      file.error().message()
    ));

    mem_free(filepath);

    return false;
  }

  space_id_t space_id{id};

  if (check_space_id) {
    /* The first page, aligned for O_DIRECT: the tablespace id and flags are on it. */
    auto buf2 = static_cast<byte *>(ut_new(2 * UNIV_PAGE_SIZE));
    auto page = static_cast<byte *>(ut_align(buf2, UNIV_PAGE_SIZE));

    if (const auto err = file->read(page, UNIV_PAGE_SIZE, 0); err) {
      log_err(std::format("Could not read the first page of {}: {}", filepath, err.message()));

      ut_delete(buf2);
      mem_free(filepath);

      return false;
    }

    space_id = m_sys.m_fsp->get_space_id(page);

    const auto space_flags = m_sys.m_fsp->get_flags(page);

    ut_delete(buf2);

    if (unlikely(space_id != id || space_flags != (flags & ~(~0UL << DICT_TF_BITS)))) {
      log_err(std::format(
        "Tablespace id and flags in file {} are {} and {}, but in the InnoDB data"
        " dictionary they are {} and {}. Have you moved InnoDB .ibd files"
        " around without using the commands DISCARD TABLESPACE and IMPORT TABLESPACE?",
        filepath,
        space_id,
        space_flags,
        id,
        flags
      ));

      mem_free(filepath);

      return false;
    }
  }

  if (space_create(filepath, space_id, flags)) {
    /* We do not measure the size of the file, that is why we pass the 0 below */
    node_create(filepath, 0, space_id, false);
  }

  mem_free(filepath);

  return true;
}

void Fil::load_single_table_tablespace(IbRecovery recovery, const char *dbname, const char *filename) {
  /* Under the data home, without its leading ./ */
  const auto filepath = (std::filesystem::path(normalize_path(m_sys.m_config.m_fil.m_data_home.c_str())) / dbname / filename).string();

  auto file = os::File::open(filepath, os::Open::READ);

  if (!file) {
    log_err(std::format(
      "Could not open single-table tablespace file {}: {}!"
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
      " and force InnoDB to continue crash recovery here.",
      filepath,
      file.error().message()
    ));

    if (recovery != IB_RECOVERY_DEFAULT) {
      log_err(std::format(
        "force_recovery was set to {}. Continuing crash recovery even though"
        " we cannot access the .ibd file of this table.",
        to_int(recovery)
      ));

      return;
    }

    log_fatal("Cannot access .ibd file: ", filepath);
  }

  const auto size = file->size();

  if (!size) {
    log_err(std::format(
      "Could not measure the size of single-table tablespace file {}: {}!"
      " We do not continue crash recovery, because the table will become"
      " corrupt if we cannot apply the log records in the InnoDB log to it."
      " To fix the problem and start the server:\n"
      "    1) If there is a permission problem in the file and the server cannot"
      " access the file, you should modify the permissions.\n"
      "    2) If the table is not needed, or you can restore it from a backup,"
      " then you can remove the .ibd file, and InnoDB will do a normal"
      " crash recovery and ignore that table.\n"
      "    3) If the file system or the disk is broken, and you cannot remove"
      " the .ibd file, you can set force_recovery != IB_RECOVERY_DEFAULT"
      " and force InnoDB to continue crash recovery here.",
      filepath,
      size.error().message()
    ));

    if (recovery != IB_RECOVERY_DEFAULT) {
      log_warn(std::format(
        "force_recovery was set to {}. Continuing crash recovery"
        " even though we cannot access the .ibd file of this table.",
        to_int(recovery)
      ));

      return;
    }

    log_fatal("Could not measure size of ", filepath);
  }

  /* Every .ibd file is created >= 4 pages in size. Smaller files cannot be ok. */
  if (*size < off_t(FIL_IBD_FILE_INITIAL_SIZE * UNIV_PAGE_SIZE)) {
    log_err(std::format(
      "The size of single-table tablespace file {} is only {}, should be at least {}!",
      filepath,
      *size,
      FIL_IBD_FILE_INITIAL_SIZE * UNIV_PAGE_SIZE
    ));

    return;
  }

  /* The first page, aligned for O_DIRECT: the tablespace id and flags are on it. */
  auto buf2 = static_cast<byte *>(ut_new(2 * UNIV_PAGE_SIZE));
  auto page = static_cast<byte *>(ut_align(buf2, UNIV_PAGE_SIZE));
  space_id_t space_id{FIL_NULL};
  ulint flags{};

  if (const auto err = file->read(page, UNIV_PAGE_SIZE, 0); err) {
    log_warn(std::format("Could not read the first page of {}: {}", filepath, err.message()));
  } else {
    space_id = m_sys.m_fsp->get_space_id(page);
    flags = m_sys.m_fsp->get_flags(page);
  }

  ut_delete(buf2);

  if (space_id == FIL_NULL || space_id == SYS_TABLESPACE) {
    log_warn(std::format("Tablespace id {} in file {} is not sensible", space_id, filepath));

    return;
  }

  if (!space_create(filepath.c_str(), space_id, flags)) {
    if (m_sys.m_config.m_recv.m_force_recovery > 0) {
      log_warn(std::format(
        "innodb_force_recovery was set to {}. Continuing crash recovery"
        " even though the tablespace creation of this table failed.",
        to_int(m_sys.m_config.m_recv.m_force_recovery)
      ));

      return;
    }

    log_fatal("During recovery");
  }

  /* We do not use the size information we have about the file, because the rounding formula for
  extents and pages is somewhat complex; we let node_open() do that task. */
  node_create(filepath.c_str(), 0, space_id, false);
}

DbErr Fil::scan_and_load_tablespaces(const std::string &dir, const std::string &dbname, IbRecovery recovery, ulint max_depth, ulint depth) {
  namespace fs = std::filesystem;

  if (depth >= max_depth) {
    log_warn(std::format("The depth of the directory '{}' is greater than the maximum scan depth '{}'", dir, max_depth));
    return DB_SUCCESS;
  }

  const fs::path path(dir);
  std::error_code ec;

  if (!fs::is_directory(path, ec)) {
    log_err(std::format("The datadir is not a directory: '{}'{}", dir, ec ? ": " + ec.message() : ""));
    return DB_ERROR;
  }

  const std::string ext = ".ibd";
  fs::directory_iterator it{path, ec};

  for (; !ec && it != fs::directory_iterator(); it.increment(ec)) {
    const auto filename = it->path().filename().string();
    std::error_code type_ec;

    if (it->is_directory(type_ec)) {
      /* A directory directly under the data home is a database. */
      const auto db = dbname.empty() ? filename : dbname + SRV_PATH_SEPARATOR + filename;

      if (scan_and_load_tablespaces(it->path().string(), db, recovery, max_depth, depth + 1) != DB_SUCCESS) {
        log_err(std::format("Failed to scan and load tablespaces in directory: '{}'", it->path().string()));
      }

    } else if (!dbname.empty() && it->is_regular_file(type_ec) && it->path().extension().string() == ext) {
      /* Single-table tablespaces live in their database directory. */
      load_single_table_tablespace(recovery, dbname.c_str(), filename.c_str());
    }
  }

  if (ec) {
    log_err(std::format("Cannot read the directory '{}': {}", dir, ec.message()));
    return DB_ERROR;
  }

  return DB_SUCCESS;
}

DbErr Fil::load_single_table_tablespaces(const std::string &dir, IbRecovery recovery, ulint max_depth) {
  return scan_and_load_tablespaces(dir, "", recovery, max_depth, 0);
}

void Fil::print_orphaned_tablespaces() {
  mutex_enter(&m_mutex);

  for (auto space : m_space_list) {
    if (space->m_id != SYS_TABLESPACE && !space->m_mark) {
      log_warn(std::format(
        "Tablespace {} of id {} has no matching table in"
        " the InnoDB data dictionary.",
        space->m_name,
        space->m_id
      ));
    }
  }

  mutex_exit(&m_mutex);
}

bool Fil::tablespace_deleted_or_being_deleted_in_mem(space_id_t id, int64_t version) {
  mutex_enter(&m_mutex);

  auto space = space_get_by_id(id);

  if (space == nullptr || space->m_is_being_deleted) {
    mutex_exit(&m_mutex);

    return true;
  }

  if (version != ((int64_t)-1) && space->m_tablespace_version != version) {
    mutex_exit(&m_mutex);

    return true;
  }

  mutex_exit(&m_mutex);

  return false;
}

bool Fil::tablespace_exists_in_mem(space_id_t id) {
  mutex_enter(&m_mutex);

  auto space = space_get_by_id(id);

  mutex_exit(&m_mutex);

  return space != nullptr;
}

bool Fil::space_for_table_exists_in_mem(
  space_id_t id, const char *name, bool is_temp, bool mark_space, bool print_error_if_does_not_exist
) {
  mutex_enter(&m_mutex);

  auto path = make_ibd_name(name, is_temp);

  /* Look if there is a space with the same id */

  auto space = space_get_by_id(id);

  /* Look if there is a space with the same name; the name is the
  directory path from the datadir to the file */

  auto fil_namespace = space_get_by_name(path);

  if (space != nullptr && space == fil_namespace) {
    /* Found */

    if (mark_space) {
      space->m_mark = true;
    }

    mem_free(path);

    mutex_exit(&m_mutex);

    return true;
  }

  if (!print_error_if_does_not_exist) {

    mem_free(path);

    mutex_exit(&m_mutex);

    return false;
  }

  auto error_exit = [this](char *path) -> bool {
    mem_free(path);

    mutex_exit(&m_mutex);

    log_err("Please refer to the Embdedded InnoDB documentation for details on how to resolve the issue.");

    return false;
  };

  if (space == nullptr) {
    if (fil_namespace == nullptr) {

      log_err(std::format(
        "Table {} in InnoDB data dictionary has tablespace id {},"
        " but tablespace with that id or name does not exist. Have"
        " you deleted or moved .ibd files?",
        name,
        id
      ));

    } else {
      log_err(std::format(
        "Table {} in InnoDB data dictionary has tablespace id %lu,"
        " but a tablespace with that id does not exist. There is"
        " a tablespace of name {} and id {}J, though. Have you"
        " deleted or moved .ibd files?",
        name,
        id,
        fil_namespace->m_name,
        fil_namespace->m_id
      ));
    }

    return error_exit(path);
  }

  if (tablename_compare(space->m_name, path) != 0) {
    log_err(std::format(
      "Table {} in InnoDB data dictionary has tablespace id {},"
      " but the tablespace with that id has name {}. Have you"
      " deleted or moved .ibd files?",
      name,
      id,
      space->m_name
    ));

    if (fil_namespace != nullptr) {
      log_warn(
        std::format("There is a tablespace with the right name {},  but its id is {}.", fil_namespace->m_name, fil_namespace->m_id)
      );
    }

    return error_exit(path);
  }

  mem_free(path);
  mutex_exit(&m_mutex);

  return false;
}

ulint Fil::get_space_id_for_table(const char *name) {
  mutex_enter(&m_mutex);

  auto path = make_ibd_name(name, false);

  /* Look if there is a space with the same name; the name is the
  directory path to the file */

  auto fil_namespace = space_get_by_name(path);

  space_id_t space_id;

  if (fil_namespace != nullptr) {
    space_id = fil_namespace->m_id;
  } else {
    space_id = FIL_NULL;
  }

  mem_free(path);

  mutex_exit(&m_mutex);

  return space_id;
}

bool Fil::extend_space_to_desired_size(page_no_t *actual_size, space_id_t space_id, page_no_t size_after_extend) {
  mutex_enter_and_prepare_for_io(space_id);

  auto space = space_get_by_id(space_id);

  /* One extension of a space at a time; the I/O below is done without
  m_mutex, so wait for one in progress (it may make ours unnecessary). The
  fsp callers hold the space latch, this matters for the others. */
  {
    Mutex_lock lock{&m_mutex};

    m_io_cv.wait(lock, [&] { return !space->m_is_being_extended; });
  }

  if (space->m_size_in_pages >= size_after_extend) {
    *actual_size = space->m_size_in_pages;

    mutex_exit(&m_mutex);

    return true;
  }

  const auto page_size = UNIV_PAGE_SIZE;

  auto node = UT_LIST_GET_LAST(space->m_chain);

  /* Keeps the file open: node->m_n_pending > 0 until node_complete_io(). */
  node_prepare_for_io(node, space);

  const page_no_t start_page_no = space->m_size_in_pages;
  const page_no_t node_start = space->m_size_in_pages - node->m_size_in_pages;

  space->m_is_being_extended = true;

  /* The I/O (and the allocation of the blocks by the file system) used to be
  done holding m_mutex, which serialized all tablespace I/O submission and
  every other extension behind it. Only this thread changes the sizes of the
  space until m_is_being_extended is cleared, and reads and writes of other
  threads are below start_page_no. */
  mutex_exit(&m_mutex);

  const off_t off = off_t(start_page_no - node_start) * page_size;
  const off_t len = off_t(size_after_extend - start_page_no) * page_size;
  page_no_t n_added{};
  bool success{true};

  /* The blocks are reserved, or written with zeros where the file system can't. */
  if (const auto err = node->m_file.allocate(off, len); !err) {
    n_added = size_after_extend - start_page_no;
  } else {
    /* Out of space: measure the file for how much of it was extended. */
    const auto measured = node->m_file.size();
    const auto file_pages = measured ? page_no_t(*measured / page_size) : page_no_t{};
    const auto node_pages = start_page_no - node_start;

    n_added = file_pages > node_pages ? file_pages - node_pages : 0;
  }

  /* No sync here (upstream flushed the space after every extension). The
  extension is redo logged (MLOG_FILE_EXTEND, written by Fsp in the mtr that
  changes FSP_SIZE) and recovery redoes it: no page of the new part reaches
  the disk before that record is durable (write ahead logging), and the file
  is marked modified below, so the checkpoint that moves past the record
  flushes the new size first. */
  mutex_enter(&m_mutex);

  node->m_size_in_pages += n_added;
  space->m_size_in_pages += n_added;

  space->m_is_being_extended = false;

  /* A write: the space is flushed (and its new size made durable) by the
  next flush of the unflushed spaces, at the latest by the checkpoint. */
  node_complete_io(node, n_added > 0 ? IoRequest::Sync_write : IoRequest::None);

  *actual_size = space->m_size_in_pages;

  m_io_cv.notify_all();

  mutex_exit(&m_mutex);

  return success;
}

bool Fil::space_reserve_free_extents(space_id_t id, ulint n_free_now, ulint n_to_reserve) {
  bool success;

  mutex_enter(&m_mutex);

  auto space = space_get_by_id(id);

  if (space->m_n_reserved_extents + n_to_reserve > n_free_now) {
    success = false;
  } else {
    space->m_n_reserved_extents += n_to_reserve;
    success = true;
  }

  mutex_exit(&m_mutex);

  return success;
}

void Fil::space_release_free_extents(space_id_t id, ulint n_reserved) {
  mutex_enter(&m_mutex);

  auto space = space_get_by_id(id);

  ut_a(space);
  ut_a(space->m_n_reserved_extents >= n_reserved);

  space->m_n_reserved_extents -= n_reserved;

  mutex_exit(&m_mutex);
}

ulint Fil::space_get_n_reserved_extents(space_id_t id) {
  mutex_enter(&m_mutex);

  auto space = space_get_by_id(id);
  auto n = space->m_n_reserved_extents;

  mutex_exit(&m_mutex);

  return n;
}

void Fil::node_prepare_for_io(FilNode *node, FilSpace *space) {
  ut_ad(mutex_own(&m_mutex));

  if (m_n_open > m_max_n_open + 5) {
    log_warn(std::format("Open files {} exceeds the limit {}", m_n_open, m_max_n_open));
  }

  if (node->open == false) {
    /* File is closed: open it */
    ut_a(node->m_n_pending == 0);

    node_open_file(node, space, false);
  }

  node->m_n_pending++;
}

void Fil::node_complete_io(FilNode *node, IoRequest io_request) {
  ut_ad(mutex_own(&m_mutex));

  ut_a(node->m_n_pending > 0);

  --node->m_n_pending;

  if (io_request == IoRequest::Sync_write) {
    ++m_modification_counter;
    node->m_modification_counter = m_modification_counter;

    if (!node->m_space->m_is_in_unflushed_spaces) {

      node->m_space->m_is_in_unflushed_spaces = true;
      UT_LIST_ADD_FIRST(m_unflushed_spaces, node->m_space);
    }
  }
}

[[noreturn]] void Fil::report_invalid_page_access(
  const PageId &page_id, const char *space_name, ulint byte_offset, ulint len, IoRequest io_request
) {
  log_fatal(std::format(
    "Trying to access page number {} in space {}, space name {}, which is"
    " outside the tablespace bounds. Byte offset {}, len {}, i/o type {}."
    " If you get this error at server startup, please check that your config"
    " file matches the ibdata files that you have in the server.",
    page_id.page_no(),
    page_id.space_id(),
    space_name,
    byte_offset,
    len,
    (int)io_request
  ));
  ut_error;
}

DbErr Fil::data_io(
  IoRequest io_request, bool batched, const PageId &page_id, ulint byte_offset, ulint len, void *buf, void *message
) {

  ut_ad(len > 0);
  ut_ad(buf != nullptr);
  ut_ad(byte_offset < UNIV_PAGE_SIZE);

  static_assert((1 << UNIV_PAGE_SIZE_SHIFT) == UNIV_PAGE_SIZE, "error (1 << UNIV_PAGE_SIZE_SHIFT) != UNIV_PAGE_SIZE");

  ut_ad(validate());

  bool is_sync_request{};

  switch (io_request) {
    case IoRequest::None:
    case IoRequest::Sync_log_read:
    case IoRequest::Async_log_read:
    case IoRequest::Sync_log_write:
    case IoRequest::Async_log_write:
    case IoRequest::Sync_log_flush:
    case IoRequest::Async_log_flush:
      ut_error;
      break;

    case IoRequest::Sync_read:
      is_sync_request = true;
      // falthrough
    case IoRequest::Async_read:
      m_sys.m_srv->m_stats.m_data_read += len;
      break;

    case IoRequest::Sync_write:
      is_sync_request = true;
      // falthrough
    case IoRequest::Async_write:
      m_sys.m_srv->m_stats.m_data_written += len;
      break;
  }

  /* Reserve the Fil::system mutex and make sure that we can open at
  least one file while holding it, if the file is not already open */

  mutex_enter_and_prepare_for_io(page_id.space_id());

  auto space = space_get_by_id(page_id.space_id());

  if (space == nullptr) {
    mutex_exit(&m_mutex);

    log_err(std::format(
      "Trying to do i/o to a tablespace which does not exist."
      " i/o type {}, space id {}, page no. {}, i/o length {} bytes",
      (int)io_request,
      page_id.space_id(),
      page_id.page_no(),
      len
    ));

    return DB_TABLESPACE_DELETED;
  }

  /* Only one data file per space is supported */
  ut_a(UT_LIST_GET_LEN(space->m_chain) == 1);

  auto fil_node = UT_LIST_GET_FIRST(space->m_chain);

  if (unlikely(fil_node == nullptr)) {
    report_invalid_page_access(page_id, space->m_name, byte_offset, len, io_request);
  }

  /* Open file if closed */
  node_prepare_for_io(fil_node, space);

  /* Check that at least the start offset is within the bounds of a
  single-table tablespace */
  if (fil_node->m_size_in_pages <= page_id.page_no() && space->m_id != SYS_TABLESPACE) {

    report_invalid_page_access(page_id, space->m_name, byte_offset, len, io_request);
  }

  /* Read the node size while we hold the mutex: extend_space_to_desired_size()
  grows it under the mutex, reading it after mutex_exit() was a data race.
  The size only grows, so the check below stays valid. */
  const auto node_size = fil_node->m_size_in_pages;

  /* Now we have made the changes in the data structures of Fil::system */
  mutex_exit(&m_mutex);

  /* Calculate the low 32 bits and the high 32 bits of the file offset */

  const off_t off = (off_t(page_id.page_no()) * off_t(UNIV_PAGE_SIZE)) + byte_offset;

  ut_a(node_size - page_id.page_no() >= ((byte_offset + len + (UNIV_PAGE_SIZE - 1)) / UNIV_PAGE_SIZE));

  /* Do aio */

  ut_a(byte_offset % IB_FILE_BLOCK_SIZE == 0);
  ut_a((len % IB_FILE_BLOCK_SIZE) == 0);

  IoCtx io_ctx = {.m_batch = batched, .m_fil_node = fil_node, .m_msg = message, .m_io_request = io_request};

  /* Queue the aio request */
  auto err = m_sys.m_aio->submit(std::move(io_ctx), buf, len, off);
  ut_a(err == DB_SUCCESS);

  if (is_sync_request) {
    /* The i/o operation is already completed when we return from os_aio: */

    mutex_enter(&m_mutex);

    node_complete_io(fil_node, io_request);

    mutex_exit(&m_mutex);

    ut_ad(validate());
  }

  return DB_SUCCESS;
}

bool Fil::aio_wait(ulint segment) {
  ut_ad(validate());

  IoCtx io_ctx{};

  auto err = m_sys.m_aio->reap(segment, io_ctx);

  if (io_ctx.is_shutdown()) {
    return false;
  }

  ut_a(io_ctx.m_ret > 0);
  ut_a(err == DB_SUCCESS);

  mutex_enter(&m_mutex);

  node_complete_io(io_ctx.m_fil_node, io_ctx.m_io_request);

  mutex_exit(&m_mutex);

  ut_ad(validate());

  /* Do the i/o handling */
  /* IMPORTANT: since i/o handling for reads will read also the insert
  buffer in tablespace 0, you have to be very careful not to introduce
  deadlocks in the i/o system. We keep tablespace 0 data files always
  open, and use a special i/o thread to serve insert buffer requests. */

  m_sys.m_buf_pool->io_complete(reinterpret_cast<BufPage *>(io_ctx.m_msg));

  return true;
}

void Fil::flush(space_id_t space_id) {
  int64_t old_mod_counter;

  mutex_enter(&m_mutex);

  auto space = space_get_by_id(space_id);

  if (space == nullptr || space->m_is_being_deleted) {
    mutex_exit(&m_mutex);

    return;
  }

  /** Prevent dropping of the space while we are flushing */
  ++space->m_n_pending_flushes;

  auto node = UT_LIST_GET_FIRST(space->m_chain);

  while (node != nullptr) {
    if (node->m_modification_counter > node->m_flush_counter) {
      ut_a(node->open);

      /* We want to flush the changes at least up to
      old_mod_counter */
      old_mod_counter = node->m_modification_counter;

      ++m_n_pending_flushes;
      ++m_n_flushes;
    retry:
      if (node->m_n_pending_flushes > 0) {
        /* We want to avoid flushing the file twice at
        the same time: wait for the flush in progress (it may cover our
        changes too), instead of sleeping 20 ms. */
        {
          Mutex_lock lock{&m_mutex};

          m_io_cv.wait(lock, [&] { return node->m_n_pending_flushes == 0; });
        }

        if (node->m_flush_counter >= old_mod_counter) {

          goto skip_flush;
        }

        goto retry;
      }

      ut_a(node->open);
      node->m_n_pending_flushes++;

      mutex_exit(&m_mutex);

      /* nosync: the writes are not made durable (tests, benchmarks), as for
      the log. */
      if (m_sys.m_config.m_fil.m_flush_method != SRV_UNIX_NOSYNC) {
        /* Open while it has flushes pending. What the writes left on the device is unknown when
        this fails: the database can't go on as if they were durable. */
        if (const auto err = node->m_file.sync(); err) {
          log_fatal(std::format("Flush of {} failed: {}", node->m_file_name, err.message()));
        }
      }

      mutex_enter(&m_mutex);

      node->m_n_pending_flushes--;

      m_io_cv.notify_all();

    skip_flush:
      if (node->m_flush_counter < old_mod_counter) {
        node->m_flush_counter = old_mod_counter;

        if (space->m_is_in_unflushed_spaces && space_is_flushed(space)) {

          space->m_is_in_unflushed_spaces = false;

          UT_LIST_REMOVE(m_unflushed_spaces, space);
        }
      }

      --m_n_pending_flushes;
    }

    node = UT_LIST_GET_NEXT(m_chain, node);
  }

  space->m_n_pending_flushes--;

  mutex_exit(&m_mutex);
}

void Fil::flush_file_spaces() {
  mutex_enter(&m_mutex);

  auto n_space_ids = UT_LIST_GET_LEN(m_unflushed_spaces);

  if (n_space_ids == 0) {
    mutex_exit(&m_mutex);
    return;
  }

  /* Assemble a list of space ids to flush.  Previously, we
  traversed unflushed_spaces and called UT_LIST_GET_NEXT()
  on a space that was just removed from the list by Fil::flush().
  Thus, the space could be dropped and the memory overwritten. */

  std::vector<space_id_t> space_ids;

  space_ids.reserve(n_space_ids + 1);

  for (auto space : m_unflushed_spaces) {
    if (!space->m_is_being_deleted) {
      space_ids.push_back(space->m_id);
    }
  }

  space_ids.push_back(FIL_NULL);

  mutex_exit(&m_mutex);

  for (auto space_id : space_ids) {
    if (space_id == FIL_NULL) {
      break;
    }
    flush(space_id);
  }
}

bool Fil::validate() {
  mutex_enter(&m_mutex);

  ulint n_open{};

  for (auto &[id, space] : m_space_by_id) {

    for (auto node : space->m_chain) {

      ut_a(node->open || node->m_n_pending == 0);

      if (node->m_n_pending > 0) {
        ut_a(node->open);
      }

      if (node->open) {
        ++n_open;
      }
    }
  }

  ut_a(m_n_open == n_open);

  mutex_exit(&m_mutex);

  return true;
}

bool Fil::addr_is_null(const FilAddr &addr) {
  return addr.m_page_no == FIL_NULL;
}

ulint Fil::page_get_prev(const byte *page) {
  return mach_read_from_4(page + FIL_PAGE_PREV);
}

ulint Fil::page_get_next(const byte *page) {
  return mach_read_from_4(page + FIL_PAGE_NEXT);
}

void Fil::page_set_type(byte *page, ulint type) {
  mach_write_to_2(page + FIL_PAGE_TYPE, type);
}

FilPageType Fil::page_get_type(const byte *page) {
  return static_cast<FilPageType>(mach_read_from_2(page + FIL_PAGE_TYPE));
}

bool Fil::rmdir(const char *dbname) {
  const auto dir = std::filesystem::path(m_sys.m_config.m_fil.m_data_home) / dbname;
  std::error_code ec;

  /* Only if it's empty. */
  if (!std::filesystem::remove(dir, ec) || ec) {
    log_err(std::format("Removing directory {}: {}", dir.string(), ec ? ec.message() : "it does not exist"));

    return false;
  }

  return true;
}

bool Fil::mkdir(const char *dbname) {
  const auto dir = std::filesystem::path(m_sys.m_config.m_fil.m_data_home) / dbname;
  std::error_code ec;

  /* One that exists is no error. */
  (void)std::filesystem::create_directory(dir, ec);

  if (ec) {
    log_err(std::format("Creating directory {}: {}", dir.string(), ec.message()));
  }

  return !ec;
}
