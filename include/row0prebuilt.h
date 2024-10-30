/****************************************************************************
Copyright (c) 1997, 2010, Innobase Oy. All Rights Reserved.
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

/** Row select prebuilt structure definition.

Created 02/03/2009 Sunny Bains
*******************************************************/

#pragma once

#include "innodb0types.h"
#include "lock0types.h"
#include "row0sel.h"

struct Trx;
struct Table;

constexpr ulint FETCH_CACHE_SIZE = 6;

/* After fetching this many rows, we start caching them in fetch_cache */
constexpr ulint FETCH_CACHE_THRESHOLD  = 4;

/* Values for hint_need_to_fetch_extra_cols */
constexpr ulint ROW_RETRIEVE_PRIMARY_KEY = 1;

constexpr ulint ROW_RETRIEVE_ALL_COLS = 2;

constexpr ulint ROW_PREBUILT_ALLOCATED = 78540783;

constexpr ulint ROW_PREBUILT_FREED = 26423527;

constexpr ulint ROW_PREBUILT_FETCH_MAGIC_N = 465765687;

/** A struct for (sometimes lazily) prebuilt structures in an Innobase table
handle used within the API; these are used to save CPU time. */
struct Prebuilt {

  /**
   * Constructor.
   * 
   * @param[in] fsp File space manager
   * @param[in] btree Btree instance
   * @param[in] table Table handle
   * @param[in] heap Memory heap from which the prebuilt struct is allocated
   */
  Prebuilt(FSP *fsp, Btree *btree, Table *table, mem_heap_t *heap) noexcept;

  /**
   * Destructor.
   */
  ~Prebuilt() noexcept;

  /**
   * Create a prebuilt struct for a user table handle.
   *
   * @param[in] fsp File space manager
   * @param[in] btree Btree instance
   * @param[in] table Table handle
   * 
   * @return own: a prebuilt struct
   */
  [[nodiscard]] static Prebuilt *create(FSP *fsp, Btree *btree, Table *table) noexcept;

  /**
   * Free a prebuilt struct for a user table handle.
   *
   * @param[in] dict Dictionary
   * @param[in] prebuilt Prebuilt struct
   * @param[in] dict_locked true if dict was locked
   */
  static void destroy(Dict *dict, Prebuilt *prebuilt, bool dict_locked) noexcept;

  /**
   * Reset a prebuilt struct for a user table handle.
   */
  void clear() noexcept;

  /**
   * Prebuild the query graph, so that it can be reused.
   */
  void prebuild_graph() noexcept;

  /**
   * @brief Gets a row from the fetch cache.
   *
   * 
   * @return a pointer to the row.
   */
  [[nodiscard]] const rec_t *cache_get_row() noexcept;

  /**
   * @brief Moves to the next row in the fetch cache.
   *
   */
  inline void cache_next() noexcept;

  /**
   * Updates the transaction pointers in query graphs stored in the prebuilt struct.
   * Used by DDL to externalize the new index, see @m_index_is_usable. 
   * 
   * @param[in] trx transaction handle
   */
  void update_trx(Trx *trx) noexcept;

  /* Cached row. */
  struct Cached_row {
    /**
     * Add a row to the cache.
     * 
     * @param[in] rec record
     * @param[in] offsets offsets
     */
    void add_row(const rec_t *rec, const ulint *offsets) noexcept {
      /* Get the size of the physical record in the page */
      const auto rec_len = rec_offs_size(offsets);

      /* Check if there is enough space for the record being added to the cache. Free existing memory if it won't fit. */
      if (m_max_len < rec_len) {
        if (m_ptr != nullptr) {

          ut_a(m_max_len > 0);
          ut_a(m_rec_len > 0);
          mem_free(m_ptr);

          m_ptr = nullptr;
          m_rec = nullptr;
          m_max_len = 0;
          m_rec_len = 0;

        } else {

          ut_a(m_ptr == nullptr);
          ut_a(m_rec == nullptr);
          ut_a(m_max_len == 0);
          ut_a(m_rec_len == 0);
        }
      }

      m_rec_len = rec_len;

      if (m_ptr == nullptr) {
        m_max_len = m_rec_len * 2;
        m_ptr = static_cast<byte *>(mem_alloc(m_max_len));
      }

      ut_a(m_max_len >= m_rec_len);

      /* Note that the pointer returned by rec_copy() is actually an offset into cache->ptr, to the start of the record data. */
      m_rec = rec_copy(m_ptr, rec, offsets);
    }

    /** Max len of rec if not nullptr */
    uint32_t m_max_len{};

    /** Length of valid data in rec */
    uint32_t m_rec_len{};

    /** Cached record, pointer into the start of the record data */
    rec_t *m_rec{};

    /** Pointer to start of record */
    rec_t *m_ptr{};
  };

  /** Cache for rows fetched when positioning the cursor. */
  struct Row_cache {
    /**
     * Clear the row cache.
     */
    void clear() noexcept {
      m_first = 0;
      m_n_cached = 0;
    }

    /**
     * Check if the cache is empty.
     * 
     * @return true if the cache is empty.
     */
    [[nodiscard]] inline  bool is_cache_empty() const noexcept {
      return m_n_cached == 0;
    }

    /**
     * Check if the cache is fetching in progress.
     * 
     * @return true if the cache is fetching in progress.
     */
    [[nodiscard]] inline bool is_cache_fetch_in_progress() const noexcept {
      return m_first > 0 && m_first < m_n_size;
    }

    /**
     * Check if the cache is full.
     * 
     * @return true if the cache is full.
     */
    [[nodiscard]] inline bool is_cache_full() const noexcept {
      ut_a(m_n_cached <= m_n_size);
      return m_n_cached == m_n_size - 1;
    }

    /**
     * Get the row from the cache.
     * 
     * @return pointer to the row.
     */
    [[nodiscard]] inline const rec_t *cache_get_row() const noexcept {
      ut_ad(!is_cache_empty());
      return m_cached_rows[m_first].m_rec;
    }

    /**
     * Move to the next row in the cache.
     */
    void cache_next() noexcept {
      if (!is_cache_empty()) {
        ++m_first;
        --m_n_cached;

        if (is_cache_empty()) {
          m_first = 0;
        }
      }
    }

    /**
    * Add a row to the cache.
    *  
    * 
    * @param[in] rec record
    * @param[in] offsets offsets
    */
    inline void add_row(const rec_t *rec, const ulint *offsets) noexcept;

    /** Memory heap for cached rows */
    mem_heap_t *m_heap{};

    /** A cache for fetched rows if we fetch many rows from
    the same cursor: it saves CPU time to fetch them in a batch. */
    Cached_row *m_cached_rows{};

    /** Max size of the row cache. */
    uint16_t m_n_max{};

    /** Current max setting, must be <= n_max */
    uint16_t m_n_size{};

    /* Position of the first not yet fetched row in fetch_cache */
    uint16_t m_first{};

    /** Number of not yet accessed rows in fetch_cache */
    uint16_t m_n_cached{};

    /** ROW_SEL_NEXT or ROW_SEL_PREV */
    ib_cur_op_t m_direction{ROW_SEL_UNDEFINED};
  };

  /** This magic number is set to ROW_PREBUILT_ALLOCATED when
   * created, or ROW_PREBUILT_FREED when the struct has been freed */
  uint32_t m_magic_n{ROW_PREBUILT_ALLOCATED};

  /** True when we start processing of an SQL statement:
   * we may have to set an intention lock on the table, create
   * a consistent read view etc. */
  bool m_sql_stat_start{true};

  /** This is set true when a client calls explicit lock on this
   * handle with a lock flag, and set false when with unlocked */
  bool m_client_has_locked{};

  /** If the user did not define a primary key, then Innobase automatically
   * generated a clustered index where the ordering column is the row id:
   * in this case this flag is set to true */
  bool m_clust_index_was_generated{};

  /** If we are fetching columns through a secondary index and at least
   * one column is not in the secondary index, then this is set to true */
  bool m_need_to_access_clustered{};

  /** Caches the value of row_merge_is_index_usable(trx,index) */
  bool m_index_usable{};

  /** true if plain select */
  bool m_simple_select{};

  /**
   * Normally 0; if the session is using READ COMMITTED isolation level, in
   * a cursor search, if we set a new  record lock on an index, this is incremented;
   * this is used in releasing the locks under the cursors if we are performing an
   * UPDATE and we determine after retrieving the row that it does not need to be
   * locked; thus, these can be used to implement a 'mini-rollback' that releases the
   * latest record locks */
  uint8_t m_new_rec_locks{};

  /** Memory heap from which these auxiliary structures are allocated when needed */
  mem_heap_t *m_heap{};

  /** Table handle */
  Table *m_table{};

  /** Current index for a search, if any */
  Index *m_index{};

  /** Current transaction handle */
  Trx *m_trx{};

  /** Persistent cursor used in selects and updates */
  Btree_pcursor *m_pcur{};

  /** Persistent cursor used in some selects and updates */
  Btree_pcursor *m_clust_pcur{};

  /** Dummy query graph used in selects */
  que_fork_t *m_sel_graph{};

  /** Prebuilt dtuple used in selects */
  DTuple *m_search_tuple{};

  /** if the clustered index was generated, the row id of the last row
   * fetched is stored here */
  std::array<std::byte, DATA_ROW_ID_LEN> m_row_id{};

  /** Prebuilt dtuple used in sel/upd/del */
  DTuple *m_clust_ref{};

  /** LOCK_NONE, LOCK_S, or LOCK_X */
  Lock_mode m_select_lock_type{LOCK_NONE};

  /** Memory heap where a previous version is built in consistent read */
  mem_heap_t *m_old_vers_heap{};

  /** Rows cached by select read ahead */
  Row_cache m_row_cache{};

  /** Result of the last compare in row_search_mvcc(). */
  int m_result{};

  /** This should be the same as magic_n */
  uint32_t m_magic_n2{ROW_PREBUILT_ALLOCATED};
};

inline void Prebuilt::cache_next() noexcept {
  return m_row_cache.cache_next();
}