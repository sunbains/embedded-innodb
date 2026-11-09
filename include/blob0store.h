/****************************************************************************
Copyright (c) 2026 Sunny Bains. All rights reserved.

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

/** @file include/blob0store.h
Where the fields go that don't fit in a record: the record keeps a prefix of the field and a
reference of BTR_EXTERN_FIELD_REF_SIZE bytes at its end. The last 8 bytes of a reference are the
same for every store, see blob0blob.h: who owns the field, and its length. The first ID_SIZE bytes
are the store's own: what it needs to find the field again, nothing else reads them.

A store is chosen for the instance by DictConfig::m_blob_store: INLINE, the default, keeps a field in
pages of the index's own segment (blob0inline.cc); MEM keeps it in memory, for tests (blob0mem.cc).

The changes are in the caller's mini-transaction, whatever else it changes: the field, the pages it
takes and the reference in the record are one atomic change, and so is a free. A store starts no
mini-transaction of its own for a write or a free. */

#pragma once

#include "btr0types.h"
#include "db0err.h"
#include "innodb0types.h"

#include <memory>

class System;
struct Index;
struct Mtr;

class BlobStore {
 public:
  /** The bytes of a reference the store owns. */
  static constexpr ulint ID_SIZE = 12;

  virtual ~BlobStore() = default;

  /**
   * Take in mtr what write() and free() need held, once per mini-transaction, after the caller's
   * own latches: they're the store's to take after the record's.
   *
   * @param[in] index           The index of the record.
   * @param[in,out] mtr         The caller's mini-transaction.
   * @param[in] space_latched   The caller's mtr already holds the tablespace latch of the index.
   */
  virtual void prepare(const Index *index, Mtr *mtr, bool space_latched) noexcept = 0;

  /**
   * Store a field of a record of index.
   *
   * @param[in] index           The index of the record.
   * @param[in] data            The bytes of the field the record doesn't keep.
   * @param[in] len             How many, > 0.
   * @param[in] near            A hint where the record is, for locality; 0 if none.
   * @param[out] id             ID_SIZE bytes: what finds the field, for the caller to put in its
   *                            reference in the same mtr.
   * @param[in,out] mtr         The caller's mini-transaction, prepared.
   *
   * @return DB_SUCCESS, or DB_OUT_OF_FILE_SPACE with nothing changed.
   */
  [[nodiscard]] virtual DbErr write(const Index *index, const byte *data, ulint len, uint64_t near, byte *id, Mtr *mtr) noexcept = 0;

  /**
   * Copy the start of a field.
   *
   * @param[in] id              The store's bytes of its reference.
   * @param[out] buf            Where to.
   * @param[in] len             How many bytes at most.
   *
   * @return the bytes copied: fewer than len only if the field is shorter.
   */
  [[nodiscard]] virtual ulint read(const byte *id, byte *buf, ulint len) noexcept = 0;

  /**
   * Free a field.
   *
   * @param[in] index           The index of the record that owned it.
   * @param[in] id              The store's bytes of its reference.
   * @param[in,out] mtr         The caller's mini-transaction, prepared.
   */
  virtual void free(const Index *index, const byte *id, Mtr *mtr) noexcept = 0;

  /** @return the pages a field of len bytes takes, for the statistics. */
  [[nodiscard]] virtual ulint n_pages(ulint len) const noexcept = 0;
};

/** The default: in pages of the index's segment, see blob0inline.cc. */
[[nodiscard]] std::unique_ptr<BlobStore> blob_store_inline(System &sys);

/** In memory, for tests: nothing survives a restart, see blob0mem.cc. */
[[nodiscard]] std::unique_ptr<BlobStore> blob_store_mem(System &sys);
