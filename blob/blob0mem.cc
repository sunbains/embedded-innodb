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

/** @file blob/blob0mem.cc
A store in memory, for tests: a field is a vector by an id, the id is the store's part of a
reference. Nothing is logged and nothing survives a restart. It proves the engine above
BlobStore doesn't depend on pages. */

#include "blob0store.h"

#include "mach0data.h"

#include <mutex>
#include <unordered_map>
#include <vector>

namespace {

struct MemStore final : BlobStore {
  void prepare(const Index *, Mtr *, bool) noexcept override {}

  DbErr write(const Index *, const byte *data, ulint len, uint64_t, byte *id, Mtr *) noexcept override {
    ut_a(len > 0);

    std::lock_guard lock{m_mutex};

    const auto field = m_next++;

    m_fields.emplace(field, std::vector<byte>(data, data + len));

    mach_write_to_8(id, field);
    mach_write_to_4(id + 8, 0);

    return DB_SUCCESS;
  }

  ulint read(const byte *id, byte *buf, ulint len) noexcept override {
    std::lock_guard lock{m_mutex};
    const auto it = m_fields.find(mach_read_from_8(id));

    ut_a(it != m_fields.end());

    const auto n = std::min<ulint>(len, it->second.size());

    memcpy(buf, it->second.data(), n);

    return n;
  }

  void free(const Index *, const byte *id, Mtr *) noexcept override {
    std::lock_guard lock{m_mutex};

    ut_a(m_fields.erase(mach_read_from_8(id)) == 1);
  }

  ulint n_pages(ulint len) const noexcept override { return (len + UNIV_PAGE_SIZE - 1) / UNIV_PAGE_SIZE; }

  std::mutex m_mutex;

  /** 0 is no field: a reference of zeros. */
  uint64_t m_next{1};

  std::unordered_map<uint64_t, std::vector<byte>> m_fields;
};

}  // namespace

std::unique_ptr<BlobStore> blob_store_mem(System &) { return std::make_unique<MemStore>(); }
