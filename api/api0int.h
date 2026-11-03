/****************************************************************************
Copyright (c) 2024 Sunny Bains. All rights reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation; version 2 of the License.
*****************************************************************************/

/** @file api/api0int.h
Internal declarations shared by the api implementation files:
api0api.cc (transactions, schemas, DDL), api0cursor.cc (cursors, rows,
tuples) and api0misc.cc (truncate, schema locks, savepoints, visitors,
parallel reads, status). Not part of the public api. */

#pragma once

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <algorithm>
#include <ranges>
#include <vector>
#include "api0api.h"
#include "btr0blob.h"
#include "btr0pcur.h"
#include "ddl0ddl.h"
#include "dict0dict.h"
#include "innodb0types.h"
#include "lock0lock.h"
#include "lock0types.h"
#include "pars0pars.h"
#include "rem0cmp.h"
#include "row0ins.h"
#include "row0merge.h"
#include "row0misc.h"
#include "row0pread.h"
#include "row0prebuilt.h"
#include "row0sel.h"
#include "row0upd.h"
#include "row0vers.h"
#include "srv0srv.h"
#include "trx0roll.h"
#include "ut0counter.h"
#include "ut0dbg.h"
#include "ut0logger.h"
#include "ut0ucode.h"

namespace api {

static constexpr const char *GEN_CLUST_INDEX = "GEN_CLUST_INDEX";

using index_def_t = MergeIndexDef;
using index_field_t = MergeIndexField;

/* The following counter is used to convey information to InnoDB
about server activity: in selects it is not sensible to call
srv_active_wake_master_thread after each fetch or search, we only do
it every INNOBASE_WAKE_INTERVAL'th step. */
constexpr ulint INNOBASE_WAKE_INTERVAL = 32;

bool btr_cursor_is_positioned(const BtreePcursor *pcur) noexcept;

void delay_dml_if_needed(System &sys);

Table *open_table_by_id(System &sys, uint64_t tid, bool locked) noexcept;

Table *open_table_by_name(System &sys, const char *name);

Table *lookup_table_by_name(System &sys, const char *name);

void wake_master_thread(System &sys) noexcept;

void read_tuple(System &sys, const Rec &rec, Tuple *tuple) noexcept;

Tuple *key_tuple_new_low(const Index *index, ulint n_cols, MemHeap *heap);

Tuple *key_tuple_new(const Index *index, ulint n_cols) noexcept;

Tuple *row_tuple_new_low(const Index *index, ulint n_cols, MemHeap *heap) noexcept;

Tuple *row_tuple_new(const Index *index, ulint n_cols) noexcept;

void normalize_table_name(char *norm_name, const char *name);

DbErr table_get_id_low(System &sys, const char *table_name, uint64_t *table_id) noexcept;

DbErr create_cursor(System &sys, Cursor **crsr, Table *table, uint64_t index_id, Trx *trx) noexcept;

const Dfield *col_get_dfield(const Tuple *tuple, ulint col_no) noexcept;

Dfield *col_get_dfield(Tuple *tuple, ulint col_no) noexcept;

ulint col_copy_value_low(const Tuple *tpl, ulint i, void *dst, ulint len) noexcept;

ColAttr col_get_attr(ulint prtype);

ulint col_get_meta_low(const Tuple *tpl, ulint i, ColMeta *col_meta) noexcept;

}  // namespace api
