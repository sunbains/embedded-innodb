/****************************************************************************
Copyright (c) 1997, 2009, Innobase Oy. All Rights Reserved.
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

/** @file read/read0read.cc
Cursor read view implementation

Created 2/16/1997 Heikki Tuuri
*******************************************************/

#include "read0read.h"
#include "srv0srv.h"
#include "trx0sys.h"

/* A cursor read view sees the versions of the rows at its snapshot, and those of
its creator made before it, by the undo number (RowVers): the creator is active for
it, a lookup says so. Its changes go through the undo log, as the changes of
any other active transaction. A secondary index record leads to the clustered
one if the page max trx id isn't below the watermark. */

std::string ReadView::to_string() const noexcept {
  std::string str{};

  if (m_type == ReadViewType::HIGH_GRANULARITY) {
    str = std::format("High-granularity read view undo_n:o {} {}\n", m_undo_no, m_undo_no);
  } else {
    str = "Normal read view\n";
  }

  str += std::format("Read view snapshot {}\n", m_snap_word);
  str += std::format("Read view watermark {}\n", m_watermark);
  str += std::format("Read view low limit trx n:o {}\n", m_low_limit_no);

  return str;
}
