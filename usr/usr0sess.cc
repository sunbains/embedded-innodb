/****************************************************************************
Copyright (c) 1996, 2009, Innobase Oy. All Rights Reserved.
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

/** @file usr/usr0sess.c
Sessions

Created 6/25/1996 Heikki Tuuri
*******************************************************/

#include "usr0sess.h"
#include "srv0srv.h"

Session *Session::create(Trx *trx) noexcept {
  auto ptr = ut_new(sizeof(Session));
  return new (ptr) Session(trx);
}

void Session::destroy(Session *&sess) noexcept {
  ut_a(sess->m_graphs.empty());
  ut_a(sess->m_trx == nullptr);

  ut_a(sess->m_state != State::ERROR);

  call_destructor(sess);
  ut_delete(sess);

  sess = nullptr;
}
