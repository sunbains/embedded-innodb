/*****************************************************************************
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

/**************************************************/ /**
 @file include/usr0types.h
 Users and sessions global types

 Created 6/25/1996 Heikki Tuuri
 *******************************************************/

#pragma once

#include "que0types.h"
#include "ut0lst.h"

struct Trx;

/** The session handle. All fields are protected by the kernel mutex */
struct Session {
  enum class State : uint8_t {
    /** Session is not initialized */
    NONE = 0,

    /** Session states */
    ACTIVE = 1,

    /** Session contains an error message which has not yet been
    communicated to the client */
    ERROR = 2
  };

  /** Constructor 
   * 
   * @param trx Transaction instance permanently assigned for the session
   */
  explicit Session(Trx *trx) noexcept : m_state(State::ACTIVE), m_trx(trx) {}

  /** Destructor */
  ~Session() = default;

  /**
   * Creates a session instance
   * 
   * @param trx Transaction instance permanently assigned for the session
   */
  [[nodiscard]] static Session *create(Trx *trx) noexcept;

  /**
   * Destroys a session instance
   * 
   * @param[in,out] sess Session instance to destroy
  */
  static void destroy(Session *&sess) noexcept;

  /** State of the session */
  State m_state{State::NONE};

  /** Transaction object permanently assigned for the session:
  the transaction instance designated by the trx id changes, but
  the memory structure is preserved */
  Trx *m_trx{};

  /** Query graphs belonging to this session */
  UT_LIST_BASE_NODE_T_EXTERN(que_t, graphs) m_graphs{};
};
