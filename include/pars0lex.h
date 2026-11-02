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

/** @file include/pars0lex.h
The SQL lexer: a flex C++ scanner whose state belongs to the Parser of an
instance. */

#pragma once

#include "innodb0types.h"
#include "que0types.h"

#if !defined(yyFlexLexerOnce)
#include <FlexLexer.h>
#endif

class System;

/** The lexer of one SQL statement. The rules in pars0lex.l run as members of
this class (flex %option yyclass). */
struct ParsLexer : public yyFlexLexer {
  explicit ParsLexer(System &sys) noexcept : m_sys(sys) {}

  ~ParsLexer() noexcept override { ut_delete(m_stringbuf); }

  ParsLexer(const ParsLexer &) = delete;
  ParsLexer &operator=(const ParsLexer &) = delete;

  /** Scan the next token; implemented by flex from pars0lex.l.
  @return the token, 0 at the end of the statement. */
  int yylex() override;

  /** Read the next chunk of the statement being parsed.
  @param[out] buf               Where to copy the characters.
  @param[in] max_size           Size of buf.
  @return number of characters copied, 0 at the end. */
  int LexerInput(char *buf, int max_size) override;

  /** Appends a string to the string buffer, used for removing quotes.
  @param[in] str                String to append.
  @param[in] len                Length of str. */
  void string_append(const char *str, ulint len) noexcept;

  /** The instance this lexer belongs to. */
  System &m_sys;

  /** Where the value of the current token is returned to the parser. */
  que_node_t **m_lval{};

  /** String buffer for removing quotes. */
  char *m_stringbuf{};

  /** Current length of m_stringbuf. */
  ulint m_stringbuf_len{};

  /** Allocated length of m_stringbuf. */
  ulint m_stringbuf_len_alloc{};
};
