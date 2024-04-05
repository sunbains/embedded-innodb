/** Copyright (c) 2008 Innobase Oy. All rights reserved.
Copyright (c) 2008 Oracle. All rights reserved.

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA

************************************************************************/
#include <ctype.h>


#ifdef HAVE_STRINGS_H
#include <strings.h>
#endif /* HAVE_STRINGS_H */

#include "api0ucode.h"
#include "ut0mem.h"

const charset_t *ib_ucode_get_connection_charset() {
  return nullptr;
}

const charset_t *ib_ucode_get_charset(ulint id) {
  return nullptr;
}

void ib_ucode_get_charset_width(const charset_t *cs, ulint *mbminlen, ulint *mbmaxlen) {
  *mbminlen = *mbmaxlen = 0;

  if (cs) {
    // FIXME
    //*mbminlen = charset_get_minlen(cs);
    //*mbmaxlen = charset_get_maxlen(cs);
  }
}

int ib_utf8_strcasecmp(const char *p1, const char *p2) {
  /* FIXME: Call the UTF-8 comparison function. */
  /* FIXME: This should take cs as the parameter. */
  return strcasecmp(p1, p2);
}

int ib_utf8_strncasecmp(const char *p1, const char *p2, ulint len) {
  /* FIXME: Call the UTF-8 comparison function. */
  /* FIXME: This should take cs as the parameter. */
  /* FIXME: Which function?  Note that this is locale-dependent.
  For example, there is a capital dotted i and a lower-case
  dotless I (U+0130 and U+0131, respectively).  In many other
  locales, I=i but not in Turkish. */
  return strncasecmp(p1, p2, len);
}

void ib_utf8_casedown(char *a) {
  /* FIXME: Call the UTF-8 tolower() equivalent. */
  /* FIXME: Is this function really needed?  The proper
  implementation is locale-dependent.  In Turkish, the
  lower-case counterpart of the upper-case I (U+0049, one byte)
  is the dotless i (U+0131, two bytes in UTF-8).  That cannot
  even be converted in place. */
  while (*a) {
    *a = tolower(*a);
    ++a;
  }
}

void ib_utf8_convert_from_table_id(const charset_t *cs, char *to, const char *from, ulint len) {
  /* FIXME: why 5*strlen(to)+1?  That is a relic from the MySQL
  5.1 filename safe encoding that encodes some chars in
  four-digit hexadecimal notation, such as @0023.  Do we even
  need this function?  Could the files be named by table id or
  something? */
  /* FIXME: Call the UTF-8 equivalent */
  strncpy(to, from, len);
}

void ib_utf8_convert_from_id(const charset_t *cs, char *to, const char *from, ulint len) {
  /* FIXME: why 3*strlen(to)+1?  I suppose that it comes from
  MySQL, where the connection charset can be 8-bit, such as
  the "latin1" (really Windows Code Page 1252).  Converting
  that to UTF-8 can take 1..3 characters per byte. */
  /* FIXME: Do we even need this function?  Can't we just assume
  that the connection character encoding always is UTF-8?  (We
  may still want to support different collations for UTF-8.) */
  /* FIXME: Call the UTF-8 equivalent */
  strncpy(to, from, len);
}

int ib_utf8_isspace(const charset_t *cs, char c) {
  /* FIXME: Call the equivalent UTF-8 function. */
  /* FIXME: Do we really need this function?  This is needed by
  the InnoDB foreign key parser in MySQL, because U+00A0 is a
  space in the MySQL connection charset latin1 but not in
  utf8. */
  return isspace(c);
}

ulint ib_ucode_get_storage_size( const charset_t *cs, ulint prefix_len, ulint str_len, const char *str)
{
  /* FIXME: Do we really need this function?  Can't we assume
  that all strings are UTF-8?  (We still may want to support
  different collations.) */
  return ut_min(prefix_len, str_len);
}
