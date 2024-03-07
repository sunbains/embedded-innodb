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
#include "api0ucode.h"
#include <ctype.h>
#include "ut0mem.h"
#ifdef HAVE_STRINGS_H
#include <strings.h>
#endif

/** @file api/api0ucode.c
Determines the connection character set.
@return	connection character set */

const charset_t *ib_ucode_get_connection_charset(void) {
  return (NULL);
}

/** Determines the character set based on id.
FIXME: If the id can't be found then what do we do, return some default ?
@return	character set or NULL */

const charset_t *ib_ucode_get_charset(ulint id) /*!< in: Charset-collation code */
{
  return (NULL);
}

/** Get the variable length bounds of the given (multibyte) character set. */

void ib_ucode_get_charset_width(
  const charset_t *cs, /*!< in: Charset */
  ulint *mbminlen,     /*!< out: min len of a char (in bytes) */
  ulint *mbmaxlen
) /*!< out: max len of a char (in bytes) */
{
  *mbminlen = *mbmaxlen = 0;

  if (cs) {
    // FIXME
    //*mbminlen = charset_get_minlen(cs);
    //*mbmaxlen = charset_get_maxlen(cs);
  }
}

/** Compare two strings ignoring case.
@return	0 if equal */

int ib_utf8_strcasecmp(
  const char *p1, /*!< in: string to compare */
  const char *p2
) /*!< in: string to compare */
{
  /* FIXME: Call the UTF-8 comparison function. */
  /* FIXME: This should take cs as the parameter. */
  return (strcasecmp(p1, p2));
}

/** Compare two strings ignoring case.
@return	0 if equal */

int ib_utf8_strncasecmp(
  const char *p1, /*!< in: string to compare */
  const char *p2, /*!< in: string to compare */
  ulint len
) /*!< in: length of string */
{
  /* FIXME: Call the UTF-8 comparison function. */
  /* FIXME: This should take cs as the parameter. */
  /* FIXME: Which function?  Note that this is locale-dependent.
  For example, there is a capital dotted i and a lower-case
  dotless I (U+0130 and U+0131, respectively).  In many other
  locales, I=i but not in Turkish. */
  return (strncasecmp(p1, p2, len));
}

/** Makes all characters in a NUL-terminated UTF-8 string lower case. */

void ib_utf8_casedown(char *a) /*!< in/out: str to put in lower case */
{
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

/** Converts an identifier to a table name. */

void ib_utf8_convert_from_table_id(
  const charset_t *cs, /*!< in: the 'from' character set */
  char *to,            /*!< out: converted identifier */
  const char *from,    /*!< in: identifier to convert */
  ulint len
) /*!< in: length of 'to', in bytes;
                         should be at least
                         5 * strlen(to) + 1 */
{
  /* FIXME: why 5*strlen(to)+1?  That is a relic from the MySQL
  5.1 filename safe encoding that encodes some chars in
  four-digit hexadecimal notation, such as @0023.  Do we even
  need this function?  Could the files be named by table id or
  something? */
  /* FIXME: Call the UTF-8 equivalent */
  strncpy(to, from, len);
}

/** Converts an identifier to UTF-8. */

void ib_utf8_convert_from_id(
  const charset_t *cs, /*!< in: the 'from' character set */
  char *to,            /*!< out: converted identifier */
  const char *from,    /*!< in: identifier to convert */
  ulint len
) /*!< in: length of 'to', in bytes;
                         should be at least
                         3 * strlen(to) + 1 */
{
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

/** Test whether a UTF-8 character is a space or not.
@return	true if isspace(c) */

int ib_utf8_isspace(
  const charset_t *cs, /*!< in: charset */
  char c
) /*!< in: character to test */
{
  /* FIXME: Call the equivalent UTF-8 function. */
  /* FIXME: Do we really need this function?  This is needed by
  the InnoDB foreign key parser in MySQL, because U+00A0 is a
  space in the MySQL connection charset latin1 but not in
  utf8. */
  return (isspace(c));
}

/** This function is used to find the storage length in bytes of the
characters that will fit into prefix_len bytes.
@return	number of bytes required to copy the characters that will fit into
prefix_len bytes. */

ulint ib_ucode_get_storage_size(
  const charset_t *cs, /*!< in: character set */
  ulint prefix_len,    /*!< in: prefix length in bytes */
  ulint str_len,       /*!< in: length of the string in bytes */
  const char *str
) /*!< in: character string */
{
  /* FIXME: Do we really need this function?  Can't we assume
  that all strings are UTF-8?  (We still may want to support
  different collations.) */
  return (ut_min(prefix_len, str_len));
}
