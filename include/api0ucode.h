#ifndef INNOBASE_UNI0CODE_H

#define INNOBASE_UNI0CODE_H

#include "univ.i"

/* Opaque type used by the Unicode implementation. */
typedef struct charset_struct charset_t;

/** Determines the connection character set.
@return	connection character set */

const charset_t *ib_ucode_get_connection_charset(void);

/** Determines the character set based on id.
@return	connection character set */

const charset_t *
ib_ucode_get_charset(ulint id); /*!< in: Charset-collation code */

/** Get the variable length bounds of the given (multibyte) character set. */

void ib_ucode_get_charset_width(
    const charset_t *cs, /*!< in: character set */
    ulint *mbminlen,     /*!< out: min len of a char (in bytes) */
    ulint *mbmaxlen);    /*!< out: max len of a char (in bytes) */

/** This function is used to find the storage length in bytes of the
characters that will fit into prefix_len bytes.
@return	number of bytes required to copy the characters that will fit into
prefix_len bytes. */

ulint ib_ucode_get_storage_size(
    const charset_t *cs, /*!< in: character set id */
    ulint prefix_len,    /*!< in: prefix length in bytes */
    ulint str_len,       /*!< in: length of the string in bytes */
    const char *str);    /*!< in: character string */

/** Compares NUL-terminated UTF-8 strings case insensitively.
@return	0 if a=b, <0 if a<b, >1 if a>b */

int ib_utf8_strcasecmp(const char *a,  /*!< in: first string to compare */
                       const char *b); /*!< in: second string to compare */

/** Compares NUL-terminated UTF-8 strings case insensitively.
@return	0 if a=b, <0 if a<b, >1 if a>b */

int ib_utf8_strncasecmp(const char *a, /*!< in: first string to compare */
                        const char *b, /*!< in: second string to compare */
                        ulint n);      /*!< in: no. of bytes to compare */

/** Makes all characters in a NUL-terminated UTF-8 string lower case. */

void ib_utf8_casedown(char *a); /*!< in/out: str to put in lower case */

/** Test whether a UTF-8 character is a space or not.
@return	TRUE if isspace(c) */

int ib_utf8_isspace(const charset_t *cs, /*!< in: character set */
                    char c);             /*!< in: character to test */

/** Converts an identifier to a UTF-8 table name. */

void ib_utf8_convert_from_table_id(
    const charset_t *cs, /*!< in: the 'from' character set */
    char *to,            /*!< out: converted identifier */
    const char *from,    /*!< in: identifier to convert */
    ulint to_len);       /*!< in: length of 'to', in bytes;
                         should be at least
                         5 * strlen(to) + 1 */

/** Converts an identifier to UTF-8. */

void ib_utf8_convert_from_id(
    const charset_t *cs, /*!< in: the 'from' character set */
    char *to,            /*!< out: converted identifier */
    const char *from,    /*!< in: identifier to convert */
    ulint to_len);       /*!< in: length of 'to', in bytes;
                         should be at least
                         3 * strlen(to) + 1 */
#endif                   /* INNOBASE_UNI0CODE_H */
