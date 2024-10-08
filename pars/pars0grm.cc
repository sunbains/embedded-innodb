/* A Bison parser, made by GNU Bison 3.8.2.  */

/* Bison implementation for Yacc-like parsers in C

   Copyright (C) 1984, 1989-1990, 2000-2015, 2018-2021 Free Software Foundation,
   Inc.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <https://www.gnu.org/licenses/>.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.

   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */

/* C LALR(1) parser skeleton written by Richard Stallman, by
   simplifying the original so-called "semantic" parser.  */

/* DO NOT RELY ON FEATURES THAT ARE NOT DOCUMENTED in the manual,
   especially those whose name start with YY_ or yy_.  They are
   private implementation details that can be changed or removed.  */

/* All symbols defined below should begin with yy or YY, to avoid
   infringing on user name space.  This should be done even for local
   variables, as they might otherwise be expanded by user macros.
   There are some unavoidable exceptions within include files to
   define necessary library symbols; they are noted "INFRINGES ON
   USER NAME SPACE" below.  */

/* Identify Bison output, and Bison version.  */
#define YYBISON 30802

/* Bison version string.  */
#define YYBISON_VERSION "3.8.2"

/* Skeleton name.  */
#define YYSKELETON_NAME "yacc.c"

/* Pure parsers.  */
#define YYPURE 0

/* Push parsers.  */
#define YYPUSH 0

/* Pull parsers.  */
#define YYPULL 1




/* First part of user prologue.  */
#line 28 "pars0grm.y"

/* The value of the semantic attribute is a pointer to a query tree node
que_node_t */

#include "pars0pars.h"
#include "mem0mem.h"
#include "que0types.h"
#include "que0que.h"
#include "row0sel.h"

#define YYSTYPE que_node_t*

/* #define __STDC__ */

int
yylex();

#line 89 "pars0grm.cc"

# ifndef YY_CAST
#  ifdef __cplusplus
#   define YY_CAST(Type, Val) static_cast<Type> (Val)
#   define YY_REINTERPRET_CAST(Type, Val) reinterpret_cast<Type> (Val)
#  else
#   define YY_CAST(Type, Val) ((Type) (Val))
#   define YY_REINTERPRET_CAST(Type, Val) ((Type) (Val))
#  endif
# endif
# ifndef YY_NULLPTR
#  if defined __cplusplus
#   if 201103L <= __cplusplus
#    define YY_NULLPTR nullptr
#   else
#    define YY_NULLPTR 0
#   endif
#  else
#   define YY_NULLPTR ((void*)0)
#  endif
# endif

#include "pars0grm.h"
/* Symbol kind.  */
enum yysymbol_kind_t
{
  YYSYMBOL_YYEMPTY = -2,
  YYSYMBOL_YYEOF = 0,                      /* "end of file"  */
  YYSYMBOL_YYerror = 1,                    /* error  */
  YYSYMBOL_YYUNDEF = 2,                    /* "invalid token"  */
  YYSYMBOL_PARS_INT_LIT = 3,               /* PARS_INT_LIT  */
  YYSYMBOL_PARS_FLOAT_LIT = 4,             /* PARS_FLOAT_LIT  */
  YYSYMBOL_PARS_STR_LIT = 5,               /* PARS_STR_LIT  */
  YYSYMBOL_PARS_FIXBINARY_LIT = 6,         /* PARS_FIXBINARY_LIT  */
  YYSYMBOL_PARS_BLOB_LIT = 7,              /* PARS_BLOB_LIT  */
  YYSYMBOL_PARS_NULL_LIT = 8,              /* PARS_NULL_LIT  */
  YYSYMBOL_PARS_ID_TOKEN = 9,              /* PARS_ID_TOKEN  */
  YYSYMBOL_PARS_AND_TOKEN = 10,            /* PARS_AND_TOKEN  */
  YYSYMBOL_PARS_OR_TOKEN = 11,             /* PARS_OR_TOKEN  */
  YYSYMBOL_PARS_NOT_TOKEN = 12,            /* PARS_NOT_TOKEN  */
  YYSYMBOL_PARS_GE_TOKEN = 13,             /* PARS_GE_TOKEN  */
  YYSYMBOL_PARS_LE_TOKEN = 14,             /* PARS_LE_TOKEN  */
  YYSYMBOL_PARS_NE_TOKEN = 15,             /* PARS_NE_TOKEN  */
  YYSYMBOL_PARS_PROCEDURE_TOKEN = 16,      /* PARS_PROCEDURE_TOKEN  */
  YYSYMBOL_PARS_IN_TOKEN = 17,             /* PARS_IN_TOKEN  */
  YYSYMBOL_PARS_OUT_TOKEN = 18,            /* PARS_OUT_TOKEN  */
  YYSYMBOL_PARS_BINARY_TOKEN = 19,         /* PARS_BINARY_TOKEN  */
  YYSYMBOL_PARS_BLOB_TOKEN = 20,           /* PARS_BLOB_TOKEN  */
  YYSYMBOL_PARS_INT_TOKEN = 21,            /* PARS_INT_TOKEN  */
  YYSYMBOL_PARS_INTEGER_TOKEN = 22,        /* PARS_INTEGER_TOKEN  */
  YYSYMBOL_PARS_FLOAT_TOKEN = 23,          /* PARS_FLOAT_TOKEN  */
  YYSYMBOL_PARS_CHAR_TOKEN = 24,           /* PARS_CHAR_TOKEN  */
  YYSYMBOL_PARS_IS_TOKEN = 25,             /* PARS_IS_TOKEN  */
  YYSYMBOL_PARS_BEGIN_TOKEN = 26,          /* PARS_BEGIN_TOKEN  */
  YYSYMBOL_PARS_END_TOKEN = 27,            /* PARS_END_TOKEN  */
  YYSYMBOL_PARS_IF_TOKEN = 28,             /* PARS_IF_TOKEN  */
  YYSYMBOL_PARS_THEN_TOKEN = 29,           /* PARS_THEN_TOKEN  */
  YYSYMBOL_PARS_ELSE_TOKEN = 30,           /* PARS_ELSE_TOKEN  */
  YYSYMBOL_PARS_ELSIF_TOKEN = 31,          /* PARS_ELSIF_TOKEN  */
  YYSYMBOL_PARS_LOOP_TOKEN = 32,           /* PARS_LOOP_TOKEN  */
  YYSYMBOL_PARS_WHILE_TOKEN = 33,          /* PARS_WHILE_TOKEN  */
  YYSYMBOL_PARS_RETURN_TOKEN = 34,         /* PARS_RETURN_TOKEN  */
  YYSYMBOL_PARS_SELECT_TOKEN = 35,         /* PARS_SELECT_TOKEN  */
  YYSYMBOL_PARS_SUM_TOKEN = 36,            /* PARS_SUM_TOKEN  */
  YYSYMBOL_PARS_COUNT_TOKEN = 37,          /* PARS_COUNT_TOKEN  */
  YYSYMBOL_PARS_DISTINCT_TOKEN = 38,       /* PARS_DISTINCT_TOKEN  */
  YYSYMBOL_PARS_FROM_TOKEN = 39,           /* PARS_FROM_TOKEN  */
  YYSYMBOL_PARS_WHERE_TOKEN = 40,          /* PARS_WHERE_TOKEN  */
  YYSYMBOL_PARS_FOR_TOKEN = 41,            /* PARS_FOR_TOKEN  */
  YYSYMBOL_PARS_DDOT_TOKEN = 42,           /* PARS_DDOT_TOKEN  */
  YYSYMBOL_PARS_READ_TOKEN = 43,           /* PARS_READ_TOKEN  */
  YYSYMBOL_PARS_ORDER_TOKEN = 44,          /* PARS_ORDER_TOKEN  */
  YYSYMBOL_PARS_BY_TOKEN = 45,             /* PARS_BY_TOKEN  */
  YYSYMBOL_PARS_ASC_TOKEN = 46,            /* PARS_ASC_TOKEN  */
  YYSYMBOL_PARS_DESC_TOKEN = 47,           /* PARS_DESC_TOKEN  */
  YYSYMBOL_PARS_INSERT_TOKEN = 48,         /* PARS_INSERT_TOKEN  */
  YYSYMBOL_PARS_INTO_TOKEN = 49,           /* PARS_INTO_TOKEN  */
  YYSYMBOL_PARS_VALUES_TOKEN = 50,         /* PARS_VALUES_TOKEN  */
  YYSYMBOL_PARS_UPDATE_TOKEN = 51,         /* PARS_UPDATE_TOKEN  */
  YYSYMBOL_PARS_SET_TOKEN = 52,            /* PARS_SET_TOKEN  */
  YYSYMBOL_PARS_DELETE_TOKEN = 53,         /* PARS_DELETE_TOKEN  */
  YYSYMBOL_PARS_CURRENT_TOKEN = 54,        /* PARS_CURRENT_TOKEN  */
  YYSYMBOL_PARS_OF_TOKEN = 55,             /* PARS_OF_TOKEN  */
  YYSYMBOL_PARS_CREATE_TOKEN = 56,         /* PARS_CREATE_TOKEN  */
  YYSYMBOL_PARS_TABLE_TOKEN = 57,          /* PARS_TABLE_TOKEN  */
  YYSYMBOL_PARS_INDEX_TOKEN = 58,          /* PARS_INDEX_TOKEN  */
  YYSYMBOL_PARS_UNIQUE_TOKEN = 59,         /* PARS_UNIQUE_TOKEN  */
  YYSYMBOL_PARS_CLUSTERED_TOKEN = 60,      /* PARS_CLUSTERED_TOKEN  */
  YYSYMBOL_PARS_DOES_NOT_FIT_IN_MEM_TOKEN = 61, /* PARS_DOES_NOT_FIT_IN_MEM_TOKEN  */
  YYSYMBOL_PARS_ON_TOKEN = 62,             /* PARS_ON_TOKEN  */
  YYSYMBOL_PARS_ASSIGN_TOKEN = 63,         /* PARS_ASSIGN_TOKEN  */
  YYSYMBOL_PARS_DECLARE_TOKEN = 64,        /* PARS_DECLARE_TOKEN  */
  YYSYMBOL_PARS_CURSOR_TOKEN = 65,         /* PARS_CURSOR_TOKEN  */
  YYSYMBOL_PARS_SQL_TOKEN = 66,            /* PARS_SQL_TOKEN  */
  YYSYMBOL_PARS_OPEN_TOKEN = 67,           /* PARS_OPEN_TOKEN  */
  YYSYMBOL_PARS_FETCH_TOKEN = 68,          /* PARS_FETCH_TOKEN  */
  YYSYMBOL_PARS_CLOSE_TOKEN = 69,          /* PARS_CLOSE_TOKEN  */
  YYSYMBOL_PARS_NOTFOUND_TOKEN = 70,       /* PARS_NOTFOUND_TOKEN  */
  YYSYMBOL_PARS_TO_CHAR_TOKEN = 71,        /* PARS_TO_CHAR_TOKEN  */
  YYSYMBOL_PARS_TO_NUMBER_TOKEN = 72,      /* PARS_TO_NUMBER_TOKEN  */
  YYSYMBOL_PARS_TO_BINARY_TOKEN = 73,      /* PARS_TO_BINARY_TOKEN  */
  YYSYMBOL_PARS_BINARY_TO_NUMBER_TOKEN = 74, /* PARS_BINARY_TO_NUMBER_TOKEN  */
  YYSYMBOL_PARS_SUBSTR_TOKEN = 75,         /* PARS_SUBSTR_TOKEN  */
  YYSYMBOL_PARS_REPLSTR_TOKEN = 76,        /* PARS_REPLSTR_TOKEN  */
  YYSYMBOL_PARS_CONCAT_TOKEN = 77,         /* PARS_CONCAT_TOKEN  */
  YYSYMBOL_PARS_INSTR_TOKEN = 78,          /* PARS_INSTR_TOKEN  */
  YYSYMBOL_PARS_LENGTH_TOKEN = 79,         /* PARS_LENGTH_TOKEN  */
  YYSYMBOL_PARS_SYSDATE_TOKEN = 80,        /* PARS_SYSDATE_TOKEN  */
  YYSYMBOL_PARS_PRINTF_TOKEN = 81,         /* PARS_PRINTF_TOKEN  */
  YYSYMBOL_PARS_ASSERT_TOKEN = 82,         /* PARS_ASSERT_TOKEN  */
  YYSYMBOL_PARS_RND_TOKEN = 83,            /* PARS_RND_TOKEN  */
  YYSYMBOL_PARS_RND_STR_TOKEN = 84,        /* PARS_RND_STR_TOKEN  */
  YYSYMBOL_PARS_ROW_PRINTF_TOKEN = 85,     /* PARS_ROW_PRINTF_TOKEN  */
  YYSYMBOL_PARS_COMMIT_TOKEN = 86,         /* PARS_COMMIT_TOKEN  */
  YYSYMBOL_PARS_ROLLBACK_TOKEN = 87,       /* PARS_ROLLBACK_TOKEN  */
  YYSYMBOL_PARS_WORK_TOKEN = 88,           /* PARS_WORK_TOKEN  */
  YYSYMBOL_PARS_UNSIGNED_TOKEN = 89,       /* PARS_UNSIGNED_TOKEN  */
  YYSYMBOL_PARS_EXIT_TOKEN = 90,           /* PARS_EXIT_TOKEN  */
  YYSYMBOL_PARS_FUNCTION_TOKEN = 91,       /* PARS_FUNCTION_TOKEN  */
  YYSYMBOL_PARS_LOCK_TOKEN = 92,           /* PARS_LOCK_TOKEN  */
  YYSYMBOL_PARS_SHARE_TOKEN = 93,          /* PARS_SHARE_TOKEN  */
  YYSYMBOL_PARS_MODE_TOKEN = 94,           /* PARS_MODE_TOKEN  */
  YYSYMBOL_95_ = 95,                       /* '='  */
  YYSYMBOL_96_ = 96,                       /* '<'  */
  YYSYMBOL_97_ = 97,                       /* '>'  */
  YYSYMBOL_98_ = 98,                       /* '-'  */
  YYSYMBOL_99_ = 99,                       /* '+'  */
  YYSYMBOL_100_ = 100,                     /* '*'  */
  YYSYMBOL_101_ = 101,                     /* '/'  */
  YYSYMBOL_NEG = 102,                      /* NEG  */
  YYSYMBOL_103_ = 103,                     /* '%'  */
  YYSYMBOL_104_ = 104,                     /* ';'  */
  YYSYMBOL_105_ = 105,                     /* '('  */
  YYSYMBOL_106_ = 106,                     /* ')'  */
  YYSYMBOL_107_ = 107,                     /* '?'  */
  YYSYMBOL_108_ = 108,                     /* ','  */
  YYSYMBOL_109_ = 109,                     /* '{'  */
  YYSYMBOL_110_ = 110,                     /* '}'  */
  YYSYMBOL_YYACCEPT = 111,                 /* $accept  */
  YYSYMBOL_top_statement = 112,            /* top_statement  */
  YYSYMBOL_statement = 113,                /* statement  */
  YYSYMBOL_statement_list = 114,           /* statement_list  */
  YYSYMBOL_exp = 115,                      /* exp  */
  YYSYMBOL_function_name = 116,            /* function_name  */
  YYSYMBOL_question_mark_list = 117,       /* question_mark_list  */
  YYSYMBOL_stored_procedure_call = 118,    /* stored_procedure_call  */
  YYSYMBOL_predefined_procedure_call = 119, /* predefined_procedure_call  */
  YYSYMBOL_predefined_procedure_name = 120, /* predefined_procedure_name  */
  YYSYMBOL_user_function_call = 121,       /* user_function_call  */
  YYSYMBOL_table_list = 122,               /* table_list  */
  YYSYMBOL_variable_list = 123,            /* variable_list  */
  YYSYMBOL_exp_list = 124,                 /* exp_list  */
  YYSYMBOL_select_item = 125,              /* select_item  */
  YYSYMBOL_select_item_list = 126,         /* select_item_list  */
  YYSYMBOL_select_list = 127,              /* select_list  */
  YYSYMBOL_search_condition = 128,         /* search_condition  */
  YYSYMBOL_for_update_clause = 129,        /* for_update_clause  */
  YYSYMBOL_lock_shared_clause = 130,       /* lock_shared_clause  */
  YYSYMBOL_order_direction = 131,          /* order_direction  */
  YYSYMBOL_order_by_clause = 132,          /* order_by_clause  */
  YYSYMBOL_select_statement = 133,         /* select_statement  */
  YYSYMBOL_insert_statement_start = 134,   /* insert_statement_start  */
  YYSYMBOL_insert_statement = 135,         /* insert_statement  */
  YYSYMBOL_column_assignment = 136,        /* column_assignment  */
  YYSYMBOL_column_assignment_list = 137,   /* column_assignment_list  */
  YYSYMBOL_cursor_positioned = 138,        /* cursor_positioned  */
  YYSYMBOL_update_statement_start = 139,   /* update_statement_start  */
  YYSYMBOL_update_statement_searched = 140, /* update_statement_searched  */
  YYSYMBOL_update_statement_positioned = 141, /* update_statement_positioned  */
  YYSYMBOL_delete_statement_start = 142,   /* delete_statement_start  */
  YYSYMBOL_delete_statement_searched = 143, /* delete_statement_searched  */
  YYSYMBOL_delete_statement_positioned = 144, /* delete_statement_positioned  */
  YYSYMBOL_assignment_statement = 145,     /* assignment_statement  */
  YYSYMBOL_row_printf_statement = 146,     /* row_printf_statement  */
  YYSYMBOL_elsif_element = 147,            /* elsif_element  */
  YYSYMBOL_elsif_list = 148,               /* elsif_list  */
  YYSYMBOL_else_part = 149,                /* else_part  */
  YYSYMBOL_if_statement = 150,             /* if_statement  */
  YYSYMBOL_while_statement = 151,          /* while_statement  */
  YYSYMBOL_for_statement = 152,            /* for_statement  */
  YYSYMBOL_exit_statement = 153,           /* exit_statement  */
  YYSYMBOL_return_statement = 154,         /* return_statement  */
  YYSYMBOL_open_cursor_statement = 155,    /* open_cursor_statement  */
  YYSYMBOL_close_cursor_statement = 156,   /* close_cursor_statement  */
  YYSYMBOL_fetch_statement = 157,          /* fetch_statement  */
  YYSYMBOL_column_def = 158,               /* column_def  */
  YYSYMBOL_column_def_list = 159,          /* column_def_list  */
  YYSYMBOL_opt_column_len = 160,           /* opt_column_len  */
  YYSYMBOL_opt_unsigned = 161,             /* opt_unsigned  */
  YYSYMBOL_opt_not_null = 162,             /* opt_not_null  */
  YYSYMBOL_not_fit_in_memory = 163,        /* not_fit_in_memory  */
  YYSYMBOL_create_table = 164,             /* create_table  */
  YYSYMBOL_column_list = 165,              /* column_list  */
  YYSYMBOL_unique_def = 166,               /* unique_def  */
  YYSYMBOL_clustered_def = 167,            /* clustered_def  */
  YYSYMBOL_create_index = 168,             /* create_index  */
  YYSYMBOL_commit_statement = 169,         /* commit_statement  */
  YYSYMBOL_rollback_statement = 170,       /* rollback_statement  */
  YYSYMBOL_type_name = 171,                /* type_name  */
  YYSYMBOL_parameter_declaration = 172,    /* parameter_declaration  */
  YYSYMBOL_parameter_declaration_list = 173, /* parameter_declaration_list  */
  YYSYMBOL_variable_declaration = 174,     /* variable_declaration  */
  YYSYMBOL_variable_declaration_list = 175, /* variable_declaration_list  */
  YYSYMBOL_cursor_declaration = 176,       /* cursor_declaration  */
  YYSYMBOL_function_declaration = 177,     /* function_declaration  */
  YYSYMBOL_declaration = 178,              /* declaration  */
  YYSYMBOL_declaration_list = 179,         /* declaration_list  */
  YYSYMBOL_procedure_definition = 180      /* procedure_definition  */
};
typedef enum yysymbol_kind_t yysymbol_kind_t;




#ifdef short
# undef short
#endif

/* On compilers that do not define __PTRDIFF_MAX__ etc., make sure
   <limits.h> and (if available) <stdint.h> are included
   so that the code can choose integer types of a good width.  */

#ifndef __PTRDIFF_MAX__
# include <limits.h> /* INFRINGES ON USER NAME SPACE */
# if defined __STDC_VERSION__ && 199901 <= __STDC_VERSION__
#  include <stdint.h> /* INFRINGES ON USER NAME SPACE */
#  define YY_STDINT_H
# endif
#endif

/* Narrow types that promote to a signed type and that can represent a
   signed or unsigned integer of at least N bits.  In tables they can
   save space and decrease cache pressure.  Promoting to a signed type
   helps avoid bugs in integer arithmetic.  */

#ifdef __INT_LEAST8_MAX__
typedef __INT_LEAST8_TYPE__ yytype_int8;
#elif defined YY_STDINT_H
typedef int_least8_t yytype_int8;
#else
typedef signed char yytype_int8;
#endif

#ifdef __INT_LEAST16_MAX__
typedef __INT_LEAST16_TYPE__ yytype_int16;
#elif defined YY_STDINT_H
typedef int_least16_t yytype_int16;
#else
typedef short yytype_int16;
#endif

/* Work around bug in HP-UX 11.23, which defines these macros
   incorrectly for preprocessor constants.  This workaround can likely
   be removed in 2023, as HPE has promised support for HP-UX 11.23
   (aka HP-UX 11i v2) only through the end of 2022; see Table 2 of
   <https://h20195.www2.hpe.com/V2/getpdf.aspx/4AA4-7673ENW.pdf>.  */
#ifdef __hpux
# undef UINT_LEAST8_MAX
# undef UINT_LEAST16_MAX
# define UINT_LEAST8_MAX 255
# define UINT_LEAST16_MAX 65535
#endif

#if defined __UINT_LEAST8_MAX__ && __UINT_LEAST8_MAX__ <= __INT_MAX__
typedef __UINT_LEAST8_TYPE__ yytype_uint8;
#elif (!defined __UINT_LEAST8_MAX__ && defined YY_STDINT_H \
       && UINT_LEAST8_MAX <= INT_MAX)
typedef uint_least8_t yytype_uint8;
#elif !defined __UINT_LEAST8_MAX__ && UCHAR_MAX <= INT_MAX
typedef unsigned char yytype_uint8;
#else
typedef short yytype_uint8;
#endif

#if defined __UINT_LEAST16_MAX__ && __UINT_LEAST16_MAX__ <= __INT_MAX__
typedef __UINT_LEAST16_TYPE__ yytype_uint16;
#elif (!defined __UINT_LEAST16_MAX__ && defined YY_STDINT_H \
       && UINT_LEAST16_MAX <= INT_MAX)
typedef uint_least16_t yytype_uint16;
#elif !defined __UINT_LEAST16_MAX__ && USHRT_MAX <= INT_MAX
typedef unsigned short yytype_uint16;
#else
typedef int yytype_uint16;
#endif

#ifndef YYPTRDIFF_T
# if defined __PTRDIFF_TYPE__ && defined __PTRDIFF_MAX__
#  define YYPTRDIFF_T __PTRDIFF_TYPE__
#  define YYPTRDIFF_MAXIMUM __PTRDIFF_MAX__
# elif defined PTRDIFF_MAX
#  ifndef ptrdiff_t
#   include <stddef.h> /* INFRINGES ON USER NAME SPACE */
#  endif
#  define YYPTRDIFF_T ptrdiff_t
#  define YYPTRDIFF_MAXIMUM PTRDIFF_MAX
# else
#  define YYPTRDIFF_T long
#  define YYPTRDIFF_MAXIMUM LONG_MAX
# endif
#endif

#ifndef YYSIZE_T
# ifdef __SIZE_TYPE__
#  define YYSIZE_T __SIZE_TYPE__
# elif defined size_t
#  define YYSIZE_T size_t
# elif defined __STDC_VERSION__ && 199901 <= __STDC_VERSION__
#  include <stddef.h> /* INFRINGES ON USER NAME SPACE */
#  define YYSIZE_T size_t
# else
#  define YYSIZE_T unsigned
# endif
#endif

#define YYSIZE_MAXIMUM                                  \
  YY_CAST (YYPTRDIFF_T,                                 \
           (YYPTRDIFF_MAXIMUM < YY_CAST (YYSIZE_T, -1)  \
            ? YYPTRDIFF_MAXIMUM                         \
            : YY_CAST (YYSIZE_T, -1)))

#define YYSIZEOF(X) YY_CAST (YYPTRDIFF_T, sizeof (X))


/* Stored state numbers (used for stacks). */
typedef yytype_int16 yy_state_t;

/* State numbers in computations.  */
typedef int yy_state_fast_t;

#ifndef YY_
# if defined YYENABLE_NLS && YYENABLE_NLS
#  if ENABLE_NLS
#   include <libintl.h> /* INFRINGES ON USER NAME SPACE */
#   define YY_(Msgid) dgettext ("bison-runtime", Msgid)
#  endif
# endif
# ifndef YY_
#  define YY_(Msgid) Msgid
# endif
#endif


#ifndef YY_ATTRIBUTE_PURE
# if defined __GNUC__ && 2 < __GNUC__ + (96 <= __GNUC_MINOR__)
#  define YY_ATTRIBUTE_PURE __attribute__ ((__pure__))
# else
#  define YY_ATTRIBUTE_PURE
# endif
#endif

#ifndef YY_ATTRIBUTE_UNUSED
# if defined __GNUC__ && 2 < __GNUC__ + (7 <= __GNUC_MINOR__)
#  define YY_ATTRIBUTE_UNUSED __attribute__ ((__unused__))
# else
#  define YY_ATTRIBUTE_UNUSED
# endif
#endif

/* Suppress unused-variable warnings by "using" E.  */
#if ! defined lint || defined __GNUC__
# define YY_USE(E) ((void) (E))
#else
# define YY_USE(E) /* empty */
#endif

/* Suppress an incorrect diagnostic about yylval being uninitialized.  */
#if defined __GNUC__ && ! defined __ICC && 406 <= __GNUC__ * 100 + __GNUC_MINOR__
# if __GNUC__ * 100 + __GNUC_MINOR__ < 407
#  define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN                           \
    _Pragma ("GCC diagnostic push")                                     \
    _Pragma ("GCC diagnostic ignored \"-Wuninitialized\"")
# else
#  define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN                           \
    _Pragma ("GCC diagnostic push")                                     \
    _Pragma ("GCC diagnostic ignored \"-Wuninitialized\"")              \
    _Pragma ("GCC diagnostic ignored \"-Wmaybe-uninitialized\"")
# endif
# define YY_IGNORE_MAYBE_UNINITIALIZED_END      \
    _Pragma ("GCC diagnostic pop")
#else
# define YY_INITIAL_VALUE(Value) Value
#endif
#ifndef YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
# define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
# define YY_IGNORE_MAYBE_UNINITIALIZED_END
#endif
#ifndef YY_INITIAL_VALUE
# define YY_INITIAL_VALUE(Value) /* Nothing. */
#endif

#if defined __cplusplus && defined __GNUC__ && ! defined __ICC && 6 <= __GNUC__
# define YY_IGNORE_USELESS_CAST_BEGIN                          \
    _Pragma ("GCC diagnostic push")                            \
    _Pragma ("GCC diagnostic ignored \"-Wuseless-cast\"")
# define YY_IGNORE_USELESS_CAST_END            \
    _Pragma ("GCC diagnostic pop")
#endif
#ifndef YY_IGNORE_USELESS_CAST_BEGIN
# define YY_IGNORE_USELESS_CAST_BEGIN
# define YY_IGNORE_USELESS_CAST_END
#endif


#define YY_ASSERT(E) ((void) (0 && (E)))

#if !defined yyoverflow

/* The parser invokes alloca or malloc; define the necessary symbols.  */

# ifdef YYSTACK_USE_ALLOCA
#  if YYSTACK_USE_ALLOCA
#   ifdef __GNUC__
#    define YYSTACK_ALLOC __builtin_alloca
#   elif defined __BUILTIN_VA_ARG_INCR
#    include <alloca.h> /* INFRINGES ON USER NAME SPACE */
#   elif defined _AIX
#    define YYSTACK_ALLOC __alloca
#   elif defined _MSC_VER
#    include <malloc.h> /* INFRINGES ON USER NAME SPACE */
#    define alloca _alloca
#   else
#    define YYSTACK_ALLOC alloca
#    if ! defined _ALLOCA_H && ! defined EXIT_SUCCESS
#     include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
      /* Use EXIT_SUCCESS as a witness for stdlib.h.  */
#     ifndef EXIT_SUCCESS
#      define EXIT_SUCCESS 0
#     endif
#    endif
#   endif
#  endif
# endif

# ifdef YYSTACK_ALLOC
   /* Pacify GCC's 'empty if-body' warning.  */
#  define YYSTACK_FREE(Ptr) do { /* empty */; } while (0)
#  ifndef YYSTACK_ALLOC_MAXIMUM
    /* The OS might guarantee only one guard page at the bottom of the stack,
       and a page size can be as small as 4096 bytes.  So we cannot safely
       invoke alloca (N) if N exceeds 4096.  Use a slightly smaller number
       to allow for a few compiler-allocated temporary stack slots.  */
#   define YYSTACK_ALLOC_MAXIMUM 4032 /* reasonable circa 2006 */
#  endif
# else
#  define YYSTACK_ALLOC YYMALLOC
#  define YYSTACK_FREE YYFREE
#  ifndef YYSTACK_ALLOC_MAXIMUM
#   define YYSTACK_ALLOC_MAXIMUM YYSIZE_MAXIMUM
#  endif
#  if (defined __cplusplus && ! defined EXIT_SUCCESS \
       && ! ((defined YYMALLOC || defined malloc) \
             && (defined YYFREE || defined free)))
#   include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
#   ifndef EXIT_SUCCESS
#    define EXIT_SUCCESS 0
#   endif
#  endif
#  ifndef YYMALLOC
#   define YYMALLOC malloc
#   if ! defined malloc && ! defined EXIT_SUCCESS
void *malloc (YYSIZE_T); /* INFRINGES ON USER NAME SPACE */
#   endif
#  endif
#  ifndef YYFREE
#   define YYFREE free
#   if ! defined free && ! defined EXIT_SUCCESS
void free (void *); /* INFRINGES ON USER NAME SPACE */
#   endif
#  endif
# endif
#endif /* !defined yyoverflow */

#if (! defined yyoverflow \
     && (! defined __cplusplus \
         || (defined YYSTYPE_IS_TRIVIAL && YYSTYPE_IS_TRIVIAL)))

/* A type that is properly aligned for any stack member.  */
union yyalloc
{
  yy_state_t yyss_alloc;
  YYSTYPE yyvs_alloc;
};

/* The size of the maximum gap between one aligned stack and the next.  */
# define YYSTACK_GAP_MAXIMUM (YYSIZEOF (union yyalloc) - 1)

/* The size of an array large to enough to hold all stacks, each with
   N elements.  */
# define YYSTACK_BYTES(N) \
     ((N) * (YYSIZEOF (yy_state_t) + YYSIZEOF (YYSTYPE)) \
      + YYSTACK_GAP_MAXIMUM)

# define YYCOPY_NEEDED 1

/* Relocate STACK from its old location to the new one.  The
   local variables YYSIZE and YYSTACKSIZE give the old and new number of
   elements in the stack, and YYPTR gives the new location of the
   stack.  Advance YYPTR to a properly aligned location for the next
   stack.  */
# define YYSTACK_RELOCATE(Stack_alloc, Stack)                           \
    do                                                                  \
      {                                                                 \
        YYPTRDIFF_T yynewbytes;                                         \
        YYCOPY (&yyptr->Stack_alloc, Stack, yysize);                    \
        Stack = &yyptr->Stack_alloc;                                    \
        yynewbytes = yystacksize * YYSIZEOF (*Stack) + YYSTACK_GAP_MAXIMUM; \
        yyptr += yynewbytes / YYSIZEOF (*yyptr);                        \
      }                                                                 \
    while (0)

#endif

#if defined YYCOPY_NEEDED && YYCOPY_NEEDED
/* Copy COUNT objects from SRC to DST.  The source and destination do
   not overlap.  */
# ifndef YYCOPY
#  if defined __GNUC__ && 1 < __GNUC__
#   define YYCOPY(Dst, Src, Count) \
      __builtin_memcpy (Dst, Src, YY_CAST (YYSIZE_T, (Count)) * sizeof (*(Src)))
#  else
#   define YYCOPY(Dst, Src, Count)              \
      do                                        \
        {                                       \
          YYPTRDIFF_T yyi;                      \
          for (yyi = 0; yyi < (Count); yyi++)   \
            (Dst)[yyi] = (Src)[yyi];            \
        }                                       \
      while (0)
#  endif
# endif
#endif /* !YYCOPY_NEEDED */

/* YYFINAL -- State number of the termination state.  */
#define YYFINAL  5
/* YYLAST -- Last index in YYTABLE.  */
#define YYLAST   752

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  111
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  70
/* YYNRULES -- Number of rules.  */
#define YYNRULES  175
/* YYNSTATES -- Number of states.  */
#define YYNSTATES  339

/* YYMAXUTOK -- Last valid token kind.  */
#define YYMAXUTOK   350


/* YYTRANSLATE(TOKEN-NUM) -- Symbol number corresponding to TOKEN-NUM
   as returned by yylex, with out-of-bounds checking.  */
#define YYTRANSLATE(YYX)                                \
  (0 <= (YYX) && (YYX) <= YYMAXUTOK                     \
   ? YY_CAST (yysymbol_kind_t, yytranslate[YYX])        \
   : YYSYMBOL_YYUNDEF)

/* YYTRANSLATE[TOKEN-NUM] -- Symbol number corresponding to TOKEN-NUM
   as returned by yylex.  */
static const yytype_int8 yytranslate[] =
{
       0,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,   103,     2,     2,
     105,   106,   100,    99,   108,    98,     2,   101,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,   104,
      96,    95,    97,   107,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,   109,     2,   110,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     1,     2,     3,     4,
       5,     6,     7,     8,     9,    10,    11,    12,    13,    14,
      15,    16,    17,    18,    19,    20,    21,    22,    23,    24,
      25,    26,    27,    28,    29,    30,    31,    32,    33,    34,
      35,    36,    37,    38,    39,    40,    41,    42,    43,    44,
      45,    46,    47,    48,    49,    50,    51,    52,    53,    54,
      55,    56,    57,    58,    59,    60,    61,    62,    63,    64,
      65,    66,    67,    68,    69,    70,    71,    72,    73,    74,
      75,    76,    77,    78,    79,    80,    81,    82,    83,    84,
      85,    86,    87,    88,    89,    90,    91,    92,    93,    94,
     102
};

#if YYDEBUG
/* YYRLINE[YYN] -- Source line where rule number YYN was defined.  */
static const yytype_int16 yyrline[] =
{
       0,   151,   151,   154,   155,   156,   157,   158,   159,   160,
     161,   162,   163,   164,   165,   166,   167,   168,   169,   170,
     171,   172,   173,   174,   175,   179,   180,   185,   186,   188,
     189,   190,   191,   192,   193,   194,   195,   196,   197,   198,
     199,   200,   201,   202,   203,   204,   205,   206,   207,   208,
     209,   210,   212,   217,   218,   219,   220,   222,   223,   224,
     225,   226,   227,   228,   231,   233,   234,   238,   243,   248,
     249,   250,   254,   258,   259,   264,   265,   266,   271,   272,
     273,   277,   278,   283,   289,   296,   297,   298,   303,   304,
     306,   310,   311,   315,   316,   321,   322,   327,   328,   329,
     333,   334,   341,   356,   361,   364,   372,   377,   378,   383,
     389,   398,   406,   414,   422,   430,   438,   444,   450,   456,
     457,   462,   463,   465,   469,   476,   482,   492,   496,   500,
     507,   514,   518,   526,   535,   536,   541,   542,   547,   548,
     554,   555,   561,   562,   568,   576,   577,   582,   583,   587,
     588,   592,   605,   610,   615,   616,   617,   618,   619,   623,
     628,   636,   637,   638,   643,   649,   651,   652,   656,   664,
     670,   671,   674,   676,   677,   681
};
#endif

/** Accessing symbol of state STATE.  */
#define YY_ACCESSING_SYMBOL(State) YY_CAST (yysymbol_kind_t, yystos[State])

#if YYDEBUG || 0
/* The user-facing name of the symbol whose (internal) number is
   YYSYMBOL.  No bounds checking.  */
static const char *yysymbol_name (yysymbol_kind_t yysymbol) YY_ATTRIBUTE_UNUSED;

/* YYTNAME[SYMBOL-NUM] -- String name of the symbol SYMBOL-NUM.
   First, the terminals, then, starting at YYNTOKENS, nonterminals.  */
static const char *const yytname[] =
{
  "\"end of file\"", "error", "\"invalid token\"", "PARS_INT_LIT",
  "PARS_FLOAT_LIT", "PARS_STR_LIT", "PARS_FIXBINARY_LIT", "PARS_BLOB_LIT",
  "PARS_NULL_LIT", "PARS_ID_TOKEN", "PARS_AND_TOKEN", "PARS_OR_TOKEN",
  "PARS_NOT_TOKEN", "PARS_GE_TOKEN", "PARS_LE_TOKEN", "PARS_NE_TOKEN",
  "PARS_PROCEDURE_TOKEN", "PARS_IN_TOKEN", "PARS_OUT_TOKEN",
  "PARS_BINARY_TOKEN", "PARS_BLOB_TOKEN", "PARS_INT_TOKEN",
  "PARS_INTEGER_TOKEN", "PARS_FLOAT_TOKEN", "PARS_CHAR_TOKEN",
  "PARS_IS_TOKEN", "PARS_BEGIN_TOKEN", "PARS_END_TOKEN", "PARS_IF_TOKEN",
  "PARS_THEN_TOKEN", "PARS_ELSE_TOKEN", "PARS_ELSIF_TOKEN",
  "PARS_LOOP_TOKEN", "PARS_WHILE_TOKEN", "PARS_RETURN_TOKEN",
  "PARS_SELECT_TOKEN", "PARS_SUM_TOKEN", "PARS_COUNT_TOKEN",
  "PARS_DISTINCT_TOKEN", "PARS_FROM_TOKEN", "PARS_WHERE_TOKEN",
  "PARS_FOR_TOKEN", "PARS_DDOT_TOKEN", "PARS_READ_TOKEN",
  "PARS_ORDER_TOKEN", "PARS_BY_TOKEN", "PARS_ASC_TOKEN", "PARS_DESC_TOKEN",
  "PARS_INSERT_TOKEN", "PARS_INTO_TOKEN", "PARS_VALUES_TOKEN",
  "PARS_UPDATE_TOKEN", "PARS_SET_TOKEN", "PARS_DELETE_TOKEN",
  "PARS_CURRENT_TOKEN", "PARS_OF_TOKEN", "PARS_CREATE_TOKEN",
  "PARS_TABLE_TOKEN", "PARS_INDEX_TOKEN", "PARS_UNIQUE_TOKEN",
  "PARS_CLUSTERED_TOKEN", "PARS_DOES_NOT_FIT_IN_MEM_TOKEN",
  "PARS_ON_TOKEN", "PARS_ASSIGN_TOKEN", "PARS_DECLARE_TOKEN",
  "PARS_CURSOR_TOKEN", "PARS_SQL_TOKEN", "PARS_OPEN_TOKEN",
  "PARS_FETCH_TOKEN", "PARS_CLOSE_TOKEN", "PARS_NOTFOUND_TOKEN",
  "PARS_TO_CHAR_TOKEN", "PARS_TO_NUMBER_TOKEN", "PARS_TO_BINARY_TOKEN",
  "PARS_BINARY_TO_NUMBER_TOKEN", "PARS_SUBSTR_TOKEN", "PARS_REPLSTR_TOKEN",
  "PARS_CONCAT_TOKEN", "PARS_INSTR_TOKEN", "PARS_LENGTH_TOKEN",
  "PARS_SYSDATE_TOKEN", "PARS_PRINTF_TOKEN", "PARS_ASSERT_TOKEN",
  "PARS_RND_TOKEN", "PARS_RND_STR_TOKEN", "PARS_ROW_PRINTF_TOKEN",
  "PARS_COMMIT_TOKEN", "PARS_ROLLBACK_TOKEN", "PARS_WORK_TOKEN",
  "PARS_UNSIGNED_TOKEN", "PARS_EXIT_TOKEN", "PARS_FUNCTION_TOKEN",
  "PARS_LOCK_TOKEN", "PARS_SHARE_TOKEN", "PARS_MODE_TOKEN", "'='", "'<'",
  "'>'", "'-'", "'+'", "'*'", "'/'", "NEG", "'%'", "';'", "'('", "')'",
  "'?'", "','", "'{'", "'}'", "$accept", "top_statement", "statement",
  "statement_list", "exp", "function_name", "question_mark_list",
  "stored_procedure_call", "predefined_procedure_call",
  "predefined_procedure_name", "user_function_call", "table_list",
  "variable_list", "exp_list", "select_item", "select_item_list",
  "select_list", "search_condition", "for_update_clause",
  "lock_shared_clause", "order_direction", "order_by_clause",
  "select_statement", "insert_statement_start", "insert_statement",
  "column_assignment", "column_assignment_list", "cursor_positioned",
  "update_statement_start", "update_statement_searched",
  "update_statement_positioned", "delete_statement_start",
  "delete_statement_searched", "delete_statement_positioned",
  "assignment_statement", "row_printf_statement", "elsif_element",
  "elsif_list", "else_part", "if_statement", "while_statement",
  "for_statement", "exit_statement", "return_statement",
  "open_cursor_statement", "close_cursor_statement", "fetch_statement",
  "column_def", "column_def_list", "opt_column_len", "opt_unsigned",
  "opt_not_null", "not_fit_in_memory", "create_table", "column_list",
  "unique_def", "clustered_def", "create_index", "commit_statement",
  "rollback_statement", "type_name", "parameter_declaration",
  "parameter_declaration_list", "variable_declaration",
  "variable_declaration_list", "cursor_declaration",
  "function_declaration", "declaration", "declaration_list",
  "procedure_definition", YY_NULLPTR
};

static const char *
yysymbol_name (yysymbol_kind_t yysymbol)
{
  return yytname[yysymbol];
}
#endif

#define YYPACT_NINF (-177)

#define yypact_value_is_default(Yyn) \
  ((Yyn) == YYPACT_NINF)

#define YYTABLE_NINF (-1)

#define yytable_value_is_error(Yyn) \
  0

/* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
   STATE-NUM.  */
static const yytype_int16 yypact[] =
{
      28,    38,    54,   -46,   -29,  -177,  -177,    56,    50,  -177,
     -75,     8,     8,    46,    56,  -177,  -177,  -177,  -177,  -177,
    -177,  -177,    63,  -177,     8,  -177,     2,   -26,   -51,  -177,
    -177,  -177,  -177,   -13,  -177,    71,    72,   587,  -177,    57,
     -21,    26,   272,   272,  -177,    13,    91,    55,    96,    67,
     -22,    99,   100,   103,  -177,  -177,  -177,    75,    29,    35,
    -177,   116,  -177,   396,  -177,    22,    23,    27,    -9,    30,
      87,    31,    32,    87,    47,    49,    52,    58,    59,    60,
      61,    62,    65,    66,    74,    77,    78,    86,    89,   102,
      75,  -177,   272,  -177,  -177,  -177,  -177,  -177,  -177,    39,
     272,    51,  -177,  -177,  -177,  -177,  -177,  -177,  -177,  -177,
    -177,  -177,  -177,   272,   272,   361,    25,   489,    45,    90,
    -177,   651,  -177,   -39,    93,   142,   124,   108,   152,   170,
    -177,   131,  -177,   143,  -177,  -177,  -177,  -177,    98,  -177,
    -177,  -177,   272,  -177,   110,  -177,  -177,   256,  -177,  -177,
    -177,  -177,  -177,  -177,  -177,  -177,  -177,  -177,  -177,  -177,
    -177,  -177,  -177,  -177,  -177,  -177,  -177,  -177,  -177,  -177,
     112,   651,   137,   101,   147,   204,    88,   272,   272,   272,
     272,   272,   587,   272,   272,   272,   272,   272,   272,   272,
     272,   587,   272,   -30,   211,   168,   212,   272,  -177,   213,
    -177,   118,  -177,   167,   217,   122,   651,   -63,   272,   175,
     651,  -177,  -177,  -177,  -177,   101,   101,    21,    21,   651,
     332,    21,    21,    21,    -6,    -6,   204,   204,   -60,   460,
     198,   222,   126,  -177,   125,  -177,  -177,   -33,   584,   140,
    -177,   128,   228,   229,   139,  -177,   125,  -177,   -53,  -177,
     272,   -49,   240,   587,   272,  -177,   224,   226,  -177,   225,
    -177,   150,  -177,   258,   272,   260,   230,   272,   272,   213,
       8,  -177,   -45,   208,   166,   164,   176,   651,  -177,  -177,
     587,   631,  -177,   254,  -177,  -177,  -177,  -177,   234,   194,
     638,   651,  -177,   182,   227,   228,   280,  -177,  -177,  -177,
     587,  -177,  -177,   273,   247,   587,   289,   214,  -177,  -177,
    -177,   195,   587,   209,   261,  -177,   524,   199,  -177,   295,
     292,   215,   299,   279,  -177,   304,  -177,  -177,   -44,  -177,
      -8,  -177,  -177,  -177,   305,  -177,  -177,  -177,  -177
};

/* YYDEFACT[STATE-NUM] -- Default reduction number in state STATE-NUM.
   Performed when YYTABLE does not specify something else to do.  Zero
   means the default is an error.  */
static const yytype_uint8 yydefact[] =
{
       0,     0,     0,     0,     0,     1,     2,   161,     0,   162,
       0,     0,     0,     0,     0,   157,   158,   154,   155,   156,
     159,   160,   165,   163,     0,   166,   172,     0,     0,   167,
     170,   171,   173,     0,   164,     0,     0,     0,   174,     0,
       0,     0,     0,     0,   128,    85,     0,     0,     0,     0,
     147,     0,     0,     0,    69,    70,    71,     0,     0,     0,
     127,     0,    25,     0,     3,     0,     0,     0,     0,     0,
      91,     0,     0,    91,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   169,     0,    29,    30,    31,    32,    33,    34,    27,
       0,    35,    53,    54,    55,    56,    57,    58,    59,    60,
      61,    62,    63,     0,     0,     0,     0,     0,     0,     0,
      88,    81,    86,    90,     0,     0,     0,     0,     0,     0,
     148,   149,   129,     0,   130,   117,   152,   153,     0,   175,
      26,     4,    78,    11,     0,   105,    12,     0,   111,   112,
      16,    17,   114,   115,    14,    15,    10,    13,     8,     5,
       6,     7,     9,    18,    20,    19,    23,    24,    21,    22,
       0,   116,     0,    50,     0,    40,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
      78,     0,     0,     0,    75,     0,     0,     0,   103,     0,
     113,     0,   150,     0,    75,    64,    79,     0,    78,     0,
      92,   168,    51,    52,    41,    48,    49,    45,    46,    47,
     121,    42,    43,    44,    37,    36,    38,    39,     0,     0,
       0,     0,     0,    76,    89,    87,    73,    91,     0,     0,
     107,   110,     0,     0,    76,   132,   131,    65,     0,    68,
       0,     0,     0,     0,     0,   119,   123,     0,    28,     0,
      84,     0,    82,     0,     0,     0,    93,     0,     0,     0,
       0,   134,     0,     0,     0,     0,     0,    80,   104,   109,
     122,     0,   120,     0,   125,    83,    77,    74,     0,    95,
       0,   106,   108,   136,   142,     0,     0,    72,    67,    66,
       0,   124,    94,     0,   100,     0,     0,   138,   143,   144,
     135,     0,   118,     0,     0,   102,     0,     0,   139,   140,
       0,     0,     0,     0,   137,     0,   133,   145,     0,    96,
      97,   126,   141,   151,     0,    98,    99,   101,   146
};

/* YYPGOTO[NTERM-NUM].  */
static const yytype_int16 yypgoto[] =
{
    -177,  -177,   -62,  -176,   -40,  -177,  -177,  -177,  -177,  -177,
    -177,  -177,   109,  -166,   120,  -177,  -177,   -69,  -177,  -177,
    -177,  -177,   -34,  -177,  -177,    48,  -177,   243,  -177,  -177,
    -177,  -177,  -177,  -177,  -177,  -177,    64,  -177,  -177,  -177,
    -177,  -177,  -177,  -177,  -177,  -177,  -177,    24,  -177,  -177,
    -177,  -177,  -177,  -177,  -177,  -177,  -177,  -177,  -177,  -177,
     -12,   307,  -177,   297,  -177,  -177,  -177,   285,  -177,  -177
};

/* YYDEFGOTO[NTERM-NUM].  */
static const yytype_int16 yydefgoto[] =
{
       0,     2,    62,    63,   206,   116,   248,    64,    65,    66,
     245,   237,   234,   207,   122,   123,   124,   148,   289,   304,
     337,   315,    67,    68,    69,   240,   241,   149,    70,    71,
      72,    73,    74,    75,    76,    77,   255,   256,   257,    78,
      79,    80,    81,    82,    83,    84,    85,   271,   272,   307,
     319,   326,   309,    86,   328,   131,   203,    87,    88,    89,
      20,     9,    10,    25,    26,    30,    31,    32,    33,     3
};

/* YYTABLE[YYPACT[STATE-NUM]] -- What to do in state STATE-NUM.  If
   positive, shift that token.  If negative, reduce the rule whose
   number is the opposite.  If YYTABLE_NINF, syntax error.  */
static const yytype_int16 yytable[] =
{
      21,   140,   115,   117,   152,   121,   220,   264,   231,   181,
     194,    24,    27,    37,    35,   229,    93,    94,    95,    96,
      97,    98,    99,   135,   228,   100,    45,    15,    16,    17,
      18,    13,    19,    14,   145,   129,   181,   130,   335,   336,
      36,   144,   251,   249,     1,   250,   258,     4,   250,   118,
     119,    28,   171,   275,     5,   276,   170,   278,     6,   250,
     173,   294,   333,   295,   334,     8,    28,    11,    12,   195,
     232,    22,    24,   175,   176,   265,     7,   280,    34,   101,
      39,    40,    90,    91,   102,   103,   104,   105,   106,    92,
     107,   108,   109,   110,   188,   189,   111,   112,   177,   178,
     125,   179,   180,   181,   126,   127,   128,   210,   132,   133,
      45,   113,   134,   120,   179,   180,   181,   136,   114,   186,
     187,   188,   189,   137,   312,   138,   141,   147,   142,   316,
     190,   143,   196,   198,   146,   150,   151,   215,   216,   217,
     218,   219,   172,   221,   222,   223,   224,   225,   226,   227,
     192,   154,   230,   155,   174,   121,   156,   238,   140,   197,
     199,   200,   157,   158,   159,   160,   161,   140,   266,   162,
     163,    93,    94,    95,    96,    97,    98,    99,   164,   201,
     100,   165,   166,   183,   184,   185,   186,   187,   188,   189,
     167,   202,   204,   168,   214,   193,   183,   184,   185,   186,
     187,   188,   189,   205,   118,   119,   169,   212,   177,   178,
     277,   179,   180,   181,   281,   208,   211,   213,   140,   181,
     233,   236,   239,   242,   210,   243,   244,   290,   291,   247,
     252,   261,   262,   263,   101,   268,   269,   270,   273,   102,
     103,   104,   105,   106,   274,   107,   108,   109,   110,   279,
     140,   111,   112,   283,   140,   254,   285,   284,   293,    93,
      94,    95,    96,    97,    98,    99,   113,   286,   100,   287,
     296,   288,   297,   114,   298,    93,    94,    95,    96,    97,
      98,    99,   301,   299,   100,   302,   303,   306,   308,   311,
     313,   314,   317,   183,   184,   185,   186,   187,   188,   189,
     320,   327,   321,   318,   260,   324,   322,   325,   330,   329,
     209,   331,   332,   246,   338,   235,   153,   292,    38,   310,
     282,    23,   101,    29,     0,     0,     0,   102,   103,   104,
     105,   106,     0,   107,   108,   109,   110,     0,   101,   111,
     112,    41,     0,   102,   103,   104,   105,   106,     0,   107,
     108,   109,   110,     0,   113,   111,   112,     0,     0,     0,
      42,   114,   253,   254,     0,    43,    44,    45,     0,     0,
     113,   177,   178,    46,   179,   180,   181,   114,     0,     0,
      47,     0,     0,    48,     0,    49,     0,     0,    50,     0,
     182,     0,     0,     0,     0,     0,     0,     0,     0,    51,
      52,    53,     0,     0,     0,    41,     0,     0,    54,     0,
       0,     0,     0,    55,    56,     0,     0,    57,    58,    59,
       0,     0,    60,   139,    42,     0,     0,     0,     0,    43,
      44,    45,     0,     0,     0,     0,     0,    46,     0,     0,
       0,    61,     0,     0,    47,     0,     0,    48,     0,    49,
       0,     0,    50,     0,     0,     0,   183,   184,   185,   186,
     187,   188,   189,    51,    52,    53,     0,     0,     0,    41,
       0,     0,    54,     0,     0,     0,     0,    55,    56,     0,
       0,    57,    58,    59,     0,     0,    60,   259,    42,     0,
       0,     0,     0,    43,    44,    45,     0,     0,     0,   177,
     178,    46,   179,   180,   181,    61,     0,     0,    47,     0,
       0,    48,     0,    49,     0,     0,    50,     0,     0,     0,
       0,   191,     0,     0,     0,     0,     0,    51,    52,    53,
       0,     0,     0,    41,     0,     0,    54,     0,     0,     0,
       0,    55,    56,     0,     0,    57,    58,    59,     0,     0,
      60,   323,    42,     0,     0,     0,     0,    43,    44,    45,
       0,     0,     0,     0,     0,    46,     0,     0,     0,    61,
       0,     0,    47,     0,     0,    48,     0,    49,     0,     0,
      50,     0,     0,     0,   183,   184,   185,   186,   187,   188,
     189,    51,    52,    53,   177,   178,    41,   179,   180,   181,
      54,     0,     0,     0,     0,    55,    56,     0,     0,    57,
      58,    59,     0,     0,    60,    42,     0,     0,     0,     0,
      43,    44,    45,     0,     0,     0,   267,     0,    46,     0,
       0,     0,     0,    61,     0,    47,     0,     0,    48,     0,
      49,   177,   178,    50,   179,   180,   181,     0,   177,   178,
       0,   179,   180,   181,    51,    52,    53,     0,     0,     0,
     300,   177,   178,    54,   179,   180,   181,     0,    55,    56,
     305,     0,    57,    58,    59,     0,     0,    60,     0,   183,
     184,   185,   186,   187,   188,   189,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,    61,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   183,   184,   185,   186,
     187,   188,   189,   183,   184,   185,   186,   187,   188,   189,
       0,     0,     0,     0,     0,     0,   183,   184,   185,   186,
     187,   188,   189
};

static const yytype_int16 yycheck[] =
{
      12,    63,    42,    43,    73,    45,   182,    40,    38,    15,
      49,     9,    24,    26,    65,   191,     3,     4,     5,     6,
       7,     8,     9,    57,   190,    12,    35,    19,    20,    21,
      22,   106,    24,   108,    68,    57,    15,    59,    46,    47,
      91,    50,   208,   106,    16,   108,   106,     9,   108,    36,
      37,    64,    92,   106,     0,   108,    90,   106,   104,   108,
     100,   106,   106,   108,   108,     9,    64,    17,    18,   108,
     100,    25,     9,   113,   114,   108,   105,   253,   104,    66,
       9,     9,    25,   104,    71,    72,    73,    74,    75,    63,
      77,    78,    79,    80,   100,   101,    83,    84,    10,    11,
       9,    13,    14,    15,    49,     9,    39,   147,     9,     9,
      35,    98,     9,   100,    13,    14,    15,    88,   105,    98,
      99,   100,   101,    88,   300,     9,   104,    40,   105,   305,
     105,   104,    39,     9,   104,   104,   104,   177,   178,   179,
     180,   181,   103,   183,   184,   185,   186,   187,   188,   189,
     105,   104,   192,   104,   103,   195,   104,   197,   220,    17,
      52,     9,   104,   104,   104,   104,   104,   229,   237,   104,
     104,     3,     4,     5,     6,     7,     8,     9,   104,     9,
      12,   104,   104,    95,    96,    97,    98,    99,   100,   101,
     104,    60,    49,   104,   106,   105,    95,    96,    97,    98,
      99,   100,   101,   105,    36,    37,   104,    70,    10,    11,
     250,    13,    14,    15,   254,   105,   104,    70,   280,    15,
       9,     9,     9,   105,   264,    58,     9,   267,   268,   107,
      55,     9,   106,   108,    66,    95,   108,     9,     9,    71,
      72,    73,    74,    75,   105,    77,    78,    79,    80,     9,
     312,    83,    84,    27,   316,    31,   106,    32,   270,     3,
       4,     5,     6,     7,     8,     9,    98,     9,    12,     9,
      62,    41,   106,   105,   110,     3,     4,     5,     6,     7,
       8,     9,    28,   107,    12,    51,    92,   105,    61,     9,
      17,    44,     3,    95,    96,    97,    98,    99,   100,   101,
     105,     9,    93,    89,   106,   106,    45,    12,     9,    94,
      54,    32,     8,   204,     9,   195,    73,   269,    33,   295,
     256,    14,    66,    26,    -1,    -1,    -1,    71,    72,    73,
      74,    75,    -1,    77,    78,    79,    80,    -1,    66,    83,
      84,     9,    -1,    71,    72,    73,    74,    75,    -1,    77,
      78,    79,    80,    -1,    98,    83,    84,    -1,    -1,    -1,
      28,   105,    30,    31,    -1,    33,    34,    35,    -1,    -1,
      98,    10,    11,    41,    13,    14,    15,   105,    -1,    -1,
      48,    -1,    -1,    51,    -1,    53,    -1,    -1,    56,    -1,
      29,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    67,
      68,    69,    -1,    -1,    -1,     9,    -1,    -1,    76,    -1,
      -1,    -1,    -1,    81,    82,    -1,    -1,    85,    86,    87,
      -1,    -1,    90,    27,    28,    -1,    -1,    -1,    -1,    33,
      34,    35,    -1,    -1,    -1,    -1,    -1,    41,    -1,    -1,
      -1,   109,    -1,    -1,    48,    -1,    -1,    51,    -1,    53,
      -1,    -1,    56,    -1,    -1,    -1,    95,    96,    97,    98,
      99,   100,   101,    67,    68,    69,    -1,    -1,    -1,     9,
      -1,    -1,    76,    -1,    -1,    -1,    -1,    81,    82,    -1,
      -1,    85,    86,    87,    -1,    -1,    90,    27,    28,    -1,
      -1,    -1,    -1,    33,    34,    35,    -1,    -1,    -1,    10,
      11,    41,    13,    14,    15,   109,    -1,    -1,    48,    -1,
      -1,    51,    -1,    53,    -1,    -1,    56,    -1,    -1,    -1,
      -1,    32,    -1,    -1,    -1,    -1,    -1,    67,    68,    69,
      -1,    -1,    -1,     9,    -1,    -1,    76,    -1,    -1,    -1,
      -1,    81,    82,    -1,    -1,    85,    86,    87,    -1,    -1,
      90,    27,    28,    -1,    -1,    -1,    -1,    33,    34,    35,
      -1,    -1,    -1,    -1,    -1,    41,    -1,    -1,    -1,   109,
      -1,    -1,    48,    -1,    -1,    51,    -1,    53,    -1,    -1,
      56,    -1,    -1,    -1,    95,    96,    97,    98,    99,   100,
     101,    67,    68,    69,    10,    11,     9,    13,    14,    15,
      76,    -1,    -1,    -1,    -1,    81,    82,    -1,    -1,    85,
      86,    87,    -1,    -1,    90,    28,    -1,    -1,    -1,    -1,
      33,    34,    35,    -1,    -1,    -1,    42,    -1,    41,    -1,
      -1,    -1,    -1,   109,    -1,    48,    -1,    -1,    51,    -1,
      53,    10,    11,    56,    13,    14,    15,    -1,    10,    11,
      -1,    13,    14,    15,    67,    68,    69,    -1,    -1,    -1,
      29,    10,    11,    76,    13,    14,    15,    -1,    81,    82,
      32,    -1,    85,    86,    87,    -1,    -1,    90,    -1,    95,
      96,    97,    98,    99,   100,   101,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   109,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    95,    96,    97,    98,
      99,   100,   101,    95,    96,    97,    98,    99,   100,   101,
      -1,    -1,    -1,    -1,    -1,    -1,    95,    96,    97,    98,
      99,   100,   101
};

/* YYSTOS[STATE-NUM] -- The symbol kind of the accessing symbol of
   state STATE-NUM.  */
static const yytype_uint8 yystos[] =
{
       0,    16,   112,   180,     9,     0,   104,   105,     9,   172,
     173,    17,    18,   106,   108,    19,    20,    21,    22,    24,
     171,   171,    25,   172,     9,   174,   175,   171,    64,   174,
     176,   177,   178,   179,   104,    65,    91,    26,   178,     9,
       9,     9,    28,    33,    34,    35,    41,    48,    51,    53,
      56,    67,    68,    69,    76,    81,    82,    85,    86,    87,
      90,   109,   113,   114,   118,   119,   120,   133,   134,   135,
     139,   140,   141,   142,   143,   144,   145,   146,   150,   151,
     152,   153,   154,   155,   156,   157,   164,   168,   169,   170,
      25,   104,    63,     3,     4,     5,     6,     7,     8,     9,
      12,    66,    71,    72,    73,    74,    75,    77,    78,    79,
      80,    83,    84,    98,   105,   115,   116,   115,    36,    37,
     100,   115,   125,   126,   127,     9,    49,     9,    39,    57,
      59,   166,     9,     9,     9,   133,    88,    88,     9,    27,
     113,   104,   105,   104,    50,   133,   104,    40,   128,   138,
     104,   104,   128,   138,   104,   104,   104,   104,   104,   104,
     104,   104,   104,   104,   104,   104,   104,   104,   104,   104,
     133,   115,   103,   115,   103,   115,   115,    10,    11,    13,
      14,    15,    29,    95,    96,    97,    98,    99,   100,   101,
     105,    32,   105,   105,    49,   108,    39,    17,     9,    52,
       9,     9,    60,   167,    49,   105,   115,   124,   105,    54,
     115,   104,    70,    70,   106,   115,   115,   115,   115,   115,
     114,   115,   115,   115,   115,   115,   115,   115,   124,   114,
     115,    38,   100,     9,   123,   125,     9,   122,   115,     9,
     136,   137,   105,    58,     9,   121,   123,   107,   117,   106,
     108,   124,    55,    30,    31,   147,   148,   149,   106,    27,
     106,     9,   106,   108,    40,   108,   128,    42,    95,   108,
       9,   158,   159,     9,   105,   106,   108,   115,   106,     9,
     114,   115,   147,    27,    32,   106,     9,     9,    41,   129,
     115,   115,   136,   171,   106,   108,    62,   106,   110,   107,
      29,    28,    51,    92,   130,    32,   105,   160,    61,   163,
     158,     9,   114,    17,    44,   132,   114,     3,    89,   161,
     105,    93,    45,    27,   106,    12,   162,     9,   165,    94,
       9,    32,     8,   106,   108,    46,    47,   131,     9
};

/* YYR1[RULE-NUM] -- Symbol kind of the left-hand side of rule RULE-NUM.  */
static const yytype_uint8 yyr1[] =
{
       0,   111,   112,   113,   113,   113,   113,   113,   113,   113,
     113,   113,   113,   113,   113,   113,   113,   113,   113,   113,
     113,   113,   113,   113,   113,   114,   114,   115,   115,   115,
     115,   115,   115,   115,   115,   115,   115,   115,   115,   115,
     115,   115,   115,   115,   115,   115,   115,   115,   115,   115,
     115,   115,   115,   116,   116,   116,   116,   116,   116,   116,
     116,   116,   116,   116,   117,   117,   117,   118,   119,   120,
     120,   120,   121,   122,   122,   123,   123,   123,   124,   124,
     124,   125,   125,   125,   125,   126,   126,   126,   127,   127,
     127,   128,   128,   129,   129,   130,   130,   131,   131,   131,
     132,   132,   133,   134,   135,   135,   136,   137,   137,   138,
     139,   140,   141,   142,   143,   144,   145,   146,   147,   148,
     148,   149,   149,   149,   150,   151,   152,   153,   154,   155,
     156,   157,   157,   158,   159,   159,   160,   160,   161,   161,
     162,   162,   163,   163,   164,   165,   165,   166,   166,   167,
     167,   168,   169,   170,   171,   171,   171,   171,   171,   172,
     172,   173,   173,   173,   174,   175,   175,   175,   176,   177,
     178,   178,   179,   179,   179,   180
};

/* YYR2[RULE-NUM] -- Number of symbols on the right-hand side of rule RULE-NUM.  */
static const yytype_int8 yyr2[] =
{
       0,     2,     2,     1,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     1,     2,     1,     4,     1,
       1,     1,     1,     1,     1,     1,     3,     3,     3,     3,
       2,     3,     3,     3,     3,     3,     3,     3,     3,     3,
       2,     3,     3,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     0,     1,     3,     6,     4,     1,
       1,     1,     3,     1,     3,     0,     1,     3,     0,     1,
       3,     1,     4,     5,     4,     0,     1,     3,     1,     3,
       1,     0,     2,     0,     2,     0,     4,     0,     1,     1,
       0,     4,     8,     3,     5,     2,     3,     1,     3,     4,
       4,     2,     2,     3,     2,     2,     3,     2,     4,     1,
       2,     0,     2,     1,     7,     6,    10,     1,     1,     2,
       2,     4,     4,     5,     1,     3,     0,     3,     0,     1,
       0,     2,     0,     1,     7,     1,     3,     0,     1,     0,
       1,    10,     2,     2,     1,     1,     1,     1,     1,     3,
       3,     0,     1,     3,     3,     0,     1,     2,     6,     4,
       1,     1,     0,     1,     2,    11
};


enum { YYENOMEM = -2 };

#define yyerrok         (yyerrstatus = 0)
#define yyclearin       (yychar = YYEMPTY)

#define YYACCEPT        goto yyacceptlab
#define YYABORT         goto yyabortlab
#define YYERROR         goto yyerrorlab
#define YYNOMEM         goto yyexhaustedlab


#define YYRECOVERING()  (!!yyerrstatus)

#define YYBACKUP(Token, Value)                                    \
  do                                                              \
    if (yychar == YYEMPTY)                                        \
      {                                                           \
        yychar = (Token);                                         \
        yylval = (Value);                                         \
        YYPOPSTACK (yylen);                                       \
        yystate = *yyssp;                                         \
        goto yybackup;                                            \
      }                                                           \
    else                                                          \
      {                                                           \
        yyerror (YY_("syntax error: cannot back up")); \
        YYERROR;                                                  \
      }                                                           \
  while (0)

/* Backward compatibility with an undocumented macro.
   Use YYerror or YYUNDEF. */
#define YYERRCODE YYUNDEF


/* Enable debugging if requested.  */
#if YYDEBUG

# ifndef YYFPRINTF
#  include <stdio.h> /* INFRINGES ON USER NAME SPACE */
#  define YYFPRINTF fprintf
# endif

# define YYDPRINTF(Args)                        \
do {                                            \
  if (yydebug)                                  \
    YYFPRINTF Args;                             \
} while (0)




# define YY_SYMBOL_PRINT(Title, Kind, Value, Location)                    \
do {                                                                      \
  if (yydebug)                                                            \
    {                                                                     \
      YYFPRINTF (stderr, "%s ", Title);                                   \
      yy_symbol_print (stderr,                                            \
                  Kind, Value); \
      YYFPRINTF (stderr, "\n");                                           \
    }                                                                     \
} while (0)


/*-----------------------------------.
| Print this symbol's value on YYO.  |
`-----------------------------------*/

static void
yy_symbol_value_print (FILE *yyo,
                       yysymbol_kind_t yykind, YYSTYPE const * const yyvaluep)
{
  FILE *yyoutput = yyo;
  YY_USE (yyoutput);
  if (!yyvaluep)
    return;
  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  YY_USE (yykind);
  YY_IGNORE_MAYBE_UNINITIALIZED_END
}


/*---------------------------.
| Print this symbol on YYO.  |
`---------------------------*/

static void
yy_symbol_print (FILE *yyo,
                 yysymbol_kind_t yykind, YYSTYPE const * const yyvaluep)
{
  YYFPRINTF (yyo, "%s %s (",
             yykind < YYNTOKENS ? "token" : "nterm", yysymbol_name (yykind));

  yy_symbol_value_print (yyo, yykind, yyvaluep);
  YYFPRINTF (yyo, ")");
}

/*------------------------------------------------------------------.
| yy_stack_print -- Print the state stack from its BOTTOM up to its |
| TOP (included).                                                   |
`------------------------------------------------------------------*/

static void
yy_stack_print (yy_state_t *yybottom, yy_state_t *yytop)
{
  YYFPRINTF (stderr, "Stack now");
  for (; yybottom <= yytop; yybottom++)
    {
      int yybot = *yybottom;
      YYFPRINTF (stderr, " %d", yybot);
    }
  YYFPRINTF (stderr, "\n");
}

# define YY_STACK_PRINT(Bottom, Top)                            \
do {                                                            \
  if (yydebug)                                                  \
    yy_stack_print ((Bottom), (Top));                           \
} while (0)


/*------------------------------------------------.
| Report that the YYRULE is going to be reduced.  |
`------------------------------------------------*/

static void
yy_reduce_print (yy_state_t *yyssp, YYSTYPE *yyvsp,
                 int yyrule)
{
  int yylno = yyrline[yyrule];
  int yynrhs = yyr2[yyrule];
  int yyi;
  YYFPRINTF (stderr, "Reducing stack by rule %d (line %d):\n",
             yyrule - 1, yylno);
  /* The symbols being reduced.  */
  for (yyi = 0; yyi < yynrhs; yyi++)
    {
      YYFPRINTF (stderr, "   $%d = ", yyi + 1);
      yy_symbol_print (stderr,
                       YY_ACCESSING_SYMBOL (+yyssp[yyi + 1 - yynrhs]),
                       &yyvsp[(yyi + 1) - (yynrhs)]);
      YYFPRINTF (stderr, "\n");
    }
}

# define YY_REDUCE_PRINT(Rule)          \
do {                                    \
  if (yydebug)                          \
    yy_reduce_print (yyssp, yyvsp, Rule); \
} while (0)

/* Nonzero means print parse trace.  It is left uninitialized so that
   multiple parsers can coexist.  */
int yydebug;
#else /* !YYDEBUG */
# define YYDPRINTF(Args) ((void) 0)
# define YY_SYMBOL_PRINT(Title, Kind, Value, Location)
# define YY_STACK_PRINT(Bottom, Top)
# define YY_REDUCE_PRINT(Rule)
#endif /* !YYDEBUG */


/* YYINITDEPTH -- initial size of the parser's stacks.  */
#ifndef YYINITDEPTH
# define YYINITDEPTH 200
#endif

/* YYMAXDEPTH -- maximum size the stacks can grow to (effective only
   if the built-in stack extension method is used).

   Do not make this value too large; the results are undefined if
   YYSTACK_ALLOC_MAXIMUM < YYSTACK_BYTES (YYMAXDEPTH)
   evaluated with infinite-precision integer arithmetic.  */

#ifndef YYMAXDEPTH
# define YYMAXDEPTH 10000
#endif






/*-----------------------------------------------.
| Release the memory associated to this symbol.  |
`-----------------------------------------------*/

static void
yydestruct (const char *yymsg,
            yysymbol_kind_t yykind, YYSTYPE *yyvaluep)
{
  YY_USE (yyvaluep);
  if (!yymsg)
    yymsg = "Deleting";
  YY_SYMBOL_PRINT (yymsg, yykind, yyvaluep, yylocationp);

  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  YY_USE (yykind);
  YY_IGNORE_MAYBE_UNINITIALIZED_END
}


/* Lookahead token kind.  */
int yychar;

/* The semantic value of the lookahead symbol.  */
YYSTYPE yylval;
/* Number of syntax errors so far.  */
int yynerrs;




/*----------.
| yyparse.  |
`----------*/

int
yyparse (void)
{
    yy_state_fast_t yystate = 0;
    /* Number of tokens to shift before error messages enabled.  */
    int yyerrstatus = 0;

    /* Refer to the stacks through separate pointers, to allow yyoverflow
       to reallocate them elsewhere.  */

    /* Their size.  */
    YYPTRDIFF_T yystacksize = YYINITDEPTH;

    /* The state stack: array, bottom, top.  */
    yy_state_t yyssa[YYINITDEPTH];
    yy_state_t *yyss = yyssa;
    yy_state_t *yyssp = yyss;

    /* The semantic value stack: array, bottom, top.  */
    YYSTYPE yyvsa[YYINITDEPTH];
    YYSTYPE *yyvs = yyvsa;
    YYSTYPE *yyvsp = yyvs;

  int yyn;
  /* The return value of yyparse.  */
  int yyresult;
  /* Lookahead symbol kind.  */
  yysymbol_kind_t yytoken = YYSYMBOL_YYEMPTY;
  /* The variables used to return semantic value and location from the
     action routines.  */
  YYSTYPE yyval;



#define YYPOPSTACK(N)   (yyvsp -= (N), yyssp -= (N))

  /* The number of symbols on the RHS of the reduced rule.
     Keep to zero when no symbol should be popped.  */
  int yylen = 0;

  YYDPRINTF ((stderr, "Starting parse\n"));

  yychar = YYEMPTY; /* Cause a token to be read.  */

  goto yysetstate;


/*------------------------------------------------------------.
| yynewstate -- push a new state, which is found in yystate.  |
`------------------------------------------------------------*/
yynewstate:
  /* In all cases, when you get here, the value and location stacks
     have just been pushed.  So pushing a state here evens the stacks.  */
  yyssp++;


/*--------------------------------------------------------------------.
| yysetstate -- set current state (the top of the stack) to yystate.  |
`--------------------------------------------------------------------*/
yysetstate:
  YYDPRINTF ((stderr, "Entering state %d\n", yystate));
  YY_ASSERT (0 <= yystate && yystate < YYNSTATES);
  YY_IGNORE_USELESS_CAST_BEGIN
  *yyssp = YY_CAST (yy_state_t, yystate);
  YY_IGNORE_USELESS_CAST_END
  YY_STACK_PRINT (yyss, yyssp);

  if (yyss + yystacksize - 1 <= yyssp)
#if !defined yyoverflow && !defined YYSTACK_RELOCATE
    YYNOMEM;
#else
    {
      /* Get the current used size of the three stacks, in elements.  */
      YYPTRDIFF_T yysize = yyssp - yyss + 1;

# if defined yyoverflow
      {
        /* Give user a chance to reallocate the stack.  Use copies of
           these so that the &'s don't force the real ones into
           memory.  */
        yy_state_t *yyss1 = yyss;
        YYSTYPE *yyvs1 = yyvs;

        /* Each stack pointer address is followed by the size of the
           data in use in that stack, in bytes.  This used to be a
           conditional around just the two extra args, but that might
           be undefined if yyoverflow is a macro.  */
        yyoverflow (YY_("memory exhausted"),
                    &yyss1, yysize * YYSIZEOF (*yyssp),
                    &yyvs1, yysize * YYSIZEOF (*yyvsp),
                    &yystacksize);
        yyss = yyss1;
        yyvs = yyvs1;
      }
# else /* defined YYSTACK_RELOCATE */
      /* Extend the stack our own way.  */
      if (YYMAXDEPTH <= yystacksize)
        YYNOMEM;
      yystacksize *= 2;
      if (YYMAXDEPTH < yystacksize)
        yystacksize = YYMAXDEPTH;

      {
        yy_state_t *yyss1 = yyss;
        union yyalloc *yyptr =
          YY_CAST (union yyalloc *,
                   YYSTACK_ALLOC (YY_CAST (YYSIZE_T, YYSTACK_BYTES (yystacksize))));
        if (! yyptr)
          YYNOMEM;
        YYSTACK_RELOCATE (yyss_alloc, yyss);
        YYSTACK_RELOCATE (yyvs_alloc, yyvs);
#  undef YYSTACK_RELOCATE
        if (yyss1 != yyssa)
          YYSTACK_FREE (yyss1);
      }
# endif

      yyssp = yyss + yysize - 1;
      yyvsp = yyvs + yysize - 1;

      YY_IGNORE_USELESS_CAST_BEGIN
      YYDPRINTF ((stderr, "Stack size increased to %ld\n",
                  YY_CAST (long, yystacksize)));
      YY_IGNORE_USELESS_CAST_END

      if (yyss + yystacksize - 1 <= yyssp)
        YYABORT;
    }
#endif /* !defined yyoverflow && !defined YYSTACK_RELOCATE */


  if (yystate == YYFINAL)
    YYACCEPT;

  goto yybackup;


/*-----------.
| yybackup.  |
`-----------*/
yybackup:
  /* Do appropriate processing given the current state.  Read a
     lookahead token if we need one and don't already have one.  */

  /* First try to decide what to do without reference to lookahead token.  */
  yyn = yypact[yystate];
  if (yypact_value_is_default (yyn))
    goto yydefault;

  /* Not known => get a lookahead token if don't already have one.  */

  /* YYCHAR is either empty, or end-of-input, or a valid lookahead.  */
  if (yychar == YYEMPTY)
    {
      YYDPRINTF ((stderr, "Reading a token\n"));
      yychar = yylex ();
    }

  if (yychar <= YYEOF)
    {
      yychar = YYEOF;
      yytoken = YYSYMBOL_YYEOF;
      YYDPRINTF ((stderr, "Now at end of input.\n"));
    }
  else if (yychar == YYerror)
    {
      /* The scanner already issued an error message, process directly
         to error recovery.  But do not keep the error token as
         lookahead, it is too special and may lead us to an endless
         loop in error recovery. */
      yychar = YYUNDEF;
      yytoken = YYSYMBOL_YYerror;
      goto yyerrlab1;
    }
  else
    {
      yytoken = YYTRANSLATE (yychar);
      YY_SYMBOL_PRINT ("Next token is", yytoken, &yylval, &yylloc);
    }

  /* If the proper action on seeing token YYTOKEN is to reduce or to
     detect an error, take that action.  */
  yyn += yytoken;
  if (yyn < 0 || YYLAST < yyn || yycheck[yyn] != yytoken)
    goto yydefault;
  yyn = yytable[yyn];
  if (yyn <= 0)
    {
      if (yytable_value_is_error (yyn))
        goto yyerrlab;
      yyn = -yyn;
      goto yyreduce;
    }

  /* Count tokens shifted since error; after three, turn off error
     status.  */
  if (yyerrstatus)
    yyerrstatus--;

  /* Shift the lookahead token.  */
  YY_SYMBOL_PRINT ("Shifting", yytoken, &yylval, &yylloc);
  yystate = yyn;
  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  *++yyvsp = yylval;
  YY_IGNORE_MAYBE_UNINITIALIZED_END

  /* Discard the shifted token.  */
  yychar = YYEMPTY;
  goto yynewstate;


/*-----------------------------------------------------------.
| yydefault -- do the default action for the current state.  |
`-----------------------------------------------------------*/
yydefault:
  yyn = yydefact[yystate];
  if (yyn == 0)
    goto yyerrlab;
  goto yyreduce;


/*-----------------------------.
| yyreduce -- do a reduction.  |
`-----------------------------*/
yyreduce:
  /* yyn is the number of a rule to reduce with.  */
  yylen = yyr2[yyn];

  /* If YYLEN is nonzero, implement the default value of the action:
     '$$ = $1'.

     Otherwise, the following line sets YYVAL to garbage.
     This behavior is undocumented and Bison
     users should not rely upon it.  Assigning to YYVAL
     unconditionally makes the parser a bit smaller, and it avoids a
     GCC warning that YYVAL may be used uninitialized.  */
  yyval = yyvsp[1-yylen];


  YY_REDUCE_PRINT (yyn);
  switch (yyn)
    {
  case 25: /* statement_list: statement  */
#line 179 "pars0grm.y"
                                { yyval = que_node_list_add_last(nullptr, yyvsp[0]); }
#line 1617 "pars0grm.cc"
    break;

  case 26: /* statement_list: statement_list statement  */
#line 181 "pars0grm.y"
                                { yyval = que_node_list_add_last(yyvsp[-1], yyvsp[0]); }
#line 1623 "pars0grm.cc"
    break;

  case 27: /* exp: PARS_ID_TOKEN  */
#line 185 "pars0grm.y"
                                { yyval = yyvsp[0];}
#line 1629 "pars0grm.cc"
    break;

  case 28: /* exp: function_name '(' exp_list ')'  */
#line 187 "pars0grm.y"
                                { yyval = pars_func(yyvsp[-3], yyvsp[-1]); }
#line 1635 "pars0grm.cc"
    break;

  case 29: /* exp: PARS_INT_LIT  */
#line 188 "pars0grm.y"
                                { yyval = yyvsp[0];}
#line 1641 "pars0grm.cc"
    break;

  case 30: /* exp: PARS_FLOAT_LIT  */
#line 189 "pars0grm.y"
                                { yyval = yyvsp[0];}
#line 1647 "pars0grm.cc"
    break;

  case 31: /* exp: PARS_STR_LIT  */
#line 190 "pars0grm.y"
                                { yyval = yyvsp[0];}
#line 1653 "pars0grm.cc"
    break;

  case 32: /* exp: PARS_FIXBINARY_LIT  */
#line 191 "pars0grm.y"
                                { yyval = yyvsp[0];}
#line 1659 "pars0grm.cc"
    break;

  case 33: /* exp: PARS_BLOB_LIT  */
#line 192 "pars0grm.y"
                                { yyval = yyvsp[0];}
#line 1665 "pars0grm.cc"
    break;

  case 34: /* exp: PARS_NULL_LIT  */
#line 193 "pars0grm.y"
                                { yyval = yyvsp[0];}
#line 1671 "pars0grm.cc"
    break;

  case 35: /* exp: PARS_SQL_TOKEN  */
#line 194 "pars0grm.y"
                                { yyval = yyvsp[0];}
#line 1677 "pars0grm.cc"
    break;

  case 36: /* exp: exp '+' exp  */
#line 195 "pars0grm.y"
                                { yyval = pars_op('+', yyvsp[-2], yyvsp[0]); }
#line 1683 "pars0grm.cc"
    break;

  case 37: /* exp: exp '-' exp  */
#line 196 "pars0grm.y"
                                { yyval = pars_op('-', yyvsp[-2], yyvsp[0]); }
#line 1689 "pars0grm.cc"
    break;

  case 38: /* exp: exp '*' exp  */
#line 197 "pars0grm.y"
                                { yyval = pars_op('*', yyvsp[-2], yyvsp[0]); }
#line 1695 "pars0grm.cc"
    break;

  case 39: /* exp: exp '/' exp  */
#line 198 "pars0grm.y"
                                { yyval = pars_op('/', yyvsp[-2], yyvsp[0]); }
#line 1701 "pars0grm.cc"
    break;

  case 40: /* exp: '-' exp  */
#line 199 "pars0grm.y"
                                { yyval = pars_op('-', yyvsp[0], nullptr); }
#line 1707 "pars0grm.cc"
    break;

  case 41: /* exp: '(' exp ')'  */
#line 200 "pars0grm.y"
                                { yyval = yyvsp[-1]; }
#line 1713 "pars0grm.cc"
    break;

  case 42: /* exp: exp '=' exp  */
#line 201 "pars0grm.y"
                                { yyval = pars_op('=', yyvsp[-2], yyvsp[0]); }
#line 1719 "pars0grm.cc"
    break;

  case 43: /* exp: exp '<' exp  */
#line 202 "pars0grm.y"
                                { yyval = pars_op('<', yyvsp[-2], yyvsp[0]); }
#line 1725 "pars0grm.cc"
    break;

  case 44: /* exp: exp '>' exp  */
#line 203 "pars0grm.y"
                                { yyval = pars_op('>', yyvsp[-2], yyvsp[0]); }
#line 1731 "pars0grm.cc"
    break;

  case 45: /* exp: exp PARS_GE_TOKEN exp  */
#line 204 "pars0grm.y"
                                { yyval = pars_op(PARS_GE_TOKEN, yyvsp[-2], yyvsp[0]); }
#line 1737 "pars0grm.cc"
    break;

  case 46: /* exp: exp PARS_LE_TOKEN exp  */
#line 205 "pars0grm.y"
                                { yyval = pars_op(PARS_LE_TOKEN, yyvsp[-2], yyvsp[0]); }
#line 1743 "pars0grm.cc"
    break;

  case 47: /* exp: exp PARS_NE_TOKEN exp  */
#line 206 "pars0grm.y"
                                { yyval = pars_op(PARS_NE_TOKEN, yyvsp[-2], yyvsp[0]); }
#line 1749 "pars0grm.cc"
    break;

  case 48: /* exp: exp PARS_AND_TOKEN exp  */
#line 207 "pars0grm.y"
                                { yyval = pars_op(PARS_AND_TOKEN, yyvsp[-2], yyvsp[0]); }
#line 1755 "pars0grm.cc"
    break;

  case 49: /* exp: exp PARS_OR_TOKEN exp  */
#line 208 "pars0grm.y"
                                { yyval = pars_op(PARS_OR_TOKEN, yyvsp[-2], yyvsp[0]); }
#line 1761 "pars0grm.cc"
    break;

  case 50: /* exp: PARS_NOT_TOKEN exp  */
#line 209 "pars0grm.y"
                                { yyval = pars_op(PARS_NOT_TOKEN, yyvsp[0], nullptr); }
#line 1767 "pars0grm.cc"
    break;

  case 51: /* exp: PARS_ID_TOKEN '%' PARS_NOTFOUND_TOKEN  */
#line 211 "pars0grm.y"
                                { yyval = pars_op(PARS_NOTFOUND_TOKEN, yyvsp[-2], nullptr); }
#line 1773 "pars0grm.cc"
    break;

  case 52: /* exp: PARS_SQL_TOKEN '%' PARS_NOTFOUND_TOKEN  */
#line 213 "pars0grm.y"
                                { yyval = pars_op(PARS_NOTFOUND_TOKEN, yyvsp[-2], nullptr); }
#line 1779 "pars0grm.cc"
    break;

  case 53: /* function_name: PARS_TO_CHAR_TOKEN  */
#line 217 "pars0grm.y"
                                { yyval = &pars_to_char_token; }
#line 1785 "pars0grm.cc"
    break;

  case 54: /* function_name: PARS_TO_NUMBER_TOKEN  */
#line 218 "pars0grm.y"
                                { yyval = &pars_to_number_token; }
#line 1791 "pars0grm.cc"
    break;

  case 55: /* function_name: PARS_TO_BINARY_TOKEN  */
#line 219 "pars0grm.y"
                                { yyval = &pars_to_binary_token; }
#line 1797 "pars0grm.cc"
    break;

  case 56: /* function_name: PARS_BINARY_TO_NUMBER_TOKEN  */
#line 221 "pars0grm.y"
                                { yyval = &pars_binary_to_number_token; }
#line 1803 "pars0grm.cc"
    break;

  case 57: /* function_name: PARS_SUBSTR_TOKEN  */
#line 222 "pars0grm.y"
                                { yyval = &pars_substr_token; }
#line 1809 "pars0grm.cc"
    break;

  case 58: /* function_name: PARS_CONCAT_TOKEN  */
#line 223 "pars0grm.y"
                                { yyval = &pars_concat_token; }
#line 1815 "pars0grm.cc"
    break;

  case 59: /* function_name: PARS_INSTR_TOKEN  */
#line 224 "pars0grm.y"
                                { yyval = &pars_instr_token; }
#line 1821 "pars0grm.cc"
    break;

  case 60: /* function_name: PARS_LENGTH_TOKEN  */
#line 225 "pars0grm.y"
                                { yyval = &pars_length_token; }
#line 1827 "pars0grm.cc"
    break;

  case 61: /* function_name: PARS_SYSDATE_TOKEN  */
#line 226 "pars0grm.y"
                                { yyval = &pars_sysdate_token; }
#line 1833 "pars0grm.cc"
    break;

  case 62: /* function_name: PARS_RND_TOKEN  */
#line 227 "pars0grm.y"
                                { yyval = &pars_rnd_token; }
#line 1839 "pars0grm.cc"
    break;

  case 63: /* function_name: PARS_RND_STR_TOKEN  */
#line 228 "pars0grm.y"
                                { yyval = &pars_rnd_str_token; }
#line 1845 "pars0grm.cc"
    break;

  case 67: /* stored_procedure_call: '{' PARS_ID_TOKEN '(' question_mark_list ')' '}'  */
#line 239 "pars0grm.y"
                                { yyval = pars_stored_procedure_call(static_cast<sym_node_t*>(yyvsp[-4])); }
#line 1851 "pars0grm.cc"
    break;

  case 68: /* predefined_procedure_call: predefined_procedure_name '(' exp_list ')'  */
#line 244 "pars0grm.y"
                                { yyval = pars_procedure_call(yyvsp[-3], yyvsp[-1]); }
#line 1857 "pars0grm.cc"
    break;

  case 69: /* predefined_procedure_name: PARS_REPLSTR_TOKEN  */
#line 248 "pars0grm.y"
                                { yyval = &pars_replstr_token; }
#line 1863 "pars0grm.cc"
    break;

  case 70: /* predefined_procedure_name: PARS_PRINTF_TOKEN  */
#line 249 "pars0grm.y"
                                { yyval = &pars_printf_token; }
#line 1869 "pars0grm.cc"
    break;

  case 71: /* predefined_procedure_name: PARS_ASSERT_TOKEN  */
#line 250 "pars0grm.y"
                                { yyval = &pars_assert_token; }
#line 1875 "pars0grm.cc"
    break;

  case 72: /* user_function_call: PARS_ID_TOKEN '(' ')'  */
#line 254 "pars0grm.y"
                                { yyval = yyvsp[-2]; }
#line 1881 "pars0grm.cc"
    break;

  case 73: /* table_list: PARS_ID_TOKEN  */
#line 258 "pars0grm.y"
                                { yyval = que_node_list_add_last(nullptr, yyvsp[0]); }
#line 1887 "pars0grm.cc"
    break;

  case 74: /* table_list: table_list ',' PARS_ID_TOKEN  */
#line 260 "pars0grm.y"
                                { yyval = que_node_list_add_last(yyvsp[-2], yyvsp[0]); }
#line 1893 "pars0grm.cc"
    break;

  case 75: /* variable_list: %empty  */
#line 264 "pars0grm.y"
                                { yyval = nullptr; }
#line 1899 "pars0grm.cc"
    break;

  case 76: /* variable_list: PARS_ID_TOKEN  */
#line 265 "pars0grm.y"
                                { yyval = que_node_list_add_last(nullptr, yyvsp[0]); }
#line 1905 "pars0grm.cc"
    break;

  case 77: /* variable_list: variable_list ',' PARS_ID_TOKEN  */
#line 267 "pars0grm.y"
                                { yyval = que_node_list_add_last(yyvsp[-2], yyvsp[0]); }
#line 1911 "pars0grm.cc"
    break;

  case 78: /* exp_list: %empty  */
#line 271 "pars0grm.y"
                                { yyval = nullptr; }
#line 1917 "pars0grm.cc"
    break;

  case 79: /* exp_list: exp  */
#line 272 "pars0grm.y"
                                { yyval = que_node_list_add_last(nullptr, yyvsp[0]);}
#line 1923 "pars0grm.cc"
    break;

  case 80: /* exp_list: exp_list ',' exp  */
#line 273 "pars0grm.y"
                                { yyval = que_node_list_add_last(yyvsp[-2], yyvsp[0]); }
#line 1929 "pars0grm.cc"
    break;

  case 81: /* select_item: exp  */
#line 277 "pars0grm.y"
                                { yyval = yyvsp[0]; }
#line 1935 "pars0grm.cc"
    break;

  case 82: /* select_item: PARS_COUNT_TOKEN '(' '*' ')'  */
#line 279 "pars0grm.y"
                                { yyval = pars_func(&pars_count_token,
				          que_node_list_add_last(nullptr,
					    sym_tab_add_int_lit(
						pars_sym_tab_global, 1))); }
#line 1944 "pars0grm.cc"
    break;

  case 83: /* select_item: PARS_COUNT_TOKEN '(' PARS_DISTINCT_TOKEN PARS_ID_TOKEN ')'  */
#line 284 "pars0grm.y"
                                { yyval = pars_func(&pars_count_token,
					    que_node_list_add_last(nullptr,
						pars_func(&pars_distinct_token,
						     que_node_list_add_last(
								nullptr, yyvsp[-1])))); }
#line 1954 "pars0grm.cc"
    break;

  case 84: /* select_item: PARS_SUM_TOKEN '(' exp ')'  */
#line 290 "pars0grm.y"
                                { yyval = pars_func(&pars_sum_token,
						que_node_list_add_last(nullptr,
									yyvsp[-1])); }
#line 1962 "pars0grm.cc"
    break;

  case 85: /* select_item_list: %empty  */
#line 296 "pars0grm.y"
                                { yyval = nullptr; }
#line 1968 "pars0grm.cc"
    break;

  case 86: /* select_item_list: select_item  */
#line 297 "pars0grm.y"
                                { yyval = que_node_list_add_last(nullptr, yyvsp[0]); }
#line 1974 "pars0grm.cc"
    break;

  case 87: /* select_item_list: select_item_list ',' select_item  */
#line 299 "pars0grm.y"
                                { yyval = que_node_list_add_last(yyvsp[-2], yyvsp[0]); }
#line 1980 "pars0grm.cc"
    break;

  case 88: /* select_list: '*'  */
#line 303 "pars0grm.y"
                                { yyval = pars_select_list(&pars_star_denoter, nullptr); }
#line 1986 "pars0grm.cc"
    break;

  case 89: /* select_list: select_item_list PARS_INTO_TOKEN variable_list  */
#line 305 "pars0grm.y"
                                { yyval = pars_select_list(yyvsp[-2], static_cast<sym_node_t*>(yyvsp[0])); }
#line 1992 "pars0grm.cc"
    break;

  case 90: /* select_list: select_item_list  */
#line 306 "pars0grm.y"
                                { yyval = pars_select_list(yyvsp[0], nullptr); }
#line 1998 "pars0grm.cc"
    break;

  case 91: /* search_condition: %empty  */
#line 310 "pars0grm.y"
                                { yyval = nullptr; }
#line 2004 "pars0grm.cc"
    break;

  case 92: /* search_condition: PARS_WHERE_TOKEN exp  */
#line 311 "pars0grm.y"
                                { yyval = yyvsp[0]; }
#line 2010 "pars0grm.cc"
    break;

  case 93: /* for_update_clause: %empty  */
#line 315 "pars0grm.y"
                                { yyval = nullptr; }
#line 2016 "pars0grm.cc"
    break;

  case 94: /* for_update_clause: PARS_FOR_TOKEN PARS_UPDATE_TOKEN  */
#line 317 "pars0grm.y"
                                { yyval = &pars_update_token; }
#line 2022 "pars0grm.cc"
    break;

  case 95: /* lock_shared_clause: %empty  */
#line 321 "pars0grm.y"
                                { yyval = nullptr; }
#line 2028 "pars0grm.cc"
    break;

  case 96: /* lock_shared_clause: PARS_LOCK_TOKEN PARS_IN_TOKEN PARS_SHARE_TOKEN PARS_MODE_TOKEN  */
#line 323 "pars0grm.y"
                                { yyval = &pars_share_token; }
#line 2034 "pars0grm.cc"
    break;

  case 97: /* order_direction: %empty  */
#line 327 "pars0grm.y"
                                { yyval = &pars_asc_token; }
#line 2040 "pars0grm.cc"
    break;

  case 98: /* order_direction: PARS_ASC_TOKEN  */
#line 328 "pars0grm.y"
                                { yyval = &pars_asc_token; }
#line 2046 "pars0grm.cc"
    break;

  case 99: /* order_direction: PARS_DESC_TOKEN  */
#line 329 "pars0grm.y"
                                { yyval = &pars_desc_token; }
#line 2052 "pars0grm.cc"
    break;

  case 100: /* order_by_clause: %empty  */
#line 333 "pars0grm.y"
                                { yyval = nullptr; }
#line 2058 "pars0grm.cc"
    break;

  case 101: /* order_by_clause: PARS_ORDER_TOKEN PARS_BY_TOKEN PARS_ID_TOKEN order_direction  */
#line 335 "pars0grm.y"
                                { yyval = pars_order_by(
                                          static_cast<sym_node_t*>(yyvsp[-1]),
                                          static_cast<pars_res_word_t*>(yyvsp[0])); }
#line 2066 "pars0grm.cc"
    break;

  case 102: /* select_statement: PARS_SELECT_TOKEN select_list PARS_FROM_TOKEN table_list search_condition for_update_clause lock_shared_clause order_by_clause  */
#line 346 "pars0grm.y"
                                { yyval = pars_select_statement(
                                         static_cast<sel_node_t*>(yyvsp[-6]),
                                         static_cast<sym_node_t*>(yyvsp[-4]),
                                         static_cast<que_node_t*>(yyvsp[-3]),
                                         static_cast<pars_res_word_t*>(yyvsp[-2]),
                                         static_cast<pars_res_word_t*>(yyvsp[-1]),
                                         static_cast<order_node_t*>(yyvsp[0])); }
#line 2078 "pars0grm.cc"
    break;

  case 103: /* insert_statement_start: PARS_INSERT_TOKEN PARS_INTO_TOKEN PARS_ID_TOKEN  */
#line 357 "pars0grm.y"
                                { yyval = yyvsp[0]; }
#line 2084 "pars0grm.cc"
    break;

  case 104: /* insert_statement: insert_statement_start PARS_VALUES_TOKEN '(' exp_list ')'  */
#line 362 "pars0grm.y"
                                { yyval = pars_insert_statement(
                                         static_cast<sym_node_t*>(yyvsp[-4]), yyvsp[-1], nullptr); }
#line 2091 "pars0grm.cc"
    break;

  case 105: /* insert_statement: insert_statement_start select_statement  */
#line 365 "pars0grm.y"
                                { yyval = pars_insert_statement(
                                         static_cast<sym_node_t*>(yyvsp[-1]),
                                         nullptr,
                                         static_cast<sel_node_t*>(yyvsp[0])); }
#line 2100 "pars0grm.cc"
    break;

  case 106: /* column_assignment: PARS_ID_TOKEN '=' exp  */
#line 372 "pars0grm.y"
                                { yyval = pars_column_assignment(
                                         static_cast<sym_node_t*>(yyvsp[-2]), yyvsp[0]); }
#line 2107 "pars0grm.cc"
    break;

  case 107: /* column_assignment_list: column_assignment  */
#line 377 "pars0grm.y"
                                { yyval = que_node_list_add_last(nullptr, yyvsp[0]); }
#line 2113 "pars0grm.cc"
    break;

  case 108: /* column_assignment_list: column_assignment_list ',' column_assignment  */
#line 379 "pars0grm.y"
                                { yyval = que_node_list_add_last(yyvsp[-2], yyvsp[0]); }
#line 2119 "pars0grm.cc"
    break;

  case 109: /* cursor_positioned: PARS_WHERE_TOKEN PARS_CURRENT_TOKEN PARS_OF_TOKEN PARS_ID_TOKEN  */
#line 385 "pars0grm.y"
                                { yyval = yyvsp[0]; }
#line 2125 "pars0grm.cc"
    break;

  case 110: /* update_statement_start: PARS_UPDATE_TOKEN PARS_ID_TOKEN PARS_SET_TOKEN column_assignment_list  */
#line 391 "pars0grm.y"
                                { yyval = pars_update_statement_start(
                                         false,
                                         static_cast<sym_node_t*>(yyvsp[-2]),
                                         static_cast<col_assign_node_t*>(yyvsp[0])); }
#line 2134 "pars0grm.cc"
    break;

  case 111: /* update_statement_searched: update_statement_start search_condition  */
#line 399 "pars0grm.y"
                                { yyval = pars_update_statement(
                                         static_cast<upd_node_t*>(yyvsp[-1]),
                                         nullptr,
                                         static_cast<que_node_t*>(yyvsp[0])); }
#line 2143 "pars0grm.cc"
    break;

  case 112: /* update_statement_positioned: update_statement_start cursor_positioned  */
#line 407 "pars0grm.y"
                                { yyval = pars_update_statement(
                                         static_cast<upd_node_t*>(yyvsp[-1]), 
                                         static_cast<sym_node_t*>(yyvsp[0]),
                                         nullptr); }
#line 2152 "pars0grm.cc"
    break;

  case 113: /* delete_statement_start: PARS_DELETE_TOKEN PARS_FROM_TOKEN PARS_ID_TOKEN  */
#line 415 "pars0grm.y"
                                { yyval = pars_update_statement_start(
                                         true,
                                         static_cast<sym_node_t*>(yyvsp[0]),
                                         nullptr); }
#line 2161 "pars0grm.cc"
    break;

  case 114: /* delete_statement_searched: delete_statement_start search_condition  */
#line 423 "pars0grm.y"
                                { yyval = pars_update_statement(
                                        static_cast<upd_node_t*>(yyvsp[-1]),
                                        nullptr,
                                        static_cast<que_node_t*>(yyvsp[0])); }
#line 2170 "pars0grm.cc"
    break;

  case 115: /* delete_statement_positioned: delete_statement_start cursor_positioned  */
#line 431 "pars0grm.y"
                                { yyval = pars_update_statement(
                                        static_cast<upd_node_t*>(yyvsp[-1]),
                                        static_cast<sym_node_t*>(yyvsp[0]),
                                        nullptr); }
#line 2179 "pars0grm.cc"
    break;

  case 116: /* assignment_statement: PARS_ID_TOKEN PARS_ASSIGN_TOKEN exp  */
#line 439 "pars0grm.y"
                                { yyval = pars_assignment_statement(
                                        static_cast<sym_node_t*>(yyvsp[-2]),
                                        static_cast<que_node_t*>(yyvsp[0])); }
#line 2187 "pars0grm.cc"
    break;

  case 117: /* row_printf_statement: PARS_ROW_PRINTF_TOKEN select_statement  */
#line 445 "pars0grm.y"
                                { yyval = pars_row_printf_statement(
                                         static_cast<sel_node_t*>(yyvsp[0])); }
#line 2194 "pars0grm.cc"
    break;

  case 118: /* elsif_element: PARS_ELSIF_TOKEN exp PARS_THEN_TOKEN statement_list  */
#line 452 "pars0grm.y"
                                { yyval = pars_elsif_element(yyvsp[-2], yyvsp[0]); }
#line 2200 "pars0grm.cc"
    break;

  case 119: /* elsif_list: elsif_element  */
#line 456 "pars0grm.y"
                                { yyval = que_node_list_add_last(nullptr, yyvsp[0]); }
#line 2206 "pars0grm.cc"
    break;

  case 120: /* elsif_list: elsif_list elsif_element  */
#line 458 "pars0grm.y"
                                { yyval = que_node_list_add_last(yyvsp[-1], yyvsp[0]); }
#line 2212 "pars0grm.cc"
    break;

  case 121: /* else_part: %empty  */
#line 462 "pars0grm.y"
                                { yyval = nullptr; }
#line 2218 "pars0grm.cc"
    break;

  case 122: /* else_part: PARS_ELSE_TOKEN statement_list  */
#line 464 "pars0grm.y"
                                { yyval = yyvsp[0]; }
#line 2224 "pars0grm.cc"
    break;

  case 123: /* else_part: elsif_list  */
#line 465 "pars0grm.y"
                                { yyval = yyvsp[0]; }
#line 2230 "pars0grm.cc"
    break;

  case 124: /* if_statement: PARS_IF_TOKEN exp PARS_THEN_TOKEN statement_list else_part PARS_END_TOKEN PARS_IF_TOKEN  */
#line 472 "pars0grm.y"
                                { yyval = pars_if_statement(yyvsp[-5], yyvsp[-3], yyvsp[-2]); }
#line 2236 "pars0grm.cc"
    break;

  case 125: /* while_statement: PARS_WHILE_TOKEN exp PARS_LOOP_TOKEN statement_list PARS_END_TOKEN PARS_LOOP_TOKEN  */
#line 478 "pars0grm.y"
                                { yyval = pars_while_statement(yyvsp[-4], yyvsp[-2]); }
#line 2242 "pars0grm.cc"
    break;

  case 126: /* for_statement: PARS_FOR_TOKEN PARS_ID_TOKEN PARS_IN_TOKEN exp PARS_DDOT_TOKEN exp PARS_LOOP_TOKEN statement_list PARS_END_TOKEN PARS_LOOP_TOKEN  */
#line 486 "pars0grm.y"
                                { yyval = pars_for_statement(
                                         static_cast<sym_node_t*>(yyvsp[-8]),
                                         yyvsp[-6], yyvsp[-4], yyvsp[-2]); }
#line 2250 "pars0grm.cc"
    break;

  case 127: /* exit_statement: PARS_EXIT_TOKEN  */
#line 492 "pars0grm.y"
                                { yyval = pars_exit_statement(); }
#line 2256 "pars0grm.cc"
    break;

  case 128: /* return_statement: PARS_RETURN_TOKEN  */
#line 496 "pars0grm.y"
                                { yyval = pars_return_statement(); }
#line 2262 "pars0grm.cc"
    break;

  case 129: /* open_cursor_statement: PARS_OPEN_TOKEN PARS_ID_TOKEN  */
#line 501 "pars0grm.y"
                                { yyval = pars_open_statement(
					ROW_SEL_OPEN_CURSOR,
                                        static_cast<sym_node_t*>(yyvsp[0])); }
#line 2270 "pars0grm.cc"
    break;

  case 130: /* close_cursor_statement: PARS_CLOSE_TOKEN PARS_ID_TOKEN  */
#line 508 "pars0grm.y"
                                { yyval = pars_open_statement(
					ROW_SEL_CLOSE_CURSOR,
                                        static_cast<sym_node_t*>(yyvsp[0])); }
#line 2278 "pars0grm.cc"
    break;

  case 131: /* fetch_statement: PARS_FETCH_TOKEN PARS_ID_TOKEN PARS_INTO_TOKEN variable_list  */
#line 515 "pars0grm.y"
                                { yyval = pars_fetch_statement(
                                        static_cast<sym_node_t*>(yyvsp[-2]),
                                        static_cast<sym_node_t*>(yyvsp[0]), nullptr); }
#line 2286 "pars0grm.cc"
    break;

  case 132: /* fetch_statement: PARS_FETCH_TOKEN PARS_ID_TOKEN PARS_INTO_TOKEN user_function_call  */
#line 519 "pars0grm.y"
                                { yyval = pars_fetch_statement(
                                        static_cast<sym_node_t*>(yyvsp[-2]),
                                        nullptr,
                                        static_cast<sym_node_t*>(yyvsp[0])); }
#line 2295 "pars0grm.cc"
    break;

  case 133: /* column_def: PARS_ID_TOKEN type_name opt_column_len opt_unsigned opt_not_null  */
#line 527 "pars0grm.y"
                                { yyval = pars_column_def(
                                        static_cast<sym_node_t*>(yyvsp[-4]),
                                        static_cast<pars_res_word_t*>(yyvsp[-3]),
                                        static_cast<sym_node_t*>(yyvsp[-2]),
                                        yyvsp[-1], yyvsp[0]); }
#line 2305 "pars0grm.cc"
    break;

  case 134: /* column_def_list: column_def  */
#line 535 "pars0grm.y"
                                { yyval = que_node_list_add_last(nullptr, yyvsp[0]); }
#line 2311 "pars0grm.cc"
    break;

  case 135: /* column_def_list: column_def_list ',' column_def  */
#line 537 "pars0grm.y"
                                { yyval = que_node_list_add_last(yyvsp[-2], yyvsp[0]); }
#line 2317 "pars0grm.cc"
    break;

  case 136: /* opt_column_len: %empty  */
#line 541 "pars0grm.y"
                                { yyval = nullptr; }
#line 2323 "pars0grm.cc"
    break;

  case 137: /* opt_column_len: '(' PARS_INT_LIT ')'  */
#line 543 "pars0grm.y"
                                { yyval = yyvsp[-1]; }
#line 2329 "pars0grm.cc"
    break;

  case 138: /* opt_unsigned: %empty  */
#line 547 "pars0grm.y"
                                { yyval = nullptr; }
#line 2335 "pars0grm.cc"
    break;

  case 139: /* opt_unsigned: PARS_UNSIGNED_TOKEN  */
#line 549 "pars0grm.y"
                                { yyval = &pars_int_token;
					/* pass any non-nullptr pointer */ }
#line 2342 "pars0grm.cc"
    break;

  case 140: /* opt_not_null: %empty  */
#line 554 "pars0grm.y"
                                { yyval = nullptr; }
#line 2348 "pars0grm.cc"
    break;

  case 141: /* opt_not_null: PARS_NOT_TOKEN PARS_NULL_LIT  */
#line 556 "pars0grm.y"
                                { yyval = &pars_int_token;
					/* pass any non-nullptr pointer */ }
#line 2355 "pars0grm.cc"
    break;

  case 142: /* not_fit_in_memory: %empty  */
#line 561 "pars0grm.y"
                                { yyval = nullptr; }
#line 2361 "pars0grm.cc"
    break;

  case 143: /* not_fit_in_memory: PARS_DOES_NOT_FIT_IN_MEM_TOKEN  */
#line 563 "pars0grm.y"
                                { yyval = &pars_int_token;
					/* pass any non-nullptr pointer */ }
#line 2368 "pars0grm.cc"
    break;

  case 144: /* create_table: PARS_CREATE_TOKEN PARS_TABLE_TOKEN PARS_ID_TOKEN '(' column_def_list ')' not_fit_in_memory  */
#line 570 "pars0grm.y"
                                { yyval = pars_create_table(
                                         static_cast<sym_node_t*>(yyvsp[-4]),
                                         static_cast<sym_node_t*>(yyvsp[-2]), yyvsp[0]); }
#line 2376 "pars0grm.cc"
    break;

  case 145: /* column_list: PARS_ID_TOKEN  */
#line 576 "pars0grm.y"
                                { yyval = que_node_list_add_last(nullptr, yyvsp[0]); }
#line 2382 "pars0grm.cc"
    break;

  case 146: /* column_list: column_list ',' PARS_ID_TOKEN  */
#line 578 "pars0grm.y"
                                { yyval = que_node_list_add_last(yyvsp[-2], yyvsp[0]); }
#line 2388 "pars0grm.cc"
    break;

  case 147: /* unique_def: %empty  */
#line 582 "pars0grm.y"
                                { yyval = nullptr; }
#line 2394 "pars0grm.cc"
    break;

  case 148: /* unique_def: PARS_UNIQUE_TOKEN  */
#line 583 "pars0grm.y"
                                { yyval = &pars_unique_token; }
#line 2400 "pars0grm.cc"
    break;

  case 149: /* clustered_def: %empty  */
#line 587 "pars0grm.y"
                                { yyval = nullptr; }
#line 2406 "pars0grm.cc"
    break;

  case 150: /* clustered_def: PARS_CLUSTERED_TOKEN  */
#line 588 "pars0grm.y"
                                { yyval = &pars_clustered_token; }
#line 2412 "pars0grm.cc"
    break;

  case 151: /* create_index: PARS_CREATE_TOKEN unique_def clustered_def PARS_INDEX_TOKEN PARS_ID_TOKEN PARS_ON_TOKEN PARS_ID_TOKEN '(' column_list ')'  */
#line 596 "pars0grm.y"
                                { yyval = pars_create_index(
                                        static_cast<pars_res_word_t*>(yyvsp[-8]),
                                        static_cast<pars_res_word_t*>(yyvsp[-7]),
                                        static_cast<sym_node_t*>(yyvsp[-5]),
                                        static_cast<sym_node_t*>(yyvsp[-3]),
                                        static_cast<sym_node_t*>(yyvsp[-1])); }
#line 2423 "pars0grm.cc"
    break;

  case 152: /* commit_statement: PARS_COMMIT_TOKEN PARS_WORK_TOKEN  */
#line 606 "pars0grm.y"
                                { yyval = pars_commit_statement(); }
#line 2429 "pars0grm.cc"
    break;

  case 153: /* rollback_statement: PARS_ROLLBACK_TOKEN PARS_WORK_TOKEN  */
#line 611 "pars0grm.y"
                                { yyval = pars_rollback_statement(); }
#line 2435 "pars0grm.cc"
    break;

  case 154: /* type_name: PARS_INT_TOKEN  */
#line 615 "pars0grm.y"
                                { yyval = &pars_int_token; }
#line 2441 "pars0grm.cc"
    break;

  case 155: /* type_name: PARS_INTEGER_TOKEN  */
#line 616 "pars0grm.y"
                                { yyval = &pars_int_token; }
#line 2447 "pars0grm.cc"
    break;

  case 156: /* type_name: PARS_CHAR_TOKEN  */
#line 617 "pars0grm.y"
                                { yyval = &pars_char_token; }
#line 2453 "pars0grm.cc"
    break;

  case 157: /* type_name: PARS_BINARY_TOKEN  */
#line 618 "pars0grm.y"
                                { yyval = &pars_binary_token; }
#line 2459 "pars0grm.cc"
    break;

  case 158: /* type_name: PARS_BLOB_TOKEN  */
#line 619 "pars0grm.y"
                                { yyval = &pars_blob_token; }
#line 2465 "pars0grm.cc"
    break;

  case 159: /* parameter_declaration: PARS_ID_TOKEN PARS_IN_TOKEN type_name  */
#line 624 "pars0grm.y"
                                { yyval = pars_parameter_declaration(
                                        static_cast<sym_node_t*>(yyvsp[-2]),
                                        PARS_INPUT,
                                        static_cast<pars_res_word_t*>(yyvsp[0])); }
#line 2474 "pars0grm.cc"
    break;

  case 160: /* parameter_declaration: PARS_ID_TOKEN PARS_OUT_TOKEN type_name  */
#line 629 "pars0grm.y"
                                { yyval = pars_parameter_declaration(
                                        static_cast<sym_node_t*>(yyvsp[-2]),
                                        PARS_OUTPUT,
                                        static_cast<pars_res_word_t*>(yyvsp[0])); }
#line 2483 "pars0grm.cc"
    break;

  case 161: /* parameter_declaration_list: %empty  */
#line 636 "pars0grm.y"
                                { yyval = nullptr; }
#line 2489 "pars0grm.cc"
    break;

  case 162: /* parameter_declaration_list: parameter_declaration  */
#line 637 "pars0grm.y"
                                { yyval = que_node_list_add_last(nullptr, yyvsp[0]); }
#line 2495 "pars0grm.cc"
    break;

  case 163: /* parameter_declaration_list: parameter_declaration_list ',' parameter_declaration  */
#line 639 "pars0grm.y"
                                { yyval = que_node_list_add_last(yyvsp[-2], yyvsp[0]); }
#line 2501 "pars0grm.cc"
    break;

  case 164: /* variable_declaration: PARS_ID_TOKEN type_name ';'  */
#line 644 "pars0grm.y"
                                { yyval = pars_variable_declaration(
                                        static_cast<sym_node_t*>(yyvsp[-2]),
                                        static_cast<pars_res_word_t*>(yyvsp[-1])); }
#line 2509 "pars0grm.cc"
    break;

  case 168: /* cursor_declaration: PARS_DECLARE_TOKEN PARS_CURSOR_TOKEN PARS_ID_TOKEN PARS_IS_TOKEN select_statement ';'  */
#line 658 "pars0grm.y"
                                { yyval = pars_cursor_declaration(
                                        static_cast<sym_node_t*>(yyvsp[-3]),
                                        static_cast<sel_node_t*>(yyvsp[-1])); }
#line 2517 "pars0grm.cc"
    break;

  case 169: /* function_declaration: PARS_DECLARE_TOKEN PARS_FUNCTION_TOKEN PARS_ID_TOKEN ';'  */
#line 665 "pars0grm.y"
                                { yyval = pars_function_declaration(
                                        static_cast<sym_node_t*>(yyvsp[-1])); }
#line 2524 "pars0grm.cc"
    break;

  case 175: /* procedure_definition: PARS_PROCEDURE_TOKEN PARS_ID_TOKEN '(' parameter_declaration_list ')' PARS_IS_TOKEN variable_declaration_list declaration_list PARS_BEGIN_TOKEN statement_list PARS_END_TOKEN  */
#line 687 "pars0grm.y"
                                { yyval = pars_procedure_definition(
                                        static_cast<sym_node_t*>(yyvsp[-9]),
                                        static_cast<sym_node_t*>(yyvsp[-7]),
                                        yyvsp[-1]); }
#line 2533 "pars0grm.cc"
    break;


#line 2537 "pars0grm.cc"

      default: break;
    }
  /* User semantic actions sometimes alter yychar, and that requires
     that yytoken be updated with the new translation.  We take the
     approach of translating immediately before every use of yytoken.
     One alternative is translating here after every semantic action,
     but that translation would be missed if the semantic action invokes
     YYABORT, YYACCEPT, or YYERROR immediately after altering yychar or
     if it invokes YYBACKUP.  In the case of YYABORT or YYACCEPT, an
     incorrect destructor might then be invoked immediately.  In the
     case of YYERROR or YYBACKUP, subsequent parser actions might lead
     to an incorrect destructor call or verbose syntax error message
     before the lookahead is translated.  */
  YY_SYMBOL_PRINT ("-> $$ =", YY_CAST (yysymbol_kind_t, yyr1[yyn]), &yyval, &yyloc);

  YYPOPSTACK (yylen);
  yylen = 0;

  *++yyvsp = yyval;

  /* Now 'shift' the result of the reduction.  Determine what state
     that goes to, based on the state we popped back to and the rule
     number reduced by.  */
  {
    const int yylhs = yyr1[yyn] - YYNTOKENS;
    const int yyi = yypgoto[yylhs] + *yyssp;
    yystate = (0 <= yyi && yyi <= YYLAST && yycheck[yyi] == *yyssp
               ? yytable[yyi]
               : yydefgoto[yylhs]);
  }

  goto yynewstate;


/*--------------------------------------.
| yyerrlab -- here on detecting error.  |
`--------------------------------------*/
yyerrlab:
  /* Make sure we have latest lookahead translation.  See comments at
     user semantic actions for why this is necessary.  */
  yytoken = yychar == YYEMPTY ? YYSYMBOL_YYEMPTY : YYTRANSLATE (yychar);
  /* If not already recovering from an error, report this error.  */
  if (!yyerrstatus)
    {
      ++yynerrs;
      yyerror (YY_("syntax error"));
    }

  if (yyerrstatus == 3)
    {
      /* If just tried and failed to reuse lookahead token after an
         error, discard it.  */

      if (yychar <= YYEOF)
        {
          /* Return failure if at end of input.  */
          if (yychar == YYEOF)
            YYABORT;
        }
      else
        {
          yydestruct ("Error: discarding",
                      yytoken, &yylval);
          yychar = YYEMPTY;
        }
    }

  /* Else will try to reuse lookahead token after shifting the error
     token.  */
  goto yyerrlab1;


/*---------------------------------------------------.
| yyerrorlab -- error raised explicitly by YYERROR.  |
`---------------------------------------------------*/
yyerrorlab:
  /* Pacify compilers when the user code never invokes YYERROR and the
     label yyerrorlab therefore never appears in user code.  */
  if (0)
    YYERROR;
  ++yynerrs;

  /* Do not reclaim the symbols of the rule whose action triggered
     this YYERROR.  */
  YYPOPSTACK (yylen);
  yylen = 0;
  YY_STACK_PRINT (yyss, yyssp);
  yystate = *yyssp;
  goto yyerrlab1;


/*-------------------------------------------------------------.
| yyerrlab1 -- common code for both syntax error and YYERROR.  |
`-------------------------------------------------------------*/
yyerrlab1:
  yyerrstatus = 3;      /* Each real token shifted decrements this.  */

  /* Pop stack until we find a state that shifts the error token.  */
  for (;;)
    {
      yyn = yypact[yystate];
      if (!yypact_value_is_default (yyn))
        {
          yyn += YYSYMBOL_YYerror;
          if (0 <= yyn && yyn <= YYLAST && yycheck[yyn] == YYSYMBOL_YYerror)
            {
              yyn = yytable[yyn];
              if (0 < yyn)
                break;
            }
        }

      /* Pop the current state because it cannot handle the error token.  */
      if (yyssp == yyss)
        YYABORT;


      yydestruct ("Error: popping",
                  YY_ACCESSING_SYMBOL (yystate), yyvsp);
      YYPOPSTACK (1);
      yystate = *yyssp;
      YY_STACK_PRINT (yyss, yyssp);
    }

  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  *++yyvsp = yylval;
  YY_IGNORE_MAYBE_UNINITIALIZED_END


  /* Shift the error token.  */
  YY_SYMBOL_PRINT ("Shifting", YY_ACCESSING_SYMBOL (yyn), yyvsp, yylsp);

  yystate = yyn;
  goto yynewstate;


/*-------------------------------------.
| yyacceptlab -- YYACCEPT comes here.  |
`-------------------------------------*/
yyacceptlab:
  yyresult = 0;
  goto yyreturnlab;


/*-----------------------------------.
| yyabortlab -- YYABORT comes here.  |
`-----------------------------------*/
yyabortlab:
  yyresult = 1;
  goto yyreturnlab;


/*-----------------------------------------------------------.
| yyexhaustedlab -- YYNOMEM (memory exhaustion) comes here.  |
`-----------------------------------------------------------*/
yyexhaustedlab:
  yyerror (YY_("memory exhausted"));
  yyresult = 2;
  goto yyreturnlab;


/*----------------------------------------------------------.
| yyreturnlab -- parsing is finished, clean up and return.  |
`----------------------------------------------------------*/
yyreturnlab:
  if (yychar != YYEMPTY)
    {
      /* Make sure we have latest lookahead translation.  See comments at
         user semantic actions for why this is necessary.  */
      yytoken = YYTRANSLATE (yychar);
      yydestruct ("Cleanup: discarding lookahead",
                  yytoken, &yylval);
    }
  /* Do not reclaim the symbols of the rule whose action triggered
     this YYABORT or YYACCEPT.  */
  YYPOPSTACK (yylen);
  YY_STACK_PRINT (yyss, yyssp);
  while (yyssp != yyss)
    {
      yydestruct ("Cleanup: popping",
                  YY_ACCESSING_SYMBOL (+*yyssp), yyvsp);
      YYPOPSTACK (1);
    }
#ifndef yyoverflow
  if (yyss != yyssa) {
    // SB: This seems to be allocated on the stack.
    // YYSTACK_FREE (yyss);
  }
#endif

  return yyresult;
}

#line 693 "pars0grm.y"

/** Release any resources used by the parser. */
void pars_close() {
  pars_lexer_close();
}
