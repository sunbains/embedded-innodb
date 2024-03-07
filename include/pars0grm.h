/* A Bison parser, made by GNU Bison 3.8.2.  */

/* Bison interface for Yacc-like parsers in C

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

/* DO NOT RELY ON FEATURES THAT ARE NOT DOCUMENTED in the manual,
   especially those whose name start with YY_ or yy_.  They are
   private implementation details that can be changed or removed.  */

#ifndef YY_YY_PARS0GRM_TAB_H_INCLUDED
#define YY_YY_PARS0GRM_TAB_H_INCLUDED
/* Debug traces.  */
#ifndef YYDEBUG
#define YYDEBUG 0
#endif
#if YYDEBUG
extern int yydebug;
#endif

/* Token kinds.  */
#ifndef YYTOKENTYPE
#define YYTOKENTYPE

enum yytokentype {
  YYEMPTY = -2,
  YYEOF = 0,                            /* "end of file"  */
  YYerror = 256,                        /* error  */
  YYUNDEF = 257,                        /* "invalid token"  */
  PARS_INT_LIT = 258,                   /* PARS_INT_LIT  */
  PARS_FLOAT_LIT = 259,                 /* PARS_FLOAT_LIT  */
  PARS_STR_LIT = 260,                   /* PARS_STR_LIT  */
  PARS_FIXBINARY_LIT = 261,             /* PARS_FIXBINARY_LIT  */
  PARS_BLOB_LIT = 262,                  /* PARS_BLOB_LIT  */
  PARS_NULL_LIT = 263,                  /* PARS_NULL_LIT  */
  PARS_ID_TOKEN = 264,                  /* PARS_ID_TOKEN  */
  PARS_AND_TOKEN = 265,                 /* PARS_AND_TOKEN  */
  PARS_OR_TOKEN = 266,                  /* PARS_OR_TOKEN  */
  PARS_NOT_TOKEN = 267,                 /* PARS_NOT_TOKEN  */
  PARS_GE_TOKEN = 268,                  /* PARS_GE_TOKEN  */
  PARS_LE_TOKEN = 269,                  /* PARS_LE_TOKEN  */
  PARS_NE_TOKEN = 270,                  /* PARS_NE_TOKEN  */
  PARS_PROCEDURE_TOKEN = 271,           /* PARS_PROCEDURE_TOKEN  */
  PARS_IN_TOKEN = 272,                  /* PARS_IN_TOKEN  */
  PARS_OUT_TOKEN = 273,                 /* PARS_OUT_TOKEN  */
  PARS_BINARY_TOKEN = 274,              /* PARS_BINARY_TOKEN  */
  PARS_BLOB_TOKEN = 275,                /* PARS_BLOB_TOKEN  */
  PARS_INT_TOKEN = 276,                 /* PARS_INT_TOKEN  */
  PARS_INTEGER_TOKEN = 277,             /* PARS_INTEGER_TOKEN  */
  PARS_FLOAT_TOKEN = 278,               /* PARS_FLOAT_TOKEN  */
  PARS_CHAR_TOKEN = 279,                /* PARS_CHAR_TOKEN  */
  PARS_IS_TOKEN = 280,                  /* PARS_IS_TOKEN  */
  PARS_BEGIN_TOKEN = 281,               /* PARS_BEGIN_TOKEN  */
  PARS_END_TOKEN = 282,                 /* PARS_END_TOKEN  */
  PARS_IF_TOKEN = 283,                  /* PARS_IF_TOKEN  */
  PARS_THEN_TOKEN = 284,                /* PARS_THEN_TOKEN  */
  PARS_ELSE_TOKEN = 285,                /* PARS_ELSE_TOKEN  */
  PARS_ELSIF_TOKEN = 286,               /* PARS_ELSIF_TOKEN  */
  PARS_LOOP_TOKEN = 287,                /* PARS_LOOP_TOKEN  */
  PARS_WHILE_TOKEN = 288,               /* PARS_WHILE_TOKEN  */
  PARS_RETURN_TOKEN = 289,              /* PARS_RETURN_TOKEN  */
  PARS_SELECT_TOKEN = 290,              /* PARS_SELECT_TOKEN  */
  PARS_SUM_TOKEN = 291,                 /* PARS_SUM_TOKEN  */
  PARS_COUNT_TOKEN = 292,               /* PARS_COUNT_TOKEN  */
  PARS_DISTINCT_TOKEN = 293,            /* PARS_DISTINCT_TOKEN  */
  PARS_FROM_TOKEN = 294,                /* PARS_FROM_TOKEN  */
  PARS_WHERE_TOKEN = 295,               /* PARS_WHERE_TOKEN  */
  PARS_FOR_TOKEN = 296,                 /* PARS_FOR_TOKEN  */
  PARS_DDOT_TOKEN = 297,                /* PARS_DDOT_TOKEN  */
  PARS_READ_TOKEN = 298,                /* PARS_READ_TOKEN  */
  PARS_ORDER_TOKEN = 299,               /* PARS_ORDER_TOKEN  */
  PARS_BY_TOKEN = 300,                  /* PARS_BY_TOKEN  */
  PARS_ASC_TOKEN = 301,                 /* PARS_ASC_TOKEN  */
  PARS_DESC_TOKEN = 302,                /* PARS_DESC_TOKEN  */
  PARS_INSERT_TOKEN = 303,              /* PARS_INSERT_TOKEN  */
  PARS_INTO_TOKEN = 304,                /* PARS_INTO_TOKEN  */
  PARS_VALUES_TOKEN = 305,              /* PARS_VALUES_TOKEN  */
  PARS_UPDATE_TOKEN = 306,              /* PARS_UPDATE_TOKEN  */
  PARS_SET_TOKEN = 307,                 /* PARS_SET_TOKEN  */
  PARS_DELETE_TOKEN = 308,              /* PARS_DELETE_TOKEN  */
  PARS_CURRENT_TOKEN = 309,             /* PARS_CURRENT_TOKEN  */
  PARS_OF_TOKEN = 310,                  /* PARS_OF_TOKEN  */
  PARS_CREATE_TOKEN = 311,              /* PARS_CREATE_TOKEN  */
  PARS_TABLE_TOKEN = 312,               /* PARS_TABLE_TOKEN  */
  PARS_INDEX_TOKEN = 313,               /* PARS_INDEX_TOKEN  */
  PARS_UNIQUE_TOKEN = 314,              /* PARS_UNIQUE_TOKEN  */
  PARS_CLUSTERED_TOKEN = 315,           /* PARS_CLUSTERED_TOKEN  */
  PARS_DOES_NOT_FIT_IN_MEM_TOKEN = 316, /* PARS_DOES_NOT_FIT_IN_MEM_TOKEN  */
  PARS_ON_TOKEN = 317,                  /* PARS_ON_TOKEN  */
  PARS_ASSIGN_TOKEN = 318,              /* PARS_ASSIGN_TOKEN  */
  PARS_DECLARE_TOKEN = 319,             /* PARS_DECLARE_TOKEN  */
  PARS_CURSOR_TOKEN = 320,              /* PARS_CURSOR_TOKEN  */
  PARS_SQL_TOKEN = 321,                 /* PARS_SQL_TOKEN  */
  PARS_OPEN_TOKEN = 322,                /* PARS_OPEN_TOKEN  */
  PARS_FETCH_TOKEN = 323,               /* PARS_FETCH_TOKEN  */
  PARS_CLOSE_TOKEN = 324,               /* PARS_CLOSE_TOKEN  */
  PARS_NOTFOUND_TOKEN = 325,            /* PARS_NOTFOUND_TOKEN  */
  PARS_TO_CHAR_TOKEN = 326,             /* PARS_TO_CHAR_TOKEN  */
  PARS_TO_NUMBER_TOKEN = 327,           /* PARS_TO_NUMBER_TOKEN  */
  PARS_TO_BINARY_TOKEN = 328,           /* PARS_TO_BINARY_TOKEN  */
  PARS_BINARY_TO_NUMBER_TOKEN = 329,    /* PARS_BINARY_TO_NUMBER_TOKEN  */
  PARS_SUBSTR_TOKEN = 330,              /* PARS_SUBSTR_TOKEN  */
  PARS_REPLSTR_TOKEN = 331,             /* PARS_REPLSTR_TOKEN  */
  PARS_CONCAT_TOKEN = 332,              /* PARS_CONCAT_TOKEN  */
  PARS_INSTR_TOKEN = 333,               /* PARS_INSTR_TOKEN  */
  PARS_LENGTH_TOKEN = 334,              /* PARS_LENGTH_TOKEN  */
  PARS_SYSDATE_TOKEN = 335,             /* PARS_SYSDATE_TOKEN  */
  PARS_PRINTF_TOKEN = 336,              /* PARS_PRINTF_TOKEN  */
  PARS_ASSERT_TOKEN = 337,              /* PARS_ASSERT_TOKEN  */
  PARS_RND_TOKEN = 338,                 /* PARS_RND_TOKEN  */
  PARS_RND_STR_TOKEN = 339,             /* PARS_RND_STR_TOKEN  */
  PARS_ROW_PRINTF_TOKEN = 340,          /* PARS_ROW_PRINTF_TOKEN  */
  PARS_COMMIT_TOKEN = 341,              /* PARS_COMMIT_TOKEN  */
  PARS_ROLLBACK_TOKEN = 342,            /* PARS_ROLLBACK_TOKEN  */
  PARS_WORK_TOKEN = 343,                /* PARS_WORK_TOKEN  */
  PARS_UNSIGNED_TOKEN = 344,            /* PARS_UNSIGNED_TOKEN  */
  PARS_EXIT_TOKEN = 345,                /* PARS_EXIT_TOKEN  */
  PARS_FUNCTION_TOKEN = 346,            /* PARS_FUNCTION_TOKEN  */
  PARS_LOCK_TOKEN = 347,                /* PARS_LOCK_TOKEN  */
  PARS_SHARE_TOKEN = 348,               /* PARS_SHARE_TOKEN  */
  PARS_MODE_TOKEN = 349,                /* PARS_MODE_TOKEN  */
  NEG = 350                             /* NEG  */
};
typedef enum yytokentype yytoken_kind_t;
#endif

/* Value type.  */
#if !defined YYSTYPE && !defined YYSTYPE_IS_DECLARED
typedef int YYSTYPE;
#define YYSTYPE_IS_TRIVIAL 1
#define YYSTYPE_IS_DECLARED 1
#endif

extern YYSTYPE yylval;

int yyparse(void);

#endif /* !YY_YY_PARS0GRM_TAB_H_INCLUDED  */
