dnl  Copyright (C) 2009 Sun Microsystems, Inc.
dnl This file is free software; Sun Microsystems, Inc.
dnl gives unlimited permission to copy and/or distribute it,
dnl with or without modifications, as long as this notice is preserved.

AC_DEFUN([_PANDORA_SEARCH_LIBHAILDB],[
  AC_REQUIRE([AC_LIB_PREFIX])

  dnl --------------------------------------------------------------------
  dnl  Check for libinnodb
  dnl --------------------------------------------------------------------

  AC_ARG_ENABLE([libinnodb],
    [AS_HELP_STRING([--disable-libinnodb],
      [Build with libinnodb support @<:@default=on@:>@])],
    [ac_enable_libinnodb="$enableval"],
    [ac_enable_libinnodb="yes"])


  AS_IF([test "x$ac_enable_libinnodb" = "xyes"],[
    AC_LIB_HAVE_LINKFLAGS(innodb,,[
      #include <innodb.h>
    ],[
      ib_set_panic_handler(NULL);
    ])
    AS_IF([test "x${ac_cv_libinnodb}" = "xyes"],[
      AC_DEFINE([HAVE_HAILDB_H],[1],[Do we have innodb.h])
      ])
  ],[
    ac_cv_libinnodb="no"
  ])
  AM_CONDITIONAL(HAVE_LIBHAILDB, [test "x${ac_cv_libinnodb}" = "xyes"])
])

AC_DEFUN([PANDORA_HAVE_LIBHAILDB],[
  AC_REQUIRE([_PANDORA_SEARCH_LIBHAILDB])
])

AC_DEFUN([PANDORA_REQUIRE_LIBHAILDB],[
  AC_REQUIRE([PANDORA_HAVE_LIBHAILDB])
  AS_IF([test "x${ac_cv_libinnodb}" = "xno"],
      AC_MSG_ERROR([libinnodb 2.2.0 or later is required for ${PACKAGE}]))
])
