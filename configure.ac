dnl Copyright (C) 2008 Oracle/Innobase Oy
dnl Copyright (C) 2010 Monty Taylor
dnl
dnl This program is free software; you can redistribute it and/or modify
dnl it under the terms of the GNU General Public License as published by
dnl the Free Software Foundation; version 2 of the License.
dnl 
dnl This program is distributed in the hope that it will be useful,
dnl but WITHOUT ANY WARRANTY; without even the implied warranty of
dnl MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
dnl GNU General Public License for more details.
dnl 
dnl You should have received a copy of the GNU General Public License
dnl along with this program; if not, write to the Free Software
dnl Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
dnl Process this file with autoconf to produce a configure script.

AC_PREREQ(2.52)		dnl              Minimum Autoconf version required.

####################################
# change version here. --> BOTH PLACES PLEASE!
# note: can't put vars in AC_INIT
AC_INIT([haildb],[2.3.2],[http://bugs.launchpad.net/haildb])

MAJOR_VERSION=2
MINOR_VERSION=3
SUB_VERSION=2
HAILDB_FULL_VERSION=$MAJOR_VERSION.$MINOR_VERSION.$SUB_VERSION
####################################

AC_CONFIG_SRCDIR([include/univ.i])
AC_CONFIG_AUX_DIR([config])
AC_SUBST(HAILDB_FULL_VERSION)

PANDORA_CANONICAL_TARGET(less-warnings, warnings-off, force-gcc42, no-vc-changelog)

AC_PATH_PROG(uname_prog, uname, no)


# increment if interfaces have been added, removed or changed
VERSION_CURRENT=6
# increment if source code has changed
# set to zero if current is incremented
VERSION_REVISION=0
# increment if interfaces have been added
# set to zero if interfaces have been removed or changed
VERSION_AGE=0
AC_DEFINE_UNQUOTED([IB_API_VERSION_CURRENT],[$VERSION_CURRENT],[Current Version])
AC_DEFINE_UNQUOTED([IB_API_VERSION_REVISION],[$VERSION_REVISION],[Revision Version])
AC_DEFINE_UNQUOTED([IB_API_VERSION_AGE],[$VERSION_AGE],[Age Version])
AC_SUBST(IB_API_VERSION_REVISION)
AC_SUBST(IB_API_VERSION_AGE)

IB_API_VERSION=$VERSION_CURRENT:$VERSION_REVISION:$VERSION_AGE
AC_SUBST(IB_API_VERSION)

PANDORA_REQUIRE_BISON

# libhaildb versioning when linked with GNU ld.
if test "$lt_cv_prog_gnu_ld" = "yes"
then
  LD_VERSION_SCRIPT="-Wl,--version-script=\$(top_srcdir)/libhaildb.ver"
fi
AC_SUBST(LD_VERSION_SCRIPT)


#####
## Check for system header files used by InnoDB.
#####
AC_CHECK_HEADERS([pthread.h])
AC_CHECK_HEADERS(valgrind/memcheck.h)

case "$target_os" in
lin*)
	TARGET_LINUX="true";
	CFLAGS="$CFLAGS -DUNIV_LINUX";;
hpux10*)
	CFLAGS="$CFLAGS -DUNIV_MUST_NOT_INLINE -DUNIV_HPUX -DUNIV_HPUX10";;
hp*)
	CFLAGS="$CFLAGS -DUNIV_MUST_NOT_INLINE -DUNIV_HPUX";;
aix*)
	CFLAGS="$CFLAGS -DUNIV_AIX";;
irix*|osf*|sysv5uw7*|openbsd*)
	CFLAGS="$CFLAGS -DUNIV_MUST_NOT_INLINE";;
*solaris*|*SunOS*)
	CFLAGS="$CFLAGS -DUNIV_SOLARIS";;
esac

  # do not reindent the following lines, they are copied from InnoDB Plugin's
  # plug.in and this way it is easier for maintenance

  AC_MSG_CHECKING(whether GCC atomic builtins are available)
  # either define HAVE_IB_GCC_ATOMIC_BUILTINS or not
  AC_TRY_RUN(
    [
      int main()
      {
	long	x;
	long	y;
	long	res;
	char	c;

	x = 10;
	y = 123;
	res = __sync_bool_compare_and_swap(&x, x, y);
	if (!res || x != y) {
          return(1);
        }

	x = 10;
	y = 123;
	res = __sync_bool_compare_and_swap(&x, x + 1, y);
	if (res || x != 10) {
          return(1);
        }

	x = 10;
	y = 123;
	res = __sync_add_and_fetch(&x, y);
	if (res != 123 + 10 || x != 123 + 10) {
	  return(1);
	}

	c = 10;
	res = __sync_lock_test_and_set(&c, 123);
	if (res != 10 || c != 123) {
	  return(1);
	}

	return(0);
      }
    ],
    [
      AC_DEFINE([HAVE_IB_GCC_ATOMIC_BUILTINS], [1],
                [GCC atomic builtins are available])
      AC_MSG_RESULT(yes)
    ],
    [
      AC_MSG_RESULT(no)
    ]
  )

  AC_MSG_CHECKING(whether pthread_t can be used by GCC atomic builtins)
  # either define HAVE_IB_ATOMIC_PTHREAD_T_GCC or not
  AC_TRY_RUN(
    [
      #include <pthread.h>
      #include <string.h>

      int main(int argc, char** argv) {
        pthread_t       x1;
        pthread_t       x2;
        pthread_t       x3;

	memset(&x1, 0x0, sizeof(x1));
	memset(&x2, 0x0, sizeof(x2));
	memset(&x3, 0x0, sizeof(x3));

        __sync_bool_compare_and_swap(&x1, x2, x3);

        return(0);
      }
    ],
    [
      AC_DEFINE([HAVE_IB_ATOMIC_PTHREAD_T_GCC], [1],
                [pthread_t can be used by GCC atomic builtins])
      AC_MSG_RESULT(yes)
    ],
    [
      AC_MSG_RESULT(no)
    ]
  )

  AC_MSG_CHECKING(whether Solaris libc atomic functions are available)
  # either define HAVE_IB_SOLARIS_ATOMICS or not
  AC_CHECK_FUNCS(atomic_add_long \
		 atomic_cas_32 \
		 atomic_cas_64 \
		 atomic_cas_ulong,

		 AC_DEFINE([HAVE_IB_SOLARIS_ATOMICS], [1],
			   [Define to 1 if Solaris libc atomic functions \
			    are available])
  )

  AC_MSG_CHECKING(whether pthread_t can be used by Solaris libc atomic functions)
  # either define HAVE_IB_ATOMIC_PTHREAD_T_SOLARIS or not
  AC_TRY_RUN(
    [
      #include <pthread.h>
      #include <string.h>

      int main(int argc, char** argv) {
        pthread_t       x1;
        pthread_t       x2;
        pthread_t       x3;

        memset(&x1, 0x0, sizeof(x1));
        memset(&x2, 0x0, sizeof(x2));
        memset(&x3, 0x0, sizeof(x3));

        if (sizeof(pthread_t) == 4) {
        
          atomic_cas_32(&x1, x2, x3);
        
        } else if (sizeof(pthread_t) == 8) {
        
          atomic_cas_64(&x1, x2, x3);
        
        } else {
        
          return(1);
        }

	return(0);
      }
    ],
    [
      AC_DEFINE([HAVE_IB_ATOMIC_PTHREAD_T_SOLARIS], [1],
                [pthread_t can be used by solaris atomics])
      AC_MSG_RESULT(yes)
    ],
    [
      AC_MSG_RESULT(no)
    ]
  )

  # this is needed to know which one of atomic_cas_32() or atomic_cas_64()
  # to use in the source
  AC_CHECK_SIZEOF([pthread_t], [], [#include <pthread.h>])

  # Check for x86 PAUSE instruction
  AC_MSG_CHECKING(for x86 PAUSE instruction)
  # We have to actually try running the test program, because of a bug
  # in Solaris on x86_64, where it wrongly reports that PAUSE is not
  # supported when trying to run an application. See
  # http://bugs.opensolaris.org/bugdatabase/printableBug.do?bug_id=6478684
  # We use ib_ prefix to avoid collisoins if this code is added to
  # mysql's configure.in.
  AC_TRY_RUN(
    [
      int main() {
        __asm__ __volatile__ ("pause");
        return(0);
      }
    ],
    [
      AC_DEFINE([HAVE_IB_PAUSE_INSTRUCTION], [1], [Does x86 PAUSE instruction exist])
      AC_MSG_RESULT(yes)
    ],
    [
      AC_MSG_RESULT(no)
    ],
    [
      AC_MSG_RESULT(no)
    ]
  )
  # end of "do not reindent the following lines"

#
# The behavior of --with-atomic-ops must be:
# if this switch is not specified then
#   if gcc atomics are present, use them
#   else if solaris atomics are present, use them
#   else use innodb atomics
# else the switch is specified with some value,
#   check if the requested atomics are present
#   if present, use them
#   else emit error during ./configure
#

AC_ARG_WITH([atomic-ops],
	AC_HELP_STRING([--with-atomic-ops=      gcc_builtins|solaris|innodb],
	[Implement the atomic operations using GCC builtin atomic functions or \
	using the Solaris 10 atomic_ops(3C) from libc or InnoDB's own \
	implementation]))

case "$with_atomic_ops" in
"gcc_builtins") 
	AC_CHECK_DECLS(
		HAVE_IB_GCC_ATOMIC_BUILTINS,
		AC_MSG_NOTICE(will use the requested GCC atomic builtins),
		AC_MSG_ERROR(the requested GCC atomic builtins are not available)
	)
	AC_DEFINE([IB_ATOMIC_MODE_GCC_ATOMIC_BUILTINS], [1],
		[Use GCC builtin atomic functions])
       	;;
"solaris")
	AC_CHECK_DECLS(
		HAVE_IB_SOLARIS_ATOMICS,
		AC_MSG_NOTICE(will use the requested Solaris atomic functions),
		AC_MSG_ERROR(the requested Solaris atomic functions are not available)
	)
	AC_DEFINE([IB_ATOMIC_MODE_SOLARIS_ATOMICS], [1],
		[Use Solaris 10 atomic_ops(3C) from libc])
       	;;
"innodb")
	AC_MSG_NOTICE(will use the requested InnoDB's own implementation of mutexes and rw_locks)
	# this macro is not used but is here for clarity
	AC_DEFINE([IB_ATOMIC_MODE_INNODB], [1],
		[Use InnoDB's own implementation])
	;;
"")
	AC_CHECK_DECLS(
          HAVE_IB_GCC_ATOMIC_BUILTINS,
	  [
            AC_MSG_NOTICE(will use the GCC atomic builtins (auto selected));
            AC_DEFINE([IB_ATOMIC_MODE_GCC_ATOMIC_BUILTINS], [1],
                      [Use GCC builtin atomic functions])
          ],
          [
            AC_CHECK_DECLS(
              HAVE_IB_SOLARIS_ATOMICS,
              [
                AC_MSG_NOTICE(will use the Solaris atomic functions (auto selected));
                AC_DEFINE([IB_ATOMIC_MODE_SOLARIS_ATOMICS], [1],
                          [Use Solaris 10 atomic_ops(3C) from libc])
              ],
              AC_MSG_NOTICE(will use InnoDB's own implementation of mutexes and rw_locks (auto selected))
            )
          ]
	)

	;;
*)
	AC_MSG_ERROR(Unknown value "$with_atomic_ops" specified for --with-atomic-ops)
	;;
esac

# For large pages support
if test "$TARGET_LINUX" = "true"; then
	# For SHM_HUGETLB on Linux
	AC_CHECK_DECLS(SHM_HUGETLB, 
		AC_DEFINE([HAVE_LARGE_PAGES], [1], 
			[Define if you have large pages support])
		AC_DEFINE([HUGETLB_USE_PROC_MEMINFO], [1],
			[Define if /proc/meminfo shows the huge page size \
			 (Linux only)])
			, ,
			[
				#include <sys/shm.h>
			])
fi

AC_C_CONST
AC_C_INLINE
AC_C_VOLATILE
AC_CHECK_TYPES([int64_t, uint64_t, int32_t, uint32_t,
		int16_t, uint16_t, int8_t,  uint8_t,
		unsigned long long int, long long int], [],
		[AC_MSG_ERROR([Need the C99 integer types from stdint.h])],
		[#include <stdint.h>])
AC_STRUCT_TM
AC_TYPE_OFF_T
AC_TYPE_SIZE_T
AC_STRUCT_ST_RDEV

AC_CHECK_SIZEOF(void*, 4)
AC_CHECK_SIZEOF(char*, 4)
AC_CHECK_SIZEOF(short, 2)
AC_CHECK_SIZEOF(int, 4)

AC_CHECK_SIZEOF(long, 4)
AC_CHECK_SIZEOF(long long, 8)
# off_t is not a builtin type
AC_CHECK_SIZEOF(off_t, 4)

AC_CHECK_TYPES([off_t, ulong], [], [], [#include <sys/types.h>])
AC_CHECK_TYPES([size_t], [], [], [#include <stdio.h>])
AC_CHECK_TYPES([u_int32_t])

AC_CHECK_LIB(pthread, pthread_mutex_trylock)

# Required on Solaris
AC_CHECK_FUNC(sched_yield, , AC_CHECK_LIB(posix4, sched_yield))
AC_CHECK_FUNCS(strdup strlcpy)

#####
# Check whether the user wants to disable compressed tables functionality
#####
AC_ARG_ENABLE(compression,
	[  --disable-compression   disable compressed tables support],
	[   ],
	enable_compression=yes)
if test $enable_compression = yes; then
	# Check for ZLib functions
	AC_CHECK_FUNC(deflate, ,
	       	AC_CHECK_LIB(z, deflate, ,
			AC_MSG_ERROR([Zlib is required for compression to work])))

	AC_CHECK_FUNC(inflate, ,
		AC_CHECK_LIB(z, inflate, ,
			AC_MSG_ERROR([Zlib is required for compression to work])))
	AC_CHECK_HEADERS(zlib.h)
	AC_DEFINE([WITH_ZIP], [1], [Compressed tables support])
fi
AM_CONDITIONAL(WITH_ZIP, test $enable_compression = yes)
# End Zip tests

#####
# Check whether the user wants to disable XA functionality
#####
AC_ARG_ENABLE(xa,
	[  --disable-xa            disable XA support],
	[   ],
	enable_xa=yes
	AC_DEFINE([WITH_XOPEN], [1], [XA support]))
AM_CONDITIONAL(WITH_XOPEN, test $enable_xa = yes)

AC_CHECK_FUNCS( \
	bcmp \
	fcntl \
	finite \
	fsync \
	ftruncate \
	getcwd \
	getrusage \
	index \
	localtime_r \
	locking \
	memcpy \
	memmove \
	perror \
	pread \
	mmap \
	getpagesize \
	pthread_attr_setstacksize \
	pthread_barrier_destroy \
	pthread_barrier_init \
	pthread_barrier_wait \
	pthread_setprio \
	rename \
	rint \
	shmget \
	shmat \
	shmdt \
	shmctl \
	sleep \
	snprintf \
	stpcpy \
	strcasecmp \
	strerror \
	strstr \
	strtoul \
	tell)

AM_CONDITIONAL([HAVE_PTHREAD_BARRIER], [test "$HAVE_PTHREAD_BARRIER_INIT" = yes])

AM_CPPFLAGS="${AM_CPPFLAGS} -I\$(top_srcdir)/include -I\$(top_builddir)/include"
AM_CFLAGS="${AM_CFLAGS} ${NO_WERROR}"

AC_CONFIG_FILES([ Makefile haildb.spec ])

AC_OUTPUT

