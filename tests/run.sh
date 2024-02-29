#!/ bin / sh
#
#Drizzle Client Library
#
#Copyright(C) 2008 Eric Day(eday @oddments.org)
#All rights reserved.
#
#Use and distribution licensed under the BSD license.See
#the COPYING file in this directory for full text.
#

#Get filename we want to run without path
name=`echo $1 | sed 's/.*\/\([^\/]*\)$/\1/'`

ext=`echo $name | sed 's/.*\.\([^.]*$\)/\1/'`
if [ "x$ext" = "x$name" ]
then
  ext=""
fi

if [ ! "x$ext" = "xsh" ]
then
  libtool_prefix="libtool --mode=execute"
fi

#Set prefix if it was given through environment
if [ -n "$LIBDRIZZLE_TEST_PREFIX" ]
then
  if [ -n "$LIBDRIZZLE_TEST_FILTER" ]
  then
#If filter variable is set, only apply prefix to those that match
    for x in $LIBDRIZZLE_TEST_FILTER
    do
      if [ "x$x" = "x$name" ]
      then
        prefix="$libtool_prefix $LIBDRIZZLE_TEST_PREFIX"
        with=" (with prefix after filter)"
        break
      fi
    done
  else
    prefix="$libtool_prefix $LIBDRIZZLE_TEST_PREFIX"
    with=" (with prefix)"
  fi
fi

#Set this to fix broken libtool test
ECHO=`which echo`
export ECHO

echo
echo "RUN: $name$with"

(cd tests && $prefix ./$name)
