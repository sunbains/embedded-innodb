#!/bin/bash
#
# Copyright (c) 2006, 2009, Innobase Oy. All Rights Reserved.
#
# This program is free software; you can redistribute it and/or modify it under
# the terms of the GNU General Public License as published by the Free Software
# Foundation; version 2 of the License.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along with
# this program; if not, write to the Free Software Foundation, Inc., 59 Temple
# Place, Suite 330, Boston, MA 02111-1307 USA
#
# Generate the parser files (pars0grm.cc, ../include/pars0grm.h) from
# pars0grm.y. Run from the pars/ directory.

set -eu

OUTFILE=pars0grm.cc
TMPFILE=${OUTFILE}.tab.cc

bison -d pars0grm.y -o "${TMPFILE}"

mv "${OUTFILE}.tab.hh" ../include/pars0grm.h

sed -e "s/${TMPFILE}/${OUTFILE}/; s/${OUTFILE}.tab.hh/pars0grm.h/" < "${TMPFILE}" > "${OUTFILE}"

rm "${TMPFILE}"
