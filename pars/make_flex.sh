#!/ bin / bash
#
#Copyright(c) 2006, 2009, Innobase Oy.All Rights Reserved.
#
#This program is free software; you can redistribute it and / or modify it under
#the terms of the GNU General Public License as published by the Free Software
#Foundation; version 2 of the License.
#
#This program is distributed in the hope that it will be useful, but WITHOUT
#ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
#FOR A PARTICULAR PURPOSE.See the GNU General Public License for more details.
#
#You should have received a copy of the GNU General Public License along with
#this program; if not, write to the Free Software Foundation, Inc., 59 Temple
#Place, Suite 330, Boston, MA 02111 - 1307 USA
#
#generate lexer files from flex input files.

set - eu

          TMPFILE = _flex_tmp.cc OUTFILE = lexyy.cc

                                               rm -
                                           f ${OUTFILE}

                                           flex -
                                           o ${TMPFILE} pars0lex.l

                                               sed -
                                           e ' s / '"$TMPFILE"' / '"$OUTFILE"' /
    ;
s /\(int offset = \)\((yy_c_buf_p) - (yytext_ptr)\);
/\1(int)(\2);
/ ;
s /\(void yy\(restart\| _\(delete\| flush\) _buffer\)\) / static \1 / ;
s /\(void yy_switch_to_buffer\) / MY_ATTRIBUTE((unused)) static \1 / ;
s /\(void yy\(push\| pop\) _buffer_state\) / MY_ATTRIBUTE((unused)) static \1 /
    ;
s /\(YY_BUFFER_STATE yy_create_buffer\) / static \1 / ;
s /\(\(int\| void\) yy[gs] et_\) / MY_ATTRIBUTE((unused)) static \1 / ;
s/\(void \*\?yy\(\(re\)\?alloc\|free\)\)/static \1/;
s /\(extern \)\?\(int yy\(leng\| lineno\| _flex_debug\)\) / static \2 / ;
s /\(int yylex_destroy\) / MY_ATTRIBUTE((unused)) static \1 / ;
s /\(extern \)\?\(int yylex \) / UNIV_INTERN \2 / ;
s / ^\(\(FILE\| char\) *\* *yyget\) / MY_ATTRIBUTE((unused)) static \1 / ;
s / ^\(extern \)\?\(\(FILE\| char\) *\* *yy\) / static \2 / ;
' < $TMPFILE >> $OUTFILE

    rm $TMPFILE
