#ifndef _MT_BASE_H
#define _MT_BASE_H

/*********************************************************************
Open a table and return a cursor for the table. */
ib_err_t
open_table(
/*=======*/
	const char*	dbname,		/*!< in: database name */
	const char*	name,		/*!< in: table name */
	ib_trx_t	ib_trx,		/*!< in: transaction */
	ib_crsr_t*	crsr);		/*!< out: innodb cursor */

#endif /* _MT_BASE_H */
