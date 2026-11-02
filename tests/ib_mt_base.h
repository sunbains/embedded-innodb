/** Copyright (c) 2024 Sunny Bains. All rights reserved. */

#pragma once

/**
Open a table and return a cursor for the table. */
DbErr open_table(const char *dbname, /*!< in: database name */
                    const char *name,   /*!< in: table name */
                    Trx * ib_trx,    /*!< in: transaction */
                    Cursor **crsr);   /*!< out: innodb cursor */
