#ifndef __DB_API_H__
#define __DB_API_H__

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "bpt.h"

int init_db (int num_buf);
int open_table (char *pathname);
int db_insert (int table_id, int64_t key, char *value);
int db_find (int table_id, int64_t key, char *ret_val);
int db_delete(int table_id, int64_t key);
int close_table(int table_id);
int shutdown_db();

#endif
