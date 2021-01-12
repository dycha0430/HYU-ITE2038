#ifndef __DB_API_H__
#define __DB_API_H__

#include <stdio.h>
#include <stdlib.h>
#include "bpt.h"

static int unique_table_id = 0;
extern int fd;

int open_table (char *pathname);
int db_insert (int64_t key, char *value);
int db_find (int64_t key, char *ret_val);
int db_delete(int64_t key);

#endif
