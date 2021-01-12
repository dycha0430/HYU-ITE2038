#ifndef __TRX_MGR_H__
#define __TRX_MGR_H__

#include <stdlib.h>
#include <pthread.h>
#include "lock_table.h"
#include "uthash.h"
#include "db_api.h"

static int global_trx_id = 0;
pthread_mutex_t trx_table_latch;
typedef struct trx_t {
    int trx_id;
    struct lock_t* next;
    int detect;
    int aborted;
    pthread_mutex_t trx_latch;
    UT_hash_handle hh;
} trx_t;

trx_t *trx_table;

void init_trx_table(void);
int trx_begin(void);
int trx_commit(int trx_id);
void trx_abort(int trx_id);

#endif /* __TRX_MGR_H__ */
