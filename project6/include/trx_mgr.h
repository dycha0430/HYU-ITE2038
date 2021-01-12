#ifndef __TRX_MGR_H__
#define __TRX_MGR_H__

#include <stdlib.h>
#include <pthread.h>
#include "lock_table.h"
#include "uthash.h"
#include "log_mgr.h"

typedef int64_t LSN_t;

int global_trx_id;
pthread_mutex_t trx_table_latch;

typedef struct updateRecord_t{
    int table_id;
    int64_t key;  
    char* old_val;
    LSN_t prevLSN;
    struct updateRecord_t* next;
} updateRecord_t;

typedef struct trx_t {
    int trx_id;
    struct lock_t* next;
    int detect;
    int aborted;
    pthread_mutex_t trx_latch;
    LSN_t lastLSN;
    struct updateRecord_t* header;
    UT_hash_handle hh;
} trx_t;

trx_t *trx_table;

void init_trx_table(void);
int trx_begin(void);
int trx_commit(int trx_id);
void trx_abort(int trx_id);
void trx_lock_release(int trx_id);

#endif /* __TRX_MGR_H__ */
