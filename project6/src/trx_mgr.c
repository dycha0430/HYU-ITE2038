#include "trx_mgr.h"

void init_trx_table(void){
    global_trx_id = 0;
    trx_table = NULL;
    trx_table_latch = (pthread_mutex_t)PTHREAD_MUTEX_INITIALIZER;
}

int trx_begin(void){
    LogRecord_t *logRecord;
    trx_t *trx;
    trx = (trx_t*)malloc(sizeof(trx_t));
    if (trx == NULL) return 0;
    
    pthread_mutex_lock(&trx_table_latch);
    trx->trx_id = ++global_trx_id;

    if (trx->trx_id < 0){
    	global_trx_id = 0;
	trx->trx_id = ++global_trx_id;
    }

    trx->next = NULL;
    trx->detect = 0;
    trx->aborted = 0;
    trx->lastLSN = -1;
    trx->header = (updateRecord_t*)malloc(sizeof(updateRecord_t));
    trx->header->next = NULL;
    trx->trx_latch = (pthread_mutex_t)PTHREAD_MUTEX_INITIALIZER;
    HASH_ADD(hh, trx_table, trx_id, sizeof(int), trx);

    pthread_mutex_unlock(&trx_table_latch);

    pthread_mutex_lock(&log_latch);
    logRecord = make_Olog(trx->trx_id, BEGIN);
    add_log(logRecord);
    pthread_mutex_unlock(&log_latch);

    return trx->trx_id;
}

int trx_commit(int trx_id){
    LogRecord_t* logRecord;
    
    pthread_mutex_lock(&log_latch);
    logRecord = make_Olog(trx_id, COMMIT);
    add_log(logRecord);
    pthread_mutex_unlock(&log_latch);

    // Log buffer flush before commit (lock release)
    flush_log();

    trx_lock_release(trx_id);
    return trx_id;
}

void trx_abort(int trx_id){
    trx_t l, *p;
    LogRecord_t* logRecord;
    updateRecord_t* updateRecord, *tmp;
    memset(&l, 0, sizeof(trx_t));
    l.trx_id = trx_id;

    pthread_mutex_lock(&trx_table_latch);
    HASH_FIND(hh, trx_table, &l.trx_id, sizeof(int), p);
    pthread_mutex_unlock(&trx_table_latch);
    if (!p) return;

    // Rollback all update
    updateRecord = p->header->next;
    while(updateRecord != NULL){
	update(updateRecord->table_id, updateRecord->key, updateRecord->old_val, trx_id, true, updateRecord->prevLSN);
	tmp = updateRecord;
	updateRecord = updateRecord->next;
	free(tmp->old_val);
	free(tmp);
    }
   
    // Add rollback record in Log buffer
    pthread_mutex_lock(&log_latch); 
    logRecord = make_Olog(trx_id, ROLLBACK);
    add_log(logRecord);
    pthread_mutex_unlock(&log_latch);

    trx_lock_release(trx_id);
}

void trx_lock_release(int trx_id){
    trx_t l, *p;
    lock_t *lock_obj;
    memset(&l, 0, sizeof(trx_t));
    l.trx_id = trx_id;

    pthread_mutex_lock(&lock_table_latch);
    pthread_mutex_lock(&trx_table_latch);
    HASH_FIND(hh, trx_table, &l.trx_id, sizeof(int), p);

    lock_obj = p->next;

    while(lock_obj != NULL){
        lock_release(lock_obj);
        lock_obj = lock_obj->trx_next_lock;
    }

    HASH_DEL(trx_table, p);
    pthread_mutex_unlock(&trx_table_latch);
    pthread_mutex_unlock(&lock_table_latch);
    free(p);
}
