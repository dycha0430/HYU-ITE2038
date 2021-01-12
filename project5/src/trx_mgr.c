#include "trx_mgr.h"

/* Initialize transaction table. */
void init_trx_table(void){
    global_trx_id = 0;
    trx_table = NULL;
    trx_table_latch = (pthread_mutex_t)PTHREAD_MUTEX_INITIALIZER;
}

/* Create a transaction and add it to a transaction table.
 * Return a transaction id.
 */
int trx_begin(void){
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
    trx->trx_latch = (pthread_mutex_t)PTHREAD_MUTEX_INITIALIZER;
    HASH_ADD(hh, trx_table, trx_id, sizeof(int), trx);
    pthread_mutex_unlock(&trx_table_latch);

    return trx->trx_id;
}

/* Release every lock acquired by this transaction
 * and remove the transaction from the transaction table.
 */
int trx_commit(int trx_id){
    trx_t l, *p;
    lock_t *lock_obj;
    memset(&l, 0, sizeof(trx_t));
    l.trx_id = trx_id;

    pthread_mutex_lock(&lock_table_latch);
    pthread_mutex_lock(&trx_table_latch);
    HASH_FIND(hh, trx_table, &l.trx_id, sizeof(int), p);

    if (!p) {
		pthread_mutex_unlock(&lock_table_latch);
		pthread_mutex_unlock(&trx_table_latch);
		return 0;
    }

    lock_obj = p->next;
    
    while(lock_obj != NULL){
    	lock_release(lock_obj);
		lock_obj = lock_obj->trx_next_lock;
    }

    HASH_DEL(trx_table, p); 
    pthread_mutex_unlock(&trx_table_latch);
    pthread_mutex_unlock(&lock_table_latch);
    free(p);
    
    return trx_id;
}

/* Abort the transaction. 
 * Rollback all updates performed by this transaction. 
 */
void trx_abort(int trx_id){
    trx_t l, *p;
    lock_t *lock_obj;
    memset(&l, 0, sizeof(trx_t));
    l.trx_id = trx_id;

    pthread_mutex_lock(&trx_table_latch);
    HASH_FIND(hh, trx_table, &l.trx_id, sizeof(int), p);
    pthread_mutex_unlock(&trx_table_latch);
    if (!p) return;

    lock_obj = p->next;
    while(lock_obj != NULL){
    	if (lock_obj->lock_mode == EXCLUSIVE){
			int ret = update(lock_obj->hash_entry->hash_key.table_id, lock_obj->hash_entry->hash_key.key, lock_obj->old_val, trx_id);
		}
		lock_obj = lock_obj->trx_next_lock;
    }

    trx_commit(trx_id);
}
