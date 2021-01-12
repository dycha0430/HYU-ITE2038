#include <lock_table.h>

/* Initialize lock table. */
int init_lock_table()
{
	lock_table_latch = (pthread_mutex_t)PTHREAD_MUTEX_INITIALIZER;
	hash_map = NULL;

	return 0;
}

/* Try to acquire the record lock. 
 * Allocate and append a new lock object to the lock list of the record having the key.
 * Set ret_lock with the address of the new lock object if no deadlock have occured.
 * Instead of releasing the transaction latch in this function, return status value.
 * Return value: 0 (ACQUIRED), 1 (NEED_TO_WAIT), 2 (DEADLOCK)
 */

int lock_acquire(int table_id, int64_t key, int trx_id, 
		int lock_mode, lock_t **ret_lock)
{
	pthread_mutex_lock(&lock_table_latch);
	
	hash_entry_t l, *p = NULL, *r = NULL;
	lock_t *lock, *traverse;
	trx_t tl, *tp;
	lock = (lock_t*)malloc(sizeof(lock_t));

	memset(&l, 0, sizeof(hash_entry_t));
	l.hash_key.key = key;
	l.hash_key.table_id = table_id;
	
	HASH_FIND(hh, hash_map, &l.hash_key, sizeof(hash_key_t), p);

	/* Case: Lock entry in hash table is null.
	 * (There is no lock associated with the key.)
	 * Acquire the lock immediately. 
	 */

	if (!p) {
		r = (hash_entry_t*)malloc(sizeof *r);
		memset(r, 0, sizeof *r);
		r->hash_key.key = key;
		r->hash_key.table_id = table_id;
		r->head = (lock_t*)malloc(sizeof(lock_t));
		r->tail = (lock_t*)malloc(sizeof(lock_t));
		
		lock->prev = r->head;
		lock->next = r->tail;
		lock->hash_entry = r;
		lock->lock_mode = lock_mode;
		lock->trx_id = trx_id;
		lock->status = ACQUIRED;
		lock->old_val = NULL;

		memset(&tl, 0, sizeof(trx_t));
		tl.trx_id = trx_id;

		pthread_mutex_lock(&trx_table_latch);
		HASH_FIND(hh, trx_table, &tl.trx_id, sizeof(int), tp);
		
		/* Attach the lock to the corresponding transaction table. */
		lock->trx_next_lock = tp->next;
		tp->next = lock;
		pthread_mutex_unlock(&trx_table_latch);
		
		lock->cond = (pthread_cond_t) PTHREAD_COND_INITIALIZER;
		r->head->next = lock;
		r->head->prev = NULL;
		r->tail->prev = lock;
		r->tail->next = NULL;
		
		HASH_ADD(hh, hash_map, hash_key, sizeof(hash_key_t), r);
		*ret_lock = lock;
		
	    pthread_mutex_unlock(&lock_table_latch);
		
		return LOCK_ACQUIRED;
	} 
	
	/* Case: One or more locks are attached to the key.
	 * Need to determine whether to acquire the lock. 
	 */

	else {
		lock->prev = p->tail->prev;
		lock->next = p->tail;
		lock->hash_entry = p;
		lock->cond = (pthread_cond_t) PTHREAD_COND_INITIALIZER;
		lock->lock_mode = lock_mode;
		lock->trx_id = trx_id;
		lock->old_val = NULL;

		traverse = p->head->next;
		int other = 0, sx_signal = 0;
		lock_t* tmp_lock = NULL;

		/* Case: Same transaction already has a lock of the same record */

		/* Traverse all acquired locks in the lock list.
		 * If the exclustive lock of this transaction exists
		 * or lock to acquire is a shared lock, 
		 * Put the existing lock in ret_lock. (Instead of attaching new lock.)
		 */

		while (traverse != p->tail){
		    if (traverse->status == WAITING) break;
		    if (traverse->trx_id == trx_id){
				tmp_lock = traverse;
		    	if (lock_mode == SHARED || traverse->lock_mode == EXCLUSIVE){
					free(lock);
					pthread_mutex_unlock(&lock_table_latch);
					*ret_lock = traverse;
		            return LOCK_ACQUIRED;
				} else sx_signal = 1;
		    } else other = 1;
		    traverse = traverse->next;
		}

		/* If the shared lock of this transaction exists
		 * and lock to acquire is an exclusive lock,
		 * change existing lock from shared to exclusive
		 * put the existing lock in ret_lock.
		 */

		if (sx_signal && !other){
		    free(lock);
		    tmp_lock->lock_mode = EXCLUSIVE;
		    pthread_mutex_unlock(&lock_table_latch);
		    *ret_lock = tmp_lock;
	
		    return LOCK_ACQUIRED;
		}

		/* Case: There is no lock of the same transaction to the record. 
		 * If one or more waiting lock exist, need to wait.
		 */
		if (lock->prev->status == WAITING) lock->status = WAITING;
		else {
		    traverse = p->head->next;
		    if (lock_mode == SHARED){
				/* If lock to acquire is shared mode and there is no exclusive lock, 
				 * can acquire the lock immediately.
				 */
				lock->status = ACQUIRED;
		    	while(traverse != p->tail){
					/* If lock to acquire is shared mode and there exists exclusive lock,
					 * need to wait.
					 */
					if (traverse->lock_mode == EXCLUSIVE && traverse->trx_id != trx_id) {
						lock->status = WAITING;
						break;
					}
					traverse = traverse->next;
				}
		    } else {
		    	lock->status = ACQUIRED;
				while(traverse != p->tail){
					/* If lock to acquire is exclusive mode
					 * and there exists any lock that is not in this transaction, 
					 * need to wait.
					 */
					if (traverse->trx_id != trx_id){
			    		lock->status = WAITING;
						break;
					}
					traverse = traverse->next;
				}
		    }
		}
		
		/* Attach the lock to lock table */
		p->tail->prev->next = lock;
		p->tail->prev = lock;

		memset(&tl, 0, sizeof(trx_t));
        tl.trx_id = trx_id;

		if(lock->status == ACQUIRED){
		    pthread_mutex_lock(&trx_table_latch);
            HASH_FIND(hh, trx_table, &tl.trx_id, sizeof(int), tp);
		    
		    lock->trx_next_lock = tp->next;
            tp->next = lock;

            pthread_mutex_unlock(&trx_table_latch);
		    pthread_mutex_unlock(&lock_table_latch);
		    *ret_lock = lock;

		    return LOCK_ACQUIRED;
		} else {
			/* If waiting is required to acquire the lock, 
			 * detect deadlock before waiting.
			 */

		    pthread_mutex_lock(&trx_table_latch);
		    int ret = deadlock_detect(lock, trx_id);
		    
		    trx_t *s, *tmp;
		    HASH_ITER(hh, trx_table, s, tmp){
		    	s->detect = 0;
		    }
		    
		    HASH_FIND(hh, trx_table, &tl.trx_id, sizeof(int), tp);
		    
			/* Case: Deadlock is detected.
			 * Set abort bit of the transaction and return.
			 */
		    if (ret){
		    	p->tail->prev = lock->prev;
				p->tail->prev->next = lock->next;
				free(lock);
				tp->aborted = 1;
				pthread_mutex_unlock(&lock_table_latch);
				pthread_mutex_unlock(&trx_table_latch);
                return DEADLOCK;
		    }
		    
			/* Case: Deadlock is not detected and transaction need to wait. */
		    lock->trx_next_lock = tp->next;
		    tp->next = lock;
		    
		    lock->trx = tp;
		    
		    pthread_mutex_lock(&tp->trx_latch);
		    pthread_mutex_unlock(&lock_table_latch);
		    pthread_mutex_unlock(&trx_table_latch);
		    
		    *ret_lock = lock;
		    
		    return NEED_TO_WAIT;
		}
	}
}

/* Sleep on the condition variable of the lock_obj, 
 * atomically releasing the transaction latch
 */

void lock_wait(lock_t* lock_obj){
    pthread_cond_wait(&lock_obj->cond, &lock_obj->trx->trx_latch);
    pthread_mutex_unlock(&lock_obj->trx->trx_latch);
    lock_obj->status = ACQUIRED;
}

/* Remove the lock_obj from the lock list.
 * If there is a successor¡¯s lock waiting for the transaction releasing the lock, wake up the successor.
 * If success, return 0. Otherwise, return non-zero value.
 */

int lock_release(lock_t* lock_obj)
{
	hash_entry_t *hash_entry;
	hash_entry = lock_obj->hash_entry;
	lock_t *traverse;
	
	/* Case: lock_obj is the only lock linked to this lock list. 
	 * Remove hash table entry of corresponding record. 
	 */
	if (lock_obj->next == hash_entry->tail && lock_obj->prev == hash_entry->head){
	    HASH_DEL(hash_map, hash_entry);
	    free(hash_entry->head);
	    free(hash_entry->tail);
	    free(hash_entry);
	} 
	/* Case: there are other locks. */
	else {
		// Remove the lock_obj from lock list.
	    lock_obj->prev->next = lock_obj->next;
	    lock_obj->next->prev = lock_obj->prev;
		
		/* If lock_obj is the only acquired lock in the lock list, 
		 * wakes up the waiting locks in sequence as much as possible.
		 * Else, return without having to wake up waiting locks.
		 */
	    if (lock_obj->next->status == WAITING && lock_obj->prev == hash_entry->head){
			// Wake up the next lock.
			pthread_mutex_lock(&lock_obj->next->trx->trx_latch);
	        pthread_cond_signal(&lock_obj->next->cond);
			pthread_mutex_unlock(&lock_obj->next->trx->trx_latch);
		
			traverse = lock_obj->next->next;
			/* If the lock just woke up is shared mode, check subsequent locks. 
			 * Else, there is no need to check subsequent locks.
			 */
			if (lock_obj->next->lock_mode == SHARED) {
				while (traverse != hash_entry->tail){
					// If the iteration lock is shared mode, wake up the lock.
					if (traverse->lock_mode == SHARED) {
						pthread_cond_signal(&traverse->cond);
					}
					else break; // Else, terminate waking up.
					traverse = traverse->next;
				}
			}
	    }
	}

	free(lock_obj);
	return 0;
}


/* Detects whether or not deadlock will occur 
 * when the lock is attached by recursive call. 
 * detect_trx_id is id of transaction to acquire the lock.
 * If deadlock detected, return 1
 * else return 0
 */

int deadlock_detect(lock_t* waiting_lock, int detect_trx_id){
    trx_t l, *p;
    int ret;
    hash_entry_t *hash_entry = waiting_lock->hash_entry;
    lock_t *traverse, *in_traverse, *last_traverse;
    traverse = hash_entry->head->next;
    
    last_traverse = waiting_lock;

	/* Last lock to traverse is the last exclusive lock before waiting_lock. */
    while (traverse != waiting_lock->next){
    	if (traverse->lock_mode == EXCLUSIVE) last_traverse = traverse;
		traverse = traverse->next;
    }

	/* If it is not a first call of the function, 
	 * traverse locks including last_traverse. 
	 */
    if (last_traverse != waiting_lock || waiting_lock->trx_id != detect_trx_id)
	    last_traverse = last_traverse->next;

    traverse = hash_entry->head->next;

	/* Deadlock is detected if there is a transaction with detect_trx_id
	 * while traversing the lock list with waiting lock before last_traverse.
	 * If the transaction of the traversing lock is not of detect_trx_id, 
	 * recursively call the deadlock_detect function for waiting locks of the
	 * transaction table of the corresponding transaction.
	 * Whenever traversing the transaction table, transaction's detect bit is set
	 * and that transaction is not traversed again.
	 */
    while (traverse != last_traverse){
		if (traverse->trx_id == detect_trx_id) return 1;
        memset(&l, 0, sizeof(trx_t));
        l.trx_id = traverse->trx_id;

        HASH_FIND(hh, trx_table, &l.trx_id, sizeof(int), p);

		if (p->next != NULL && p->detect == 0){
			p->detect = 1;
			in_traverse = p->next;
			while (in_traverse != NULL && in_traverse->status == WAITING && in_traverse != traverse){
	    		ret = deadlock_detect(in_traverse, detect_trx_id);
				if (ret == 1) {
					return 1;
				}
				in_traverse = in_traverse->trx_next_lock;
			}
		}

	traverse = traverse->next;
    }

    return 0;
}
