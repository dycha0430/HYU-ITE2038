#include <lock_table.h>

int init_lock_table()
{
	lock_table_latch = (pthread_mutex_t)PTHREAD_MUTEX_INITIALIZER;
	hash_map = NULL;

	return 0;
}

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

		memset(&tl, 0, sizeof(trx_t));
		tl.trx_id = trx_id;

		pthread_mutex_lock(&trx_table_latch);
		HASH_FIND(hh, trx_table, &tl.trx_id, sizeof(int), tp);
		
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
	} else {
		lock->prev = p->tail->prev;
		lock->next = p->tail;
		lock->hash_entry = p;
		lock->cond = (pthread_cond_t) PTHREAD_COND_INITIALIZER;
		lock->lock_mode = lock_mode;
		lock->trx_id = trx_id;

		traverse = p->head->next;
		int other = 0, sx_signal = 0;
		lock_t* tmp_lock = NULL;
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

		if (sx_signal && !other){
		    free(lock);
		    tmp_lock->lock_mode = EXCLUSIVE;
		    pthread_mutex_unlock(&lock_table_latch);
		    *ret_lock = tmp_lock;
	
		    return LOCK_ACQUIRED;
		}

		if (lock->prev->status == WAITING) lock->status = WAITING;
		else {
		    traverse = p->head->next;
		    if (lock_mode == SHARED){
			lock->status = ACQUIRED;
		    	while(traverse != p->tail){
			    if (traverse->lock_mode == EXCLUSIVE && traverse->trx_id != trx_id) {
			        lock->status = WAITING;
				break;
			    }
			    traverse = traverse->next;
			}
		    } else {
		    	lock->status = ACQUIRED;
			while(traverse != p->tail){
			    if (traverse->trx_id != trx_id){
			    	lock->status = WAITING;
				break;
			    }
			    traverse = traverse->next;
			}
		    }
		}
		
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
		    // Deadlock detection
		    pthread_mutex_lock(&trx_table_latch);
		    int ret = deadlock_detect(lock, trx_id);
		    
		    trx_t *s, *tmp;
		    HASH_ITER(hh, trx_table, s, tmp){
		    	s->detect = 0;
		    }
		    
		    HASH_FIND(hh, trx_table, &tl.trx_id, sizeof(int), tp);
		    // Deadlock detected
		    if (ret){
		    	p->tail->prev = lock->prev;
			p->tail->prev->next = lock->next;
			free(lock);
			tp->aborted = 1;
                        pthread_mutex_unlock(&lock_table_latch);
			pthread_mutex_unlock(&trx_table_latch);
                        return DEADLOCK;
		    }
		    
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

void lock_wait(lock_t* lock_obj){
    pthread_cond_wait(&lock_obj->cond, &lock_obj->trx->trx_latch);
    pthread_mutex_unlock(&lock_obj->trx->trx_latch);
    lock_obj->status = ACQUIRED;
}

int lock_release(lock_t* lock_obj)
{
	hash_entry_t *hash_entry;
	hash_entry = lock_obj->hash_entry;
	lock_t *traverse;
	
	if (lock_obj->next == hash_entry->tail && lock_obj->prev == hash_entry->head){
	    HASH_DEL(hash_map, hash_entry);
	    free(hash_entry->head);
	    free(hash_entry->tail);
	    free(hash_entry);
	} else {
	    lock_obj->prev->next = lock_obj->next;
	    lock_obj->next->prev = lock_obj->prev;
   
	    if (lock_obj->next->status == WAITING && lock_obj->prev == hash_entry->head){
		pthread_mutex_lock(&lock_obj->next->trx->trx_latch);
	        pthread_cond_signal(&lock_obj->next->cond);
		pthread_mutex_unlock(&lock_obj->next->trx->trx_latch);
		
		traverse = lock_obj->next->next;
		if (lock_obj->next->lock_mode == SHARED) {
		    int signal = -1;
		    while (traverse != hash_entry->tail){
			if (traverse->lock_mode == EXCLUSIVE) {
			    if (traverse->trx_id != lock_obj->next->trx_id)
				break;
			    else if (traverse->lock_mode == EXCLUSIVE){
				if (signal != 1){
				    pthread_mutex_lock(&traverse->trx->trx_latch);
			    	    pthread_cond_signal(&traverse->cond);
				    pthread_mutex_unlock(&traverse->trx->trx_latch);
				    signal = 0;
				} else break;
			    }
			} else{
			    if (signal != 0){
				pthread_cond_signal(&traverse->cond);
				if (traverse->trx_id != lock_obj->next->trx_id)
				    signal = 1;
			    } else {
				if (traverse->trx_id == lock_obj->next->trx_id){
				    pthread_mutex_lock(&traverse->trx->trx_latch);
				    pthread_cond_signal(&traverse->cond);
				    pthread_mutex_unlock(&traverse->trx->trx_latch);
				}
				else break;
			    }
			}
			traverse = traverse->next;
		    }
		}
	    }
	}

	free(lock_obj);
	return 0;
}


// If deadlock detected, return 1
// else return 0
int deadlock_detect(lock_t* waiting_lock, int detect_trx_id){
    trx_t l, *p;
    int ret;
    hash_entry_t *hash_entry = waiting_lock->hash_entry;
    lock_t *traverse, *in_traverse, *last_traverse;
    traverse = hash_entry->head->next;
    
    last_traverse = waiting_lock;

    while (traverse != waiting_lock->next){
    	if (traverse->lock_mode == EXCLUSIVE) last_traverse = traverse;
	traverse = traverse->next;
    }

    if (last_traverse != waiting_lock || waiting_lock->trx_id != detect_trx_id)
	    last_traverse = last_traverse->next;

    traverse = hash_entry->head->next;

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
