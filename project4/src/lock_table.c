#include <lock_table.h>

typedef struct lock_t {
	struct lock_t* prev;
	struct lock_t* next;
	hash_entry_t* hash_entry;
	pthread_cond_t cond;
} lock_t;

typedef struct hash_key_t {
	int table_id;
	int64_t key;
} hash_key_t;

typedef struct hash_entry_t {
	hash_key_t hash_key;
	lock_t* head;
	lock_t* tail;
	UT_hash_handle hh;
} hash_entry_t;

int init_lock_table()
{
	lock_table_latch = (pthread_mutex_t)PTHREAD_MUTEX_INITIALIZER;
	hash_map = NULL;

	return 0;
}

lock_t* lock_acquire(int table_id, int64_t key)
{
	pthread_mutex_lock(&lock_table_latch);
	
	hash_entry_t l, *p, *r;
	lock_t *lock;
	lock = (lock_t*)malloc(sizeof(lock_t));
	if (lock == NULL) return NULL;

	memset(&l, 0, sizeof(hash_entry_t));
	l.hash_key.key = key;
	l.hash_key.table_id = table_id;

	HASH_FIND(hh, hash_map, &l.hash_key, sizeof(hash_key_t), p);
	if (!p) {
		r = (hash_entry_t*)malloc(sizeof *r);
		if (r == NULL) return NULL;
		memset(r, 0, sizeof *r);
		r->hash_key.key = key;
		r->hash_key.table_id = table_id;
		r->head = (lock_t*)malloc(sizeof(lock_t));
		r->tail = (lock_t*)malloc(sizeof(lock_t));
		
		lock->prev = r->head;
		lock->next = r->tail;
		lock->hash_entry = r;
		
		lock->cond = (pthread_cond_t) PTHREAD_COND_INITIALIZER;
		r->head->next = lock;
		r->tail->prev = lock;
		
		HASH_ADD(hh, hash_map, hash_key, sizeof(hash_key_t), r);
	} else {
		lock->prev = p->tail->prev;
		lock->next = p->tail;
		lock->hash_entry = p;
		lock->cond = (pthread_cond_t) PTHREAD_COND_INITIALIZER;
		
		p->tail->prev->next = lock;
		p->tail->prev = lock;
		
		HASH_DEL(hash_map, p);
		HASH_ADD(hh, hash_map, hash_key, sizeof(hash_key_t), p);
		pthread_cond_wait(&lock->cond, &lock_table_latch);
	}

	pthread_mutex_unlock(&lock_table_latch);

	return lock;
}

int lock_release(lock_t* lock_obj)
{
	hash_entry_t *hash_entry;
	pthread_mutex_lock(&lock_table_latch);
	hash_entry = lock_obj->hash_entry;
	
	if (lock_obj->next == hash_entry->tail){
		HASH_DEL(hash_map, hash_entry);
		free(hash_entry->head);
		free(hash_entry->tail);
		free(hash_entry);
	} else {
		hash_entry->head->next = lock_obj->next;
		HASH_DEL(hash_map, hash_entry);
		HASH_ADD(hh, hash_map, hash_key, sizeof(hash_key_t), hash_entry);
		pthread_cond_signal(&lock_obj->next->cond);
	}

        pthread_mutex_unlock(&lock_table_latch);

	free(lock_obj);
	return 0;
}
