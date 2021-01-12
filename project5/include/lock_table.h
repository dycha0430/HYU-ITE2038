#ifndef __LOCK_TABLE_H__
#define __LOCK_TABLE_H__

#include <stdint.h>
#include <pthread.h>
#include "uthash.h"
#include "trx_mgr.h"

#define SHARED 0
#define EXCLUSIVE 1
#define WAITING 0
#define ACQUIRED 1

#define LOCK_ACQUIRED 0
#define NEED_TO_WAIT 1
#define DEADLOCK 2

pthread_mutex_t lock_table_latch;

typedef struct lock_t lock_t;
typedef struct hash_entry_t hash_entry_t;
typedef struct hash_key_t hash_key_t;

struct lock_t {
        struct lock_t* prev;
        struct lock_t* next;
        hash_entry_t* hash_entry;
        pthread_cond_t cond;
	int lock_mode;
	int trx_id;
	int status;
	struct lock_t* trx_next_lock;
	char* old_val;
	struct trx_t* trx;
};

struct hash_key_t {
        int table_id;
        int64_t key;
};

struct hash_entry_t {
        hash_key_t hash_key;
        lock_t* head;
        lock_t* tail;
        UT_hash_handle hh;
};

hash_entry_t *hash_map;

/* APIs for lock table */
int init_lock_table();
int lock_acquire(int table_id, int64_t key, int trx_id, 
		int lock_mode, struct lock_t **lock);
void lock_wait(lock_t* lock_obj);
int lock_release(lock_t* lock_obj);
int deadlock_detect(lock_t* waiting_lock, int detect_trx_id);

#endif /* __LOCK_TABLE_H__ */
