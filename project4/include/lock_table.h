#ifndef __LOCK_TABLE_H__
#define __LOCK_TABLE_H__

#include <stdint.h>
#include <pthread.h>
#include "uthash.h"

typedef struct lock_t lock_t;
typedef struct hash_entry_t hash_entry_t;
typedef struct hash_key_t hash_key_t;
pthread_mutex_t lock_table_latch;

hash_entry_t *hash_map;

/* APIs for lock table */
int init_lock_table();
lock_t* lock_acquire(int table_id, int64_t key);
int lock_release(lock_t* lock_obj);

#endif /* __LOCK_TABLE_H__ */
