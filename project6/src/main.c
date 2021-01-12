#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <pthread.h>
#include <unistd.h>
#include <time.h>
#include "db_api.h"

#define THREAD_NUM 10
#define COUNT 10

void* trx_func(void* data);
void* fast_func(void* data);
// MAIN

int commit, aborted;

int main( int argc, char ** argv ) {

    char *value, *ret_val, *pathname;
    int input, res, table_id, table_id2, table_id3, trx_id;
    uint64_t root_pagenum;
    commit = 0;
    aborted = 0;

    value = (char*)malloc(sizeof(char)*120);
    ret_val = (char*)malloc(sizeof(char)*120);
    pathname = (char*)malloc(sizeof(char)*120);

   /* 
    init_db(100, 0, 10,"logfile.data", "logmsg.txt");
    
    table_id = open_table("DATA1.db");
    
    for (int i = 0; i < 100; i++){
        sprintf(ret_val, "%d", i);
        res = db_insert(table_id, i, ret_val);
    }
   
    root_pagenum = find_root_pagenum(table_id);
    print_tree(table_id, root_pagenum);
   */     
    srand(time(NULL));
    init_db(1000, 0, 100, "logfile.data", "logmsg.txt");
    table_id = open_table("DATA1.db");
    printf("open table...\n");
    trx_id = trx_begin();

    for (int i = 0; i < 20; i++){ 
        db_find(table_id, i, ret_val, trx_id);
        printf("%s\n", ret_val);
    }
    trx_commit(trx_id);

    pthread_t thread;
    pthread_create(&thread, 0, fast_func, &table_id);
    pthread_join(thread, NULL);    
/*
    pthread_t threads[THREAD_NUM+1];


    pthread_create(&threads[THREAD_NUM], 0, fast_func, &table_id);
    for (int i = 0; i < THREAD_NUM; i++) {
        pthread_create(&threads[i], 0, trx_func, &table_id);
    }

    pthread_join(threads[THREAD_NUM], NULL);
    for (int i = 0; i < THREAD_NUM; i++) {
        pthread_join(threads[i], NULL);
    }
*/
    res = shutdown_db();

    return EXIT_SUCCESS;
}

void* trx_func(void* data){
    char* ret_val = (char*)malloc(sizeof(char)*120);
    char* find = (char*)malloc(sizeof(char)*120);
    int trx_id = trx_begin();
    int table_id = *(int*)data;
    int random;

    for (int i = 0; i < COUNT; i++){
	random = rand() % COUNT;

	sprintf(ret_val, "modified %d", i);
	if (db_update(table_id, random, ret_val, trx_id)){
	    aborted++;
            return NULL;
        }
    }
    
    trx_commit(trx_id);

    commit++;
    return NULL;
}

void *fast_func(void *data){
	char* ret_val = malloc(sizeof(char)*120);
	int table_id = *(int*)data;
	int trx_id = trx_begin();
	
	for (int i = 0; i < 10; i++){
		sprintf(ret_val, "commit1 %d", i);
		db_update(table_id, i, ret_val, trx_id);
	}
	trx_commit(trx_id);

	int new_id = trx_begin();
	for (int i = 0; i < 20; i++){
		sprintf(ret_val, "abort %d", i);
		db_update(table_id, i, ret_val, new_id);
	}
	trx_abort(new_id);

	trx_id = trx_begin();
	for (int i = 5; i < 20; i++){
		sprintf(ret_val, "commit2 %d", i);
                db_update(table_id, i, ret_val, trx_id);
	}
	trx_commit(trx_id);

	trx_id = trx_begin();
	for (int i = 0; i < 20; i++){
		sprintf(ret_val, "crash %d", i);
		db_update(table_id, i, ret_val, trx_id);
	}

	flush_log();
	exit(0);
}
