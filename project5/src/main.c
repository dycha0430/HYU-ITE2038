#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <pthread.h>
#include <unistd.h>
#include <time.h>
#include "db_api.h"

#define THREAD_NUM 10
#define COUNT 100

void* trx_func(void* data);
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

    init_db(100, 0,0,NULL,NULL);
    
    table_id = open_table("DATA1.db");
    
    for (int i = 0; i < 100; i++){
        sprintf(ret_val, "%d", i);
        res = db_insert(table_id, i, ret_val);
    }
   
    root_pagenum = find_root_pagenum(table_id);
    print_tree(table_id, root_pagenum);

    pthread_t threads[THREAD_NUM];

    srand(time(NULL));

    for (int i = 0; i < THREAD_NUM; i++) {
        pthread_create(&threads[i], 0, trx_func, &table_id);
    }

    for (int i = 0; i < THREAD_NUM; i++) {
        pthread_join(threads[i], NULL);
    }

    printf("COMMIT : %d\n", commit);
    printf("ABORT : %d\n", aborted);

    printf("------------------------------------------\n");
    trx_id = trx_begin();
    for (int i = 0; i < COUNT; i++){
    	db_find(table_id, i, ret_val, trx_id);
	//printf("%s\n", ret_val);
    }
    trx_commit(trx_id);

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

	printf("find start\n");
    	if (db_find(table_id, random, ret_val, trx_id)){
	    aborted++;
	    //printf("INCORRET : fail to db_find()// Answer : %d, My : %d\n", random, atoi(ret_val));
	    //printf("%d ABORTED\n", trx_id);
	    return NULL;
	}
	printf("find end\n");
	
	if (atoi(ret_val) != random) {
		//printf("ret_val : %d, find : %d\n", atoi(ret_val), random);
		//printf("INCORRECT : value is wrong\n");
	}

//	printf("trx_id %d, key %d, val %s\n", trx_id, random, ret_val);

	sprintf(ret_val, "hello %d", trx_id);
	printf("update start\n");
	if (db_update(table_id, random, ret_val, trx_id)){
	    aborted++;
	    printf("%d ABORTED\n", trx_id);
            return NULL;
        }
	printf("update end\n");

	/*
	if (db_find(table_id, random, ret_val, trx_id)){
	    aborted++;
	    //printf("%d ABORTED\n", trx_id);
            return NULL;
        }
	*/
	//printf("trx_id %d, key %d, val %s\n", trx_id, random, ret_val);

    }
    trx_commit(trx_id);
    commit++;
    //printf("%d committed\n", trx_id);
    return NULL;
}

