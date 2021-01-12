#include "db_api.h"

int init_db (int buf_num, int flag, int log_num, char* log_path, char* logmsg_path){
    int** loser = (int**)malloc(sizeof(int*));
    int* open_arr = (int*)malloc(sizeof(int)*MAX_TABLE_NUM);
    int loserNum;
    
    init_lock_table();
    init_trx_table();
    init_log(log_path, logmsg_path);
    init_index(buf_num);

    loserNum = analysis(loser); 
    redo(flag, log_num, open_arr);

    if (flag != 1)
    	undo(*loser, loserNum, flag, log_num);

    // Close table and flush dirty pages
    for (int i = 0; i < MAX_TABLE_NUM; i++){
        if (open_arr[i]) buf_close_table(i+1);
    }
    
    // Flush log buffer
    flush_log();

    free(open_arr);
    return 0;
}

int open_table (char *pathname){
    return index_open(pathname);
}

int db_insert (int table_id, int64_t key, char *value){
    pagenum_t root_pagenum = find_root_pagenum(table_id);
    pagenum_t insert_result = insert(table_id, root_pagenum, key, value);

    if (insert_result == -1) return -1;
    return 0;
}

int db_update(int table_id, int64_t key, char* values, int trx_id){
    int ret;
    trx_t l, *p;
    memset(&l, 0, sizeof(trx_t));
    l.trx_id = trx_id;
    HASH_FIND(hh, trx_table, &l.trx_id, sizeof(int), p);
    // Already aborted
    if (!p || p->aborted) return -1;

    // If ret is 0, SUCCESS
    // else FAILED
    ret = update(table_id, key, values, trx_id, false, -1);

    if (ret != 0) {
    	trx_abort(trx_id);
	return -1;
    }
    return 0;
}

int db_find (int table_id, int64_t key, char *ret_val, int trx_id){
    char * find_val = NULL;
    trx_t l, *p;
    memset(&l, 0, sizeof(trx_t));
    l.trx_id = trx_id;
    HASH_FIND(hh, trx_table, &l.trx_id, sizeof(int), p);
    // Already aborted
    if (!p || p->aborted) return -1;

    find_val = find(table_id, key, false, trx_id);
   
    // FAILED
    if (find_val == NULL) {
	trx_abort(trx_id);
	return -1;
    }
    //SUCCESS
    else strcpy(ret_val, find_val);

    return 0;
}

int db_delete (int table_id, int64_t key){
    pagenum_t root_pagenum = find_root_pagenum(table_id);
    int delete_res = delete(table_id, root_pagenum, key);
    
    return delete_res; 
}

int close_table(int table_id){
    return index_close_table(table_id);
}

int shutdown_db(void){
    return index_shutdown_db();
}
