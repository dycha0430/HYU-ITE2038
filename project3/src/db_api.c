#include "db_api.h"

int init_db (int buf_num){
   return init_index(buf_num);
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

int db_find (int table_id, int64_t key, char *ret_val){
    pagenum_t root_pagenum = find_root_pagenum(table_id);
    char * find_val = find(table_id, root_pagenum, key, false);

    if (find_val != NULL)
        strcpy(ret_val, find_val);

    if (find_val == NULL) return -1;
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
