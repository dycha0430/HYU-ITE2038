#include "db_api.h"

int open_table (char *pathname){
    bool ret = index_open(pathname);
    if (!ret) return -1;

    return unique_table_id++;
}

int db_insert (int64_t key, char *value){
    if (fd == -1) return -1;
    pagenum_t root_pagenum = find_root_pagenum();
    pagenum_t insert_result = insert(root_pagenum, key, value);

    if (insert_result == 0) return -1;
    return 0;    
}

int db_find (int64_t key, char *ret_val){
    if (fd == -1) return -1;
    pagenum_t root_pagenum = find_root_pagenum();
    char * find_val = find(root_pagenum, key, 1);

    if (find_val != NULL)
        strcpy(ret_val, find_val);

    if (find_val == NULL) return -1;
    return 0;
}

int db_delete (int64_t key){
    if (fd == -1) return -1;
    pagenum_t root_pagenum = find_root_pagenum();
    pagenum_t delete_result = delete(root_pagenum, key);
    if (delete_result == 0) return -1;
    return 0; 
}
