#include "db_api.h"

/* Initialize database */
int init_db (int buf_num, int flag, int log_num, char* log_path, char* logmsg_path){
    init_lock_table();
    init_trx_table();
    return init_index(buf_num);
}
/* Open existing data file using ¡®pathname¡¯ 
 * or create one if not existed 
 * If success, return the unique table id, 
 * which represents the own table in this database.
 * Otherwise, return negative value.
 */
int open_table (char *pathname){
    return index_open(pathname);
}

/* Insert input ¡®key/value¡¯ (record) to data file at the right place. 
 * If success, return 0. Otherwise, return non-zero value.
 */
int db_insert (int table_id, int64_t key, char *value){
    pagenum_t root_pagenum = find_root_pagenum(table_id);
    pagenum_t insert_result = insert(table_id, root_pagenum, key, value);

    if (insert_result == -1) return -1;
    return 0;
}

/* Find the matching key and modify the values. 
 * If operation is successfully done, return 0. 
 * Otherwise, return non-zero value and transaction is aborted.
 */
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
    ret = update(table_id, key, values, trx_id);

    if (ret != 0) {
    	trx_abort(trx_id);
		return -1;
    }
    return 0;
}

/* Read a value in the table with matching key for the transaction having trx_id.
 * If found matching ¡®key¡¯, store matched ¡®value¡¯ 
 * string in ret_val and return 0. Otherwise, 
 * return non-zero value and transaction is aborted. */

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
    // SUCCESS
    else strcpy(ret_val, find_val);

    return 0;
}

/* Find the matching record and delete it if found.
 * If success, return 0. Otherwise, return non-zero value 
 */

int db_delete (int table_id, int64_t key){
    pagenum_t root_pagenum = find_root_pagenum(table_id);
    int delete_res = delete(table_id, root_pagenum, key);
    
    return delete_res; 
}

/* Write all pages of this table from buffer to disk.
 * If success, return 0. Otherwise, return non-zero value.
 */
int close_table(int table_id){
    return index_close_table(table_id);
}


/* Flush all data from buffer and destroy allocated buffer.
 * If success, return 0. Otherwise, return non-zero value.
 */
int shutdown_db(void){
    return index_shutdown_db();
}
