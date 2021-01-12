#include <stdio.h>
#include <stdlib.h>
#include "db_api.h"

// MAIN

int main( int argc, char ** argv ) {

    char *value, *ret_val, *pathname;
    int input, res, table_id, table_id2, table_id3;
    char instruction;
    pagenum_t root_pagenum;

    value = (char*)malloc(sizeof(char)*120);
    ret_val = (char*)malloc(sizeof(char)*120);
    pathname = (char*)malloc(sizeof(char)*120);

    init_db(100);
    table_id = open_table("test.db");
    table_id2 = open_table("2.db");
    
    for (int i = 0; i < 100000; i++){
	sprintf(ret_val, "%d", i);
    	res = db_insert(table_id, i, ret_val);
	res = db_insert(table_id2, i, ret_val);
    }
    
    root_pagenum = find_root_pagenum(table_id);
    print_tree(table_id, root_pagenum);

    for (int i = 0; i < 100000; i++){
    	res = db_delete(table_id, i);
	res = db_delete(table_id2, i);
	//res = db_delete(table_id2, i);
    }
    printf("DELETE END\n");

    root_pagenum = find_root_pagenum(table_id);
    print_tree(table_id, root_pagenum);

    root_pagenum = find_root_pagenum(table_id2);
    print_tree(table_id2, root_pagenum);

    res = shutdown_db();
    license_notice();

    return EXIT_SUCCESS;
}
