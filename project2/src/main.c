#include <stdio.h>
#include <stdlib.h>
#include "db_api.h"

// MAIN

int main( int argc, char ** argv ) {

    char *value, *ret_val, *pathname;
    int input, res, table_id;
    char instruction;
    pagenum_t root_pagenum;

    value = (char*)malloc(sizeof(char)*120);
    ret_val = (char*)malloc(sizeof(char)*120);
    pathname = (char*)malloc(sizeof(char)*120);


    license_notice();

    printf("> ");
    while (scanf("%c", &instruction) != EOF) {
        switch (instruction) {
        case 'd':
            scanf("%d", &input);
            res = db_delete(input);
            break;
        case 'i':
            scanf("%d %[^\n]s", &input, value);
            res = db_insert(input, value);
            break;
        case 'f':
            scanf("%d", &input);
            res = db_find(input, ret_val);
	    if (res == 0)
		    printf("value : %s\n", ret_val);
            break;
	case 'o':
	    scanf("%s", pathname);
	    table_id = open_table(pathname);
	    break;
        case 'q':
            while (getchar() != (int)'\n');
            return EXIT_SUCCESS;
        case 't':
	    root_pagenum = find_root_pagenum();
            print_tree(root_pagenum);
            break;
        }
        while (getchar() != (int)'\n');
        printf("> ");
    }
    printf("\n");

    return EXIT_SUCCESS;
}
