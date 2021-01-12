#ifndef __BPT_H__
#define __BPT_H__

// Uncomment the line below if you are compiling on Windows.
// #define WINDOWS
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

#include "buf_mgr.h"
#include "trx_mgr.h"
#include "lock_table.h"
#include "log_mgr.h"

#ifdef WINDOWS
#define bool char
#define false 0
#define true 1
#endif

#define VALUE_SIZE 120
#define PAIR_SIZE 128

// Order of each page
#define LEAF_ORDER 32
#define INTERNAL_ORDER 249

// minimum allowable size of page
#define MIN_KEYS 1

// Constants for printing part or all of the GPL license.
#define LICENSE_FILE "LICENSE.txt"
#define LICENSE_WARRANTEE 0
#define LICENSE_WARRANTEE_START 592
#define LICENSE_WARRANTEE_END 624
#define LICENSE_CONDITIONS 1
#define LICENSE_CONDITIONS_START 70
#define LICENSE_CONDITIONS_END 625

// TYPES.

/* Type representing the record
 * to which a given key refers.
 * In a real B+ tree system, the
 * record would hold data (in a database)
 * or a file (in an operating system)
 * or some other information.
 * Users can rewrite this part of the code
 * to change the type and content
 * of the value field.
 */

typedef int64_t LSN_t;

typedef struct leaf_record_t {
    int64_t key;
    char value[VALUE_SIZE];
} leaf_record_t;

typedef struct internal_record_t {
    int64_t key;
    pagenum_t child_pagenum;
} internal_record_t;

union records_t {
    leaf_record_t leaf_records[LEAF_ORDER-1];
    internal_record_t internal_records[INTERNAL_ORDER-1];
};

// leaf or internal page
typedef struct bpt_page_t {
    pagenum_t parent;
    int is_leaf;
    int num_keys;
    int padding[2];
    LSN_t pageLSN;
    int padding2[22];
    pagenum_t special_pagenum;
    union records_t records;
} bpt_page_t;

// header page
typedef struct header_page_t {
    pagenum_t free_pagenum;
    pagenum_t root_pagenum;
    uint64_t num_pages;
    LSN_t pageLSN;
    int padding[1016];
} header_page_t;

// free page
typedef struct free_page_t {
    pagenum_t next_free_pagenum;
    LSN_t pageLSN;
    int padding[1020];
} free_page_t;

// for queue
typedef struct queue_t {
    pagenum_t pagenum;
    struct queue_t* next;
} queue_t;

// GLOBALS.

/* The queue is used to print the tree in
 * level order, starting from the root
 * printing each entire rank on a separate
 * line, finishing with the leaves.
 */
extern queue_t * queue;

/* The user can toggle on and off the "verbose"
 * property, which causes the pointer addresses
 * to be printed out in hexadecimal notation
 * next to their corresponding keys.
 */
extern bool verbose_output;


// FUNCTION PROTOTYPES.

// Output and utility.

void license_notice( void );
void print_license( int licence_part );

int init_index(int num_buf);
int index_close_table(int table_id);
int index_shutdown_db();
int index_open(char * pathname);
pagenum_t find_root_pagenum(int table_id);

void enqueue( pagenum_t new_node );
pagenum_t dequeue( void );
int path_to_root( int table_id, pagenum_t root, pagenum_t child );
void print_tree( int table_id, pagenum_t root );

int update(int table_id, int64_t key, char* values, int trx_id, bool isRollback, LSN_t next_undo_LSN);
pagenum_t find_leaf( int table_id, pagenum_t root, int64_t key, bool verbose, bpt_page_t* c );
char * find(int table_id, int64_t key, bool verbose, int trx_id);
int cut( int length );

// Insertion.

pagenum_t make_page( int table_id );
pagenum_t make_leaf( int table_id );
int get_left_index(bpt_page_t* parent_page, pagenum_t left);
void insert_into_leaf( int table_id, int64_t key, char * value, pagenum_t leaf_pagenum );
pagenum_t insert_into_leaf_after_splitting(int table_id, pagenum_t root, pagenum_t leaf, int64_t key, char * value);
pagenum_t insert_into_node(int table_id, pagenum_t root, pagenum_t n, int left_index, int64_t key, pagenum_t right);
pagenum_t insert_into_node_after_splitting(int table_id, pagenum_t root, pagenum_t old_pagenum, int left_index, int64_t key, pagenum_t right);
pagenum_t insert_into_parent(int table_id, pagenum_t root, pagenum_t left, int64_t key, pagenum_t right);
pagenum_t insert_into_new_root(int table_id, pagenum_t left, int64_t key, pagenum_t right);
pagenum_t start_new_tree(int table_id, int64_t key, char * value);
pagenum_t insert( int table_id, pagenum_t root, int64_t key, char * value );


// Deletion.

int get_neighbor_index(pagenum_t n, bpt_page_t* parent_page );
pagenum_t remove_entry_from_node(pagenum_t n, bpt_page_t* n_page, int64_t key);
pagenum_t adjust_root(int table_id, pagenum_t root);
pagenum_t coalesce_nodes(int table_id, pagenum_t root, pagenum_t n, pagenum_t neighbor, int neighbor_index, int64_t k_prime);
pagenum_t redistribute_nodes(int table_id, pagenum_t root, pagenum_t n, pagenum_t neighbor, int neighbor_index, int k_prime_index, int64_t k_prime);
pagenum_t delete_entry( int table_id, pagenum_t root, pagenum_t n, int64_t key );
pagenum_t delete( int table_id, pagenum_t root, int64_t key );

#endif /* __BPT_H__*/
