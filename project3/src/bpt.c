/*
 *  bpt.c  
 */
#define Version "1.14"
/*
 *
 *  bpt:  B+ Tree Implementation
 *  Copyright (C) 2010-2016  Amittai Aviram  http://www.amittai.com
 *  All rights reserved.
 *  Redistribution and use in source and binary forms, with or without
 *  modification, are permitted provided that the following conditions are met:
 *
 *  1. Redistributions of source code must retain the above copyright notice, 
 *  this list of conditions and the following disclaimer.
 *
 *  2. Redistributions in binary form must reproduce the above copyright notice, 
 *  this list of conditions and the following disclaimer in the documentation 
 *  and/or other materials provided with the distribution.
 
 *  3. Neither the name of the copyright holder nor the names of its 
 *  contributors may be used to endorse or promote products derived from this 
 *  software without specific prior written permission.
 
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 *  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE 
 *  IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE 
 *  ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE 
 *  LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR 
 *  CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF 
 *  SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS 
 *  INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN 
 *  CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) 
 *  ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE 
 *  POSSIBILITY OF SUCH DAMAGE.
 
 *  Author:  Amittai Aviram 
 *    http://www.amittai.com
 *    amittai.aviram@gmail.edu or afa13@columbia.edu
 *  Original Date:  26 June 2010
 *  Last modified: 17 June 2016
 *
 *  This implementation demonstrates the B+ tree data structure
 *  for educational purposes, includin insertion, deletion, search, and display
 *  of the search path, the leaves, or the whole tree.
 *  
 *  Must be compiled with a C99-compliant C compiler such as the latest GCC.
 *
 *  Usage:  bpt [order]
 *  where order is an optional argument
 *  (integer MIN_ORDER <= order <= MAX_ORDER)
 *  defined as the maximal number of pointers in any node.
 *
 */

#include "bpt.h"

// GLOBALS.

/* The queue is used to print the tree in
 * level order, starting from the root
 * printing each entire rank on a separate
 * line, finishing with the leaves.
 */
queue_t * queue = NULL;

/* The user can toggle on and off the "verbose"
 * property, which causes the pointer addresses
 * to be printed out in hexadecimal notation
 * next to their corresponding keys.
 */
bool verbose_output = false;


// FUNCTION DEFINITIONS.

// OUTPUT AND UTILITIES

/* Copyright and license notice to user at startup. 
 */
void license_notice( void ) {
    printf("bpt version %s -- Copyright (C) 2010  Amittai Aviram "
            "http://www.amittai.com\n", Version);
    printf("This program comes with ABSOLUTELY NO WARRANTY; for details "
            "type `show w'.\n"
            "This is free software, and you are welcome to redistribute it\n"
            "under certain conditions; type `show c' for details.\n\n");
}


/* Routine to print portion of GPL license to stdout.
 */
void print_license( int license_part ) {
    int start, end, line;
    FILE * fp;
    char buffer[0x100];

    switch(license_part) {
    case LICENSE_WARRANTEE:
        start = LICENSE_WARRANTEE_START;
        end = LICENSE_WARRANTEE_END;
        break;
    case LICENSE_CONDITIONS:
        start = LICENSE_CONDITIONS_START;
        end = LICENSE_CONDITIONS_END;
        break;
    default:
        return;
    }

    fp = fopen(LICENSE_FILE, "r");
    if (fp == NULL) {
        perror("print_license: fopen");
        exit(EXIT_FAILURE);
    }
    for (line = 0; line < start; line++)
        fgets(buffer, sizeof(buffer), fp);
    for ( ; line < end; line++) {
        fgets(buffer, sizeof(buffer), fp);
        printf("%s", buffer);
    }
    fclose(fp);
}

int init_index(int num_buf){
    return init_buf(num_buf);
}

int index_close_table(int table_id){
    return buf_close_table(table_id);
}

int index_shutdown_db(){
    return buf_shutdown_db();
}

int index_open(char * pathname){
    return buf_open_file(pathname);
}

pagenum_t find_root_pagenum(int table_id){
    pagenum_t root_pagenum;
    header_page_t* header_page;
    header_page = (header_page_t*)malloc(sizeof(header_page_t));
    
    buf_read_page(table_id, HEADER_PAGENUM, (page_t*)header_page);
    
    root_pagenum = header_page->root_pagenum;
    buf_write_page(table_id, HEADER_PAGENUM, NULL);
    
    free(header_page);
    
    return root_pagenum;
}


void enqueue( pagenum_t new_pagenum ) {
    queue_t * c;
    queue_t * new_page = (queue_t *)malloc(sizeof(queue_t));
    new_page->pagenum = new_pagenum;

    if (queue == NULL) {
        queue = new_page;
        queue->next = NULL;
    }
    else {
        c = queue;
        while(c->next != NULL) {
            c = c->next;
        }
        c->next = new_page;
        new_page->next = NULL;
    }
}

pagenum_t dequeue( void ) {
    queue_t * n = queue;
    queue = queue->next;
    n->next = NULL;

    return n->pagenum;
}



int path_to_root( int table_id, pagenum_t root, pagenum_t child ) {
    int length = 0, pagenum;
    bpt_page_t * c = (bpt_page_t*)malloc(sizeof(bpt_page_t));
    pagenum = child;
    buf_read_page(table_id, pagenum, (page_t*)c);

    while (c->parent != root) {
	buf_write_page(table_id, pagenum, NULL);
	pagenum = c->parent;
        buf_read_page(table_id, pagenum, (page_t*)c);
        length++;
    }

    buf_write_page(table_id, pagenum, NULL);
    return length + 1;
}

void print_tree( int table_id, pagenum_t root ) {

    pagenum_t n;
    int i = 0;
    int rank = 0;
    int new_rank = 0;
    bpt_page_t * n_page = (bpt_page_t*)malloc(sizeof(bpt_page_t));
    bpt_page_t * parent_page = (bpt_page_t*)malloc(sizeof(bpt_page_t));


    if (root == 0) {
        printf("Empty tree.\n");
        return;
    }
    queue = NULL;
    enqueue(root);
    while( queue != NULL ) {
        n = dequeue();
	buf_read_page(table_id, n, (page_t*)n_page);
	buf_read_page(table_id, n_page->parent, (page_t*)parent_page);
	buf_write_page(table_id, n, NULL);
	buf_write_page(table_id, n_page->parent, NULL);
        if (n_page->parent != 0 && n == parent_page->special_pagenum) {
	    if (n == root) new_rank = 0;
	    else new_rank = path_to_root( table_id, root, n );
            if (new_rank != rank) {
                rank = new_rank;
                printf("\n");
            }
        }
        if (verbose_output) {

            printf("(%ld)", (unsigned long)n);
	    if (!n_page->is_leaf)
		printf("[%ld] ", (unsigned long)n_page->special_pagenum);
	}
        for (i = 0; i < n_page->num_keys; i++) {
	    if (!n_page->is_leaf)
                printf("%ld ", n_page->records.internal_records[i].key);
            else
                printf("%ld ", n_page->records.leaf_records[i].key);

            if (verbose_output){
		if (!n_page->is_leaf)
                    printf("[%ld] ", (unsigned long)n_page->records.internal_records[i].child_pagenum);
		else 
		    printf("[%s] ", n_page->records.leaf_records[i].value);
	    }
        }
        if (!n_page->is_leaf){
	    enqueue(n_page->special_pagenum);
            for (i = 0; i < n_page->num_keys; i++){
                enqueue(n_page->records.internal_records[i].child_pagenum);
	    }
	}

        printf("| ");
    }
    printf("\n");
    free(n_page);
    free(parent_page);
}



pagenum_t find_leaf( int table_id, pagenum_t root, int64_t key, bool verbose, bpt_page_t* c ) {
    int i = 0, tmp = 0;
    
    if (root == 0) {
        if (verbose)
            printf("Empty tree.\n");
        return 0;
    }

    pagenum_t next_pagenum;
    buf_read_page(table_id, root, (page_t*)c);
    
    next_pagenum = root;
    while (!c->is_leaf) {
        if (verbose) {
            printf("[");
            for (i = 0; i < c->num_keys - 1; i++)
                printf("%ld ", c->records.internal_records[i].key);
            printf("%ld] ", c->records.internal_records[i].key);
        }
        i = 0;

        while (i < c->num_keys) {
            if (key >= c->records.internal_records[i].key) i++;
            else break;
        }

        if (verbose)
            printf("%d ->\n", i);
	buf_write_page(table_id, next_pagenum, NULL);

	if (i == 0) next_pagenum = c->special_pagenum;
	else next_pagenum = c->records.internal_records[i-1].child_pagenum;
	buf_read_page(table_id, next_pagenum, (page_t*)c);
    }
 
    if (verbose) {
        printf("Leaf [");
        for (i = 0; i < c->num_keys - 1; i++)
            printf("%ld ", c->records.leaf_records[i].key);
        printf("%ld] ->\n", c->records.leaf_records[i].key);
    }
    
    return next_pagenum;
}


char * find( int table_id, pagenum_t root, int64_t key, bool verbose ) {
    int i = 0;
    char * ret = NULL;
    if (root == 0) return NULL;
    bpt_page_t * c = (bpt_page_t*)malloc(sizeof(bpt_page_t));
    pagenum_t leaf_pagenum = find_leaf( table_id, root, key, verbose, c);
    
    for (i = 0; i < c->num_keys; i++){
        if (c->records.leaf_records[i].key == key) break;
    }
    ret = c->records.leaf_records[i].value;

    buf_write_page(table_id, leaf_pagenum, NULL);
	
    if (i == c->num_keys){
	free(c);
        return NULL;
    }
    else{
	free(c);
        return ret;
    }
}


int cut( int length ) {
    if (length % 2 == 0)
        return length/2;
    else
        return length/2 + 1;
}


// INSERTION


pagenum_t make_page( int table_id ) {
    bpt_page_t* new_page = (bpt_page_t*)malloc(sizeof(bpt_page_t));
    pagenum_t new_pagenum = buf_alloc_page(table_id);
    buf_read_page(table_id, new_pagenum, (page_t*)new_page);
  
    new_page->is_leaf = 0;
    new_page->num_keys = 0;
    new_page->parent = 0;
    new_page->special_pagenum = 0;
    
    buf_write_page(table_id, new_pagenum, (page_t*)new_page);

    free(new_page);
    return new_pagenum;
}


pagenum_t make_leaf( int table_id ) {
    pagenum_t leaf_pagenum;
    bpt_page_t* leaf = (bpt_page_t*)malloc(sizeof(bpt_page_t));

    leaf_pagenum = make_page(table_id);
    buf_read_page(table_id, leaf_pagenum, (page_t*)leaf);
    leaf->is_leaf = 1;
    buf_write_page(table_id, leaf_pagenum, (page_t*)leaf);

    free(leaf);
    return leaf_pagenum;
}


int get_left_index( bpt_page_t* parent_page, pagenum_t left) {

    int left_index = 0;
    
    if (parent_page->special_pagenum == left) return -1;
    while (left_index < parent_page->num_keys && 
            parent_page->records.internal_records[left_index].child_pagenum != left)
        left_index++;
    return left_index;
}


void insert_into_leaf( int table_id, int64_t key, char * value, pagenum_t leaf_pagenum ) {

    int i, insertion_point;
    bpt_page_t* leaf_page = (bpt_page_t*)malloc(sizeof(bpt_page_t));
    insertion_point = 0;
    buf_read_page(table_id, leaf_pagenum, (page_t*)leaf_page);

    while (insertion_point < leaf_page->num_keys && leaf_page->records.leaf_records[insertion_point].key < key)
        insertion_point++;

    for (i = leaf_page->num_keys; i > insertion_point; i--) {
        leaf_page->records.leaf_records[i].key = leaf_page->records.leaf_records[i-1].key;
        strcpy(leaf_page->records.leaf_records[i].value, leaf_page->records.leaf_records[i-1].value);
    }
    
    leaf_page->records.leaf_records[insertion_point].key = key;
    strcpy(leaf_page->records.leaf_records[insertion_point].value, value);
    leaf_page->num_keys++;
    buf_write_page(table_id, leaf_pagenum, (page_t*)leaf_page);

    free(leaf_page);
    return;
}


pagenum_t insert_into_leaf_after_splitting(int table_id, pagenum_t root, pagenum_t leaf, int64_t key, char * value) {

    bpt_page_t* leaf_page = (bpt_page_t*)malloc(sizeof(bpt_page_t));
    bpt_page_t* new_leaf_page = (bpt_page_t*)malloc(sizeof(bpt_page_t));
    pagenum_t new_leaf, ret;
    int64_t * temp_keys;
    char ** temp_values;
    int64_t new_key;
    int insertion_index, split, i, j;

    new_leaf = make_leaf(table_id);
    buf_read_page(table_id, leaf, (page_t*)leaf_page);
    buf_read_page(table_id, new_leaf, (page_t*)new_leaf_page);

    temp_keys = (int64_t*)malloc( LEAF_ORDER * sizeof(int64_t) );
    if (temp_keys == NULL) {
        perror("Temporary keys array.");
        exit(EXIT_FAILURE);
    }

    temp_values = (char **)malloc(LEAF_ORDER * sizeof(char *));
    for (i = 0; i < LEAF_ORDER; i++) 
	temp_values[i] = (char *)malloc(VALUE_SIZE * sizeof(char));
    if (temp_values == NULL) {
        perror("Temporary pointers array.");
        exit(EXIT_FAILURE);
    }

    insertion_index = 0;
    while (insertion_index < LEAF_ORDER - 1 && leaf_page->records.leaf_records[insertion_index].key < key)
        insertion_index++;

    for (i = 0, j = 0; i < leaf_page->num_keys; i++, j++) {
        if (j == insertion_index) j++;
        temp_keys[j] = leaf_page->records.leaf_records[i].key;
        strcpy(temp_values[j], leaf_page->records.leaf_records[i].value);
    }

    temp_keys[insertion_index] = key;
    strcpy(temp_values[insertion_index], value);

    leaf_page->num_keys = 0;

    split = cut(LEAF_ORDER - 1);

    for (i = 0; i < split; i++) {
        strcpy(leaf_page->records.leaf_records[i].value, temp_values[i]);
        leaf_page->records.leaf_records[i].key = temp_keys[i];
        leaf_page->num_keys++;
    }

    for (i = split, j = 0; i < LEAF_ORDER; i++, j++) {
	strcpy(new_leaf_page->records.leaf_records[j].value, temp_values[i]);
        new_leaf_page->records.leaf_records[j].key = temp_keys[i];
        new_leaf_page->num_keys++;
    }

    for (int i = 0; i < LEAF_ORDER; i++)
	    free(temp_values[i]);
    free(temp_values);
    free(temp_keys);

    new_leaf_page->special_pagenum = leaf_page->special_pagenum;
    leaf_page->special_pagenum = new_leaf;
    
    for (i = leaf_page->num_keys; i < LEAF_ORDER - 1; i++)
        leaf_page->records.leaf_records[i].value[0] = '\0';
    for (i = new_leaf_page->num_keys; i < LEAF_ORDER - 1; i++)
        new_leaf_page->records.leaf_records[i].value[0] = '\0';

    
    new_leaf_page->parent = leaf_page->parent;
    new_key = new_leaf_page->records.leaf_records[0].key;
    
    buf_write_page(table_id, new_leaf, (page_t*)new_leaf_page);
    buf_write_page(table_id, leaf, (page_t*)leaf_page);
    
    free(new_leaf_page);
    free(leaf_page);

    ret = insert_into_parent(table_id, root, leaf, new_key, new_leaf);

    return ret;
}


pagenum_t insert_into_node(int table_id, pagenum_t root, pagenum_t n,
        int left_index, int64_t key, pagenum_t right) {
    int i;
    bpt_page_t* n_page = (bpt_page_t*)malloc(sizeof(bpt_page_t));
    buf_read_page(table_id, n, (page_t*)n_page);

    for (i = n_page->num_keys-1; i > left_index; i--) {
        n_page->records.internal_records[i+1].child_pagenum = n_page->records.internal_records[i].child_pagenum;
        n_page->records.internal_records[i+1].key = n_page->records.internal_records[i].key;
    }
  
    n_page->records.internal_records[left_index+1].child_pagenum = right;
    n_page->records.internal_records[left_index+1].key = key;
    n_page->num_keys++;

    buf_write_page(table_id, n, (page_t*)n_page);
    free(n_page);

    return root;
}


pagenum_t insert_into_node_after_splitting(int table_id, pagenum_t root, pagenum_t old_pagenum, int left_index, int64_t key, pagenum_t right) {
    int i, j, split;
    int64_t k_prime;
    pagenum_t new_pagenum, child, ret;
    int64_t * temp_keys;
    pagenum_t * temp_values;

    bpt_page_t* old_page = (bpt_page_t*)malloc(sizeof(bpt_page_t));
    bpt_page_t* new_page = (bpt_page_t*)malloc(sizeof(bpt_page_t));
    bpt_page_t* child_page = (bpt_page_t*)malloc(sizeof(bpt_page_t));

    temp_values = (pagenum_t *)malloc( INTERNAL_ORDER * sizeof(pagenum_t) );
    if (temp_values == NULL) {
        perror("Temporary pointers array for splitting nodes.");
        exit(EXIT_FAILURE);
    }
    
    temp_keys = (int64_t *)malloc( INTERNAL_ORDER * sizeof(int64_t) );
    if (temp_keys == NULL) {
        perror("Temporary keys array for splitting nodes.");
        exit(EXIT_FAILURE);
    }

    buf_read_page(table_id, old_pagenum, (page_t*)old_page);
    for (i = 0, j = 0; i < old_page->num_keys; i++, j++) {
        if (j == left_index+1) j++;
        temp_values[j] = old_page->records.internal_records[i].child_pagenum;
	temp_keys[j] = old_page->records.internal_records[i].key;
    }

    
    temp_values[left_index+1] = right;
    temp_keys[left_index+1] = key;

    
    split = cut(INTERNAL_ORDER);
    new_pagenum = make_page(table_id);
    buf_read_page(table_id, new_pagenum, (page_t*)new_page);

    old_page->num_keys = 0;
    for (i = 0; i < split - 1; i++) {
        old_page->records.internal_records[i].child_pagenum = temp_values[i];
        old_page->records.internal_records[i].key = temp_keys[i];
        old_page->num_keys++;
    }
   
    new_page->special_pagenum = temp_values[i];
    k_prime = temp_keys[i];
    for (++i, j = 0; i < INTERNAL_ORDER; i++, j++) {
        new_page->records.internal_records[j].child_pagenum = temp_values[i];
        new_page->records.internal_records[j].key = temp_keys[i];
        new_page->num_keys++;
    }

    free(temp_values);
    free(temp_keys);
    new_page->parent = old_page->parent;
    
    for (i = 0; i < new_page->num_keys; i++) {
        child = new_page->records.internal_records[i].child_pagenum;
        buf_read_page(table_id, child, (page_t*)child_page);
	child_page->parent = new_pagenum;
	buf_write_page(table_id, child, (page_t*)child_page);
    }

    child = new_page->special_pagenum;
    buf_read_page(table_id, child, (page_t*)child_page);
    child_page->parent = new_pagenum;

    buf_write_page(table_id, child, (page_t*)child_page);    
    buf_write_page(table_id, old_pagenum, (page_t*)old_page);
    buf_write_page(table_id, new_pagenum, (page_t*)new_page);

    free(child_page);
    free(new_page);
    free(old_page);

    ret = insert_into_parent(table_id, root, old_pagenum, k_prime, new_pagenum);

    return ret;
}


pagenum_t insert_into_parent(int table_id, pagenum_t root, pagenum_t left, int64_t key, pagenum_t right) {

    int left_index, num_keys;
    pagenum_t parent;
    bpt_page_t* parent_page;
    bpt_page_t* left_page = (bpt_page_t*)malloc(sizeof(bpt_page_t));
   
    buf_read_page(table_id, left, (page_t*)left_page);
    parent = left_page->parent;
    buf_write_page(table_id, left, NULL);
    free(left_page);

    if (parent == 0)
        return insert_into_new_root(table_id, left, key, right);

    parent_page = (bpt_page_t*)malloc(sizeof(bpt_page_t));
    buf_read_page(table_id, parent, (page_t*)parent_page);
    left_index = get_left_index(parent_page, left);
    buf_write_page(table_id, parent, NULL);

    num_keys = parent_page->num_keys;
    free(parent_page);
    
    if (num_keys < INTERNAL_ORDER - 1)
        return insert_into_node(table_id, root, parent, left_index, key, right);
    else 
	return insert_into_node_after_splitting(table_id, root, parent, left_index, key, right);

}


pagenum_t insert_into_new_root(int table_id, pagenum_t left, int64_t key, pagenum_t right) {

    bpt_page_t* left_page = (bpt_page_t*)malloc(sizeof(bpt_page_t));
    bpt_page_t* right_page = (bpt_page_t*)malloc(sizeof(bpt_page_t));
    bpt_page_t* root_page = (bpt_page_t*)malloc(sizeof(bpt_page_t));
    pagenum_t root = make_page(table_id);
    header_page_t* header_page = (header_page_t*)malloc(sizeof(header_page_t));
    
    buf_read_page(table_id, root, (page_t*)root_page);
    buf_read_page(table_id, right, (page_t*)right_page);
    buf_read_page(table_id, left, (page_t*)left_page);
    buf_read_page(table_id, HEADER_PAGENUM, (page_t*)header_page);

    root_page->records.internal_records[0].key = key;
    root_page->special_pagenum = left;
    root_page->records.internal_records[0].child_pagenum = right;
    root_page->num_keys++;
    root_page->parent = 0;
    left_page->parent = root;
    right_page->parent = root;
    header_page->root_pagenum = root;

    buf_write_page(table_id, root, (page_t*)root_page);
    buf_write_page(table_id, HEADER_PAGENUM, (page_t*)header_page);
    buf_write_page(table_id, right, (page_t*)right_page);
    buf_write_page(table_id, left, (page_t*)left_page);

    free(root_page);
    free(header_page);
    free(right_page);
    free(left_page);

    return root;
}


pagenum_t start_new_tree(int table_id, int64_t key, char * value) {
    pagenum_t root_pagenum;
    header_page_t* header_page = (header_page_t*)malloc(sizeof(header_page_t));
    bpt_page_t* root_page = (bpt_page_t*)malloc(sizeof(bpt_page_t));

    root_pagenum = make_leaf(table_id);
    buf_read_page(table_id, root_pagenum, (page_t*)root_page);
    buf_read_page(table_id, HEADER_PAGENUM, (page_t*)header_page);

    root_page->records.leaf_records[0].key = key;
    strcpy(root_page->records.leaf_records[0].value, value);
    root_page->num_keys++;
    header_page->root_pagenum = root_pagenum;

    buf_write_page(table_id, root_pagenum, (page_t*)root_page);
    buf_write_page(table_id, HEADER_PAGENUM, (page_t*)header_page);

    free(header_page);
    free(root_page);
    
    return root_pagenum;
}



/* Master insertion function.*/
pagenum_t insert( int table_id, pagenum_t root, int64_t key, char * value ) {

    pagenum_t leaf_pagenum;
    bpt_page_t* leaf = (bpt_page_t*)malloc(sizeof(bpt_page_t));
    int num_keys;


    if (find(table_id, root, key, false) != NULL){
	    return -1;
    }

    if (root == 0) 
        return start_new_tree(table_id, key, value);


    leaf_pagenum = find_leaf(table_id, root, key, false, leaf);
    buf_write_page(table_id, leaf_pagenum, NULL);
    num_keys = leaf->num_keys;
    free(leaf);

    if (num_keys < LEAF_ORDER - 1) {
        insert_into_leaf(table_id, key, value, leaf_pagenum);
    } else {
	insert_into_leaf_after_splitting(table_id, root, leaf_pagenum, key, value);
    }

    return 0;
}




// DELETION.

int get_neighbor_index( pagenum_t n, bpt_page_t* parent_page ) {

    int i;
    
    if (parent_page->special_pagenum == n) return -2;
    for (i = 0; i < parent_page->num_keys; i++)
        if (parent_page->records.internal_records[i].child_pagenum == n)
            return i-1;

    // Error state.
    exit(EXIT_FAILURE);
}

pagenum_t remove_entry_from_node(pagenum_t n, bpt_page_t* n_page, int64_t key) {

    int i, num_pointers;
    i = 0;

    if (n_page->is_leaf){
    	while (n_page->records.leaf_records[i].key != key) {
		i++;
	}
	
    	for (++i; i < n_page->num_keys; i++){
       	    n_page->records.leaf_records[i-1].key = n_page->records.leaf_records[i].key;
	    strcpy(n_page->records.leaf_records[i-1].value, n_page->records.leaf_records[i].value);
	}
    } else {
    	while (n_page->records.internal_records[i].key != key) i++;
	for (++i; i < n_page->num_keys; i++){
	    n_page->records.internal_records[i-1].key = n_page->records.internal_records[i].key;
	    n_page->records.internal_records[i-1].child_pagenum = n_page->records.internal_records[i].child_pagenum;
	}
    }

    n_page->num_keys--;

    if (n_page->is_leaf)
        for (i = n_page->num_keys; i < LEAF_ORDER - 1; i++)
            n_page->records.leaf_records[i].value[0] = '\0';

    return n;
}


pagenum_t adjust_root(int table_id, pagenum_t root) {

    pagenum_t new_root;
    bpt_page_t* new_root_page;
    header_page_t* header_page = (header_page_t*)malloc(sizeof(header_page_t));
    bpt_page_t* root_page = (bpt_page_t*)malloc(sizeof(bpt_page_t));

    buf_read_page(table_id, root, (page_t*)root_page);
    if (root_page->num_keys > 0){
	buf_write_page(table_id, root, NULL);
	free(root_page);
	free(header_page);
        return root;
    }

    if (!root_page->is_leaf) {
        bpt_page_t* new_root_page = (bpt_page_t*)malloc(sizeof(bpt_page_t));
	new_root = root_page->special_pagenum;
        buf_read_page(table_id, new_root, (page_t*)new_root_page);
        new_root_page->parent = 0;
	buf_write_page(table_id, new_root, (page_t*)new_root_page);
	free(new_root_page);
    }
    else{
        new_root = 0;
    }

    buf_read_page(table_id, HEADER_PAGENUM, (page_t*)header_page);
    header_page->root_pagenum = new_root;
    buf_write_page(table_id, HEADER_PAGENUM, (page_t*)header_page);
    buf_free_page(table_id, root);
    
    free(header_page);
    free(root_page);

    return new_root;
}


pagenum_t coalesce_nodes(int table_id, pagenum_t root, pagenum_t n, pagenum_t neighbor, int neighbor_index, int64_t k_prime) {

    int i, j, neighbor_insertion_index, n_end;
    pagenum_t parent, tmp;
    bpt_page_t *tmp_page, *n_page, *neighbor_page;
    n_page = (bpt_page_t*)malloc(sizeof(bpt_page_t));
    neighbor_page = (bpt_page_t*)malloc(sizeof(bpt_page_t));

    if (neighbor_index == -2) {
	tmp = n;
	n = neighbor;
	neighbor = tmp;
    }

    buf_read_page(table_id, n, (page_t*)n_page);
    buf_read_page(table_id, neighbor, (page_t*)neighbor_page);

    parent = n_page->parent;

    neighbor_insertion_index = neighbor_page->num_keys;
    if (!n_page->is_leaf) {
        neighbor_page->records.internal_records[neighbor_insertion_index].key = k_prime;
        neighbor_page->records.internal_records[neighbor_insertion_index].child_pagenum = n_page->special_pagenum;
	neighbor_page->num_keys++;
	

        n_end = n_page->num_keys;

        for (i = neighbor_insertion_index + 1, j = 0; j < n_end; i++, j++) {
            neighbor_page->records.internal_records[i].key = n_page->records.internal_records[j].key;
            neighbor_page->records.internal_records[i].child_pagenum = n_page->records.internal_records[j].child_pagenum;
            neighbor_page->num_keys++;
            n_page->num_keys--;
        }

	tmp_page = (bpt_page_t*)malloc(sizeof(bpt_page_t));
        for (i = 0; i < neighbor_page->num_keys; i++) {
            tmp = neighbor_page->records.internal_records[i].child_pagenum;
	    buf_read_page(table_id, tmp, (page_t*)tmp_page);
	    tmp_page->parent = neighbor;
            buf_write_page(table_id, tmp, (page_t*)tmp_page);
        }
	free(tmp_page);
    }

    else {
        for (i = neighbor_insertion_index, j = 0; j < n_page->num_keys; i++, j++) {
            neighbor_page->records.leaf_records[i].key = n_page->records.leaf_records[i].key;
            strcpy(neighbor_page->records.leaf_records[i].value, n_page->records.leaf_records[j].value);
            
	    neighbor_page->num_keys++;
        }
        neighbor_page->special_pagenum = n_page->special_pagenum;
    }

    buf_free_page(table_id, n);
    buf_write_page(table_id, neighbor, (page_t*)neighbor_page);

    free(n_page);
    free(neighbor_page);

    root = delete_entry(table_id, root, parent, k_prime);

    return root;
}


pagenum_t redistribute_nodes(int table_id, pagenum_t root, pagenum_t n, pagenum_t neighbor, int neighbor_index, int k_prime_index, int64_t k_prime) {  
    int i;
    pagenum_t tmp;
    bpt_page_t *tmp_page, *parent_page, *neighbor_page, *n_page;
    tmp_page = (bpt_page_t*)malloc(sizeof(bpt_page_t));
    parent_page = (bpt_page_t*)malloc(sizeof(bpt_page_t));
    n_page = (bpt_page_t*)malloc(sizeof(bpt_page_t));
    neighbor_page = (bpt_page_t*)malloc(sizeof(bpt_page_t));

    buf_read_page(table_id, n, (page_t*)n_page);
    buf_read_page(table_id, neighbor, (page_t*)neighbor_page);

    if (neighbor_index != -2) {
        for (i = n_page->num_keys; i > 0; i--) {
	    if (n_page->is_leaf){
                n_page->records.leaf_records[i].key = n_page->records.leaf_records[i - 1].key;
                strcpy(n_page->records.leaf_records[i].value, n_page->records.leaf_records[i - 1].value);
	    } else {
	        n_page->records.internal_records[i].key = n_page->records.internal_records[i - 1].key;
		n_page->records.internal_records[i].child_pagenum = n_page->records.internal_records[i - 1].child_pagenum;
	    }
        }

        if (!n_page->is_leaf) {
	    n_page->records.internal_records[0].child_pagenum = n_page->special_pagenum;
            n_page->special_pagenum = neighbor_page->records.internal_records[neighbor_page->num_keys-1].child_pagenum;
            tmp = n_page->special_pagenum;
	    
	    buf_read_page(table_id, tmp, (page_t*)tmp_page);
            tmp_page->parent = n;
	    buf_write_page(table_id, tmp, (page_t*)tmp_page);

            n_page->records.internal_records[0].key = k_prime;
	    buf_read_page(table_id, n_page->parent, (page_t*)parent_page);
            parent_page->records.internal_records[k_prime_index].key = neighbor_page->records.internal_records[neighbor_page->num_keys - 1].key;
	    buf_write_page(table_id, n_page->parent, (page_t*)parent_page);
        }
        else {
            strcpy(n_page->records.leaf_records[0].value, neighbor_page->records.leaf_records[neighbor_page->num_keys - 1].value);
            neighbor_page->records.leaf_records[neighbor_page->num_keys - 1].value[0] = '\0';
            n_page->records.leaf_records[0].key = neighbor_page->records.leaf_records[neighbor_page->num_keys - 1].key;
            
	    buf_read_page(table_id, n_page->parent, (page_t*)parent_page);
            parent_page->records.leaf_records[k_prime_index].key = n_page->records.leaf_records[0].key;
            buf_write_page(table_id, n_page->parent, (page_t*)parent_page);
        }
    }

    else {  
        if (n_page->is_leaf) {
            n_page->records.leaf_records[n_page->num_keys].key = neighbor_page->records.leaf_records[0].key;
            strcpy(n_page->records.leaf_records[n_page->num_keys].value, neighbor_page->records.leaf_records[0].value);
            buf_read_page(table_id, n_page->parent, (page_t*)parent_page);
	    parent_page->records.internal_records[k_prime_index].key = neighbor_page->records.leaf_records[1].key;
	    buf_write_page(table_id, n_page->parent, NULL);
        
	    for (i = 0; i < neighbor_page->num_keys - 1; i++) {
                neighbor_page->records.leaf_records[i].key = neighbor_page->records.leaf_records[i + 1].key;
                strcpy(neighbor_page->records.leaf_records[i].value, neighbor_page->records.leaf_records[i + 1].value);
            }
	}
        else {
            n_page->records.internal_records[n_page->num_keys].key = k_prime;
            n_page->records.internal_records[n_page->num_keys].child_pagenum = neighbor_page->special_pagenum;
            tmp = n_page->records.internal_records[n_page->num_keys].child_pagenum;
            buf_read_page(table_id, tmp, (page_t*)tmp_page);
	    tmp_page->parent = n;
	    buf_write_page(table_id, tmp, (page_t*)tmp_page);

	    buf_read_page(table_id, n_page->parent, (page_t*)parent_page);
            parent_page->records.internal_records[k_prime_index].key = neighbor_page->records.internal_records[0].key;
	    buf_write_page(table_id, n_page->parent, (page_t*)parent_page);

	    neighbor_page->special_pagenum = neighbor_page->records.internal_records[0].child_pagenum;

	    for (i = 0; i < neighbor_page->num_keys - 1; i++) {
                neighbor_page->records.internal_records[i].key = neighbor_page->records.internal_records[i + 1].key;
                neighbor_page->records.internal_records[i].child_pagenum = neighbor_page->records.internal_records[i + 1].child_pagenum;
            }
        }
    }

    
    n_page->num_keys++;
    neighbor_page->num_keys--;
    
    buf_write_page(table_id, n, (page_t*)n_page);
    buf_write_page(table_id, neighbor, (page_t*)neighbor_page);

    free(tmp_page);
    free(parent_page);
    free(n_page);
    free(neighbor_page);

    return root;
}


pagenum_t delete_entry( int table_id, pagenum_t root, pagenum_t n, int64_t key ) {

    pagenum_t neighbor;
    int neighbor_index;
    int k_prime_index;
    int64_t k_prime;
    int capacity, ret;
    bpt_page_t *parent_page, *neighbor_page, *n_page;
    n_page = (bpt_page_t*)malloc(sizeof(bpt_page_t));
  
    buf_read_page(table_id, n, (page_t*)n_page);
    n = remove_entry_from_node(n, n_page, key);
    buf_write_page(table_id, n, (page_t*)n_page);

    if (n == root){ 
	free(n_page);
        ret = adjust_root(table_id, root);
	return ret;
    }

    if (n_page->num_keys >= MIN_KEYS){
	free(n_page);
        return root;
    }
    
    parent_page = (bpt_page_t*)malloc(sizeof(bpt_page_t));
    buf_read_page(table_id, n_page->parent, (page_t*)parent_page);

    neighbor_index = get_neighbor_index( n, parent_page );
    k_prime_index = neighbor_index == -2 ? 0 : neighbor_index+1;
    k_prime = parent_page->records.internal_records[k_prime_index].key;
    
    if (neighbor_index == -2)
        neighbor = parent_page->records.internal_records[0].child_pagenum;
    else if (neighbor_index == -1)
        neighbor = parent_page->special_pagenum;
    else
	neighbor = parent_page->records.internal_records[neighbor_index].child_pagenum;
    
    neighbor_page = (bpt_page_t*)malloc(sizeof(bpt_page_t));
    buf_read_page(table_id, neighbor, (page_t*)neighbor_page);

    capacity = n_page->is_leaf ? LEAF_ORDER : INTERNAL_ORDER-1;

    buf_write_page(table_id, n_page->parent, NULL);
    buf_write_page(table_id, neighbor, NULL);
    /* Coalescence. */

    if (neighbor_page->num_keys + n_page->num_keys < capacity){
        ret = coalesce_nodes(table_id, root, n, neighbor, neighbor_index, k_prime);
    }
    /* Redistribution. */
    else{
        ret = redistribute_nodes(table_id, root, n, neighbor, neighbor_index, k_prime_index, k_prime);
    }

    free(n_page);
    free(neighbor_page);
    free(parent_page);
    return ret;
}



/* Master deletion function.
 */

pagenum_t delete(int table_id, pagenum_t root, int64_t key) {

    pagenum_t leaf_pagenum;
    char * key_record;
    bpt_page_t* leaf_page = (bpt_page_t*)malloc(sizeof(bpt_page_t));
    
    key_record = find(table_id, root, key, false);
    leaf_pagenum = find_leaf(table_id, root, key, false, leaf_page);
    buf_write_page(table_id, leaf_pagenum, NULL);

    if (key_record != NULL && leaf_pagenum != 0) {
        root = delete_entry( table_id, root, leaf_pagenum, key);
	return 0;
    }

    return -1;
}
