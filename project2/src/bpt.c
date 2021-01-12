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

bool index_open(char * pathname){
    return open_file(pathname);
}

pagenum_t find_root_pagenum(){
    pagenum_t root_pagenum;
    header_page_t* header_page;
    header_page = (header_page_t*)malloc(sizeof(header_page_t));
    
    file_read_page(HEADER_PAGENUM, (page_t*)header_page);
    root_pagenum = header_page->root_pagenum;
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



int path_to_root( pagenum_t root, pagenum_t child ) {
    int length = 0;
    bpt_page_t * c = (bpt_page_t*)malloc(sizeof(bpt_page_t));
    file_read_page(child, (page_t*)c);

    while (c->parent != root) {
        file_read_page(c->parent, (page_t*)c);
        length++;
    }
    return length + 1;
}

void print_tree( pagenum_t root ) {

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
	file_read_page(n, (page_t*)n_page);
	file_read_page(n_page->parent, (page_t*)parent_page);
        if (n_page->parent != 0 && n == parent_page->special_pagenum) {
	    if (n == root) new_rank = 0;
	    else new_rank = path_to_root( root, n );
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



pagenum_t find_leaf( pagenum_t root, int64_t key, bool verbose ) {
    int i = 0, tmp = 0;
    bpt_page_t * c = (bpt_page_t*)malloc(sizeof(bpt_page_t));
    if (root == 0) {
        if (verbose)
            printf("Empty tree.\n");
        return 0;
    }

    pagenum_t next_pagenum;
    file_read_page(root, (page_t*)c);
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
	if (i == 0) next_pagenum = c->special_pagenum;
	else next_pagenum = c->records.internal_records[i-1].child_pagenum;
	
	file_read_page(next_pagenum, (page_t*)c);
    }
 
    if (verbose) {
        printf("Leaf [");
        for (i = 0; i < c->num_keys - 1; i++)
            printf("%ld ", c->records.leaf_records[i].key);
        printf("%ld] ->\n", c->records.leaf_records[i].key);
    }
    free(c);
    return next_pagenum;
}


char * find( pagenum_t root, int64_t key, bool verbose ) {
    int i = 0;
    if (root == 0) return NULL;
    bpt_page_t * c = (bpt_page_t*)malloc(sizeof(bpt_page_t));
    pagenum_t leaf_pagenum = find_leaf( root, key, verbose);
    file_read_page(leaf_pagenum, (page_t*)c);

    for (i = 0; i < c->num_keys; i++)
        if (c->records.leaf_records[i].key == key) break;
    if (i == c->num_keys)
        return NULL;
    else
        return c->records.leaf_records[i].value;
}


int cut( int length ) {
    if (length % 2 == 0)
        return length/2;
    else
        return length/2 + 1;
}


// INSERTION


pagenum_t make_page( void ) {
    bpt_page_t * new_page = (bpt_page_t*)malloc(sizeof(bpt_page_t));
    pagenum_t new_pagenum = file_alloc_page();

    new_page->is_leaf = 0;
    new_page->num_keys = 0;
    new_page->parent = 0;
    new_page->special_pagenum = 0;

    file_write_page(new_pagenum, (page_t*)new_page);
    free(new_page);

    return new_pagenum;
}


pagenum_t make_leaf( void ) {
    pagenum_t leaf_pagenum = make_page();
    bpt_page_t * leaf = (bpt_page_t*)malloc(sizeof(bpt_page_t));
    
    file_read_page(leaf_pagenum, (page_t*)leaf);
    leaf->is_leaf = 1;
    file_write_page(leaf_pagenum, (page_t*)leaf);
    free(leaf);

    return leaf_pagenum;
}


int get_left_index(pagenum_t parent, pagenum_t left) {

    bpt_page_t* parent_page = (bpt_page_t*)malloc(sizeof(bpt_page_t));
    int left_index = 0;
    file_read_page(parent, (page_t*)parent_page);
    
    if (parent_page->special_pagenum == left) return -1;
    while (left_index < parent_page->num_keys && 
            parent_page->records.internal_records[left_index].child_pagenum != left)
        left_index++;
    return left_index;
}


pagenum_t insert_into_leaf( pagenum_t leaf, int64_t key, char * value ) {

    int i, insertion_point;
    bpt_page_t* leaf_page = (bpt_page_t*)malloc(sizeof(bpt_page_t));
    
    file_read_page(leaf, (page_t*)leaf_page);
    insertion_point = 0;
    
    while (insertion_point < leaf_page->num_keys && leaf_page->records.leaf_records[insertion_point].key < key)
        insertion_point++;

    for (i = leaf_page->num_keys; i > insertion_point; i--) {
        leaf_page->records.leaf_records[i].key = leaf_page->records.leaf_records[i-1].key;
        strcpy(leaf_page->records.leaf_records[i].value, leaf_page->records.leaf_records[i-1].value);
    }
    leaf_page->records.leaf_records[insertion_point].key = key;
    strcpy(leaf_page->records.leaf_records[insertion_point].value, value);
    leaf_page->num_keys++;
    file_write_page(leaf, (page_t*)leaf_page);
    
    free(leaf_page);
    return leaf;
}


pagenum_t insert_into_leaf_after_splitting(pagenum_t root, pagenum_t leaf, int64_t key, char * value) {

    bpt_page_t* leaf_page = (bpt_page_t*)malloc(sizeof(bpt_page_t));
    bpt_page_t* new_leaf_page = (bpt_page_t*)malloc(sizeof(bpt_page_t));
    pagenum_t new_leaf;
    int64_t * temp_keys;
    char ** temp_values;
    int64_t new_key;
    int insertion_index, split, i, j;

    new_leaf = make_leaf();
    file_read_page(leaf, (page_t*)leaf_page);
    file_read_page(new_leaf, (page_t*)new_leaf_page);

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

    for (i = split, j = 0; i < LEAF_ORDER - 1; i++, j++) {
        strcpy(new_leaf_page->records.leaf_records[j].value, temp_values[i]);
        new_leaf_page->records.leaf_records[j].key = temp_keys[i];
        new_leaf_page->num_keys++;
    }

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

    file_write_page(new_leaf, (page_t*)new_leaf_page);
    file_write_page(leaf, (page_t*)leaf_page);

    return insert_into_parent(root, leaf, new_key, new_leaf);
}


pagenum_t insert_into_node(pagenum_t root, pagenum_t n, 
        int left_index, int64_t key, pagenum_t right) {
    int i;
    bpt_page_t* n_page = (bpt_page_t*)malloc(sizeof(bpt_page_t));
    file_read_page(n, (page_t*)n_page);

    for (i = n_page->num_keys-1; i > left_index; i--) {
        n_page->records.internal_records[i+1].child_pagenum = n_page->records.internal_records[i].child_pagenum;
        n_page->records.internal_records[i+1].key = n_page->records.internal_records[i].key;
    }
    n_page->records.internal_records[left_index+1].child_pagenum = right;
    n_page->records.internal_records[left_index+1].key = key;
    n_page->num_keys++;
    file_write_page(n, (page_t*)n_page);

    return root;
}


pagenum_t insert_into_node_after_splitting(pagenum_t root, pagenum_t old_pagenum, int left_index, int64_t key, pagenum_t right) {

    int i, j, split;
    int64_t k_prime;
    pagenum_t new_pagenum, child;
    int64_t * temp_keys;
    pagenum_t * temp_values;
    bpt_page_t* old_page = (bpt_page_t*)malloc(sizeof(bpt_page_t));
    bpt_page_t* new_page = (bpt_page_t*)malloc(sizeof(bpt_page_t));
    bpt_page_t* child_page = (bpt_page_t*)malloc(sizeof(bpt_page_t));

    temp_values = (pagenum_t *)malloc( (INTERNAL_ORDER) * sizeof(pagenum_t) );
    if (temp_values == NULL) {
        perror("Temporary pointers array for splitting nodes.");
        exit(EXIT_FAILURE);
    }
    
    temp_keys = (int64_t *)malloc( INTERNAL_ORDER * sizeof(int64_t) );
    if (temp_keys == NULL) {
        perror("Temporary keys array for splitting nodes.");
        exit(EXIT_FAILURE);
    }

    
    file_read_page(old_pagenum, (page_t*)old_page);
    for (i = 0, j = 0; i < old_page->num_keys; i++, j++) {
        if (j == left_index+1) j++;
        temp_values[j] = old_page->records.internal_records[i].child_pagenum;
	temp_keys[j] = old_page->records.internal_records[i].key;
    }

    
    temp_values[left_index] = right;
    temp_keys[left_index] = key;

    
    split = cut(INTERNAL_ORDER);
    new_pagenum = make_page();
    old_page->num_keys = 0;
    for (i = 0; i < split - 1; i++) {
        old_page->records.internal_records[i].child_pagenum = temp_values[i];
        old_page->records.internal_records[i].key = temp_keys[i];
        old_page->num_keys++;
    }
    
   
    new_page->special_pagenum = temp_values[i];
    k_prime = temp_keys[split - 1];
    for (++i, j = 0; i < INTERNAL_ORDER-1; i++, j++) {
        new_page->records.internal_records[i].child_pagenum = temp_values[i];
        new_page->records.internal_records[i].key = temp_keys[i];
        new_page->num_keys++;
    }

    free(temp_values);
    free(temp_keys);
    new_page->parent = old_page->parent;
    for (i = 0; i < new_page->num_keys; i++) {
        child = new_page->records.internal_records[i].child_pagenum;
        file_read_page(child, (page_t*)child_page);
	child_page->parent = new_pagenum;
	file_write_page(child, (page_t*)child_page);
    }

    file_write_page(old_pagenum, (page_t*)old_page);
    file_write_page(new_pagenum, (page_t*)new_page);
    

    return insert_into_parent(root, old_pagenum, k_prime, new_pagenum);
}


pagenum_t insert_into_parent(pagenum_t root, pagenum_t left, int64_t key, pagenum_t right) {

    int left_index;
    pagenum_t parent;
    bpt_page_t* left_page = (bpt_page_t*)malloc(sizeof(bpt_page_t));
    bpt_page_t* parent_page = (bpt_page_t*)malloc(sizeof(bpt_page_t));
    file_read_page(left, (page_t*)left_page);
    parent = left_page->parent;

    if (parent == 0)
        return insert_into_new_root(left, key, right);


    file_read_page(parent, (page_t*)parent_page);
    left_index = get_left_index(parent, left);


    if (parent_page->num_keys < INTERNAL_ORDER - 1)
        return insert_into_node(root, parent, left_index, key, right);


    return insert_into_node_after_splitting(root, parent, left_index, key, right);
}


pagenum_t insert_into_new_root(pagenum_t left, int64_t key, pagenum_t right) {

    pagenum_t root = make_page();
    bpt_page_t* root_page = (bpt_page_t*)malloc(sizeof(bpt_page_t));
    bpt_page_t* left_page = (bpt_page_t*)malloc(sizeof(bpt_page_t));
    bpt_page_t* right_page = (bpt_page_t*)malloc(sizeof(bpt_page_t));
    header_page_t* header_page = (header_page_t*)malloc(sizeof(header_page_t));

    file_read_page(root, (page_t*)root_page);
    file_read_page(left, (page_t*)left_page);
    file_read_page(right, (page_t*)right_page);
    file_read_page(HEADER_PAGENUM, (page_t*)header_page);

    root_page->records.internal_records[0].key = key;
    root_page->special_pagenum = left;
    root_page->records.internal_records[0].child_pagenum = right;
    root_page->num_keys++;
    root_page->parent = 0;
    left_page->parent = root;
    right_page->parent = root;
    header_page->root_pagenum = root;

    file_write_page(root, (page_t*)root_page);
    file_write_page(left, (page_t*)left_page);
    file_write_page(right, (page_t*)right_page);
    file_write_page(HEADER_PAGENUM, (page_t*)header_page);

    return root;
}


pagenum_t start_new_tree(int64_t key, char * value) {
    pagenum_t root = make_leaf();
    header_page_t* header_page = (header_page_t*)malloc(sizeof(header_page_t));
    bpt_page_t* root_page = (bpt_page_t*)malloc(sizeof(bpt_page_t));
    
    file_read_page(HEADER_PAGENUM, (page_t*)header_page);
    file_read_page(root, (page_t*)root_page);

    root_page->records.leaf_records[0].key = key;
    strcpy(root_page->records.leaf_records[0].value, value);
    root_page->num_keys++;
    header_page->root_pagenum = root;

    file_write_page(root, (page_t*)root_page);
    file_write_page(HEADER_PAGENUM, (page_t*)header_page);

    free(header_page);
    free(root_page);
    return root;
}



/* Master insertion function.*/
pagenum_t insert( pagenum_t root, int64_t key, char * value ) {

    pagenum_t leaf_pagenum;
    bpt_page_t* leaf = (bpt_page_t*)malloc(sizeof(bpt_page_t));


    if (find(root, key, false) != NULL)
        return 0;


    if (root == 0) 
        return start_new_tree(key, value);


    leaf_pagenum = find_leaf(root, key, false);


    file_read_page(leaf_pagenum, (page_t*)leaf);
    if (leaf->num_keys < LEAF_ORDER - 1) {
        leaf_pagenum = insert_into_leaf(leaf_pagenum, key, value);
	free(leaf);
        return root;
    }

    free(leaf);
    return insert_into_leaf_after_splitting(root, leaf_pagenum, key, value);
}




// DELETION.

int get_neighbor_index( pagenum_t n ) {

    int i;
    bpt_page_t* n_page = (bpt_page_t*)malloc(sizeof(bpt_page_t));
    bpt_page_t* parent_page = (bpt_page_t*)malloc(sizeof(bpt_page_t));
    file_read_page(n, (page_t*)n_page);
    file_read_page(n_page->parent, (page_t*)parent_page);

    
    if (parent_page->special_pagenum == n) return -2;
    for (i = 0; i < parent_page->num_keys; i++)
        if (parent_page->records.internal_records[i].child_pagenum == n)
            return i-1;

    // Error state.
    exit(EXIT_FAILURE);
}

pagenum_t remove_entry_from_node(pagenum_t n, int64_t key) {

    int i, num_pointers;
    bpt_page_t* n_page = (bpt_page_t*)malloc(sizeof(bpt_page_t));
    file_read_page(n, (page_t*)n_page);

    
    i = 0;
    if (n_page->is_leaf){
    	while (n_page->records.leaf_records[i].key != key) i++;
    	for (++i; i < n_page->num_keys; i++){
       	    n_page->records.leaf_records[i-1].key = n_page->records.leaf_records[i].key;
	    strcpy(n_page->records.leaf_records[i-1].value, n_page->records.leaf_records[i].value);
	}
    } else {
	i = 0;
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

    file_write_page(n, (page_t*)n_page);

    return n;
}


pagenum_t adjust_root(pagenum_t root) {

    pagenum_t new_root;
    bpt_page_t* new_root_page, *root_page;
    header_page_t* header_page = (header_page_t*)malloc(sizeof(header_page_t));
    root_page = (bpt_page_t*)malloc(sizeof(bpt_page_t));

    file_read_page(root, (page_t*)root_page);

    if (root_page->num_keys > 0)
        return root;


    if (!root_page->is_leaf) {
        bpt_page_t* new_root_page = (bpt_page_t*)malloc(sizeof(bpt_page_t));
	new_root = root_page->special_pagenum;
        file_read_page(new_root, (page_t*)new_root_page);
        new_root_page->parent = 0;
	file_write_page(new_root, (page_t*)new_root_page);
    }

    
    else{
        new_root = 0;
    }

    file_read_page(0, (page_t*)header_page);
    header_page->root_pagenum = new_root;
    file_write_page(0, (page_t*)header_page);
    file_free_page(root);
    free(new_root_page);
    free(root_page);
    free(header_page);

    return new_root;
}


pagenum_t coalesce_nodes(pagenum_t root, pagenum_t n, pagenum_t neighbor, int neighbor_index, int64_t k_prime) {

    int i, j, neighbor_insertion_index, n_end;
    pagenum_t tmp, parent;
    bpt_page_t* n_page, *neighbor_page, *tmp_page;
    n_page = (bpt_page_t*)malloc(sizeof(bpt_page_t));
    neighbor_page = (bpt_page_t*)malloc(sizeof(bpt_page_t));
    tmp_page = (bpt_page_t*)malloc(sizeof(bpt_page_t));



    if (neighbor_index == -2) {
        tmp = n;
        n = neighbor;
        neighbor = tmp;
    }

    file_read_page(n, (page_t*)n_page);
    file_read_page(neighbor, (page_t*)neighbor_page);
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


        for (i = 0; i < neighbor_page->num_keys; i++) {
            tmp = neighbor_page->records.internal_records[i].child_pagenum;
	    file_read_page(tmp, (page_t*)tmp_page);
	    tmp_page->parent = neighbor;
            file_write_page(tmp, (page_t*)tmp_page);
        }

	file_write_page(neighbor, (page_t*)neighbor_page);
	file_free_page(n);
    }


    else {
        for (i = neighbor_insertion_index, j = 0; j < n_page->num_keys; i++, j++) {
            neighbor_page->records.leaf_records[i].key = n_page->records.leaf_records[i].key;
            strcpy(neighbor_page->records.leaf_records[i].value, n_page->records.leaf_records[j].value);
            
	    neighbor_page->num_keys++;
        }
        neighbor_page->special_pagenum = n_page->special_pagenum;
	file_write_page(neighbor, (page_t*)neighbor_page);
	file_free_page(n);
    }

    root = delete_entry(root, parent, k_prime);
    free(n_page);
    free(neighbor_page);
    free(tmp_page); 
    return root;
}


pagenum_t redistribute_nodes(pagenum_t root, pagenum_t n, pagenum_t neighbor, int neighbor_index, int k_prime_index, int64_t k_prime) {  

    int i;
    pagenum_t tmp;
    bpt_page_t* tmp_page, *n_page, *neighbor_page, *parent_page;
    tmp_page = (bpt_page_t*)malloc(sizeof(bpt_page_t));
    n_page = (bpt_page_t*)malloc(sizeof(bpt_page_t));
    neighbor_page = (bpt_page_t*)malloc(sizeof(bpt_page_t));
    parent_page = (bpt_page_t*)malloc(sizeof(bpt_page_t));



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
	    
	    file_read_page(tmp, (page_t*)tmp_page);
            tmp_page->parent = n;
	    file_write_page(tmp, (page_t*)tmp_page);

            n_page->records.internal_records[0].key = k_prime;
	    file_read_page(n_page->parent, (page_t*)parent_page);
            parent_page->records.internal_records[k_prime_index].key = neighbor_page->records.internal_records[neighbor_page->num_keys - 1].key;
	    file_write_page(n_page->parent, (page_t*)parent_page);
        }
        else {
            strcpy(n_page->records.leaf_records[0].value, neighbor_page->records.leaf_records[neighbor_page->num_keys - 1].value);
            neighbor_page->records.leaf_records[neighbor_page->num_keys - 1].value[0] = '\0';
            n_page->records.leaf_records[0].key = neighbor_page->records.leaf_records[neighbor_page->num_keys - 1].key;
            
	    file_read_page(n_page->parent, (page_t*)parent_page);
            parent_page->records.leaf_records[k_prime_index].key = n_page->records.leaf_records[0].key;
            file_write_page(n_page->parent, (page_t*)parent_page);
        }
    }


    else {  
        if (n_page->is_leaf) {
            n_page->records.leaf_records[n_page->num_keys].key = neighbor_page->records.leaf_records[0].key;
            strcpy(n_page->records.leaf_records[n_page->num_keys].value, neighbor_page->records.leaf_records[0].value);
            file_read_page(n_page->parent, (page_t*)parent_page);
	    parent_page->records.internal_records[k_prime_index].key = neighbor_page->records.leaf_records[1].key;
        
	    for (i = 0; i < neighbor_page->num_keys - 1; i++) {
                neighbor_page->records.leaf_records[i].key = neighbor_page->records.leaf_records[i + 1].key;
                strcpy(neighbor_page->records.leaf_records[i].value, neighbor_page->records.leaf_records[i + 1].value);
            }
	}
        else {
            n_page->records.internal_records[n_page->num_keys].key = k_prime;
            n_page->records.internal_records[n_page->num_keys].child_pagenum = neighbor_page->special_pagenum;
            tmp = n_page->records.internal_records[n_page->num_keys].child_pagenum;
            file_read_page(tmp, (page_t*)tmp_page);
	    tmp_page->parent = n;
	    file_write_page(tmp, (page_t*)tmp_page);

	    file_read_page(n_page->parent, (page_t*)parent_page);
            parent_page->records.internal_records[k_prime_index].key = neighbor_page->records.internal_records[0].key;
	    file_write_page(n_page->parent, (page_t*)parent_page);

	    neighbor_page->special_pagenum = neighbor_page->records.internal_records[0].child_pagenum;

	    for (i = 0; i < neighbor_page->num_keys - 1; i++) {
                neighbor_page->records.internal_records[i].key = neighbor_page->records.internal_records[i + 1].key;
                neighbor_page->records.internal_records[i].child_pagenum = neighbor_page->records.internal_records[i + 1].child_pagenum;
            }
        }
    }

    
    n_page->num_keys++;
    neighbor_page->num_keys--;
    file_write_page(n, (page_t*)n_page);
    file_write_page(neighbor, (page_t*)neighbor_page);

    free(n_page);
    free(neighbor_page);
    free(tmp_page);
    free(parent_page);
    return root;
}


pagenum_t delete_entry( pagenum_t root, pagenum_t n, int64_t key ) {

    pagenum_t neighbor;
    int neighbor_index;
    int k_prime_index;
    int64_t k_prime;
    int capacity;
    bpt_page_t* n_page, *parent_page, *neighbor_page;
    n_page = (bpt_page_t*)malloc(sizeof(bpt_page_t));

    n = remove_entry_from_node(n, key);


    if (n == root) 
        return adjust_root(root);


    file_read_page(n, (page_t*)n_page);
    if (n_page->num_keys >= MIN_KEYS)
        return root;

    
    parent_page = (bpt_page_t*)malloc(sizeof(bpt_page_t));
    file_read_page(n_page->parent, (page_t*)parent_page);

    neighbor_index = get_neighbor_index( n );
    k_prime_index = neighbor_index == -2 ? 0 : neighbor_index+1;
    k_prime = parent_page->records.internal_records[k_prime_index].key;
    
    if (neighbor_index == -2)
        neighbor = parent_page->records.internal_records[0].child_pagenum;
    else if (neighbor_index == -1)
        neighbor = parent_page->special_pagenum;
    else
	neighbor = parent_page->records.internal_records[neighbor_index].child_pagenum;
    
    neighbor_page = (bpt_page_t*)malloc(sizeof(bpt_page_t));
    file_read_page(neighbor, (page_t*)neighbor_page);

    capacity = n_page->is_leaf ? LEAF_ORDER : INTERNAL_ORDER-1;

    /* Coalescence. */

    if (neighbor_page->num_keys + n_page->num_keys < capacity)
        return coalesce_nodes(root, n, neighbor, neighbor_index, k_prime);

    /* Redistribution. */

    else
        return redistribute_nodes(root, n, neighbor, neighbor_index, k_prime_index, k_prime);
}



/* Master deletion function.
 */
pagenum_t delete(pagenum_t root, int64_t key) {

    pagenum_t key_leaf;
    char * key_record;
    
    key_record = find(root, key, false);
    key_leaf = find_leaf(root, key, false);
   
    if (key_record != NULL && key_leaf != 0) {
        root = delete_entry(root, key_leaf, key);
	return root;
    }

    return 0;
}

