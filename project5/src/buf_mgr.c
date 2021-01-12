#include "buf_mgr.h"
#include "bpt.h"

/* Initialize buffer pool with given number and buffer manager */
int init_buf (int buf_num){
    if (buf_pool != NULL) return -1;
    buf_pool = (buf_block_t*)malloc(sizeof(buf_block_t)*(buf_num+2));
    if (buf_pool == NULL) return -1;

    //buf_pool[0] is head of list
    header = 0;
    //buf_pool[buf_num+1] is tail of list
    tail = buf_num+1;
    for (int i = 0; i < buf_num+2; i++){
    	buf_pool[i].frame = (page_t*)malloc(sizeof(page_t));
		buf_pool[i].table_id = -1;
		buf_pool[i].page_num = -1;
		buf_pool[i].is_dirty = 0;
		buf_pool[i].page_latch = (pthread_mutex_t)PTHREAD_MUTEX_INITIALIZER;
		buf_pool[i].next = NULL;
		buf_pool[i].prev = NULL;
    }
    
    buf_pool[header].next = &buf_pool[tail];
    buf_pool[tail].prev = &buf_pool[header];
    buf_frame_num = buf_num;
    open_table_num = 0;

    buf_mgr_latch = (pthread_mutex_t)PTHREAD_MUTEX_INITIALIZER;
    return 0;
}

int buf_open_file(char* pathname){
    pthread_mutex_lock(&buf_mgr_latch);
    int ret = 0, table_id;

	/* If the pathname has been opened more than once, 
	 * return the same table id as before. 
	 */
    for (int i = 0; i < open_table_num; i++) {
        if (!strcmp(table_info[i].pathname, pathname)){
			if (table_info[i].table_fd != -1) return i+1;
            table_info[i].table_fd = open_file(pathname);
            return i;
        }
    }

	/* If opened table number is exceed MAX_TABLE_NUM, return -1 */
    if (open_table_num == MAX_TABLE_NUM) return -1;

	/* For this project, data file name is fixed as 'DATA1' to 'DATA10' */
    char* tmp = (char*)malloc(sizeof(char)*2);
    for (int i = 4; i < strlen(pathname); i++){
        tmp[i-4] = pathname[i];
    }
    table_id = atoi(tmp);
    free(tmp);

    ret = open_file(pathname);
    if (ret == -1) return ret;

    table_info[table_id].table_fd = ret;
    table_info[table_id].pathname = (char*)malloc(sizeof(char)*(MAX_PATH_LEN+1));
    strcpy(table_info[table_id].pathname, pathname);

    open_table_num++;
    pthread_mutex_unlock(&buf_mgr_latch);
    return table_id;
}

/* Allocate a page to disk and load into the buffer */
pagenum_t buf_alloc_page(int table_id){
    pagenum_t new_pagenum;
    page_t* tmp_page = (page_t*)malloc(sizeof(page_t));
    header_page_t* header_page = (header_page_t*)malloc(sizeof(header_page_t));
    header_page_t* header_page2 = (header_page_t*)malloc(sizeof(header_page_t));

    fd = table_info[table_id].table_fd;
    if (fd == -1) return fd;

    new_pagenum = file_alloc_page();
    buf_read_page(table_id, new_pagenum, tmp_page);

    file_read_page(HEADER_PAGENUM, (page_t*)header_page);
    buf_read_page(table_id, HEADER_PAGENUM, (page_t*)header_page2);
    
    header_page->root_pagenum = header_page2->root_pagenum;
    
    buf_write_page(table_id, HEADER_PAGENUM, (page_t*)header_page);
    buf_write_page(table_id, new_pagenum, NULL);
    
    free(header_page);
    free(header_page2);
    free(tmp_page);
    
    return new_pagenum;
}

/* Make the page with pagenum a free page */
void buf_free_page(int table_id, pagenum_t pagenum){
    header_page_t *header_page = (header_page_t*)malloc(sizeof(header_page_t));
    header_page_t *header_page2 = (header_page_t*)malloc(sizeof(header_page_t));
    free_page_t* free_page = (free_page_t*)malloc(sizeof(free_page_t));

    fd = table_info[table_id].table_fd;
    if (fd == -1) return;
    file_free_page(pagenum);

    file_read_page(HEADER_PAGENUM, (page_t*)header_page);
    buf_read_page(table_id, HEADER_PAGENUM, (page_t*)header_page2);

    buf_read_page(table_id, pagenum, (page_t*)free_page);
    file_read_page(pagenum, (page_t*)free_page);

    header_page->root_pagenum = header_page2->root_pagenum;

    buf_write_page(table_id, pagenum, (page_t*)free_page);
    buf_write_page(table_id, HEADER_PAGENUM, (page_t*)header_page);

    free(free_page);
    free(header_page);
    free(header_page2);

}

/* Load a page to buffer. 
 * Whenever a page is loaded, the page is linked to the head
 * of the buffer pool linked list to follow the LRU policy.
 */
void buf_read_page(int table_id, pagenum_t pagenum, page_t* dest){
    buf_block_t* buf_block;
    uint32_t pool_num;

    pthread_mutex_lock(&buf_mgr_latch);
    buf_block = buf_pool[header].next;
    
    while(buf_block->next != NULL){
		/* Case: the page already exists in buffer */
		if (buf_block->table_id == table_id && buf_block->page_num == pagenum){
			pthread_mutex_lock(&buf_block->page_latch);
			memcpy(dest->data, buf_block->frame->data, PAGE_SIZE);
			buf_block->next->prev = buf_block->prev;
			buf_block->prev->next = buf_block->next;
			buf_block->next = buf_pool[header].next;
			buf_block->next->prev = buf_block;
			buf_block->prev = &buf_pool[header];
			buf_pool[header].next = buf_block;
			pthread_mutex_unlock(&buf_mgr_latch);
	    
			return;
		}
		buf_block = buf_block->next;
    }

	/* Case: the page does not exist yet in buffer pool.
	 * (Rest of function body.)
	 */

    fd = table_info[table_id].table_fd;
    
	/* Case: the empty frame exists. */

    file_read_page(pagenum, dest);
    // Find the empty frame.
    for (int i = 1; i <= buf_frame_num; i++){
		if (buf_pool[i].page_num == -1){
			pthread_mutex_lock(&buf_pool[i].page_latch);
			memcpy(buf_pool[i].frame->data, dest->data, PAGE_SIZE);
			buf_pool[i].table_id = table_id;
			buf_pool[i].page_num = pagenum;
			buf_pool[i].is_dirty = 0;
			buf_pool[i].next = buf_pool[header].next;
			buf_pool[i].next->prev = &buf_pool[i];
			buf_pool[i].prev = &buf_pool[header];
			buf_pool[header].next = &buf_pool[i];
	    
			pthread_mutex_unlock(&buf_mgr_latch);
			return;
		}
    }

	/* Case: empty frame does not exist.
	 * Frame replacement is needed.
	 * According to LRU policy, replaces the least recently used page, ie the tail side.
	 */
    
    buf_block = buf_pool[tail].prev;
	
    pthread_mutex_lock(&buf_block->page_latch); // buffer block page's latch is used as a pin. 
    
	// If the page to be replaced is dirty, flush the page.
    if (buf_block->is_dirty){
		int tmp = fd;
		fd = table_info[buf_block->table_id].table_fd;
		file_write_page(buf_block->page_num, buf_block->frame);
		fd = tmp;
    }

    memcpy(buf_block->frame->data, dest->data, PAGE_SIZE);
    buf_block->table_id = table_id;
    buf_block->page_num = pagenum;
    buf_block->is_dirty = 0;
    buf_block->next->prev = buf_block->prev;
    buf_block->prev->next = buf_block->next;
    buf_block->next = buf_pool[header].next;
    buf_block->next->prev = buf_block;
    buf_block->prev = &buf_pool[header];
    buf_pool[header].next = buf_block;

    pthread_mutex_unlock(&buf_mgr_latch);	   
    return;
}

/* Write the page to a buffer. */
void buf_write_page(int table_id, pagenum_t pagenum, const page_t* src){
    buf_block_t* buf_block;
    buf_block = buf_pool[header].next;
    while(buf_block->next != NULL){
    	if (buf_block->table_id == table_id && buf_block->page_num == pagenum){
	    if (src != NULL){
	    	memcpy(buf_block->frame->data, src->data, PAGE_SIZE);
	    	buf_block->is_dirty = 1; // Set dirty bit.
	    }
	   
	    pthread_mutex_unlock(&buf_block->page_latch); // Unlock the page latch.
	    break;
	}

	buf_block = buf_block->next;
    }
    return;
}

/* Flush all pages belonging to table_id and remove from buffer. 
 * Close the data file.
 */
int buf_close_table(int table_id){
    pthread_mutex_lock(&buf_mgr_latch);
    int ret;
    buf_block_t* buf_block, *temp_block;
    buf_block = buf_pool[header].next;
    fd = table_info[table_id].table_fd;

    while(buf_block->next != NULL){
		temp_block = buf_block->next;
    	if (buf_block->table_id == table_id){
			pthread_mutex_lock(&buf_block->page_latch);
			if (buf_block->is_dirty) file_write_page(buf_block->page_num, buf_block->frame);
            buf_block->table_id = -1;
            buf_block->page_num = -1;
            buf_block->is_dirty = 0;
			buf_block->next->prev = buf_block->prev;
			buf_block->prev->next = buf_block->next;
            buf_block->next = NULL;
            buf_block->prev = NULL;
			pthread_mutex_unlock(&buf_block->page_latch);
		}
		buf_block = temp_block;
    }

    close_file();

    table_info[table_id].table_fd = -1;
    pthread_mutex_unlock(&buf_mgr_latch);
    return 0;
}

/* Flush all pages and destroy the buffer.
 * Close all data files.  
 */
int buf_shutdown_db(){
    pthread_mutex_lock(&buf_mgr_latch);
    buf_block_t* buf_block;
    buf_block = &buf_pool[header];
    while(buf_block->next != NULL){
		pthread_mutex_lock(&buf_block->page_latch);
        if (buf_block->is_dirty){
			fd = table_info[buf_block->table_id].table_fd;
            file_write_page(buf_block->page_num, buf_block->frame);
		}

        free(buf_block->frame);
		pthread_mutex_unlock(&buf_block->page_latch);
		pthread_mutex_destroy(&buf_block->page_latch);

		buf_block = buf_block->next;
    }
    
    free(buf_pool);
    buf_pool = NULL;

    for (int i = 0; i < open_table_num; i++){
    	if (table_info[i].table_fd != -1){
			fd = table_info[i].table_fd;
			close_file();
			table_info[i].table_fd = -1;
		}
    }
    pthread_mutex_unlock(&buf_mgr_latch);

    return 0;
}
