#include "buf_mgr.h"
#include "bpt.h"

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
	buf_pool[i].is_pinned = 0;
	buf_pool[i].next = NULL;
	buf_pool[i].prev = NULL;
    }
    
    buf_pool[header].next = &buf_pool[tail];
    buf_pool[tail].prev = &buf_pool[header];
    buf_frame_num = buf_num;
    return 0;
}

int buf_open_file(char* pathname){
    int ret = -1;

    for (int i = 0; i < open_table_num; i++) {
        if (!strcmp(table_info[i].pathname, pathname)){
	    if (table_info[i].table_fd != -1) return i+1;
            table_info[i].table_fd = open_file(pathname);
            return i+1;
        }
    }

    if (open_table_num == MAX_TABLE_NUM) return -1;

    ret = open_file(pathname);
    if (ret == -1) return ret;
    table_info[open_table_num].table_fd = ret;
    table_info[open_table_num].pathname = (char*)malloc(sizeof(char)*21);
    strcpy(table_info[open_table_num].pathname, pathname);
    
    return ++open_table_num;
}

pagenum_t buf_alloc_page(int table_id){
    pagenum_t new_pagenum;
    page_t* tmp_page = (page_t*)malloc(sizeof(page_t));
    header_page_t* header_page = (header_page_t*)malloc(sizeof(header_page_t));
    header_page_t* header_page2 = (header_page_t*)malloc(sizeof(header_page_t));

    fd = table_info[table_id-1].table_fd;
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

void buf_free_page(int table_id, pagenum_t pagenum){
    header_page_t *header_page = (header_page_t*)malloc(sizeof(header_page_t));
    header_page_t *header_page2 = (header_page_t*)malloc(sizeof(header_page_t));
    free_page_t* free_page = (free_page_t*)malloc(sizeof(free_page_t));

    fd = table_info[table_id-1].table_fd;
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

void buf_read_page(int table_id, pagenum_t pagenum, page_t* dest){
    buf_block_t* buf_block;
    uint32_t pool_num;

    buf_block = buf_pool[header].next;
    
    while(buf_block->next != NULL){
	// page exists in buffer
	if (buf_block->table_id == table_id && buf_block->page_num == pagenum){
	    memcpy(dest->data, buf_block->frame->data, PAGE_SIZE);
	    buf_block->is_pinned = 1;
	    buf_block->next->prev = buf_block->prev;
	    buf_block->prev->next = buf_block->next;
	    buf_block->next = buf_pool[header].next;
	    buf_block->next->prev = buf_block;
	    buf_block->prev = &buf_pool[header];
	    buf_pool[header].next = buf_block;
	 

	    return;
	}
	buf_block = buf_block->next;
    }

    fd = table_info[table_id-1].table_fd;
    file_read_page(pagenum, dest);
    // search the buffer pool except head and tail
    for (int i = 1; i <= buf_frame_num; i++){
    	//empty frame exists
	if (buf_pool[i].page_num == -1){
	    memcpy(buf_pool[i].frame->data, dest->data, PAGE_SIZE);
	    buf_pool[i].table_id = table_id;
	    buf_pool[i].page_num = pagenum;
	    buf_pool[i].is_dirty = 0;
	    buf_pool[i].is_pinned = 1;
	    buf_pool[i].next = buf_pool[header].next;
	    buf_pool[i].next->prev = &buf_pool[i];
	    buf_pool[i].prev = &buf_pool[header];
	    buf_pool[header].next = &buf_pool[i];
	    return;
	}
    }

    // there is no empty frame
    // frame replacement needed
    buf_block = buf_pool[tail].prev;
    while (buf_block->prev != NULL){
    	if (!buf_block->is_pinned) {
	    if (buf_block->is_dirty){
		int tmp = fd;
		fd = table_info[buf_block->table_id-1].table_fd;
	    	file_write_page(buf_block->page_num, buf_block->frame);
		fd = tmp;
	    }
	    memcpy(buf_block->frame->data, dest->data, PAGE_SIZE);
	    buf_block->table_id = table_id;
	    buf_block->page_num = pagenum;
	    buf_block->is_dirty = 0;
	    buf_block->is_pinned = 1;
	    buf_block->next->prev = buf_block->prev;
	    buf_block->prev->next = buf_block->next;
	    buf_block->next = buf_pool[header].next;
	    buf_block->next->prev = buf_block;
	    buf_block->prev = &buf_pool[header];
	    buf_pool[header].next = buf_block;
	   
	    return;
	}

	buf_block = buf_block->prev;
    }

    exit(0);
}

void buf_write_page(int table_id, pagenum_t pagenum, const page_t* src){
    buf_block_t* buf_block;
    buf_block = buf_pool[header].next;
    while(buf_block->next != NULL){
    	if (buf_block->table_id == table_id && buf_block->page_num == pagenum){
	    if (src != NULL){
	    	memcpy(buf_block->frame->data, src->data, PAGE_SIZE);
	    	buf_block->is_dirty = 1;
	    }
	    buf_block->is_pinned = 0;
	    break;
	}

	buf_block = buf_block->next;
    }
    return;
}

int buf_close_table(int table_id){
    int ret;
    buf_block_t* buf_block, *temp_block;
    buf_block = buf_pool[header].next;
    fd = table_info[table_id-1].table_fd;

    while(buf_block->next != NULL){
	temp_block = buf_block->next;
    	if (buf_block->table_id == table_id){
	    if (buf_block->is_pinned){
		    exit(1);
	    }
	    if (buf_block->is_dirty)
	    	file_write_page(buf_block->page_num, buf_block->frame);
            buf_block->table_id = -1;
            buf_block->page_num = -1;
            buf_block->is_dirty = 0;
            buf_block->is_pinned = 0;
	    buf_block->next->prev = buf_block->prev;
	    buf_block->prev->next = buf_block->next;
            buf_block->next = NULL;
            buf_block->prev = NULL;
	}
	buf_block = temp_block;
    }

    close_file();

    table_info[table_id-1].table_fd = -1;
    return 0;
}

int buf_shutdown_db(){
    buf_block_t* buf_block;
    buf_block = &buf_pool[header];
    while(buf_block->next != NULL){
	if (buf_block->is_pinned) exit(1);
        if (buf_block->is_dirty){
	    fd = table_info[buf_block->table_id-1].table_fd;
            file_write_page(buf_block->page_num, buf_block->frame);
	}
        free(buf_block->frame);

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

    return 0;
}
