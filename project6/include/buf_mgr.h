#ifndef __BUF_MGR_H__
#define __BUF_MGR_H__

#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include "file.h"

#define MAX_TABLE_NUM 10
#define MAX_PATH_LEN 20

typedef struct buf_block_t {
    page_t *frame;
    int table_id;
    pagenum_t page_num;
    char is_dirty;
    pthread_mutex_t page_latch;
    struct buf_block_t* next;
    struct buf_block_t* prev;
    uint32_t pool_num;
} buf_block_t;

typedef struct table_t {
    char* pathname;
    int table_fd;
} table_t;

pthread_mutex_t buf_mgr_latch;

extern int fd;
table_t table_info[MAX_TABLE_NUM];
static int open_table_num;

buf_block_t * buf_pool;
static uint32_t buf_frame_num;
uint32_t header;
uint32_t tail;

int init_buf (int buf_num);
pagenum_t buf_alloc_page(int table_id);
void buf_free_page(int table_id, pagenum_t pagenum);
void buf_read_page(int table_id, pagenum_t pagenum, page_t* dest);
void buf_write_page(int table_id, pagenum_t pagenum, const page_t* src);
int buf_close_table(int table_id);
int buf_shutdown_db();
int buf_open_file(char *pathname);

#endif /* __BUF_MGR_H__ */
