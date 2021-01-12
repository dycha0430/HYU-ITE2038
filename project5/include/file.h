#ifndef __FILE_H__
#define __FILE_H__

#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdint.h>
#include <assert.h>
#include <stdlib.h>
#include <stdbool.h>

#define PAGE_SIZE 4096
#define HEADER_PAGENUM 0

#ifndef O_DIRECT
#define O_DIRECT 000400000
#endif

typedef uint64_t pagenum_t;
typedef struct page_t {
  // in-memory page structure
  char data[PAGE_SIZE];
} page_t;

extern int fd;

pagenum_t file_alloc_page();
void file_free_page(pagenum_t pagenum);
void file_read_page(pagenum_t pagenum, page_t* dest);
void file_write_page(pagenum_t pagenum, const page_t* src);
int open_file(char* pathname);
int close_file();

#endif /* __FILE_H__ */
