#include "file.h"
/* Disk Space Manager */

int fd;
/* Header Page Struct 
*  Free page number[0-7] | Root page number[8-15] | Number of pages[16-23]
*/

/* Allocate a free page */
pagenum_t file_alloc_page(){
    int ret;
    pagenum_t free_pagenum, new_free_pagenum;
    uint64_t num_pages;

	// Get free page number of the header of free page list
    ret = pread(fd, &free_pagenum, sizeof(pagenum_t), HEADER_PAGENUM);
    assert(ret != -1);

    /* If free page exists */
	// return that free page (header of free page list)
    if (free_pagenum != 0) {
	    ret = pread(fd, &new_free_pagenum, sizeof(pagenum_t), free_pagenum*PAGE_SIZE);
	    assert(ret != -1);
	    ret = pwrite(fd, &new_free_pagenum, sizeof(pagenum_t), HEADER_PAGENUM);
	    assert(ret != -1);

	    return free_pagenum;
    }

    /* else if there is no free page */
    ret = pread(fd, &num_pages, sizeof(uint64_t), HEADER_PAGENUM + 2*sizeof(pagenum_t));
    assert(ret != -1);

	// extend data file by a page size (4096 byte)
    ret = pwrite(fd, &free_pagenum, PAGE_SIZE, num_pages*PAGE_SIZE);
    assert(ret != -1);

    ++num_pages;
    ret = pwrite(fd, &num_pages, sizeof(uint64_t), HEADER_PAGENUM + 2*sizeof(pagenum_t));
    assert(ret != -1);

    return num_pages-1;
}

/* Change the page to a free page */
void file_free_page(pagenum_t pagenum){
    int ret;
    pagenum_t free_pagenum;

	// Add page to the free page list
    ret = pread(fd, &free_pagenum, sizeof(pagenum_t), HEADER_PAGENUM);
    assert(ret != -1);
    ret = pwrite(fd, &free_pagenum, sizeof(pagenum_t), pagenum*PAGE_SIZE);
    assert(ret != -1);
    ret = pwrite(fd, &pagenum, sizeof(pagenum_t), HEADER_PAGENUM);
    assert(ret != -1);
}

/* Read a page from the data file and save it to dest */
void file_read_page(pagenum_t pagenum, page_t* dest){
    int ret = pread(fd, dest, PAGE_SIZE, pagenum*PAGE_SIZE);
    assert(ret != -1);
}

/* Write a page to the data file */
void file_write_page(pagenum_t pagenum, const page_t* src){
    int ret = pwrite(fd, src, PAGE_SIZE, pagenum*PAGE_SIZE);
    assert(ret != -1);
}

/* Open existing data file using ¡®pathname¡¯ or create one if not existed */
int open_file(char* pathname){
    int ret, filesize;
    fd = open(pathname, O_RDWR | O_CREAT | O_SYNC | O_DIRECT, S_IRUSR | S_IWUSR);
    if (fd == -1) return -1;

    filesize = lseek(fd, (off_t)0, SEEK_END);
	// If data file not existed
    if (filesize == 0){
		pagenum_t free_pagenum = 0, root_pagenum = 0;
		uint64_t num_pages = 1;
		// allocate header page
		ret = pwrite(fd, &free_pagenum, PAGE_SIZE, HEADER_PAGENUM);
		assert(ret != -1);
		// initialize free page number, root page number and number of pages
		ret = pwrite(fd, &free_pagenum, sizeof(pagenum_t), HEADER_PAGENUM);
		assert(ret != -1);
		ret = pwrite(fd, &root_pagenum, sizeof(pagenum_t), HEADER_PAGENUM + sizeof(pagenum_t));
		assert(ret != -1);
		ret = pwrite(fd, &num_pages, sizeof(uint64_t), HEADER_PAGENUM + 2*sizeof(pagenum_t));
		assert(ret != -1);
    }

    return fd;
}

/* Close the data file currently in use */
int close_file(){
    int ret = close(fd);
    assert(ret != -1);
    return ret;
}
