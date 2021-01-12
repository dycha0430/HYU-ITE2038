#include "file.h"

int fd = -1;

pagenum_t file_alloc_page(){
    int ret;
    pagenum_t free_pagenum, new_free_pagenum;
    uint64_t num_pages;
    ret = pread(fd, &free_pagenum, sizeof(pagenum_t), HEADER_PAGENUM);
    assert(ret != -1);
    
    // free page exists
    // return head of free page list
    if (free_pagenum != 0) {
	    ret = pread(fd, &new_free_pagenum, sizeof(pagenum_t), free_pagenum*PAGE_SIZE);
	    assert(ret != -1);
	    ret = pwrite(fd, &new_free_pagenum, sizeof(pagenum_t), HEADER_PAGENUM);
	    assert(ret != -1);
	    
	    return free_pagenum;
    }

    // there is no free page
    ret = pread(fd, &num_pages, sizeof(uint64_t), HEADER_PAGENUM + 2*sizeof(pagenum_t));
    assert(ret != -1);
    ret = pwrite(fd, &free_pagenum, PAGE_SIZE, num_pages*PAGE_SIZE);
    assert(ret != -1);
    num_pages++;
    ret = pwrite(fd, &num_pages, sizeof(uint64_t), HEADER_PAGENUM + 2*sizeof(pagenum_t));
    assert(ret != -1);

    return num_pages-1;
}

void file_free_page(pagenum_t pagenum){
    int ret;
    pagenum_t free_pagenum;

    page_t* page = (page_t*)malloc(sizeof(page_t));
    ret = pread(fd, &free_pagenum, sizeof(pagenum_t), HEADER_PAGENUM);
    assert(ret != -1);
    ret = pwrite(fd, &free_pagenum, sizeof(pagenum_t), pagenum*PAGE_SIZE);
    assert(ret != -1);
    ret = pwrite(fd, &pagenum, sizeof(pagenum_t), HEADER_PAGENUM);
    assert(ret != -1);
}

void file_read_page(pagenum_t pagenum, page_t* dest){
    int ret = pread(fd, dest, PAGE_SIZE, pagenum*PAGE_SIZE);
    assert(ret != -1);
}

void file_write_page(pagenum_t pagenum, const page_t* src){
    int ret = pwrite(fd, src, PAGE_SIZE, pagenum*PAGE_SIZE);
    assert(ret != -1);
}

bool open_file(char* pathname){
    int ret, filesize;
    if (fd != -1) {
	ret = close(fd);
	assert(ret != -1);
    }
    fd = open(pathname, O_RDWR | O_CREAT | O_SYNC | O_DIRECT, S_IRUSR | S_IWUSR);
    if (fd == -1) return false;
    filesize = lseek(fd, (off_t)0, SEEK_END);
    if (filesize == 0){
	pagenum_t free_pagenum = 0, root_pagenum = 0;
	uint64_t num_pages = 1;
	ret = pwrite(fd, &free_pagenum, PAGE_SIZE, HEADER_PAGENUM);
	assert(ret != -1);
	ret = pwrite(fd, &free_pagenum, sizeof(pagenum_t), HEADER_PAGENUM);
	assert(ret != -1);
	ret = pwrite(fd, &root_pagenum, sizeof(pagenum_t), HEADER_PAGENUM + sizeof(pagenum_t));
	assert(ret != -1);
	ret = pwrite(fd, &num_pages, sizeof(uint64_t), HEADER_PAGENUM + 2*sizeof(pagenum_t));
	assert(ret != -1);
    }
    return true;
}


