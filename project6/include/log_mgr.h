#ifndef __LOG_MGR_H__
#define __LOG_MGR_H__

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <pthread.h>
#include <unistd.h>
#include <stdint.h>
#include <assert.h>
#include <stdlib.h>
#include "bpt.h"
#include "buf_mgr.h"
#define VALUE_SIZE 120
#define UPDATE_SIZE 288
#define COMPENSATE_SIZE 296
#define OTHER_SIZE 28
#define LOG_BUF_SIZE 200000
#define PAGELSN_OFF 24

typedef int64_t LSN_t;
typedef enum Type {BEGIN = 0, UPDATE, COMMIT, ROLLBACK, COMPENSATE} Type;
typedef struct LogRecord_t LogRecord_t;
// Log Record Structure
struct LogRecord_t{
    int logSize;
    LSN_t LSN;
    LSN_t prevLSN;
    int trx_id;
    Type type;
    int table_id;
    pagenum_t pagenum;
    int offset;
    int length;
    char old_image[VALUE_SIZE];
    char new_image[VALUE_SIZE];
    LSN_t next_undo_LSN;
} __attribute__((packed));

LSN_t currentLSN;
LSN_t flushedLSN;
int num_logs;
pthread_mutex_t log_latch;
struct LogRecord_t* log_buf[LOG_BUF_SIZE];
int log_fd;
FILE* logmsg_fp;

void init_log(char* log_path, char* logmsg_path);
LogRecord_t* make_Olog(int trx_id, Type type);
LogRecord_t* make_Ulog(int trx_id, Type type, int table_id, pagenum_t pagenum, int offset, int length, char *old_image, char *new_image);
LogRecord_t* make_Clog(int trx_id, Type type, int table_id, pagenum_t pagenum, int offset, int length, char *old_image, char *new_image, LSN_t next_undo_LSN);
void add_log(LogRecord_t* logRecord);
void flush_log();
int analysis(int** loser);
void redo(int flag, int log_num, int* open_arr);
void undo(int* loser, int loserNum, int flag, int log_num);

#endif /*__LOG_MGR_H__ */
