#include "log_mgr.h"

// Initialize Log Manager
void init_log(char* log_path, char* logmsg_path){
    log_latch = (pthread_mutex_t)PTHREAD_MUTEX_INITIALIZER;
    flushedLSN = 0;
    currentLSN = 0;
    num_logs = 0;
   
    log_fd = open(log_path, O_RDWR | O_CREAT | O_SYNC | O_DIRECT, S_IRUSR | S_IWUSR);
    logmsg_fp = fopen(logmsg_path, "w");
}


// Make BEGIN/COMMIT/ROLLBACK Log Record
LogRecord_t* make_Olog(int trx_id, Type type){
    trx_t l, *p;
    LogRecord_t* logRecord = (LogRecord_t*)malloc(sizeof(LogRecord_t));
    if (type==BEGIN || type==COMMIT || type==ROLLBACK){
	logRecord->LSN = currentLSN;
	currentLSN += OTHER_SIZE;
	logRecord->logSize = OTHER_SIZE;
    } else if (type==UPDATE){
	logRecord->LSN = currentLSN;
	currentLSN += UPDATE_SIZE;
	logRecord->logSize = UPDATE_SIZE;
    } else if (type==COMPENSATE){
	logRecord->LSN = currentLSN;
	currentLSN += COMPENSATE_SIZE;
	logRecord->logSize = COMPENSATE_SIZE;
    }

    memset(&l, 0, sizeof(trx_t));
    l.trx_id = trx_id;
    pthread_mutex_lock(&trx_table_latch);
    HASH_FIND(hh, trx_table, &l.trx_id, sizeof(int), p);
    
    logRecord->prevLSN = p->lastLSN;
    p->lastLSN = logRecord->LSN;
    
    pthread_mutex_unlock(&trx_table_latch);

    logRecord->trx_id = trx_id;
    logRecord->type = type;
    
    logRecord->table_id = -1;
    logRecord->pagenum = -1;
    logRecord->offset = -1;
    logRecord->length = 0;
    logRecord->next_undo_LSN = -1;

    return logRecord;
}

// Make UPDATE Log Record
LogRecord_t* make_Ulog(int trx_id, Type type, int table_id, pagenum_t pagenum, int offset, int length, char *old_image, char *new_image){
    LogRecord_t* logRecord = make_Olog(trx_id, type);
    logRecord->table_id = table_id;
    logRecord->pagenum = pagenum;
    logRecord->offset = offset;
    logRecord->length = length;
    memcpy(logRecord->old_image, old_image, VALUE_SIZE);
    memcpy(logRecord->new_image, new_image, VALUE_SIZE);

    return logRecord;
}

// Make COMPENSATE Log Record
LogRecord_t* make_Clog(int trx_id, Type type, int table_id, pagenum_t pagenum, int offset, int length, char *old_image, char *new_image, LSN_t next_undo_LSN){
    LogRecord_t* logRecord = make_Ulog(trx_id, type, table_id, pagenum, offset, length, old_image, new_image);
    logRecord->next_undo_LSN = next_undo_LSN;
     
    return logRecord;
}

void add_log(LogRecord_t* logRecord){
    log_buf[num_logs++] = logRecord;

    // Log buffer flush right after log buffer is full
    if (num_logs == LOG_BUF_SIZE){
    	flush_log();
    }

    LogRecord_t* logRecord2;
}

void flush_log(){
    LogRecord_t* logRecord;
    pthread_mutex_lock(&log_latch);

    for (int i = 0; i < num_logs; i++){
	logRecord = log_buf[i];
	assert(pwrite(log_fd, logRecord, logRecord->logSize, flushedLSN) != -1);

	flushedLSN = logRecord->LSN + logRecord->logSize;
	free(logRecord);
    }

    num_logs = 0;
    pthread_mutex_unlock(&log_latch);

}

int analysis(int **loser){
    LogRecord_t *logRecord = (LogRecord_t*)malloc(sizeof(LogRecord_t));
    LSN_t start = 0;
    int ret, max_trx_id = 0, size = 100, num_winners = 0, record_size, loserNum = 0;
    int* arr = (int*)calloc(size, sizeof(int));

    fprintf(logmsg_fp, "[ANALYSIS] Analysis pass start\n"); 
    while (true){
	ret = pread(log_fd, &record_size, sizeof(int), start);
        if (ret <= 0) break;
	ret = pread(log_fd, logRecord, record_size, start);
	if (ret <= 0) break;
        start += record_size;

	if (logRecord->trx_id > size){
            arr = (int*)realloc(arr, sizeof(int)*2*logRecord->trx_id);
	    memset(arr, 0, sizeof(arr));
            size  = 2*logRecord->trx_id;
        }

	if (logRecord->type == ROLLBACK || logRecord->type == COMMIT) {
	    arr[logRecord->trx_id] = 1;
	    num_winners++;
	} 

	if (max_trx_id < logRecord->trx_id) max_trx_id = logRecord->trx_id;
	if (logRecord->LSN > flushedLSN) 
	    currentLSN = logRecord->LSN + logRecord->logSize;
    }

    flushedLSN = currentLSN;
    global_trx_id = max_trx_id;

    if (max_trx_id > num_winners)
    	*loser = (int*)malloc(sizeof(int)*(max_trx_id-num_winners));
    fprintf(logmsg_fp, "[ANALYSIS] Analysis success. Winner:");
    
    for (int i = 1; i <= max_trx_id; i++){
    	if (arr[i] == 1) {
	    fprintf(logmsg_fp, " %d", i);
	} else {
	    (*loser)[loserNum++] = i;
	}
    }
    
    fprintf(logmsg_fp, ", Loser:");
    for (int i = 0; i < loserNum; i++){
	fprintf(logmsg_fp, " %d", (*loser)[i]);
    }
    fprintf(logmsg_fp, "\n");

    logRecord = NULL;
    free(logRecord);
    free(arr);

    fflush(logmsg_fp);
    return loserNum;
}

void redo(int flag, int log_num, int* open_arr){
    LogRecord_t *logRecord = (LogRecord_t*)malloc(sizeof(LogRecord_t));
    LSN_t start = 0, pageLSN;
    int ret, size, num = 0;
    page_t* page = (page_t*)malloc(sizeof(page_t));
    char* pathname = (char*)malloc(sizeof(char)*MAX_PATH_LEN);

    for (int i = 0; i < MAX_TABLE_NUM; i++) open_arr[i] = 0;
    fprintf(logmsg_fp, "[REDO] Redo pass start\n");
    
    while (true){
	// Crash during redo for test
	if (flag == 1 && (++num) == log_num) break;

	ret = pread(log_fd, &size, sizeof(int), start);
	if (ret <= 0) break;
        ret = pread(log_fd, logRecord, size, start);
	if (ret <= 0) break;
        start += size;

	if (logRecord->type == BEGIN){
	    fprintf(logmsg_fp, "LSN %lu [BEGIN] Transaction id %d\n", logRecord->LSN + logRecord->logSize, logRecord->trx_id);
	} else if (logRecord->type == COMMIT){
	    fprintf(logmsg_fp, "LSN %lu [COMMIT] Transaction id %d\n", logRecord->LSN + logRecord->logSize, logRecord->trx_id);
	} else if (logRecord->type == ROLLBACK){
	    fprintf(logmsg_fp, "LSN %lu [ROLLBACK] Transaction id %d\n", logRecord->LSN + logRecord->logSize, logRecord->trx_id);
	} else if (logRecord->type == UPDATE || logRecord->type == COMPENSATE){
	    // Open file
	    if (!open_arr[logRecord->table_id-1]){
		sprintf(pathname, "DATA%d.db", logRecord->table_id);
            	buf_open_file(pathname);
		open_arr[logRecord->table_id-1] = 1;
	    }

	    buf_read_page(logRecord->table_id, logRecord->pagenum, page);
	    
	    pageLSN = ((bpt_page_t*)page)->pageLSN;
	   
	    /* Consider redo because pageLSN is already up-to-date */ 
	    if (pageLSN >= logRecord->LSN) {
	    	fprintf(logmsg_fp, "LSN %lu [CONSIDER-REDO] Transaction id %d\n", logRecord->LSN + logRecord->logSize, logRecord->trx_id);

		buf_write_page(logRecord->table_id, logRecord->pagenum, NULL);
		continue;
	    }

	    /* Redo and update pageLSN */
	    strncpy(page->data+logRecord->offset, logRecord->new_image, logRecord->length);
	    
	    ((bpt_page_t*)page)->pageLSN = logRecord->LSN;
	    
	    buf_write_page(logRecord->table_id, logRecord->pagenum, page);
	    
	    if (logRecord->type == UPDATE)
		fprintf(logmsg_fp, "LSN %lu [UPDATE] Transaction id %d redo apply\n", logRecord->LSN + logRecord->logSize, logRecord->trx_id);
	    else if (logRecord->type == COMPENSATE)
		fprintf(logmsg_fp, "LSN %lu [CLR] next undo lsn %lu\n", logRecord->LSN + logRecord->logSize, logRecord->next_undo_LSN);
	}
    }

    page = NULL;
    free(page);
    free(pathname);
        
    if (flag != 1)
    	fprintf(logmsg_fp, "[REDO] Redo pass end\n");

    fflush(logmsg_fp);

    return;
}

void undo(int* loser, int loserNum, int flag, int log_num){
    LogRecord_t* need_to_undo = (LogRecord_t*)malloc(sizeof(LogRecord_t)*loserNum);
    LSN_t* lastLSN_list = (LSN_t*)malloc(sizeof(LSN_t)*loserNum);
    LogRecord_t maxLSN_log;
    LogRecord_t *logRecord = (LogRecord_t*)malloc(sizeof(LogRecord_t));
    LogRecord_t *newLog;
    page_t* page = (page_t*)malloc(sizeof(page_t));
    LSN_t maxLSN = 0, start = 0;
    int isLoser = 0, i, max_i = 0, record_size, ret, num = 0;

    // Choose loser's last log record
    while (true){
        ret = pread(log_fd, &record_size, sizeof(int), start);
        if (ret <= 0) break;
	ret = pread(log_fd, logRecord, record_size, start);

	for (i = 0; i < loserNum; i++){
	    if (logRecord->trx_id == loser[i]){
	        isLoser = 1;
		break;
	    }
	}
	if (isLoser){
	    lastLSN_list[i] = logRecord->LSN;
	    need_to_undo[i] = *logRecord;
	}

        start += record_size;
    }

    fprintf(logmsg_fp, "[UNDO] Undo pass start\n");
    while (true){
	// Crash during undo for test
    	if (flag == 2 && (++num) == log_num) break;

	// Choose loser log that has the biggest LSN.
	maxLSN = -1;
	
    	for (int i = 0; i < loserNum; i++){
	    if (maxLSN < need_to_undo[i].LSN) {
		max_i = i;
		maxLSN = need_to_undo[i].LSN;
	    }
	}
	
	// All Undo is done.
	if (maxLSN == -1) break;

	maxLSN_log = need_to_undo[max_i];

	/* Log to undo is COMPENSATE type */
	if (maxLSN_log.type == COMPENSATE){
	    ret = pread(log_fd, &need_to_undo[max_i], UPDATE_SIZE, maxLSN_log.prevLSN);
	    assert(ret != -1);
	    fprintf(logmsg_fp, "LSN %lu [CLR] next undo lsn %lu\n", maxLSN_log.LSN + maxLSN_log.logSize, maxLSN_log.next_undo_LSN);
        continue;
	}

	/* Log to undo is BEGIN type */
	if (maxLSN_log.type == BEGIN){
	    need_to_undo[max_i].LSN = -1;
	    continue;
	}

	/* Log to undo is UPDATE type */
	buf_read_page(maxLSN_log.table_id, maxLSN_log.pagenum, page);
	/* Undo and add compensate log */
	strncpy(page->data+maxLSN_log.offset, maxLSN_log.old_image, maxLSN_log.length);
	
	// Make compensate log record for undo phase
	newLog = (LogRecord_t*)malloc(sizeof(LogRecord_t));
	newLog->logSize = COMPENSATE_SIZE;
	newLog->LSN = currentLSN;
	currentLSN += COMPENSATE_SIZE;
	newLog->prevLSN = lastLSN_list[max_i];
	lastLSN_list[max_i] = newLog->LSN;
	newLog->trx_id = maxLSN_log.trx_id;
	newLog->type = COMPENSATE;
	newLog->table_id = maxLSN_log.table_id;
	newLog->pagenum = maxLSN_log.pagenum;
	newLog->offset = maxLSN_log.offset;
	newLog->length = maxLSN_log.length;
	memcpy(newLog->old_image, maxLSN_log.new_image, VALUE_SIZE);
        memcpy(newLog->new_image, maxLSN_log.old_image, VALUE_SIZE);
	newLog->next_undo_LSN = maxLSN_log.prevLSN;
	
	add_log(newLog);

	// Update pageLSN
        ((bpt_page_t*)page)->pageLSN = newLog->LSN;
        buf_write_page(maxLSN_log.table_id, maxLSN_log.pagenum, page);

	// If all log of the transaction finished undo
	ret = pread(log_fd, logRecord, OTHER_SIZE, maxLSN_log.prevLSN);
	
        if (logRecord->type == BEGIN){
            need_to_undo[max_i].LSN = -1;

            // Make a ROLLBACK log record 
            newLog = (LogRecord_t*)malloc(sizeof(LogRecord_t));
            newLog->logSize = OTHER_SIZE;
	    newLog->LSN = currentLSN;
            currentLSN += OTHER_SIZE;
            newLog->prevLSN = lastLSN_list[max_i];
            lastLSN_list[max_i] = newLog->LSN;
            newLog->trx_id = maxLSN_log.trx_id;
            newLog->type = ROLLBACK;

            add_log(newLog);
        } else if (logRecord->type == UPDATE){
            ret = pread(log_fd, &need_to_undo[max_i], UPDATE_SIZE, maxLSN_log.prevLSN);
            assert(ret != -1);
        }

	fprintf(logmsg_fp, "LSN %lu [UPDATE] Transaction id %d undo apply\n", maxLSN_log.LSN + maxLSN_log.logSize, maxLSN_log.trx_id);
    }

    if (flag != 2)
    	fprintf(logmsg_fp, "[UNDO] Undo pass end\n");
    
    fflush(logmsg_fp);
}
