/*
 * hashTable.h
 *
 *  Created on: 10-Apr-2013
 *      Author: spec
 */

#include "constants.h"
#ifndef HASHTABLE_H_
#define HASHTABLE_H_

#ifdef VMALLOC
#include "vmalloc.h"
#define MALLOC(size) vmalloc(vmPCM, size) 
#define FREE(ptr) vmfree(vmPCM, ptr) 
#else
#define MALLOC(size) malloc(size)
#define FREE(ptr) free(ptr)
#endif

typedef struct grpRecHdr {
    float aggregate;
} grpRecHdr;

typedef struct hashEntry {
    void *ptr;
    UINT8 hash;
} hashEntry;

typedef struct pageHash {
    BITMAP *valid;
    struct pageHash* pNextPage;
    hashEntry *entries;
} pageHash;

typedef struct bucket {
    pageHash *firstPage;
} sBucket;

typedef struct hashTbl {
    sBucket *pBucket;
    pageHash *pFreeList;
    UINT32 HTBucketCount;
    UINT8 entriesPerPage;
} hashTbl;

typedef struct overflowPartition {
    char *pFreeSpaceBeg;
    char *pPartitionBeg;
    int freeSpace;
} sOverflowPartition;

#ifdef VMALLOC
int initHT(Vmalloc_t *PCMStructPtr, UINT32 HTBucketCount, UINT8 entriesPerPage);
#else
int initHT(UINT32 HTBucketCount, UINT8 entriesPerPage);
#endif
void insertHashEntry(void* tuple, char* attr, UINT32 attrSize);
UINT8 searchHashEntry(char* attr, UINT32 attrSize, void** returnEntryPtr,
        void **pLastPage,
        void **pLastIndex);

void printMemoryRec();
void printPCMRec();
void testWriteout();
void calculateFinalTally();
int initOverflowPartitions();
void processNewTupleByHashing(char* pAttr, float aggColValue, int pass);
void freeHashTable();

#endif /* HASHTABLE_H_ */
