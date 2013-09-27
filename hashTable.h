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
#define FREE(ptr) free(vmPCM, ptr) 
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
    BITMAP valid[ENTRIES_PER_PAGE / BITS_PER_BYTE];
    struct pageHash* pNextPage;
    hashEntry entry[ENTRIES_PER_PAGE];
} pageHash;

typedef struct bucket {
    pageHash *firstPage;
} sBucket;

typedef struct hashTbl {
    sBucket *pBucket;
    pageHash *pFreeList;
    int HTBucketCount;
} hashTbl;

typedef struct overflowPartition {
    char *pFreeSpaceBeg;
    char *pPartitionBeg;
    int freeSpace;
} sOverflowPartition;

#ifdef VMALLOC
int initHT(Vmalloc_t *PCMStructPtr, UINT32 HTBucketCount);
#else
int initHT(UINT32 HTBucketCount);
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
void freeSpace();

#endif /* HASHTABLE_H_ */
