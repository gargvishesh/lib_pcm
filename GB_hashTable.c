/*
 * hashTable.c
 *
 *  Created on: 10-Apr-2013
 *      Author: spec
 */
#include "GB_hashTable.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <assert.h>

#include "constants.h"
#include "GeneralHashFunctions.h"

#ifdef VMALLOC
#include <vmalloc.h>
#endif

GB_hashTbl *sHT;
UINT32 gPageCount;
UINT32 lastVictim;
UINT32 finalCount;
UINT8 gNumPasses;
UINT32 *a;
/*Debug*/
char recordNo;
/**/

#ifdef VMALLOC
Vmalloc_t *vmDRAM;
Vmalloc_t *vmPCM;
#endif
func_getBucketId GB_getBucketId;
func_getHashValue GB_getHashValue;
func_compareRID GB_compareRID;

/*Initialize a new page for Hash Table*/
void GB_initNewPage(GB_pageHash *pNewPage) {
    memset(pNewPage->valid, 0, sHT->entriesPerPage/BITS_PER_BYTE);
    pNewPage->pNextPage = NULL;
}
//Get Free Page from Free List
GB_pageHash* GB_getFreePageFromFreeList() {
	GB_pageHash *pNewPage;
        if (sHT->pFreeList) {
		pNewPage = sHT->pFreeList;
		sHT->pFreeList = sHT->pFreeList->pNextPage;
		GB_initNewPage(pNewPage);
		return pNewPage;
	}
	//Free List Now permanently empty. Point to diff func
	return NULL;
}
void GB_changeRIDComparison(func_compareRID gb_compareRID){
    GB_compareRID = gb_compareRID;
}
#ifdef VMALLOC
int GB_initHT(Vmalloc_t *PCMStructPtr, UINT32 HTBucketCount, UINT8 entriesPerPage, 
        UINT32 pageCount, UINT32 numPasses, func_getBucketId gb_getBucketId, func_getHashValue gb_getHashValue, func_compareRID gb_compareRID) {
#else
int GB_initHT(UINT32 HTBucketCount, UINT8 entriesPerPage, 
        UINT32 pageCount, UINT32 numPasses, func_getBucketId gb_getBucketId, func_getHashValue gb_getHashValue, func_compareRID gb_compareRID) {
#endif
    //Init Main HT structure
    
#ifdef VMALLOC
    vmPCM = PCMStructPtr;
#endif
    gPageCount = pageCount;
    gNumPasses = numPasses;
    GB_compareRID = gb_compareRID;
    GB_getBucketId = gb_getBucketId;
    GB_getHashValue = gb_getHashValue;
    
    sHT = (GB_hashTbl*) MALLOC(sizeof (sHT));
    assert(sHT != NULL);
    memset(sHT, 0, sizeof (sHT));


    sHT->pBucket = (GB_sBucket*) MALLOC(HTBucketCount * sizeof (GB_sBucket));
    assert(sHT->pBucket != NULL);
    
    memset(sHT->pBucket, 0, HTBucketCount * sizeof (GB_sBucket));
    sHT->HTBucketCount = HTBucketCount;
    sHT->entriesPerPage = entriesPerPage;
	/*Initalize function pointer getFreePage to point to
	 * GB_getFreePageFromFreeList. Later free list permanently empty, so then change
	 * pointer to getFreePageByEviction*/
	printf("gPageCount %d\n",gPageCount);
        UINT32 pageNo;
        GB_pageHash *pNewPage,
                 *pCurrPage;
        
		//Init Free List
        for (pageNo = 0; pageNo < gPageCount; pageNo++) {

        pNewPage = (GB_pageHash*) MALLOC(sizeof (GB_pageHash));
        assert(pNewPage != NULL);
        //printf("gPageCount: %d\n", gPageCount);
        pNewPage->valid = (BITMAP*) MALLOC(sHT->entriesPerPage / BITS_PER_BYTE);
        assert(pNewPage->valid != NULL);
        //pNewPage->entries = (GB_hashEntry*) MALLOC(sizeof (GB_hashEntry) * sHT->entriesPerPage);
        pNewPage->entries = (GB_hashEntryWithoutAgg*) MALLOC(sizeof (GB_hashEntryWithoutAgg) * sHT->entriesPerPage);
        assert(pNewPage->entries != NULL);
        GB_initNewPage(pNewPage);
        if (pageNo == 0) {
            sHT->pFreeList = pNewPage;
        } else {
            pCurrPage->pNextPage = pNewPage;
        }
        pCurrPage = pNewPage;

    }
    return 0;
}

void GB_insertHashEntry(UINT32 rid, UINT8 aggValue) {
    GB_pageHash *pCurrPage;
    UINT16 index = 0;
    UINT16 bucketId = GB_getBucketId(rid);
    UINT8 hashValue = GB_getHashValue(rid);
    if (!sHT->pBucket[bucketId].firstPage) {
        GB_pageHash * pNewPage = GB_getFreePageFromFreeList();
        assert(pNewPage != NULL);
        pNewPage->pNextPage = sHT->pBucket[bucketId].firstPage;
        sHT->pBucket[bucketId].firstPage = pNewPage;
        /*Write Code to Insert a Record here. Remember to set valid flag*/
        /*Since an entirely new page, set the LSB indicating first record*/
        SET_VALID(pNewPage->valid, 0);
        pNewPage->entries[0].rid = rid;
        pNewPage->entries[0].hash = hashValue;
        //pNewPage->entries[0].aggValue = aggValue;
        //printf("Inserted [Bucket:%d HashVal%d]\n", bucketId, hashValue);
        return;
    } else {
        pCurrPage = sHT->pBucket[bucketId].firstPage;
        for (index = 0; index < sHT->entriesPerPage; index++) {
            if (CHECK_VALID(pCurrPage->valid, index) == 0) {
                SET_VALID(pCurrPage->valid, index);
                pCurrPage->entries[index].rid = rid;
                pCurrPage->entries[index].hash = hashValue;
                //pCurrPage->entries[index].aggValue = aggValue;
                //printf("Inserted [Bucket:%d HashVal%d]\n", bucketId, hashValue);
                return;
            }
        }
        GB_pageHash * pNewPage = GB_getFreePageFromFreeList();
        assert (pNewPage != NULL) ;
        pNewPage->pNextPage = sHT->pBucket[bucketId].firstPage;
        sHT->pBucket[bucketId].firstPage = pNewPage;
        SET_VALID(pNewPage->valid, 0);
        pNewPage->entries[0].rid = rid;
        pNewPage->entries[0].hash = hashValue;
        //pNewPage->entries[0].aggValue = aggValue;
    }

}
#if 0
void insertRec(UINT32 rid, int bucketID) {
	if (!sHT->pBucket[bucketID].firstPage
			|| sHT->pBucket[bucketID].firstPage->freeSpace < recordSize) {
		pageHdr * pNewPage = GB_getFreePageFromFreeList();
                assert(pNewPage!=NULL);
		pNewPage->pNextPage = sHT->pBucket[bucketID].firstPage;
		sHT->pBucket[bucketID].firstPage = pNewPage;

	}

	*(float*) (sHT->pBucket[bucketID].firstPage->pFreeSpaceBeg) = aggColValue;
	sHT->pBucket[bucketID].firstPage->pFreeSpaceBeg += sizeof(float);
	memcpy(sHT->pBucket[bucketID].firstPage->pFreeSpaceBeg, attr, attrSize);
	sHT->pBucket[bucketID].firstPage->pFreeSpaceBeg += attrSize;
	sHT->pBucket[bucketID].firstPage->freeSpace -= sizeof(int) + attrSize;

}
#endif
//Search for a Rec in HT
GB_hashEntryWithoutAgg* GB_searchHashEntry(UINT32 rid) {
	GB_pageHash *pCurrPage;
    UINT16 index = 0;
    UINT16 bucketId = GB_getBucketId(rid);
    UINT8 hashValue = GB_getHashValue(rid);

	pCurrPage = sHT->pBucket[bucketId].firstPage;

	while (pCurrPage) {
        for (index = 0; index < sHT->entriesPerPage; index++) {
            if (CHECK_VALID(pCurrPage->valid, index) == 1) {
                if(pCurrPage->entries[index].hash != hashValue){
                    continue;
                }
                if (GB_compareRID(rid, pCurrPage->entries[index].rid) == 0) {
                    return &(pCurrPage->entries[index]);
                }
            }
        }
        pCurrPage = pCurrPage->pNextPage;
    }
	//printf("Search returning NULL\n");
	return NULL;
}

//Update Aggregate Value of Record found in HT

void GB_updateHashEntry(GB_hashEntry* pEntry, UINT32 aggColValue) {
    #if 0
    pEntry->aggValue = aggColValue;
    #endif
}

//Print entire contents of HT in Memory
void GB_printMemoryRec(char* outputBuffer, UINT32 *finalCount) {
    int bucketId;
    GB_pageHash *pCurrPage;
    UINT32 index;
    *finalCount = 0;
    UINT32 *localCount;
    
    printf("\n*****Printing Memory Records**********\n");
    for (bucketId = 0; bucketId < sHT->HTBucketCount; bucketId++) {
        
        localCount = (UINT32*)outputBuffer;
        outputBuffer += sizeof(UINT32);
        *localCount = 0;
    
        
        pCurrPage = sHT->pBucket[bucketId].firstPage;

        while (pCurrPage) {
            for (index = 0; index < sHT->entriesPerPage; index++) {
                if (CHECK_VALID(pCurrPage->valid, index) == 1) {
                    (*finalCount)++;
                    (*localCount)++;
                    //printf("rid %d agg %d\n",pCurrPage->entries[index].rid, pCurrPage->entries[index].aggValue);
                    *(UINT32*)outputBuffer = pCurrPage->entries[index].rid;
                    outputBuffer += sizeof (UINT32);
                    //*(UINT32*)outputBuffer = pCurrPage->entries[index].aggValue;
                    //outputBuffer += sizeof (UINT32);

                }
            }
            pCurrPage = pCurrPage->pNextPage;
        }
    }
    printf("FinalCount:%d\n", *finalCount);
}

//Merge Matching/Dependent Records in Memory to PCM
void GB_freePages() {

    GB_pageHash *pCurrPage, *pNextPage, *pNext, *pFreeTail;
    int bucketID;
    pNext = pFreeTail = sHT->pFreeList;
    while(pNext){
        pFreeTail = pNext;
        pNext = pFreeTail->pNextPage;
    }

    //Free HT
    for (bucketID = 0; bucketID < sHT->HTBucketCount; bucketID++) {
        pCurrPage = sHT->pBucket[bucketID].firstPage;

        while (pCurrPage) {
            pNextPage = pCurrPage->pNextPage;
            pCurrPage->pNextPage = NULL;
            if (pFreeTail == NULL) {
                sHT->pFreeList = pCurrPage;
            } else {
                pFreeTail->pNextPage = pCurrPage;
            }
            pFreeTail = pCurrPage;
            pCurrPage = pNextPage;
        }
        sHT->pBucket[bucketID].firstPage = NULL;
    }
}
void GB_freeHashTable() {

    GB_pageHash *pCurrPage, *pNextPage;
    int bucketID;

    //Free HT
    for (bucketID = 0; bucketID < sHT->HTBucketCount; bucketID++) {

        pCurrPage = sHT->pBucket[bucketID].firstPage;
        while (pCurrPage) {
            pNextPage = pCurrPage->pNextPage;

            FREE(pCurrPage->valid);
            FREE(pCurrPage->entries);
            FREE(pCurrPage);
            pCurrPage = pNextPage;

        }
    }
    FREE(sHT->pBucket);

    pCurrPage = sHT->pFreeList;
    
    while (pCurrPage) {
        pNextPage = pCurrPage->pNextPage;

        FREE(pCurrPage->valid);
        FREE(pCurrPage->entries);
        FREE(pCurrPage);
        pCurrPage = pNextPage;

    }
    FREE(sHT);
}

void GB_searchNoUpdateAndInsert(UINT32 rid, int pass){
	 //Return if record is not to be processed in this pass
         UINT16 bucketId = GB_getBucketId(rid);
	 if(bucketId % gNumPasses != pass)
		 return;
	 //printf("Processing Attr:%d, AggColValue:%d\n", pAttr, aggColValue);
	 if (GB_searchHashEntry(rid) == NULL)
	 {
		 GB_insertHashEntry(rid, 1);
	 }
	 //If record not present in HT, insert it
	 
 }
void GB_searchUpdateAndInsert(UINT32 rid, int pass){
	 UINT16 bucketId = GB_getBucketId(rid);
	 if(bucketId % gNumPasses != pass)
		 return;
         GB_hashEntryWithoutAgg *pEntry = GB_searchHashEntry(rid);
	 //printf("Processing Attr:%d, AggColValue:%d\n", pAttr, aggColValue);
	 if (pEntry == NULL)
	 {
		 GB_insertHashEntry(rid, 1);
	 }
         else
         {
#if 0
                 int newCount = pEntry->aggValue + 1;
                 printf("Updating [rid:%d count:%d]", pEntry->rid, newCount);
		 GB_updateHashEntry(pEntry, newCount);
#endif
		 
	 }
	 //If record not present in HT, insert it
	 
 }
#if 0
UINT32 hashValue(UINT32 rid){
    return ((PJWHash((char*)&(a[rid]), sizeof(UINT32)) & HASH_MASK) >> HASH_SHIFT);
}
UINT32 bucketId(UINT32 rid) {
    return (PJWHash((char*)&(a[rid]), sizeof(UINT32)) % sHT->HTBucketCount);
}
UINT32 compRid(UINT32 rid1, UINT32 rid2){
    return (a[rid1] - a[rid2]);
}

int main(){
    //UINT32 ARRAY[] = {5,1,2,3,6,4, 5, 6};
    UINT32 count = 100;
    a = (UINT32 *)malloc(sizeof(UINT32) * count);
    int i,j;
    for(i=0; i<count; i++){
        a[i] = rand()%1000;
        printf("%d\n", a[i]);
    }
    
#define DRAM_MEMORY_SIZE (1*1024*1024)
    char *pcmMemory = (char*)malloc(DRAM_MEMORY_SIZE);
    assert (pcmMemory != NULL);
    
    Vmalloc_t *virtMem = vmemopen(pcmMemory, DRAM_MEMORY_SIZE, 0);
#define NUM_PASS 1
    GB_initHT(virtMem, 16, 64, 10, NUM_PASS, bucketId, hashValue, compRid);
    assert(vmPCM != NULL);
    UINT32 pass;
    for(pass=0; pass<NUM_PASS; pass++){
    for(j=0; j<count; j++){
        GB_searchUpdateAndInsert(j, pass);
    }
    }
    GB_printMemoryRec(NULL);
    GB_freePages();
    for(pass=0; pass<NUM_PASS; pass++){
    for(j=0; j<count; j++){
        GB_searchUpdateAndInsert(j, pass);
    }
    }
    GB_printMemoryRec(NULL);
    
    GB_freeHashTable();
    return 0;
}
#endif