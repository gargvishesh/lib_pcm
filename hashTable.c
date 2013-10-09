/*
 * hashTable.c
 *
 *  Created on: 10-Apr-2013
 *      Author: spec
 */
#include "hashTable.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <assert.h>

#include "constants.h"
#include "GeneralHashFunctions.h"


#ifdef VMALLOC
#include <vmalloc.h>
#endif

hashTbl *sHT;
sOverflowPartition *pOvflow;
SINT32 pageCount;
UINT32 lastVictim;
extern UINT32 finalCount;

/*Debug*/
//UINT32 hitCount[BUCKET_COUNT];
//UINT32 hitLast[BUCKET_COUNT];
//float hitInterval[BUCKET_COUNT];
//char recordNo;
/**/

#ifdef VMALLOC
Vmalloc_t *vmDRAM;
Vmalloc_t *vmPCM;
#endif

extern void *regionDRAM;
extern void *regionPCM;
extern UINT32 recordSize;
extern UINT32 numPasses;
int pageCount=0;

/*Initialize a new page for Hash Table*/
void initNewPage(pageHash *pNewPage) {
    memset(pNewPage->valid, 0, sHT->entriesPerPage/BITS_PER_BYTE);
}
#if 0
#if 1

pageHash* selectVictimPage(int *bucketId) {
    pageHash *victimPage;
    for (*bucketId = sHT->HTBucketCount - 1; *bucketId >= 0; (*bucketId)--)
        //for (*bucketId = 0; *bucketId < sHT->HTBucketCount; (*bucketId)++)
        if (sHT->pBucket[*bucketId].firstPage) {
            victimPage = sHT->pBucket[*bucketId].firstPage;
            sHT->pBucket[*bucketId].firstPage =
                    sHT->pBucket[*bucketId].firstPage->pNextPage;
            //Victim Page chosen from this partition. Hence Partition Dirty
            sHT->pBucket[*bucketId].dirty = 1;
            //printf("Victim Chosen:%d\n", *bucketId);
            return (victimPage);
        }
    return NULL;
}
#else
//Select a Page to Write Out to PCM

pageHash* selectVictimPage(int *bucketId) {
    pageHash *victimPage;
    UINT32 i;
    *bucketId = (lastVictim + 1) % (sHT->HTBucketCount);
    for (i = 0; i < sHT->HTBucketCount; i++) {
        if (sHT->pBucket[*bucketId].firstPage) {
            victimPage = sHT->pBucket[*bucketId].firstPage;
            sHT->pBucket[*bucketId].firstPage =
                    sHT->pBucket[*bucketId].firstPage->pNextPage;
            //Victim Page chosen from this partition. Hence Partition Dirty
            sHT->pBucket[*bucketId].dirty = 1;
            lastVictim = *bucketId;
            //printf("Victim Chosen:%d\n", *bucketId);
            return (victimPage);
        }
        *bucketId = (*bucketId + 1) % (sHT->HTBucketCount);
    }
    return NULL;
}
#endif

//Write out the selected Page to PCM and Merge with Existing Records

void writeOutPage(int bucketId, pageHash* pWriteOutPage) {
    char *pWriteOutRec, *pPCMRec, found;
    assert(pWriteOutPage != NULL);
    pWriteOutRec = (char*) pWriteOutPage + sizeof (pageHash);
    while (pWriteOutRec < pWriteOutPage->pFreeSpaceBeg) {
        found = 0;
        pPCMRec = pOvflow[bucketId].pPartitionBeg;
        while (pPCMRec < pOvflow[bucketId].pFreeSpaceBeg) {
            if (memcmp(pWriteOutRec + sizeof (float), (pPCMRec + sizeof (float)), attrSize) == 0) {
                *(float*) (pPCMRec) += *(float*) (pWriteOutRec);
                found = 1;
                break;
            }
            pPCMRec += recordSize;
        }
        if (!found) {
            if (pOvflow[bucketId].FREESpace < recordSize) {
                printf("Overflow Partition %d Out of Free Space \n", bucketId);
                return;
            }

            *(float*) (pPCMRec) = *(float*) (pWriteOutRec);
            memcpy(pPCMRec + sizeof (float), pWriteOutRec + sizeof (float), attrSize);
            pOvflow[bucketId].pFreeSpaceBeg += attrSize + sizeof (float);
            pOvflow[bucketId].FREESpace -= (attrSize + sizeof (float));
        }
        pWriteOutRec += recordSize;
    }
}

//Get Free Page By Evicting an Allocated Page

pageHash* getFreePageByEviction() {
    pageHash *pNewPage;
    int bucketId;
    pNewPage = selectVictimPage(&bucketId);
    //printf("CallerBucketID:%d\n",bucketId);
    writeOutPage(bucketId, pNewPage);
    initNewPage(pNewPage);
    return pNewPage;
}
#endif
//Get Free Page from Free List

pageHash* getFreePage() {
    pageHash *pNewPage;
    pNewPage = (pageHash*) MALLOC(sizeof (pageHash));
    assert(pNewPage != NULL);
    pageCount++;
    //printf("pageCount: %d\n", pageCount);
    pNewPage->valid = (BITMAP*) MALLOC(sHT->entriesPerPage / BITS_PER_BYTE);
    assert(pNewPage->valid != NULL);
    pNewPage->entries = (hashEntry*) MALLOC(sizeof (hashEntry) * sHT->entriesPerPage);
    assert(pNewPage->entries != NULL);
    initNewPage(pNewPage);
    return pNewPage;

}

//Initialize Hash Table Structure and Free List
#ifdef VMALLOC
int initHT(Vmalloc_t *PCMStructPtr, UINT32 HTBucketCount, UINT8 entriesPerPage) {
#else
int initHT(UINT32 HTBucketCount, UINT8 entriesPerPage) {
#endif
    //Init Main HT structure
    
#ifdef VMALLOC
    vmPCM = PCMStructPtr;
#endif
    sHT = (hashTbl*) MALLOC(sizeof (sHT));
    assert(sHT != NULL);
    memset(sHT, 0, sizeof (sHT));


    sHT->pBucket = (sBucket*) MALLOC(HTBucketCount * sizeof (sBucket));
    assert(sHT->pBucket != NULL);
    
    memset(sHT->pBucket, 0, HTBucketCount * sizeof (sBucket));
    sHT->HTBucketCount = HTBucketCount;
    sHT->entriesPerPage = entriesPerPage;
    /*Initalize function pointer getFreePage to point to
     * getFreePageFromFreeList. Later FREE list permanently empty, so then change
     * pointer to getFreePageByEviction*/
#if 1
    //pageCount = (HT_MEMORY_SIZE / (sizeof (pageHash)));
    
#else
    //Leave space for twice the number of DB records + hash table structures
    pageCount = (HT_MEMORY_SIZE / (sizeof (page))-
            (NUM_PASSES * BUCKET_COUNT * DBRecordSize * 2) / sizeof (page) -
            (BUCKET_COUNT * sizeof (sBucket)) / sizeof (page));

#endif
    //printf("pageCount %d\n", pageCount);
    //assert(pageCount > 0);

    //Init Free List
#if 0
    for (pageNo = 0; pageNo < pageCount; pageNo++) {

        pNewPage = (pageHash*) MALLOC(sizeof (pageHash));
        //printf("PageNo:%d\n", pageNo);
        assert(pNewPage != NULL);

        if (pNewPage == NULL) {
            int deletePage;

            for (deletePage = 0; deletePage < 30; deletePage++) {
                pCurrPage = sHT->pFreeList;
                sHT->pFreeList = sHT->pFreeList->pNextPage;
                vmFREE(vmDRAM, pCurrPage);

            }
            break;

        }
        initNewPage(pNewPage);
        if (pageNo == 0) {
            sHT->pFreeList = pNewPage;
        } else {
            pCurrPage->pNextPage = pNewPage;
        }
        pCurrPage = pNewPage;
    }
#endif
    return 0;
}

#if 0
//Allocate Space for Overflow Partition for Each Bucket

int initOverflowPartitions() {
    int partitionID;

    pOvflow = (sOverflowPartition*) MALLOC(sHT->HTBucketCount * sizeof (sOverflowPartition));

    assert(pOvflow != NULL);
    for (partitionID = 0; partitionID < sHT->HTBucketCount; partitionID++) {

        pOvflow[partitionID].pPartitionBeg = (char*) MALLOC(OVERFLOW_PARTITION_SIZE);

        assert(pOvflow[partitionID].pPartitionBeg != NULL);
        pOvflow[partitionID].pFreeSpaceBeg = pOvflow[partitionID].pPartitionBeg;
        pOvflow[partitionID].FREESpace = OVERFLOW_PARTITION_SIZE;
    }
    return 0;
}

//Calculate Hash Key of given Attribute Value

long long hash(char* pValue) {
    long long num = (*(UINT32*) pValue * 6125423371) / (1 << 16);
    return ( num % (sHT->HTBucketCount * NUM_PASSES));
}
#endif

UINT32 getHashValue(char *attr, UINT32 attrSize) {
    return ((PJWHash(attr, attrSize) & HASH_MASK) >> HASH_SHIFT);
}

UINT32 getBucketId(char *attr, UINT32 attrSize) {
    return (PJWHash(attr, attrSize) % sHT->HTBucketCount);
}
//Insert New Record in HT

void insertHashEntry(void* tuple, char* attr, UINT32 attrSize) {
    pageHash *pCurrPage;
    UINT16 index = 0;
    UINT16 bucketId = getBucketId(attr, attrSize);
    UINT8 hashValue = getHashValue(attr, attrSize);
    if (!sHT->pBucket[bucketId].firstPage) {
        pageHash * pNewPage = getFreePage();
        if (pNewPage == NULL) {
            printf("FATAL ERROR: No FREE pages in HT\n");
            return;
        }
        pNewPage->pNextPage = sHT->pBucket[bucketId].firstPage;
        sHT->pBucket[bucketId].firstPage = pNewPage;
        /*Write Code to Insert a Record here. Remember to set valid flag*/
        /*Since an entirely new page, set the LSB indicating first record*/
        SET_VALID(pNewPage->valid, 0);
        pNewPage->entries[0].ptr = tuple;
        pNewPage->entries[0].hash = hashValue;
        //printf("Inserted [Bucket:%d HashVal%d]\n", bucketId, hashValue);
        return;
    } else {
        pCurrPage = sHT->pBucket[bucketId].firstPage;
        while (pCurrPage) {
            for (index = 0; index < sHT->entriesPerPage; index++) {
                if (CHECK_VALID(pCurrPage->valid, index) == 0) {
                    SET_VALID(pCurrPage->valid, index);
                    pCurrPage->entries[index].ptr = tuple;
                    pCurrPage->entries[index].hash = hashValue;
                    //printf("Inserted [Bucket:%d HashVal%d]\n", bucketId, hashValue);
                    return;
                }
            }
            pCurrPage = pCurrPage->pNextPage;
        }
        pageHash * pNewPage = getFreePage();
        if (pNewPage == NULL) {
            printf("FATAL ERROR: No FREE pages in HT\n");
            return;
        }
        pNewPage->pNextPage = sHT->pBucket[bucketId].firstPage;
        sHT->pBucket[bucketId].firstPage = pNewPage;
        SET_VALID(pNewPage->valid, 0);
        pNewPage->entries[0].ptr = tuple;
        pNewPage->entries[0].hash = hashValue;
    }

}
/*Search for a Rec in HT. For multiple matches, call this function with the 
 last returned pLastPage and pLastIndex*/

/* We take void** arguments because we want to store our own pointers for context 
 * void* would mean when we do *ptr, the value has to match calling type value.
 * (void**) enables us to ambiguous double redirection and enables us tp write a 
 * pointer at *ptr with calling type itself being (void*) */
UINT8 searchHashEntry(char* attr, UINT32 attrSize, void** returnEntryPtr,
        void **pLastPage,
        void **pLastIndex) {
    hashEntry* pCurrEntry;
    pageHash *pCurrPage;
    UINT16 index = 0;
    UINT16 bucketId = getBucketId(attr, attrSize);
    UINT8 hashValue = getHashValue(attr, attrSize);

    /*This if the first time we are searching in this bucket*/
    if (*pLastPage == NULL) {
        pCurrPage = sHT->pBucket[bucketId].firstPage;
        index = 0;
    }        /*Continue search from next to last checkpoint*/
    else {
        pCurrPage = *(pageHash**) pLastPage;
        index = *(UINT16*) pLastIndex + 1;
    }


    while (pCurrPage) {
        pCurrEntry = pCurrPage->entries;
        for (; index < sHT->entriesPerPage; index++) {
            if (CHECK_VALID(pCurrPage->valid, index) && pCurrEntry[index].hash == hashValue) {
                //printf("Found at Bucket ID : %d\n", bucketID);
                *returnEntryPtr = (void*) (pCurrEntry[index].ptr);
                *pLastPage = (void*) pCurrPage;
                *pLastIndex = (void*) index;
                return 1;
            }
        }
        pCurrPage = pCurrPage->pNextPage;
        index = 0;
    }
    //printf("Search Value :%d Not found in HT\n", *(int*)attr);
    return 0;
}

void freeHashTable() {

    pageHash *pCurrPage, *pNextPage;
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
    FREE(sHT);
}
#if 0
//Update Aggregate Value of Record found in HT

void updateRec(char* pRecAddr, float aggColValue) {
    *(float*) pRecAddr += aggColValue;
}

//Print entire contents of HT in Memory

void printMemoryRec() {
    int bucketID;
    char * pCurrEntry;
    pageHash *pCurrPage;
    printf("\n*****Printing Memory Records**********\n");
    for (bucketID = 0; bucketID < sHT->HTBucketCount; bucketID++) {
        pCurrPage = sHT->pBucket[bucketID].firstPage;
        if (hitCount[bucketID] != 0) {
            //printf("Bucket:%d [HitCount:%d AvgInterval:%f]\n",
            //bucketID, hitCount[bucketID], (float)hitInterval[bucketID]/(float)hitCount[bucketID]);
        }
        while (pCurrPage) {
            //printf("Page Dependent : %d\n", pCurrPage->fDependent);
            pCurrEntry = (char*) pCurrPage + sizeof (pageHash);
            while (pCurrEntry < pCurrPage->pFreeSpaceBeg) {
                //printf("Attr:%d AggValue:%f\n",
                //	*(int*) (pCurrEntry + sizeof(int)), *(float*) (pCurrEntry));
                finalCount++;
                pCurrEntry += recordSize;
            }
            pCurrPage = pCurrPage->pNextPage;
        }
    }
    printf("FinalCount:%d\n", finalCount);
}

//Print Records in PCM

void printPCMRec() {
    int partitionID;
    char * pCurrEntry;
    printf("\n*****Printing PCM Records**********\n");
    for (partitionID = 0; partitionID < sHT->HTBucketCount; partitionID++) {
        pCurrEntry = pOvflow[partitionID].pPartitionBeg;
        while (pCurrEntry < pOvflow[partitionID].pFreeSpaceBeg) {
            if (*(int*) (pCurrEntry) == 0xffffffff) {
                pCurrEntry += recordSize;
                continue;
            }
            //printf("Attr:%d AggValue:%f\n", *(int*) (pCurrEntry + sizeof(int)),
            //	*(float*) (pCurrEntry));
            pCurrEntry += recordSize;
            finalCount++;
        }

    }
    printf("FinalCount:%d\n", finalCount);

}

//Merge Matching/Dependent Records in Memory to PCM

void calculateFinalTally() {
    int bucketID;
    pageHash *pCurrPage;
    printf("\n*****Printing merged aggregates*******\n");
    printf("**************************************\n");

    for (bucketID = 0; bucketID < sHT->HTBucketCount; bucketID++) {
        pCurrPage = sHT->pBucket[bucketID].firstPage;
        while (pCurrPage) {
            if (pCurrPage->fDependent) {
                char *pCurrEntry, *pPCMRec;
                pCurrEntry = (char*) pCurrPage + sizeof (pageHash);
                while (pCurrEntry < pCurrPage->pFreeSpaceBeg) {
                    pPCMRec = pOvflow[bucketID].pPartitionBeg;
                    while (pPCMRec < pOvflow[bucketID].pFreeSpaceBeg) {
                        if (memcmp(pCurrEntry + sizeof (float), (pPCMRec + sizeof (float)), attrSize) == 0) {
                            *(float*) (pCurrEntry) += *(float*) (pPCMRec);
                            /*A temporary fix to mark the page as invalid
                             * in PCM while printing m/m records since corresponding PCM
                             * record has been merged with this record in m/m*/
                            *(int*) (pPCMRec) = 0xffffffff;
                            break;
                        }
                        pPCMRec += recordSize;
                    }
                    pCurrEntry += recordSize;
                }

            }
            pCurrPage = pCurrPage->pNextPage;
        }

    }
}

void FREESpace() {

    pageHash *pCurrPage, *pNextPage;
    int partitionID, bucketID;

    //Free Overflow partitions
    for (partitionID = 0; partitionID < sHT->HTBucketCount; partitionID++) {
        FREE(pOvflow[partitionID].pPartitionBeg);
    }

    FREE(pOvflow);
    //Free HT
    for (bucketID = 0; bucketID < sHT->HTBucketCount; bucketID++) {
        pCurrPage = sHT->pBucket[bucketID].firstPage;

        while (pCurrPage) {
            pNextPage = pCurrPage->pNextPage;

            FREE(pCurrPage);

            pCurrPage = pNextPage;

        }
    }
    pCurrPage = sHT->pFreeList;
    while (pCurrPage) {
        pNextPage = pCurrPage->pNextPage;

        FREE(pCurrPage);

        pCurrPage = pNextPage;

    }

    FREE(sHT->pBucket);
    FREE(sHT);
}

void processNewTupleByHashing(char* pAttr, float aggColValue, int pass) {
    char * pRecAddr;
    int bucketID = hash(pAttr);

    //Return if record is not to be processed in this pass
    if (bucketID % NUM_PASSES != pass)
        return;
    bucketID = bucketID / NUM_PASSES;
    hitCount[bucketID]++;
    hitInterval[bucketID] += recordNo - hitLast[bucketID];
    hitLast[bucketID] = recordNo;
    //printf("bucketID: %d, hitCount:%d, hitInterval :%d, hitLast :%d\n", bucketID, hitCount[bucketID],hitInterval[bucketID], hitLast[bucketID]);
    recordNo++;
    //printf("Processing Attr:%d, AggColValue:%d\n", pAttr, aggColValue);
    if ((pRecAddr = searchHashEntry(pAttr, bucketID)) != NULL) {
        updateRec(pRecAddr, aggColValue);
        return;
    }
    //If record not present in HT, insert it
    insertHashEntry(pAttr, aggColValue, bucketID);
}
#endif