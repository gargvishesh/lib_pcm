/* 
 * File:   sorting.c
 * Author: root
 *
 * Created on 2 October, 2013, 2:49 PM
 */

#include <stdio.h>
#include <stdlib.h>
#include <limits.h>
#include <inttypes.h>
#include <assert.h>
#include <string.h>

/*Files for Unix open/write calls */
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h> 
#include <sys/types.h>


#include "constants.h"
#include "hashTable.h"
#include "sorting.h"
/*
 * 
 */
#define INC_WRITES writes++

#define DISK_SIZE 1*GB
#define DRAM_SIZE 256*KB
#define PCM_SIZE 256*MB

#define qsortWithUndo _quicksort 
#define MSB_ZERO 0
#define MSB_ONE 1
#define BYTE 8

#ifdef VMALLOC
#include "vmalloc.h"
#define MALLOC(size) vmalloc(vmPCM, size) 
#define FREE(ptr) vmfree(vmPCM, ptr) 
#else
#define MALLOC(size) malloc(size)
#define FREE(ptr) free(ptr)
#endif
#undef VMALLOC
#ifdef VMALLOC
Vmalloc_t *vmPCM;
#endif

compareTuples Compare;
char *pivots;
/*Used as partition start ptr for quicksort. Replace with something cleaner later*/
char *partitionStart;
/*--------------------*/

UINT32 arrayElemCount;
char *array;
UINT32 numSplits;
UINT32 numPivots;
UINT32 arrayElemSize;
int writes;
UINT32 *partitionBeginnings = NULL;
char* outputBufferPtr = NULL;
UINT32 maxPartitionSize;
UINT32 gCurrStart;

#ifdef UNDO
POS_ARRAY_TYPE *pos;

void initSortElems(int numElem) {
    int i;
    for (i = 0; i < numElem; i++) {
        pos[i] = i;
    }
}
#endif

void swapTuples(char* ptr1, char* ptr2) {
    char * temp = (char*) malloc(arrayElemSize);
    memcpy(temp, ptr1, arrayElemSize);
    memcpy(ptr1, ptr2, arrayElemSize);
    memcpy(ptr2, temp, arrayElemSize);
    free(temp);
}

void arrayInit() {
    int i;
    for (i = 0; i < arrayElemCount; i++) {
        array[i] = i;
        //array[i] = rand();        
    }
}

void sortPivots() {
    /*Using Selection Sort to keep it simple*/
    int i,
            j,
            min;
    for (i = 0; i < numPivots; i++) {
        min = i;
        for (j = i + 1; j < numPivots; j++) {
            if (Compare(pivots + (arrayElemSize * min), pivots + (arrayElemSize * j)) > 0) {
                min = j;
            }
        }
        if (min != i) {
            swapTuples(pivots + (arrayElemSize * min), pivots + (arrayElemSize * i));
        }
    }
}
#ifdef VMALLOC
int InitSort(Vmalloc_t *PCMStructPtr, char* arrayToBeSorted, int arrElemCount, int stepSize, compareTuples compareTup, char* outputBuffer, UINT32 numPartitions) {
#else
int InitSort(char* arrayToBeSorted, int arrElemCount, int stepSize, compareTuples compareTup, char* outputBuffer, UINT32 numPartitions) {
#endif   
    //numSplits = PCM_SIZE/2*DRAM_SIZE; 
    
    /*Don't store just indices in pivots to the original array. When you 
     more around elements in arrays, those indices are no longer valid*/
    //pivots = (UINT32*)malloc((numSplits-1) * sizeof(UINT32));
   
    UINT32 count;
    Compare = compareTup;
    array = arrayToBeSorted;
    outputBufferPtr = outputBuffer;
    arrayElemCount = arrElemCount;
    arrayElemSize = stepSize;
#ifdef VMALLOC
    assert(PCMStructPtr != NULL);
    vmPCM = PCMStructPtr;
#endif
    srand(220587);
    /*Pivots will be 1 less than #splits*/
    /**********************************************************************************
     * ********************************************************************************
     * *******************************************************************************
     * These numbers have to be decided
     * numSplits
     * length of pos array
     * ********************************************************************************/
    //numSplits = 1000;
    numSplits = numPartitions;
    numPivots = numSplits - 1;
    pivots = (char*) MALLOC((numSplits - 1) * arrayElemSize);
    //UINT32 numElem = 10000;
    
     /***********************************************************************************/
    
    UINT32 index;
    for (index = 0; index < numPivots; index++) {
        memcpy(pivots + (index * arrayElemSize),
                array + (arrayElemSize * (rand() % arrayElemCount)),
                arrayElemSize);
    }

    sortPivots();

    count = 1;
    for (index = 1; index < numSplits - 1; index++) {
        if (Compare(pivots + (arrayElemSize * index),
                pivots + (arrayElemSize * (index - 1))) != 0) {
            memcpy(pivots + (arrayElemSize * count),
                    pivots + (arrayElemSize * index),
                    arrayElemSize);
            count += 1;
        }
    }
    numPivots = count;
    numSplits = numPivots + 1;
    partitionBeginnings = (UINT32*) MALLOC((numSplits + 1) * sizeof (int));
    
    
    
    return 0;
}
void cleanupSort(){
    FREE(pivots);
    FREE(partitionBeginnings);
    //FREE(pos);
}

UINT32 getPartitionId(char *tuplePtr) {
    //long long num = (value*612);
    UINT32 beg = 0, end = numPivots - 1, mid;
    SINT32 comparisonResult;
    if (numPivots == 0){
        return 0;
    }
    /*Do array binary search here*/
    while (beg <= end) {
        mid = (beg + end) / 2;
        comparisonResult = Compare(pivots + (arrayElemSize * mid), tuplePtr);
        if (comparisonResult < 0) {
            /*Case if it is greater than greatest pivot*/
            if (mid == numPivots - 1) {
                return mid + 1;
            }/*If it is greater than both this and next pivot*/
            else if (Compare(pivots + (arrayElemSize * (mid + 1)), tuplePtr) < 0) {
                beg = mid + 1;
            }/*Exactly equal to the next pivot*/
            else if (Compare(pivots + (arrayElemSize * (mid + 1)), tuplePtr) == 0) {
                return mid + 2;
            }/*If it lies between this and next pivot*/
            else {
                return mid + 1;
            }
        }/*It is lesser than this pivot*/
        else if (comparisonResult > 0) {
            /* If lesser than first pivot*/
            if (mid == 0) {
                return mid;
            }
            end = mid - 1;
        }/*It is equal to this pivot*/
        else
            return mid + 1;
    }
    printf("Shouldn't have reached here");
    assert(0);
    return -1;

}

int createPartitionsByMovingTuples(int maxThreshhold) {

    UINT32 i, j, k;
    UINT32 partitionPresent = INT_MAX, partitionLast;
    UINT32 partitionStart, partitionEnd, firstVictimLoc;
    BOOL flag;
    char *presentVictim = (char*) MALLOC(arrayElemSize);
    UINT32 *currPartitionPtr = (UINT32*) MALLOC((numSplits + 1) * sizeof (int));;
    
    UINT32 partitionId;
    int * count = (int*) MALLOC(numSplits * sizeof (int));
    memset(count, 0, numSplits * sizeof (int));
    
    for (i = 0; i < arrayElemCount; i++) {
        partitionId = getPartitionId(array + arrayElemSize * i);
        count[partitionId]++;
    }
    /*Coalesce empty partitions / partitions less than threshold.
      Each split i corresponds to pivot (i-1) and pivot (i) except. 
     * Example split 2 corresponds to pivot 1 and pivot 2. 
     * Hence, when we coalesce array split i with i+1, pivot i
     * should be removed. Note: We do this run 1 less times than numSplits 
     * as last split's count is immaterial __|__|__|__|__ */
    UINT32 runningCount = 0,
            windowCount;
    UINT32 newNumPivots = 0, newNumSplits = 1;
    partitionBeginnings[0] = 0;
    for (i = 0; i < numSplits - 1; i++) {
        runningCount += count[i];
        windowCount = count[i];
        while ((i < (numSplits - 1)) && (windowCount < maxThreshhold)) {
            i++;
            runningCount += count[i];
            windowCount += count[i];
        }
        if (i != (numSplits - 1)) {
            memcpy(pivots + (newNumPivots * arrayElemSize),
                    pivots + (arrayElemSize * i),
                    arrayElemSize);
            newNumPivots++;
            partitionBeginnings[newNumSplits++] = runningCount;

        }


    }
    partitionBeginnings[newNumSplits] = arrayElemCount;
    numPivots = newNumPivots;
    numSplits = newNumSplits;
    printf("Partition Counting Over [Partitions:%d]\n", numSplits);
    
#if 0 /*Can be used for debuggin later if required*/
    for (i = 0; i < numPivots; i++) {
        printf("Pivots:%d\n", *(int*) (pivots + (sizeof (int)*i)));
    }
#endif
    maxPartitionSize=0;
    for (i = 0; i < numSplits; i++) {
        if(partitionBeginnings[i+1] - partitionBeginnings[i] > maxPartitionSize){
            maxPartitionSize = partitionBeginnings[i+1] - partitionBeginnings[i];
        }
        printf("partitions[%d]:%d\n", i, partitionBeginnings[i]);
    }
    printf("maxPartitionSize is %d\n", maxPartitionSize);

    partitionBeginnings[numSplits] = arrayElemCount;
    memcpy(currPartitionPtr, partitionBeginnings, (numSplits + 1) * sizeof (int));
    for (i = 0; i < numSplits; i++) {
        for (j = currPartitionPtr[i]; j < partitionBeginnings[i + 1]; j++) {
            partitionPresent = getPartitionId(array + arrayElemSize * j);
            if (j >= partitionBeginnings[partitionPresent] && partitionPresent < partitionBeginnings[partitionPresent + 1]) {
                currPartitionPtr[i]++;
                continue;
            }
            firstVictimLoc = j;
            memcpy(presentVictim, array + arrayElemSize*j, arrayElemSize);
            flag = 1;
            while (flag) {
                partitionStart = currPartitionPtr[partitionPresent];
                partitionEnd = partitionBeginnings[partitionPresent + 1];
                partitionLast = partitionPresent;
                for (k = partitionStart; k < partitionEnd; k++) {
                    partitionPresent = getPartitionId(array + arrayElemSize * k);
                    if (k >= partitionBeginnings[partitionPresent] && k < partitionBeginnings[partitionPresent + 1])
                        continue;
                    if (k == firstVictimLoc) {
                        flag = 0;
                    }
                    swapTuples(presentVictim, array + (arrayElemSize * k));
                    INC_WRITES;
                    currPartitionPtr[partitionLast] = k;
                    break;
                }
            }
        }
    }
    printf("Partitioning Over\n");
    
    FREE(presentVictim);
    FREE(currPartitionPtr);
    FREE(count);

    return (EXIT_SUCCESS);
}

int createPartitionsByMovingPos(int maxThreshhold) {

    UINT32 i, j, k;
    UINT32 partitionPresent = INT_MAX, partitionLast;
    UINT32 partitionStart, partitionEnd, firstVictimLoc;
    BOOL flag;
    UINT32 presentVictim;
    UINT32 *currPartitionPtr = (UINT32*) MALLOC((numSplits + 1) * sizeof (int));;
    
    UINT32 partitionId;
    int * count = (int*) MALLOC(numSplits * sizeof (int));
    memset(count, 0, numSplits * sizeof (int));
    
    for (i = 0; i < arrayElemCount; i++) {
        partitionId = getPartitionId(array + arrayElemSize * i);
        count[partitionId]++;
    }
    /*Coalesce empty partitions / partitions less than threshold.
      Each split i corresponds to pivot (i-1) and pivot (i) except. 
     * Example split 2 corresponds to pivot 1 and pivot 2. 
     * Hence, when we coalesce array split i with i+1, pivot i
     * should be removed. Note: We do this run 1 less times than numSplits 
     * as last split's count is immaterial __|__|__|__|__ */
    UINT32 runningCount = 0,
            windowCount;
    UINT32 newNumPivots = 0, newNumSplits = 1;
    partitionBeginnings[0] = 0;
    for (i = 0; i < numSplits - 1; i++) {
        runningCount += count[i];
        windowCount = count[i];
        while ((i < (numSplits - 1)) && (windowCount < maxThreshhold)) {
            i++;
            runningCount += count[i];
            windowCount += count[i];
        }
        if (i != (numSplits - 1)) {
            memcpy(pivots + (newNumPivots * arrayElemSize),
                    pivots + (arrayElemSize * i),
                    arrayElemSize);
            newNumPivots++;
            partitionBeginnings[newNumSplits++] = runningCount;

        }


    }
    partitionBeginnings[newNumSplits] = arrayElemCount;
    numPivots = newNumPivots;
    numSplits = newNumSplits;
    printf("Partition Counting Over [Partitions:%d]\n", numSplits);
    
#if 0 /*Can be used for debuggin later if required*/
    for (i = 0; i < numPivots; i++) {
        printf("Pivots:%d\n", *(int*) (pivots + (sizeof (int)*i)));
    }
#endif
    maxPartitionSize=0;
    for (i = 0; i < numSplits; i++) {
        if(partitionBeginnings[i+1] - partitionBeginnings[i] > maxPartitionSize){
            maxPartitionSize = partitionBeginnings[i+1] - partitionBeginnings[i];
        }
        printf("partitions[%d]:%d\n", i, partitionBeginnings[i]);
    }
    printf("maxPartitionSize is %d\n", maxPartitionSize);

    partitionBeginnings[numSplits] = arrayElemCount;
    
    memcpy(currPartitionPtr, partitionBeginnings, (numSplits + 1) * sizeof (int));
    
    UINT32 temp;
    for (i = 0; i < numSplits; i++) {
        for (j = currPartitionPtr[i]; j < partitionBeginnings[i + 1]; j++) {
            partitionPresent = getPartitionId(array + arrayElemSize * j);
            
            if (j >= partitionBeginnings[partitionPresent] && partitionPresent < partitionBeginnings[partitionPresent + 1]) {
                currPartitionPtr[i]++;
                continue;
            }
            
            firstVictimLoc = j;
            
            presentVictim = j;
            flag = 1;
            while (flag) {
                partitionStart = currPartitionPtr[partitionPresent];
                partitionEnd = partitionBeginnings[partitionPresent + 1];
                partitionLast = partitionPresent;
                for (k = partitionStart; k < partitionEnd; k++) {
                    partitionPresent = getPartitionId(array + arrayElemSize * pos[k]);
                    if (k >= partitionBeginnings[partitionPresent] && k < partitionBeginnings[partitionPresent + 1])
                        continue;
                    if (k == firstVictimLoc) {
                        flag = 0;
                    }
                    //swapTuples(presentVictim, array + (arrayElemSize * k));
                    temp = presentVictim;
                    presentVictim = pos[k];
                    pos[k] = temp;
                    
                    INC_WRITES;
                    currPartitionPtr[partitionLast] = k;
                    break;
                }
            }
        }
    }
    printf("Partitioning Over\n");
    
    //FREE(presentVictim);
    FREE(currPartitionPtr);
    FREE(count);

    return (EXIT_SUCCESS);
}
int compareRecords(const void *p1, const void *p2) {
    //printf("Comparing %d %d\n", *(int*) p1, *(int*) p2);
    return (*(int*) p1 - *(int*) p2);
}

int comparePos(const void *p1, const void *p2){
    //char *presentArrayStart = array + (gCurrStart * arrayElemSize);
    char *presentArrayStart = array;
    POS_ARRAY_TYPE pos1 = *(POS_ARRAY_TYPE*)p1;
    POS_ARRAY_TYPE pos2 = *(POS_ARRAY_TYPE*)p2;
    
    return ( Compare(presentArrayStart+(arrayElemSize*pos1), presentArrayStart+(arrayElemSize*pos2)) );   
}

void sortPartition(int start, int end){
    gCurrStart = start;
    //int i;
    
    qsort(pos+start, end - start, sizeof(POS_ARRAY_TYPE), comparePos);
#if 0
    *(UINT32*) outputBufferPtr = end - start;
    outputBufferPtr += sizeof (UINT32);
    for (i = 0; i < end - start; i++) {
        *(POS_ARRAY_TYPE*) outputBufferPtr = (pos[i]);
        //assert(pos[i] < (end-start));
        //pos[i] = i;
        outputBufferPtr += sizeof (POS_ARRAY_TYPE);
    }
#endif
    
}
#ifdef VMALLOC
int sortMultiPivotAndUndo(Vmalloc_t *PCMStructPtr, char* arrayToBeSorted, UINT32 elemCount, UINT32 elemSize, 
        compareTuples compareFunc, char* outputBuffer, UINT32 numPartitions, UINT32 maxThreshhold){
#else
int sortMultiPivotAndUndo(char* arrayToBeSorted, UINT32 elemCount, UINT32 elemSize, 
        compareTuples compareFunc, char* outputBuffer, UINT32 numPartitions, UINT32 maxThreshhold){
#endif

#ifdef VMALLOC
    InitSort(PCMStructPtr, arrayToBeSorted, elemCount, elemSize, compareFunc, outputBuffer, numPartitions);
#else
    InitSort(arrayToBeSorted, elemCount, elemSize, compareFunc, outputBuffer, numPartitions);
#endif
    
    pos = (UINT32*)outputBufferPtr;
    initSortElems(elemCount);
    
    createPartitionsByMovingPos(maxThreshhold);
#if 0
    /******Debug******************/
    int j, k;
    for(j=0; j<numSplits; j++){
        printf("====Partition %d====\n", j);
        for(k=partitionBeginnings[j]; k<partitionBeginnings[j+1]; k++){
            printf("%s", *(arrayToBeSorted+(k*elemSize)));
        }
    }
    /***************************/
#endif
    UINT32 i;
    //pos = (POS_ARRAY_TYPE*) MALLOC(maxPartitionSize * sizeof (POS_ARRAY_TYPE));
    //initSortElems(maxPartitionSize);
    for(i=0; i<arrayElemCount; i++){
        printf("pos[%d]: %d\n", i, pos[i]);
    }
    
    for (i = 0; i < numSplits; i++) {
        /*Used as partition start ptr for quicksort. Replace with something cleaner later*/
        partitionStart = array + (arrayElemSize * partitionBeginnings[i]);
        sortPartition(partitionBeginnings[i], partitionBeginnings[i + 1]);
        //qsort(partitionStart + arrayElemSize* partitionBeginnings[i], partitionBeginnings[i+1]-partitionBeginnings[i], arrayElemSize, Compare);
        printf("Sorting over for Partition :%d\n", i);
    
    }
    
    cleanupSort();
    return 0;
}
#if 0
int main() {

    /* For debugging            */
    long long ARRAY[] = {8, 3, 4, 5, 2, 6, 7, 2, 3, 13, 15, 3, 4, 3, 4, 19, 5, 7};
    //int ARRAY[] = {2, 3, 13, 15, 3, 4, 3, 4, 19, 5, 7};
    

    int COUNT = 18;
    int i;
    char *outputBuffer = (char*) malloc(sizeof (int)*1000);
    
    
    sortMultiPivotAndUndo(ARRAY, COUNT, sizeof (long long), compareRecords, outputBuffer);
    
    int remainingSize = COUNT;
    int currSize;
    int *pos;
    char *tuples;

    printf("\n ===Final Array====\n");
    while (remainingSize > 0) {
        currSize = *(int*) outputBuffer;
        pos = outputBuffer + sizeof (UINT32);
        tuples = (char*) pos + currSize * sizeof (int);
        for (i = 0; i < currSize; i++) {
            printf("Elem: %d\n", *(long long*) (tuples + (arrayElemSize * pos[i])));
        }
        /*One extra UINT32 size jump to jump the size parameter itself*/
        outputBuffer += currSize * (arrayElemSize + sizeof (UINT32)) + sizeof (UINT32);
        remainingSize -= currSize;
    }
    cleanupSort();
    return 0;
}
#endif





