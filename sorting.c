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

#ifdef VMALLOC
Vmalloc_t *vmDRAM;
Vmalloc_t *vmPCM;
#endif

int resolved = 1 << (BYTE * sizeof (int) - 1);


compareTuples Compare;
char *pivots;
/*Used as partition start ptr for quicksort. Replace with something cleaner later*/
char *partitionStart;
/*--------------------*/

UINT32 arrayElemCount;
char *array;
UINT32 numSplits;
UINT32 numPivots;
UINT8 arrayElemSize;
int writes;
UINT32 *partitionBeginnings = NULL;
char* outputBufferPtr = NULL;

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
int InitSort(Vmalloc_t *PCMStructPtr, char* arrayToBeSorted, int arrElemCount, int stepSize, compareTuples compareTup, char* outputBuffer) {
#else
int InitSort(char* arrayToBeSorted, int arrElemCount, int stepSize, compareTuples compareTup, char* outputBuffer) {
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
    srand(13);
    /*Pivots will be 1 less than #splits*/
    /**********************************************************************************
     * ********************************************************************************
     * *******************************************************************************
     * These numbers have to be decided
     * numSplits
     * length of pos array
     * ********************************************************************************/
    numSplits = 20;
    numPivots = numSplits - 1;
    pivots = (char*) MALLOC((numSplits - 1) * arrayElemSize);
    UINT32 numElem = 10000;
     /***********************************************************************************/
    
    pos = (POS_ARRAY_TYPE*) MALLOC(numElem * sizeof (POS_ARRAY_TYPE));
    initSortElems(numElem);
    UINT8 index;
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
    FREE(pos);
}

UINT32 getPartitionId(char *tuplePtr) {
    //long long num = (value*612);
    UINT32 beg = 0, end = numPivots - 1, mid;
    SINT32 comparisonResult;
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

int createPartitions() {

    UINT32 i, j, k;
    UINT32 partitionPresent = INT_MAX, partitionLast;
    UINT32 partitionStart, partitionEnd, firstVictimLoc;
    BOOL flag;
    char *presentVictim = (char*) MALLOC(arrayElemSize);
    UINT32 *currPartitionPtr = (UINT32*) MALLOC((numSplits + 1) * sizeof (int));;
    
    UINT8 partitionId;
    /*Changing count too to vmalloc causing some problems. Have to check it later*/
    int * count = (int*) malloc(numSplits * sizeof (int));
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
    UINT8 newNumPivots = 0, newNumSplits = 1;
    UINT8 THRESHHOLD = 1;
    for (i = 0; i < numSplits - 1; i++) {
        runningCount += count[i];
        windowCount = count[i];
        while ((i < (numSplits - 1)) && (windowCount < THRESHHOLD)) {
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
    numPivots = newNumPivots;
    numSplits = newNumSplits;
#if 0 /*Can be used for debuggin later if required*/
    for (i = 0; i < numPivots; i++) {
        printf("Pivots:%d\n", *(int*) (pivots + (sizeof (int)*i)));
    }
    for (i = 0; i < numSplits; i++) {
        printf("partitions[%d]:%d\n", i, partitionBeginnings[i]);
    }
#endif
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

    FREE(presentVictim);
    FREE(currPartitionPtr);
    free(count);

    return (EXIT_SUCCESS);
}

int compareRecords(const void *p1, const void *p2) {
    //printf("Comparing %d %d\n", *(int*) p1, *(int*) p2);
    return (*(int*) p1 - *(int*) p2);
}
extern void _quicksort(void *const pbase, size_t total_elems, size_t size,
        __compar_fn_t cmp);

void sortPartitionWithUndo(int start, int end) {
    int i;

    qsortWithUndo(array + (start * arrayElemSize), end - start, arrayElemSize, Compare);

    POS_ARRAY_TYPE presentTargetLoc, nextTargetLoc;
    char *presentArrayCandidate = (char*) malloc(arrayElemSize);
    char flag;
    UINT32 firstVictimLoc;
    for (i = 0; i < end - start; i++) {
        if (!!(pos[i] & resolved) == MSB_ONE) //!! to convert to boolean
            continue;
        if (pos[i] == i) {
            pos[i] |= (1 << (BYTE * sizeof (int) - 1)) | i;
            continue;
        }
        firstVictimLoc = i;
        presentTargetLoc = pos[i];
        memcpy(presentArrayCandidate, array + (arrayElemSize * (start + i)), arrayElemSize);
        flag = 1;
        pos[i] |= resolved;
        while (flag) {
            if (presentTargetLoc == firstVictimLoc) {
                flag = 0;
            }
            assert(presentTargetLoc < end - start);

            swapTuples(array + ((start + presentTargetLoc) * arrayElemSize),
                    presentArrayCandidate);

            nextTargetLoc = pos[presentTargetLoc];
            pos[presentTargetLoc] |= resolved;
            presentTargetLoc = nextTargetLoc;
        }
    }

    *(UINT32*) outputBufferPtr = end - start;
    outputBufferPtr += sizeof (UINT32);
    for (i = 0; i < end - start; i++) {
        *(POS_ARRAY_TYPE*) outputBufferPtr = (pos[i] & (~resolved));
        outputBufferPtr += sizeof (POS_ARRAY_TYPE);
    }

    for (i = 0; i < end - start; i++) {
        memcpy(outputBufferPtr,
                array + ((start + i) * arrayElemSize),
                arrayElemSize);
        outputBufferPtr += arrayElemSize;
        pos[i] = i;
    }
    free(presentArrayCandidate);
}
#ifdef VMALLOC
int sortMultiPivotAndUndo(Vmalloc_t *PCMStructPtr, char* arrayToBeSorted, UINT32 elemCount, UINT32 elemSize, 
        compareTuples compareFunc, char* outputBuffer){
#else
int sortMultiPivotAndUndo(char* arrayToBeSorted, UINT32 elemCount, UINT32 elemSize, 
        compareTuples compareFunc, char* outputBuffer){
#endif

#ifdef VMALLOC
    InitSort(PCMStructPtr, arrayToBeSorted, elemCount, elemSize, compareFunc, outputBuffer);
#else
    InitSort(arrayToBeSorted, elemCount, elemSize, compareFunc, outputBuffer);
#endif
    createPartitions();
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
    for (i = 0; i < numSplits; i++) {
        /*Used as partition start ptr for quicksort. Replace with something cleaner later*/
        partitionStart = array + (arrayElemSize * partitionBeginnings[i]);
        sortPartitionWithUndo(partitionBeginnings[i], partitionBeginnings[i + 1]);
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





