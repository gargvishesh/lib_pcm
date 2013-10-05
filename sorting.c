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
/*
 * 
 */
#define INC_WRITES writes++

#define DISK_SIZE 1*GB
#define DRAM_SIZE 256*KB
#define PCM_SIZE 256*MB

#define qsort _quicksort 
#define MSB_ZERO 0
#define MSB_ONE 1
#define BYTE 8
int resolved = 1 << (BYTE * sizeof (int) - 1);

typedef int (*compareTuples)(const void * p1, const void * p2);

compareTuples Compare;
char *pivots;
/*Used as partition start ptr for quicksort. Replace with something cleaner later*/
char *partitionStart;
/*--------------------*/
#define UNDO 

UINT32 arrayElemCount;
char *a;
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
    //printf("Putting %d in place of %d\n", *(int*) ptr2, *(int*) ptr1);
    free(temp);
}

void arrayInit() {
    int i;
    for (i = 0; i < arrayElemCount; i++) {
        a[i] = i;
        //a[i] = rand();        
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

int InitSort(UINT32** start, UINT32** currPartitionPtr, int stepSize, compareTuples compareTup) {

    //numSplits = PCM_SIZE/2*DRAM_SIZE; 
    numSplits = 5;
    numPivots = numSplits - 1;
    /*Don't store just indices in pivots to the original array. When you 
     more around elements in arrays, those indices are no longer valid*/
    //pivots = (UINT32*)malloc((numSplits-1) * sizeof(UINT32));
    arrayElemSize = stepSize;
    UINT32 count;
    Compare = compareTup;
    srand(13);
    /*Pivots will be 1 less than #splits*/
    pivots = (char*) malloc((numSplits - 1) * arrayElemSize);
    UINT8 index;
    for (index = 0; index < numPivots; index++) {
        memcpy(pivots + (index * arrayElemSize),
                a + (arrayElemSize * (rand() % arrayElemCount)),
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
    *start = (UINT32*) malloc((numSplits + 1) * sizeof (int));
    *currPartitionPtr = (UINT32*) malloc((numSplits + 1) * sizeof (int));
    return 0;
}
#if 1

/*We have p Pivots 0...p-1 and p+1 Partitions 0..p 
  Hence when we return a Partition on the basis of Pivot :
 * if value < Pivot[0]
 * if value between Pivot[p] & Pivot[p+1], partition is p+1 
 * if value greater than partition p, partition is p+1*/

UINT32 getPartitionId(char *tuplePtr) {
    //long long num = (value*612);
    UINT32 beg = 0, end = numPivots - 1, mid;
    SINT32 comparisonResult;
    /*Do a binary search here*/
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
#else

int getPartitionId(int value) {
    return value % numSplits;
    //return  ( num  % (sHT->HTBucketCount*NUM_PASSES) );
}
#endif

int createPartitions(int start, int arrElemCount, int arrElemSize, compareTuples compareTup, char* outputBuffer) {



    UINT32 i, j, k;
    UINT32 partitionPresent = INT_MAX, partitionLast;
    UINT32 partitionStart, partitionEnd, firstVictimLoc;
    BOOL flag;
    char *presentVictim = (char*) malloc(arrElemSize);
    UINT32 *currPartitionPtr;
    //arrayInit(); 
    arrayElemCount = arrElemCount;
    outputBufferPtr = outputBuffer;
    InitSort(&partitionBeginnings, &currPartitionPtr, arrElemSize, compareTup);
    for (i = 0; i < numPivots; i++) {
        printf("Pivots:%d\n", *(int*) (pivots + (sizeof (int)*i)));
    }
    //for(i=0; i<arrayElemCount; i++) 
    //printf("%d ", a[i]);
#if 0
    /*------------If using FlashSort---------------*/
    UINT32 currentMax, currentMin;
    currentMax = currentMin = a[0];
    for (i = 0; i < arrayElemCount; i++) {
        if (a[i] > currentMax) {
            currentMax = a[i];
        }
        if (a[i] < currentMin) {
            currentMin = a[i];
        }
    }
    /*---------------------------------------------*/
#endif 
    UINT8 partitionId;
    int * count = (int*) malloc(numSplits * sizeof (int));
    for (i = 0; i < arrayElemCount; i++) {
        partitionId = getPartitionId(a + arrayElemSize * i);
        count[partitionId]++;
    }
    for (i = 0; i < numSplits; i++) {
        printf("Count:%d\n", count[i]);
    }
    /*Coalesce empty partitions / partitions less than threshold.
      Each split i corresponds to pivot (i-1) and pivot (i) except. 
     * Example split 2 corresponds to pivot 1 and pivot 2. 
     * Hence, when we coalesce a split i with i+1, pivot i
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

    for (i = 0; i < numPivots; i++) {
        printf("Pivots:%d\n", *(int*) (pivots + (sizeof (int)*i)));
    }
    for (i = 0; i < numSplits; i++) {
        printf("partitions[%d]:%d\n", i, partitionBeginnings[i]);
    }

#if 0

    for (i = 0; i < numSplits; i++) {
        printf("Partition Beginnings:%d\n", partitionBeginnings[i]);
    }
#endif
    /*For Debug*/

    /*-----------*/
    partitionBeginnings[numSplits] = arrayElemCount;
    memcpy(currPartitionPtr, partitionBeginnings, (numSplits + 1) * sizeof (int));
    for (i = 0; i < numSplits; i++) {
        for (j = currPartitionPtr[i]; j < partitionBeginnings[i + 1]; j++) {
            partitionPresent = getPartitionId(a + arrayElemSize * j);
            if (j >= partitionBeginnings[partitionPresent] && partitionPresent < partitionBeginnings[partitionPresent + 1]) {
                currPartitionPtr[i]++;
                continue;
            }
            firstVictimLoc = j;
            memcpy(presentVictim, a + arrayElemSize*j, arrElemSize);
            flag = 1;
            while (flag) {
                partitionStart = currPartitionPtr[partitionPresent];
                partitionEnd = partitionBeginnings[partitionPresent + 1];
                partitionLast = partitionPresent;
                for (k = partitionStart; k < partitionEnd; k++) {
                    partitionPresent = getPartitionId(a + arrayElemSize * k);
                    if (k >= partitionBeginnings[partitionPresent] && k < partitionBeginnings[partitionPresent + 1])
                        continue;
                    if (k == firstVictimLoc) {
                        flag = 0;
                    }
                    swapTuples(presentVictim, a + (arrayElemSize * k));
                    INC_WRITES;
                    currPartitionPtr[partitionLast] = k;
                    break;
                }
            }
        }
    }

    //printf("\nSaving in Writes:%f\%Expected%f%\n", (float)((arrayElemCount - (writes + 2*numSplits))*100)/(float)arrayElemCount, 1/(float)numSplits); 
    //for(i=0; i<arrayElemCount; i++) 
    //printf("%d ", a[i]); 

    free(presentVictim);

    return (EXIT_SUCCESS);
}

int compareRecords(const void *p1, const void *p2) {
    printf("Comparing %d %d\n", *(int*) p1, *(int*) p2);
    return (*(int*) p1 - *(int*) p2);
}
extern void _quicksort(void *const pbase, size_t total_elems, size_t size,
        __compar_fn_t cmp);

void sortPartition(int start, int end) {
    int i;
    //printf("\n\n==Before Sort==\n\n");
    //for (i=start; i<end;i++){
    //printf("Array: %u\n", array[i]);
    //}
    printf("Start %d, End %d", start, end);
    printf("\n\n==Original==\n\n");
    for (i = 0; i < end - start; i++) {
        printf("%d\n", *(int*) (a + ((start + i) * arrayElemSize)));
    }
    qsort(a + (start * arrayElemSize), end - start, arrayElemSize, Compare);
    printf("\n\n==Sorted==\n\n");
    for (i = 0; i < end - start; i++) {
        printf("%d\n", *(int*) (a + ((start + i) * arrayElemSize)));
    }


#ifdef UNDO
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
        memcpy(presentArrayCandidate, a + (arrayElemSize * (start + i)), arrayElemSize);
        flag = 1;
        pos[i] |= resolved;
        while (flag) {
            if (presentTargetLoc == firstVictimLoc) {
                flag = 0;
            }
            assert(presentTargetLoc < end - start);

            swapTuples(a + ((start + presentTargetLoc) * arrayElemSize),
                    presentArrayCandidate);
            //tempArrayVictim = array[start + presentTargetLoc];
            //array[start + presentTargetLoc] = presentArrayCandidate;
            //presentArrayCandidate = tempArrayVictim;

            nextTargetLoc = pos[presentTargetLoc];
            pos[presentTargetLoc] |= resolved;
            presentTargetLoc = nextTargetLoc;
        }
    }

    //start_counter();
#if 1
    *(UINT32*) outputBufferPtr = end - start;
    outputBufferPtr += sizeof (UINT32);
    for (i = 0; i < end - start; i++) {
        *(POS_ARRAY_TYPE*) outputBufferPtr = (pos[i] & (~resolved));
        outputBufferPtr += sizeof (POS_ARRAY_TYPE);
    }

    for (i = 0; i < end - start; i++) {
        memcpy(outputBufferPtr,
                a + ((start + i) * arrayElemSize),
                arrayElemSize);
        outputBufferPtr += arrayElemSize;
        pos[i] = i;
    }
#else
    write(fp1, array, sizeof (UINT32) *(end - start));
#endif
    //printf("\n\n==Original==\n\n");
    //for (i=0; i<end-start;i++){
    //printf("Array After Undo:%d\n", array[start+i]);
    //printf("Pos After Undo:%d\n", pos[i]);
    //}
#else 
    //write(array, sizeof(UINT32), end-start, fp1);
    write(fp1, array, sizeof (UINT32) *(end - start));
#endif
    //sync();
    //    totalCycles += get_counter();
}

int main() {

    /* For debugging            */
    int ARRAY[] = {8, 3, 4, 5, 2, 6, 7, 2, 3, 13, 15, 3, 4, 3, 4, 19, 5, 7};
    //int ARRAY[] = {2, 3, 13, 15, 3, 4, 3, 4, 19, 5, 7};
    a = (char*) ARRAY;

    int SIZE = 18;
    int i;
    char *outputBuffer = (char*) malloc(sizeof (int)*1000);
    printf("OutputBuffer Initial :%d", outputBuffer);
    printf("\n ===Orig Array====\n");
    for (i = 0; i < SIZE; i++) {
        printf("a[%d]: %d\n", i, ARRAY[i]);
    }

    createPartitions(0, 18, sizeof (int), compareRecords, outputBuffer);

    printf("\n ===Partitioned Array====\n");
    for (i = 0; i < SIZE; i++) {
        printf("a[%d]: %d\n", i, ARRAY[i]);
    }

#ifdef UNDO
    UINT32 numElem = 20;
    pos = (POS_ARRAY_TYPE*) malloc(numElem * sizeof (POS_ARRAY_TYPE));
    initSortElems(numElem);
#endif
#if 0
    for (index = 0; index < arrayElemCount; index++) {
        //array[i] = rand();	
        array[index] = arrayElemCount - index;
#endif
        for (i = 0; i < numSplits; i++) {
            /*Used as partition start ptr for quicksort. Replace with something cleaner later*/

            partitionStart = a + (arrayElemSize * partitionBeginnings[i]);
            sortPartition(partitionBeginnings[i], partitionBeginnings[i + 1]);
        }
        int remainingSize = SIZE;
        int currSize;
        int *pos;
        char *tuples;
        int * integerPointer = outputBuffer;

        printf("\n ===Final Array====\n");
        while (remainingSize > 0) {
            currSize = *(int*) outputBuffer;
            printf("CurrSize:%d\n", currSize);
            pos = outputBuffer + sizeof (UINT32);
            tuples = (char*) pos + currSize * sizeof (int);


            for (i = 0; i < currSize; i++) {
                printf("Elem: %d\n", *(int*) (tuples + (arrayElemSize * pos[i])));
            }
            /*One extra UINT32 size jump to jump the size parameter itself*/
            outputBuffer += currSize * (arrayElemSize + sizeof (UINT32)) + sizeof (UINT32);
            remainingSize -= currSize;
        }

        return 0;
    }
#if 0
    typedef struct sortElemNoUndo {
        UINT32 value;
    } sortElemNoUndo;
#endif





