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
#include "partitioning.h"
/*
 * 
 */
#define DISK_SIZE 1*GB
#define DRAM_SIZE 2*MB
#define PCM_SIZE 256*MB

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

/*Input Variables to Partitioning*/
char *array;
UINT32 arrayElemCount;
UINT32 arrayElemSize;
UINT32 numPartitions;
compareTuples Compare;
/**********************************/

UINT32 maxPartitionSize;
UINT32 numPivots;
UINT32 *partitionBeginnings = NULL;
UINT32 gCurrStart;
char *pivots;
func_sort_getHashValue sort_getHashValue;


POS_ARRAY_TYPE *pos;

#define TYPE_SORT 0
#define TYPE_HASH 1

void swap_tuples(char* ptr1, char* ptr2) {
    char * temp = (char*) malloc(arrayElemSize);
    memcpy(temp, ptr1, arrayElemSize);
    memcpy(ptr1, ptr2, arrayElemSize);
    memcpy(ptr2, temp, arrayElemSize);
    free(temp);
}

void sort_pivots() {
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
            swap_tuples(pivots + (arrayElemSize * min), pivots + (arrayElemSize * i));
        }
    }
}

/*Returns Parition ID for a particular element. 
 * For Hash Type, returns HashValue%numPartitions
 * For Sort Type, returns Sor*/
UINT32 get_partition_ID(char type, char *tuplePtr) {
    UINT32 beg = 0, end = numPivots - 1, mid;
    SINT32 comparisonResult;
    if (type == TYPE_HASH) {
        assert(sort_getHashValue);
        return (sort_getHashValue(tuplePtr) % numPartitions);
    }
    if (numPivots == 0) {
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

int calculate_partitions_beginnings(int type, int maxElementsPerPartitionSuggestion) {

    UINT32 i;
     
    UINT32 partitionId;
    
    memset(partitionBeginnings, 0, numPartitions * sizeof (int));
    UINT32 * count ;
    if(type == TYPE_SORT){
    count= (UINT32*) MALLOC(numPartitions * sizeof (UINT32));
    }
    else{
      count =  partitionBeginnings;
    }
    memset(count, 0, numPartitions * sizeof (int));
    /*Counting the number of elements in each partition*/
    for (i = 0; i < arrayElemCount; i++) {
        partitionId = get_partition_ID(type, array + arrayElemSize * i);
        count[partitionId]++;
    }

    /*Calculating the beginning positions of each partition by using a 
     * Running Sum similar to Count Sort*/
    if (type == TYPE_HASH) {
        int currValAtIndex = partitionBeginnings[0];
        int lastCurrVal;
        partitionBeginnings[0] = 0;
        for (i = 1; i <= numPartitions; i++) {
            lastCurrVal = currValAtIndex;
            currValAtIndex = partitionBeginnings[i];
            partitionBeginnings[i] = partitionBeginnings[i - 1] + lastCurrVal;
        }
        partitionBeginnings[numPartitions] = arrayElemCount;

        return; //Since we shouldn't go through coalescing stage.
    }

    /**************************Beginnings Calc Over**************************************/
    
    /*Coalesce empty partitions / partitions less than threshold.
      Each split i corresponds to pivot (i-1) and pivot (i) except. 
     * Example split 2 corresponds to pivot 1 and pivot 2. 
     * Hence, when we coalesce array split i with i+1, pivot i
     * should be removed. Note: We do this run 1 less times than numPartitions 
     * as last split's count is immaterial __|__|__|__|__ */
    UINT32 runningCount = 0,
            windowCount;
    UINT32 newNumPivots = 0, newNumSplits = 1;
    partitionBeginnings[0] = 0;
    for (i = 0; i < numPartitions - 1; i++) {
        runningCount += count[i];
        windowCount = count[i];
        while ((i < (numPartitions - 1)) && (windowCount < maxElementsPerPartitionSuggestion)) {
            i++;
            runningCount += count[i];
            windowCount += count[i];
        }
        if (i != (numPartitions - 1)) {
            memcpy(pivots + (newNumPivots * arrayElemSize),
                    pivots + (arrayElemSize * i),
                    arrayElemSize);
            newNumPivots++;
            partitionBeginnings[newNumSplits++] = runningCount;

        }


    }
    partitionBeginnings[newNumSplits] = arrayElemCount;

    numPivots = newNumPivots;
    numPartitions = newNumSplits;
    FREE(count);
    printf("Partition Counting Over [Partitions:%d]\n", numPartitions);
}

/*Starting actual partitioning by shuffling elements in Pos Array*/
void create_partitions_by_moving_pos(int type) {
    UINT32 *currPartitionPtr = (UINT32*) MALLOC((numPartitions + 1) * sizeof (int));
    UINT32 partitionPresent = INT_MAX, partitionLast;
    UINT32 partitionStart, partitionEnd, firstVictimLoc;
    BOOL flag;
    UINT32 presentVictim;

    maxPartitionSize = 0;

    int i, j, k;
    for (i = 0; i < numPartitions; i++) {
        if (partitionBeginnings[i + 1] - partitionBeginnings[i] > maxPartitionSize) {
            maxPartitionSize = partitionBeginnings[i + 1] - partitionBeginnings[i];
        }
        printf("partitions[%d]:%d\n", i, partitionBeginnings[i]);
    }
    printf("maxPartitionSize is %d\n", maxPartitionSize);

    partitionBeginnings[numPartitions] = arrayElemCount;


    memcpy(currPartitionPtr, partitionBeginnings, (numPartitions + 1) * sizeof (int));
    UINT32 temp;
    for (i = 0; i < numPartitions; i++) {
        for (j = currPartitionPtr[i]; j < partitionBeginnings[i + 1]; j++) {
            partitionPresent = get_partition_ID(type, array + arrayElemSize * pos[j]);

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
                    partitionPresent = get_partition_ID(type, array + arrayElemSize * pos[k]);
                    if (k >= partitionBeginnings[partitionPresent] && k < partitionBeginnings[partitionPresent + 1])
                        continue;
                    if (k == firstVictimLoc) {
                        flag = 0;
                    }
                    //swap_tuples(presentVictim, array + (arrayElemSize * k));
                    temp = presentVictim;
                    presentVictim = pos[k];
                    pos[k] = temp;

                    currPartitionPtr[partitionLast] = k;
                    break;
                }
            }
        }
    }
    printf("Partitioning Over\n");

    //FREE(presentVictim);
    FREE(currPartitionPtr);
    
        
    

    return;
}

/*Select Random Pivots and Sort them, taking care to remove duplicates. 
 * Update numPartitions and numPivots accordingly since we may not always get the desired amount*/
void identify_pivots_for_sort() {
   
    UINT32 index;
    for (index = 0; index < numPivots; index++) {
        memcpy(pivots + (index * arrayElemSize),
                array + (arrayElemSize * (rand() % arrayElemCount)),
                arrayElemSize);
    }
    for(index = 0; index<numPivots ; index++)
    {
        printf("Pivot %d\n", ((int*)pivots)[index]);
    }
        sort_pivots();
    

    UINT32 count = 1;
    for (index = 1; index < numPartitions - 1; index++) {
        if (Compare(pivots + (arrayElemSize * index),
                pivots + (arrayElemSize * (index - 1))) != 0) {
            memcpy(pivots + (arrayElemSize * count),
                    pivots + (arrayElemSize * index),
                    arrayElemSize);
            count += 1;
        }
    }
    numPivots = count;
    numPartitions = numPivots + 1;
    //partitionBeginnings = (UINT32*) MALLOC((numPartitions + 1) * sizeof (int));
}

/*As the name suggests, init global variables for later use throughout functions*/
int init_global_variables(char* arrayBeg, int numElem, int size, compareTuples compareTup, 
        UINT32 partitionsCount, UINT32 *partitionBeginningsArray, UINT32 *posArray) {
    Compare = compareTup;
    array = arrayBeg;
    arrayElemCount = numElem;
    arrayElemSize = size;
#ifdef VMALLOC
    assert(PCMStructPtr != NULL);
    vmPCM = PCMStructPtr;
#endif
    numPartitions = partitionsCount;
    numPivots = numPartitions - 1;
    partitionBeginnings = partitionBeginningsArray;
    pos = posArray;
    srand(220587);

    return 0;
}

/*Cleanup pivots heap location*/
void cleanup_pivots() {
    if (pivots != NULL) {
        FREE(pivots);
        pivots = NULL;
    }
}

/*Partition the array using some random pivots. Output is a PARTIALLY SORTED array,
  i.e the elements in between pivots belong to the right range, albeit unsorted 
  *************NOTE: Provide an already populated POS array to the fn************/
int partition_with_pivots_using_pos(char* arrayBeg, int numElem, int size, compareTuples compareTup,
        UINT32 partitionsCount, UINT32 *partitionBeginningsArray, UINT32 *posArray,
        UINT32 maxElementsPerPartitionSuggestion) {

    if (numElem < maxElementsPerPartitionSuggestion) {
        printf("Partition already smaller than Max Elements allowed in a Partition. Returning Directly");
        return 0;
    }
#ifdef VMALLOC
    InitSort(PCMStructPtr, arrayToBeSorted, elemCount, elemSize, compareFunc, outputBuffer, numPartitions);
#else
    init_global_variables(arrayBeg, numElem, size, compareTup, partitionsCount, partitionBeginningsArray, posArray);
#endif
    pivots = (char*) MALLOC((numPartitions - 1) * arrayElemSize);

    identify_pivots_for_sort();
    calculate_partitions_beginnings(TYPE_SORT, maxElementsPerPartitionSuggestion);
    create_partitions_by_moving_pos(TYPE_SORT);
    return numPartitions;
}

/*Partition the array using hashing. OUtput is a NON-SORTED but hash partitioned array*/
/*************NOTE: Provide an already populated POS array to the fn************/
int partition_with_hash_using_pos(char* arrayBeg, int numElem, int size, compareTuples compareTup,
        UINT32 partitionsCount, UINT32 *partitionBeginningsArray, UINT32 *posArray,
        UINT32 maxElementsPerPartitionSuggestion, func_sort_getHashValue f_sort_getHashValue) {

    if (numElem < maxElementsPerPartitionSuggestion) {
        printf("Partition already smaller than Max Elements allowed in a Partition. Returning Directly");
        return 0;
    }
    init_global_variables(arrayBeg, numElem, size, compareTup, partitionsCount, partitionBeginningsArray, posArray);

    sort_getHashValue = f_sort_getHashValue;

    calculate_partitions_beginnings(TYPE_HASH, maxElementsPerPartitionSuggestion);
    create_partitions_by_moving_pos(TYPE_HASH);
    return numPartitions;
}