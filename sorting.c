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

typedef SINT32 (*compareTuples)(void * p1, void * p2);

compareTuples Compare;
UINT32 *pivots;

#define POS_ARRAY_TYPE UINT32
#define UNDO 

UINT32 arrayElemCount;
char *a;
UINT32 numSplits;
UINT32 numPivots;
UINT8 arrayElemSize;
int writes;

void arrayInit() {
        int i;
        for(i=0; i<arrayElemCount; i++){
                a[i] = i;
                //a[i] = rand();        
        }
}


void sortPivots(){
    /*Using Selection Sort to keep it simple*/
    int i,
            j,
            min,
            temp;
    for (i=0; i<numPivots; i++){
        min = i;
        for(j=i+1;j<numPivots; j++){
            if(Compare(a + (arrayElemSize*pivots[min]), a+(arrayElemSize*pivots[j])) > 0){
                min = j;
            }
        }
        if (min != i){
                temp = pivots[min];
                pivots[min] = pivots[i];
                pivots[i] = temp;
            }
    }
}
int InitSort(UINT32** start, UINT32** currPartitionPtr, int stepSize, compareTuples compareTup){
    
        //numSplits = PCM_SIZE/2*DRAM_SIZE; 
    numSplits = 5;
        numPivots = numSplits - 1;
        /*Don't store just indices in pivots to the original array. When you 
         more around elements in arrays, those indices are no longer valid*/
        pivots = (UINT32*)malloc((numSplits-1) * sizeof(UINT32));
        arrayElemSize = stepSize;
        UINT32 count;
        Compare = compareTup;
        srand(13);
        /*Pivots will be 1 less than #splits*/
        UINT8 index;
        for(index=0; index < numPivots; index++){
            pivots[index] = rand()%arrayElemCount;
        }
        
        sortPivots();
        
        count = 1;
        for(index=1; index< numSplits -1; index++){
            if ((pivots[index] != pivots[index-1]) && 
                    (Compare(a + (arrayElemSize*pivots[index]), a+(arrayElemSize*pivots[index-1])) != 0 ) ){
                pivots[count++] = pivots[index];
            }
        }       
        numPivots = count;
        numSplits = numPivots+1;
        *start = (int*)malloc((numSplits+1) * sizeof(int));
        *currPartitionPtr = (int*)malloc((numSplits+1) * sizeof(int));
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
    UINT32 beg =0 , end = numPivots - 1, mid;
    SINT32 comparisonResult;
    /*Do a binary search here*/
    while (beg <= end){
        mid = (beg+end)/2;
        comparisonResult = Compare(a + (arrayElemSize * pivots[mid]), tuplePtr);
        if (comparisonResult < 0){
            /*Case if it is greater than greatest pivot*/
            if (mid == numPivots-1){
                return mid+1;
            }
            /*If it is greater than both this and next pivot*/
            else if(Compare(a + (arrayElemSize* pivots[mid+1]), tuplePtr) < 0){
                beg = mid + 1;
            }
            else if(Compare(a + (arrayElemSize* pivots[mid+1]), tuplePtr) == 0){
                return mid + 2;
            }
            /*If it lies between this and next pivot*/
            else{
                return mid + 1;
            }
        }
        /*It is lesser than this pivot*/
        else if (comparisonResult > 0){
            /* If lesser than first pivot*/
            if(mid == 0){
                return mid;
            }
            end = mid - 1;
        }
        /*It is equal to this pivot*/
        else
            return mid + 1;
    }
        //return  ( num  % (sHT->HTBucketCount*NUM_PASSES) );
}
#else
int getPartitionId(int value) {
        return value%numSplits ;
        //return  ( num  % (sHT->HTBucketCount*NUM_PASSES) );
}
#endif
void swapTuples(char* ptr1, char* ptr2){
    char * temp = (char*)malloc(arrayElemSize);
    memcpy(temp, ptr1, arrayElemSize);
    memcpy(ptr1, ptr2, arrayElemSize);
    memcpy(ptr2, temp, arrayElemSize);
    printf("Putting %d in place of %d\n", *(int*)ptr2, *(int*)ptr1);
    free(temp);
}

int createPartitions(int start, int arrElemCount, int arrElemSize, compareTuples compareTup) {


    UINT32 *partitionBeginnings = NULL;
    UINT32 i, j, k;
    UINT32 presentCount = 0, partitionPresent = INT_MAX, partitionLast;
    UINT32 partitionStart, partitionEnd, firstVictimLoc;
    BOOL flag;
    char *presentVictim = (char*) malloc(arrElemSize);
    UINT32 *currPartitionPtr;
    //arrayInit(); 
    arrayElemCount = arrElemCount;
    InitSort(&partitionBeginnings, &currPartitionPtr, arrElemSize, compareTup);
        for (i = 0; i < numPivots; i++) {
        printf("Pivots:%d\n", pivots[i]);
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
    int * count = (int*)malloc(numSplits * sizeof(int));
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
    UINT32 runningCount=0,
            windowCount;
    UINT8 newNumPivots  = 0, newNumSplits = 1;
    UINT8 THRESHHOLD  = 1;
    for (i = 0; i < numSplits -1 ; i++) {
        runningCount += count[i];
        windowCount = count[i];
        while((i < (numSplits -1)) && (windowCount < THRESHHOLD) )
        {
            i++;
            runningCount += count[i];
            windowCount += count[i];
        }
        if(i != (numSplits - 1)){
            pivots[newNumPivots++] = pivots[i];
        partitionBeginnings[newNumSplits++] = runningCount;
            
        }
        
        
        }
    numPivots = newNumPivots;
    numSplits = newNumSplits;
        
    for (i = 0; i < numPivots; i++) {
        printf("Pivots[%d]:%d\n", i, pivots[i]);
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
SINT32 compareRecords(void *p1, void *p2){
    printf("Comparing %d %d\n", *(int*)p1 , *(int*)p2);
    return (*(int*)p1 - *(int*)p2);
}
void main(){
    //int ARRAY[] = {8,3,4,5,2,6,7, 2, 3, 13, 15, 3, 4,3,4,19,5,7};
    int ARRAY[] = {2, 3, 13, 15, 3, 4, 3, 4, 19, 5, 7};
    a = (char*)ARRAY;
    int i;
    //printf("Sizeof Array %d\n", sizeof(ARRAY));
    printf("\n ===Orig Array====\n"); 
    for(i=0 ; i<11; i++){
        printf("a[%d]: %d\n", i, ARRAY[i]);
    }
    createPartitions(0, 11, sizeof(int), compareRecords);
    
    printf("\n ===Final Array====\n"); 
    for(i=0 ; i<11; i++){
        printf("a[%d]: %d\n", i, ARRAY[i]);
    }
}

#if 0

typedef struct sortElemNoUndo {
    UINT32 value;
} sortElemNoUndo;
#define qsort _quicksort 
#define MSB_ZERO 0
#define MSB_ONE 1
#define BYTE 8
int resolved = 1 << (BYTE * sizeof (int) - 1);
UINT32 array[arrayElemCount];
UINT32 dummyArray[arrayElemCount];
UINT32 numElem;
double totalCycles;
void *regionDRAM, *regionPCM;

extern void _quicksort(void *const pbase, size_t total_elems, size_t size,
        __compar_fn_t cmp);

#ifdef UNDO
POS_ARRAY_TYPE *pos;

void initSortElems() {
    int i;
    for (i = 0; i < numElem; i++) {
        pos[i] = i;
    }
}
#endif

int compareElem(const void *a, const void *b) {
    UINT32 sE1 = *(UINT32*) a;
    UINT32 sE2 = *(UINT32*) b;
    return sE1 - sE2;
}

void sortPartition(int start, int currPartitionPtr) {
    int i;
    //printf("\n\n==Before Sort==\n\n");
    //for (i=start; i<currPartitionPtr;i++){
    //printf("Array: %u\n", array[i]);
    //}
    qsort(array + start, currPartitionPtr - start, sizeof (int), compareElem);
    printf("\n\n==Sorted==\n\n");

    //}
    int fp1 = open("sortOutput", O_WRONLY);
    clock_t end1;
#ifdef UNDO
    POS_ARRAY_TYPE presentTargetLoc, nextTargetLoc, tempVictim;
    UINT32 tempArrayVictim, presentArrayCandidate;
    char flag;
    UINT32 firstVictimLoc;
    for (i = 0; i < currPartitionPtr - start; i++) {
        if (!!(pos[i] & resolved) == MSB_ONE) //!! to convert to boolean
            continue;
        if (pos[i] == i) {
            pos[i] |= (1 << (BYTE * sizeof (int) - 1)) | i;
            continue;
        }
        firstVictimLoc = i;
        presentTargetLoc = pos[i];
        presentArrayCandidate = array[start + i];
        flag = 1;
        pos[i] |= resolved;
        while (flag) {
            if (presentTargetLoc == firstVictimLoc) {
                flag = 0;
            }
            assert(presentTargetLoc < currPartitionPtr - start);

            tempArrayVictim = array[start + presentTargetLoc];
            array[start + presentTargetLoc] = presentArrayCandidate;
            presentArrayCandidate = tempArrayVictim;

            nextTargetLoc = pos[presentTargetLoc];
            pos[presentTargetLoc] |= resolved;
            presentTargetLoc = nextTargetLoc;
        }
    }

    start_counter();
#if 1
    for (i = 0; i < currPartitionPtr - start; i++) {
        //write(array+(pos[i] & (~resolved)), sizeof(UINT32), 1, fp1);
        write(fp1, array + (pos[i] & (~resolved)), sizeof(UINT32));
        pos[i] = i;
    }
#else
    write(fp1, array, sizeof (UINT32) *(currPartitionPtr - start));
#endif
    //printf("\n\n==Original==\n\n");
    //for (i=0; i<currPartitionPtr-start;i++){
    //printf("Array After Undo:%d\n", array[start+i]);
    //printf("Pos After Undo:%d\n", pos[i]);
    //}
#else 
    //write(array, sizeof(UINT32), currPartitionPtr-start, fp1);
    write(fp1, array, sizeof(UINT32) *(currPartitionPtr - start));
#endif
    //sync();
    totalCycles += get_counter();
}

int main() {
    int i, index;
    //Dividing by 2 to give space for program and stack data
    /*Not using macro SORTELEM for numElem to make sure same 
    number of elements are compared, i.e. space overhead in 
    UNDO case is utilized for caching in NOUNDO case*/
    numElem = DRAM_MEMORY_SIZE / (2 * sizeof (UINT32));
#ifdef UNDO
    pos = (POS_ARRAY_TYPE*) malloc(numElem * sizeof (POS_ARRAY_TYPE));
    initSortElems(numElem);
#endif
    int numPasses = arrayElemCount / numElem;
    int fp2;
    fp2 = open("sortOutput", O_RDONLY);

    for (i = 0; i < numPasses; i++) {
/* Code to model time taken to read the input records from file */
    start_counter();
    read(fp2, array, DRAM_MEMORY_SIZE / 2);
    totalCycles += get_counter();
    close(fp2);
/* *************************************************************/
    for (index = 0; index < arrayElemCount; index++)
        //array[i] = rand();	
        array[index] = arrayElemCount - index;

        sortPartition(i*numElem, (i + 1) * numElem);
    }
    //Sort Remaining Part of Array 
    sortPartition(numPasses*numElem, arrayElemCount);
    //printf("Time : %" PRIu64 "!cycles \n", totalCycles);
    start_counter(); 
    sync();
    totalCycles += get_counter();

    printf("Time : %lf cycles \n", totalCycles);
    printf("Forcing PCM cache data to be written to PCM\n");
    UINT32 sum = array[rand() % arrayElemCount];
    for (i = 0; i < arrayElemCount; i++) {
        sum += array[i];
    }
    printf("Sum=%u\n", sum);
    printf("random pcm element=%u\n", array[rand() % arrayElemCount]);
    return 0;
}
#endif