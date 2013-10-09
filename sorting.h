/* 
 * File:   sorting.h
 * Author: root
 *
 * Created on 5 October, 2013, 3:58 PM
 */

#ifndef SORTING_H
#define	SORTING_H

#ifdef	__cplusplus
extern "C" {
#endif
    
typedef int (*compareTuples)(const void * p1, const void * p2);

#ifdef VMALLOC
#include "vmalloc.h"
int sortMultiPivotAndUndo(Vmalloc_t *PCMStructPtr, char* arrayToBeSorted, UINT32 elemCount, UINT32 elemSize, 
        compareTuples compareFunc, char* outputBuffer, UINT32 numPartitions, UINT32 maxThreshhold);
#else
int sortMultiPivotAndUndo(char* arrayToBeSorted, UINT32 elemCount, UINT32 elemSize, 
        compareTuples compareFunc, char* outputBuffer, UINT32 numPartitions, UINT32 maxThreshhold);
#endif

#ifdef	__cplusplus
}
#endif

#endif	/* SORTING_H */

