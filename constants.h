/*
 * constants.h
 *
 *  Created on: 10-Apr-2013
 *      Author: spec
 */

#ifndef CONSTANTS_H_
#define CONSTANTS_H_

#define UINT8 unsigned char
#define UINT16 unsigned short int
#define UINT32 unsigned int
#define SINT32 signed int


#define KB (1024)
#define MB (1024 * KB)
#define GB (1024 * MB)


#define NUM_PASSES 1
#define DB_FILE "../tables/lineitem.raw.1"
#define FUNC_GROUPBY readAndEnjoyByHashing
//#define FUNC_GROUPBY readAndEnjoyBySorting
#ifdef VMALLOC
#define MALLOC(size) malloc(size); 
#define FREE(ptr) free(ptr);
#else
#define MALLOC(size) vmalloc(vmPCM, size);
#define FREE(ptr) free(vmPCM, ptr);
#endif

/*Arrangement of valid bits : 8-1 16-9....*/

#define CHECK_VALID(valid, index) ( valid[index/8] & ( 1<<(index%8)) )
#define SET_VALID(valid, index)   ( valid[index/8] |= ( 1<<(index%8)) )

#define DRAM_MEMORY_SIZE (256*MB)
#define PCM_MEMORY_SIZE (1*GB)
#define OVERFLOW_PARTITION_SIZE (256*KB)



#define ENTRIES_PER_PAGE 64
#define BITS_PER_BYTE 8
#define BITMAP char

/******NOTE: Change HASH_SHIFT acc to HASH_MASK**********/
#define HASH_MASK 0x0000FF00
#define HASH_SHIFT 8
/********************************************************/

#define TYPE_NUM 0
#define TYPE_STR 1
#define TYPE_FLOAT 2

#endif /* CONSTANTS_H_ */
