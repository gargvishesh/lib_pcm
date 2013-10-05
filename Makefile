CC = gcc -g

CFLAGS = -c -Wall 

HASH_INCLUDE = -I $(CURDIR)/../GeneralHashFunctions_-_C/
VMEM_INCLUDE = -I $(CURDIR)/../vmalloc_mem/include
LIB_VMEM = -L $(CURDIR)/../vmalloc_mem/lib -l vmalloc
LIB_HASHING = $(CURDIR)/../GeneralHashFunctions_-_C/GeneralHashFunctions.o


all: libHashTable.o libSorting.o qsort.o

libHashTable.o: hashTable.c
	$(CC) $(CFLAGS) $(VMEM_INCLUDE) $(HASH_INCLUDE) -DVMALLOC hashTable.c -o libHashTable.o
	
libSorting.o: sorting.c
	$(CC) $(CFLAGS) -DUNDO sorting.c -o libSorting.o
	
qsort.o: qsort.c
	$(CC) $(CFLAGS) -DUNDO qsort.c -o qsort.o

clean:
	rm -rf *.o *.out

update: clean all

run:
	$(CURDIR)/../ptlsim_pcm/ptlsim -stats pcmGroupBy.stats -logfile pcmGroupBy.log -- pcmGroupBy
	$(CURDIR)/../ptlsim_pcm/ptlstats -snapshot 0 -histogram-thresh 0.0 pcmGroupBy.stats > stats.txt
