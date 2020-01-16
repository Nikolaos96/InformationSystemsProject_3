#ifndef __QUEUE__
#define __QUEUE__

#include "semaphore.h"
#include "structs.h"
#include "semaphore.h"
#include <stdlib.h>


typedef struct { 

	int queryNum;
	q* predicates;
	int number_of_predicates;
	int* tables;
	int relation_number;
	checksum_struct *checksums;
	int number_of_checksums;
	int* predicatesOrder;

}queueElement;

sem_t semQueue;
int queue_size;
int queue_count;
int queue_head;
int queue_tail;
int queriesChecked;
queueElement* queue; 

pthread_cond_t cond_nonempty;
pthread_cond_t cond_nonfull;
pthread_mutex_t mtx;


void create_queue(int size);
void remove_queue(void);
int queue_empty(void);
int queue_full(void);
void add_queue(void);
void delete_queue(void);
void initializeQueriesNumber(int value);

#endif