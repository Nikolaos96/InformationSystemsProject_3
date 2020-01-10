#ifndef  __HASH_T__
#define __HASH_T__
#include <stdlib.h>
#include <stdio.h>



typedef struct hash_node{
    int cost;
    int *order;

    int index;
}hash_node;



typedef struct hash_st{
    hash_node * hash_point;
} hash_st;




void dimiourgia(hash_st *table, int size);
void eisagwgi_hash_table(hash_st *table, int thesi, int stoixeio);
int is_in_hast_t(hash_st *table, int bucket, int stoixeio);

#endif
