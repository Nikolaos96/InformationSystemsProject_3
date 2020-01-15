#ifndef  _HASH_T_
#define _HASH_T_
#include <stdlib.h>
#include <stdio.h>
#include "structs.h"



typedef struct hash_node{
    int cost;
    int *order;
    int index;

}hash_node;



typedef struct hash_st{
    hash_node *hash_point;
} hash_st;


int is_in_hash_t(hash_node *table, int bucket, int stoixeio);
int connected(hash_node* table, int s, int j,q* predicates,int number_of_predicates,int flag);

#endif
