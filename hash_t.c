#include "hash_t.h"



void dimiourgia(hash_st *table, int size){

    table = malloc(sizeof(hash_st));
    if(table == NULL){
        printf("Error malloc hash_t \n");
	exit(1);
    }

    table->hash_point = malloc(size * sizeof(hash_node));
    if(table->hash_point == NULL){
        printf("Error malloc table.hash_point   \n");
	exit(1);
    }

    for(int i = 0 ; i < size ; i++){
        table->hash_point[i].order = malloc(size * sizeof(int));
        if( table->hash_point[i].order == NULL){
            printf("Error malloc \n");
	    exit(1);
        }
        table->hash_point[i].cost = 0;
	table->hash_point[i].index = 0;
    }

}







void eisagwgi_hash_table(hash_st *table, int thesi, int stoixeio){
    printf("000000000 \n");
    int index = table->hash_point[thesi].index;

    table->hash_point[thesi].order[index] = stoixeio;
    table->hash_point[thesi].index++;

    printf("111111111 \n");
}








int is_in_hast_t(hash_st *table, int bucket, int stoixeio){

    for(int i = 0 ; i < table->hash_point[bucket].index ; i++){
        if( table->hash_point[bucket].order[i] == stoixeio ) return 0;
    }
    return 1;
}
