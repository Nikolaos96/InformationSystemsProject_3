//File: Hash_Table.c
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "HashTable.h"
#define   SIZE_BUCKET 1000


 /*
  Pinakas deiktwn - Hash_Table me oliki apokripsi
 */
 typedef struct hash_table{
	deiktis_b arxi;
  int unique_ids;
 }hash_table;



 /*
  Komvos gia ta bucket tou Hash_Table
  Ta Dedomena einai oi user
  H metavliti adeia_thesi mas leei kathe stigmi poses adeies theseis exei to bucket
  Kai o epomenos einai deiktis gia to epomeno bucket yperxilisis
 */
 typedef struct bucket{
 	dedomena_deikti * Dedomena;
	int adeia_thesi;

	deiktis_b epomenos;
 }bucket;



 /*
  Dimiourgeia diktwn gia to Hash_Table
 */
 deiktis_ht HashTable_dimiourgia(deiktis_ht* lista){// Dimiourgia hash_Table

     deiktis_ht linfo;
     linfo = malloc(sizeof(hash_table));
     linfo->arxi = NULL;
     linfo->unique_ids=0;

//	printf("Hash Table ook \n");
     return linfo;
 }



 /*
  Dimiourgeia arxikwn bucket gia kathe thesi tou Hash_Table
 */
 void dimoiourgeia_arxikwn_bucket(deiktis_ht* linfo){ // gia kathe deikti tou hash table arxika bucket

     deiktis_b prosorinos = malloc(sizeof(bucket));
     prosorinos->Dedomena = malloc(SIZE_BUCKET * sizeof(dedomena_deikti));
     prosorinos->adeia_thesi = 0;
     prosorinos->epomenos = NULL;
    (*linfo)->arxi = prosorinos;

//	printf("bucket ok \n");
     return;
 }



 void eisagogi_rowId(deiktis_ht* linfo, uint64_t rowId, uint64_t rowId2, uint64_t rowId3, int periptosi){

     deiktis_b bucket = (*linfo)->arxi;
     int find = 0, i;

     while(bucket != NULL){
     	for(i = 0 ; i < bucket->adeia_thesi ; i++){
            if(bucket->Dedomena[i].rowId == rowId){
	        find = 1;
	        break;
	    }
         }


	 if(find == 1) break;
	 else	       bucket = bucket->epomenos;
     }




     //printf("ssssssssssssssss \n");
     if(find == 1){ // tote vrethike to rowId ara prepei na valoume ta alla rowId stin lista

	rows_node *t = bucket->Dedomena[i].row;
        while(t->next_row != NULL) t = t->next_row;

        rows_node *t2 = malloc(sizeof(rows_list));
        t->next_row = t2;

	t2->row_id = rowId2;
	if(periptosi == 2) t2->row_id2 = rowId3;

	t2->next_row = NULL;

  //      printf("Egine eisagwgi rowId se idi yparxon rowId \n");

     }else{ // den vrethike, prepei na to valoume sto hash table an exei xwro
   (*linfo)->unique_ids++;
	 bucket = (*linfo)->arxi;
	 while(bucket->epomenos != NULL) bucket = bucket->epomenos;

         if(bucket->adeia_thesi < SIZE_BUCKET){

	      bucket->Dedomena[bucket->adeia_thesi].rowId = rowId;

	      bucket->Dedomena[bucket->adeia_thesi].row = malloc(sizeof(rows_list));

	      if(periptosi == 2) bucket->Dedomena[bucket->adeia_thesi].row->row_id2 = rowId3;


	      bucket->Dedomena[bucket->adeia_thesi].row->row_id = rowId2;
	      bucket->Dedomena[bucket->adeia_thesi].row->next_row = NULL;

	      bucket->adeia_thesi++;
//	      printf("Eisagogi eggrafis se yparxon bucket \n");

	 }else{

	     deiktis_b new_bucket = malloc(sizeof(bucket));
             new_bucket->Dedomena = malloc(SIZE_BUCKET * sizeof(dedomena_deikti));
	     new_bucket->adeia_thesi = 0;
	     new_bucket->epomenos = NULL;
	     bucket->epomenos = new_bucket;


	     new_bucket->Dedomena[0].rowId = rowId;
	     new_bucket->Dedomena[0].row = malloc(sizeof(rows_list));
	     new_bucket->Dedomena[0].row->row_id = rowId2;

	     if(periptosi == 2) bucket->Dedomena[bucket->adeia_thesi].row->row_id2 = rowId3;

	     new_bucket->Dedomena[0].row->next_row = NULL;
	     new_bucket->adeia_thesi++;


//	     printf("Eisagwgi se neo bucket  %d \n", new_bucket->adeia_thesi);
	 }

     }


 }

 uint64_t emfanisi_ht(deiktis_ht* linfo,int j){
     deiktis_b bucket = (*linfo)->arxi;

     while(1){//bucket != NULL

           if(j<SIZE_BUCKET)
	          return bucket->Dedomena[j].rowId;
           else{
            j=j-SIZE_BUCKET;
            bucket = bucket->epomenos;
           }

     }

 }


 void HashTable_diagrafi(deiktis_ht* linfo){
     deiktis_b bucket2, bucket = (*linfo)->arxi;

     while(bucket != NULL){
         bucket2 = bucket;

	 for(int i = 0 ; i < bucket2->adeia_thesi ; i++){
             rows_node *t2, *t = bucket2->Dedomena[i].row;
             while(t != NULL){
	         t2 = t;
                 t = t->next_row;
	         free(t2);
             }
	 }
         bucket = bucket->epomenos;
	 free(bucket2->Dedomena);
	 free(bucket2);
     }
     free((*linfo));
//     printf("Diagrafi hashTable \n");
 }



 rows_node * take_list(deiktis_ht* linfo, uint64_t rowid){
     deiktis_b bucket = (*linfo)->arxi;

     while(bucket != NULL){
         for(int i = 0 ; i < bucket->adeia_thesi ; i++){
	     if(bucket->Dedomena[i].rowId == rowid){
	         return bucket->Dedomena[i].row;
	     }
	 }
         bucket = bucket->epomenos;
     }
 }
int take_unique_ids(deiktis_ht* linfo){
  return (*linfo)->unique_ids;
}

 uint64_t hash(uint64_t x, int p) {
//    x = (x ^ (x >> 30)) * UINT64_C(0xbf58476d1ce4e5b9);
//    x = (x ^ (x >> 27)) * UINT64_C(0x94d049bb133111eb);
//    x = x ^ (x >> 31);

    uint64_t a = x % p;

    return a;
}
