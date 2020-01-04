#ifndef __HASHTABLE__
#define __HASHTABLE__
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>


typedef struct hash_table    * deiktis_ht;
typedef struct bucket * deiktis_b;
typedef struct rows_list  rows_node;


typedef struct rows_list{
        uint64_t row_id;
	uint64_t row_id2;

	rows_node * next_row;
} rows_list;



typedef struct dedomena_deikti{
        uint64_t rowId;

	rows_node * row;
} dedomena_deikti;



deiktis_ht HashTable_dimiourgia(deiktis_ht*);
void dimoiourgeia_arxikwn_bucket(deiktis_ht* linfo);



void eisagogi_rowId(deiktis_ht* linfo, uint64_t rowId, uint64_t rowId2, uint64_t rowId3, int periptosi);


 uint64_t emfanisi_ht(deiktis_ht* linfo,int j);
 void HashTable_diagrafi(deiktis_ht* linfo);
 rows_node * take_list(deiktis_ht* linfo, uint64_t rowid);
 int take_unique_ids(deiktis_ht* linfo);


 uint64_t hash(uint64_t x, int p);
#endif
