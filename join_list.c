//File: join_list.c
#include <stdio.h>
#include <stdlib.h>
#include "join_list.h"
#include "structs.h"


 typedef struct info_node{
 	int size;				// einai to plithos ton komvon
	int results;			/////////////////////////		einai to plithos ton apotelesmaton tou join diladi poses grammes exei o 

	typos_deikti arxi;
	typos_deikti telos;
 }info_node;


 typedef struct typos_komvou{
 	tuple *array;
	int   index;
	int   size;


	typos_deikti epomenos;
 }typos_komvou;



 info_deikti LIST_dimiourgia(info_deikti* lista){

 	info_deikti linfo;
 	linfo = malloc(sizeof(info_node));
 	linfo->size = 0;
 	linfo->arxi = NULL;
	linfo->telos = NULL;

	linfo->results = 0;
 	return linfo;
 }



 void eisagogi_komvou(info_deikti* linfo){

	int i;
 	typos_deikti prosorinos;
 	prosorinos = malloc(sizeof(typos_komvou));


	prosorinos->index = 0;
	prosorinos->size = 65000;
	prosorinos->array = malloc(prosorinos->size * sizeof(tuple));
	if(prosorinos->array == NULL){
	    printf("Error malloc prosorinos->array  \n");
	    exit(1);
	}

 	if((*linfo)->size == 0){
 		(*linfo)->arxi = prosorinos;
		(*linfo)->telos = prosorinos;
		prosorinos->epomenos = NULL;

 	}
 	if((*linfo)->size > 0){
		prosorinos->epomenos = NULL;
 		(*linfo)->telos->epomenos = prosorinos;
		(*linfo)->telos = prosorinos;
 	}
 	(*linfo)->size++;
 }


 void eisagogi_eggrafis(info_deikti* linfo, uint64_t a, uint64_t b){

        if((*linfo)->telos == NULL) eisagogi_komvou(linfo);
	typos_deikti mapas = (*linfo)->telos;

	if(mapas->index == mapas->size){
	    eisagogi_komvou(linfo);
	    eisagogi_eggrafis(linfo, a, b);
	}else{

	    mapas->array[mapas->index].key  = a;
	    mapas->array[mapas->index].payload  = b;
	    mapas->index++;

	    (*linfo)->results++;
	}

	return;
 }


 /*
	epistrefei to plithos to results
	diladi pose grammes exei o pinakas apo tuple
 */
 int take_crowd_results(info_deikti *linfo){
	 return (*linfo)->results;
 }


 /*
	epistrefei to tuple sti grammi row
	ara an kseroume poses grammes exei sinolika to apotelesma mporoume na tis paroume mia - mia
 */

 tuple take_row(info_deikti *linfo, int row){
	typos_deikti mapas = (*linfo)->arxi;

	while(mapas != NULL){
 	    if(row < mapas->size){
		tuple t;
	        t.key = mapas->array[row].key;
	  	t.payload = mapas->array[row].payload;
		return t;
	    }else{
		row = row - mapas->size;
 	        mapas = mapas->epomenos;
	    }
 	}

	printf("Kati phge LATHOS \n\n");
 }







 void emfanisi(info_deikti* linfo){
        int i;
	typos_deikti mapas = (*linfo)->arxi;

/*        FILE *f;
        if(mapas != NULL){
	    f = fopen("results.csv", "w");
	    if(f == NULL){
	        printf("Error file for results. \n");
		exit(1);
	    }
        }
*/
 	while(mapas != NULL){
 	    for(int i = 0 ; i < mapas->index ; i++) printf("%lu  -  %lu \n", mapas->array[i].key, mapas->array[i].payload);
	    mapas = mapas->epomenos;
 	}

//        if( (mapas = (*linfo)->arxi) != NULL) fclose(f);
 }






 void lista_diagrafi(info_deikti* linfo){
	int i;

        if((*linfo)->arxi != NULL){

 	    typos_deikti mapas = (*linfo)->arxi;
	    typos_deikti mapas2;

 	    while(mapas != NULL){
		mapas2 = mapas;
		mapas = mapas->epomenos;
		free(mapas2->array);
		free(mapas2);
	    }
	    (*linfo)->arxi = NULL;
	    (*linfo)->telos = NULL;
	    (*linfo)->size = 0;
	    (*linfo)->results = 0;
        }
	free(*linfo);
	(*linfo) = NULL;
 }
