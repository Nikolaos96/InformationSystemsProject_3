//File: lista.c
#include <stdio.h>
#include <stdlib.h>
#include "mid_list.h"



 typedef struct main_node{
 	int size;
	int results;

	int columns;
	int *array_relations;
	int *array_columns;

	node_pointer arxi;
	node_pointer telos;
 }main_node;



 typedef struct node{
 	uint64_t *array;
	int   index;
	int   size;

	node_pointer epomenos;
 }node;




 main_pointer MID_dimiourgia(main_pointer *lista, int col, int r1, int c1, int r2, int c2, int r3, int c3, int r4, int c4){
 	main_pointer linfo;
 	linfo = malloc(sizeof(main_node));
 	linfo->size = 0;
 	linfo->results = 0;
	linfo->columns = col;

	linfo->array_relations = malloc(col * sizeof(int));
	if(linfo->array_relations == NULL){
		printf("Error malloc linfo->array_columns \n");
		exit(1);
	}

	linfo->array_columns = malloc(col * sizeof(int));
	if(linfo->array_columns == NULL){
		printf("Error malloc linfo->array_columns \n");
		exit(1);
	}

	if(col == 2){
		linfo->array_relations[0] = r1;
		linfo->array_relations[1] = r2;

		linfo->array_columns[0] = c1;
		linfo->array_columns[1] = c2;
	}else if(col == 3){
		linfo->array_relations[0] = r1;
		linfo->array_relations[1] = r2;
		linfo->array_relations[2] = r3;

		linfo->array_columns[0] = c1;
		linfo->array_columns[1] = c2;
		linfo->array_columns[2] = c3;
	}else{
		linfo->array_relations[0] = r1;
		linfo->array_relations[1] = r2;
		linfo->array_relations[2] = r3;
		linfo->array_relations[3] = r4;

		linfo->array_columns[0] = c1;
		linfo->array_columns[1] = c2;
		linfo->array_columns[2] = c3;
		linfo->array_columns[3] = c4;
	}

	linfo->arxi = NULL;
	linfo->telos = NULL;
 	return linfo;
 }



 void eisagogi_komvou_mid(main_pointer *linfo){

	int i;
 	node_pointer prosorinos;
 	prosorinos = malloc(sizeof(node));


	prosorinos->index = 0;
	prosorinos->size =  (*linfo)->columns * 65000;
	prosorinos->array = malloc(prosorinos->size * sizeof(uint64_t));
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


 void eisagogi_eggrafis_mid(main_pointer *linfo, uint64_t a){

        if((*linfo)->telos == NULL) eisagogi_komvou_mid(linfo);
	node_pointer mapas = (*linfo)->telos;

	if(mapas->index == mapas->size){
	    eisagogi_komvou_mid(linfo);
	    eisagogi_eggrafis_mid(linfo, a);
	}else{

	    mapas->array[mapas->index] = a;
	    mapas->index++;
 	    (*linfo)->results++;
	}

	return;
 }


 /*
	epistrefei to plithos to results
	diladi pose grammes exei o pinakas apo tuple
 */
 int take_crowd_results_mid(main_pointer *linfo){
	 return (*linfo)->results / (*linfo)->columns;
 }



 int take_columns(main_pointer *linfo){
     return (*linfo)->columns;
 }


 int take_relation(main_pointer *linfo, int r){
     if(r >= (*linfo)->columns){
	 printf("Error take_relation mid_list \n");
	 exit(1);
     }
     return (*linfo)->array_relations[r];
 }


 int take_col(main_pointer *linfo, int r){
     if(r >= (*linfo)->columns){
         printf("Error take_relation mid_list \n");
         exit(1);
     }
     return (*linfo)->array_columns[r];
 }



 uint64_t take_rowid(main_pointer *linfo, int r){
     node_pointer mapas = (*linfo)->arxi;

     while(1){
        if(r < mapas->size){
            return mapas->array[r];
        }else{
            r = r - mapas->size;
            mapas = mapas->epomenos;
        }
    }
    printf("Kati phge LATHOS \n\n");
 }



/*

 void emfanisi_mid(main_pointer *linfo){
        int i;
	node_pointer mapas = (*linfo)->arxi;

        FILE *f;
        if(mapas != NULL){
	    f = fopen("results.csv", "w");
	    if(f == NULL){
	        printf("Error file for results. \n");
		exit(1);
	    }
        }

 	while(mapas != NULL){
 	    for(int i = 0 ; i < mapas->index ; i++) fprintf(f, "%lu  -  %lu \n", mapas->array[i].key, mapas->array[i].payload);
	    mapas = mapas->epomenos;
 	}

        if( (mapas = (*linfo)->arxi) != NULL) fclose(f);
 }

*/


 void lista_diagrafi_mid(main_pointer *linfo){
	int i;

        if((*linfo)->arxi != NULL){

 	    node_pointer mapas = (*linfo)->arxi;
	    node_pointer mapas2;

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
	    free((*linfo)->array_relations);
	    free((*linfo)->array_columns);
        }
	free(*linfo);
	(*linfo) = NULL;
 }

