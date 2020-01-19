/* function.c */
#include "function.h"
#include "sort_join.h"
#include "join_list.h"
#include "mid_list.h"
#include "HashTable.h"
#include "hash_t.h"
#include "queue.h"
#include <inttypes.h>

#define  BYTEPOS  7
#define HASH_TABLE_SIZE 10000
#define MAX_N 40000000

extern int queriesNumber;



 /*
   take arguments from command line and init variables
 */
 void take_arguments(int argc,char *argv[],char **file,char **dir,char **query_file){
    char *arg;
    if(argc != 7 ){
        printf("\nError in arguments command line. \n\n");
        exit(1);
    }

    while(--argc){
        arg = *++argv;
        if(!strcmp(arg, "-F")){
	    *file = malloc((strlen(*++argv) + 1) * sizeof(char));
            strcpy(*file, *argv);
        }else if(!strcmp(arg, "-D")){
            *dir = malloc((strlen(*++argv) + 1) * sizeof(char));
            strcpy(*dir, *argv);
        }
        else if(!strcmp(arg, "-Q")){
            *query_file = malloc((strlen(*++argv) + 1) * sizeof(char));
            strcpy(*query_file, *argv);
        }
        if(argc > 1) argc--;
     }

     return;
 }




 int find_relation_number(char* init_file){
   int lines = 0;
   char s[20];

   FILE *f=fopen(init_file,"r");
   if(f==NULL){
     printf("error in opening init file\n");
     exit(1);
   }

   while(1){
       if( fscanf(f, "%s", s) != 1) break;
       lines++;
   }
   fclose(f);

   return lines;
 }




 /*
  function to read all the files and save to memory
 */
 int create_init_relations(char *directory, char *workload_file, main_array **array,statistics_array **stats_array){
   char init_file[150];
   char r[10];
   strcpy(init_file,directory);
   strcat(init_file,"/");
   strcat(init_file,workload_file);
   int relation_number = find_relation_number(init_file);

   *stats_array=malloc(relation_number * sizeof(statistics_array));

   *array = malloc(relation_number * sizeof(main_array));
   if(array==NULL || stats_array==NULL){
     printf("Error in array malloc\n");
     exit(1);
   }

   FILE *f=fopen(init_file,"r");
   for(int i=0 ; i < relation_number ; i++){
       fscanf(f,"%s[^\n]",r);

       char file[150];
       strcpy(file,directory);
       strcat(file,"/");
       strcat(file,r);

       FILE *fp = fopen(file, "rb");
       if(fp == NULL){
	        printf("Error in fopen for file  %s \n", file);
	        exit(1);
       }

       uint64_t tuples, columns, x;
       fread(&tuples, sizeof(tuples), 1, fp);
       fread(&columns, sizeof(columns), 1, fp);

       (*stats_array)[i].stats = malloc(columns * sizeof(statistics));

       /////////////////////////////////////////////////

       (*array)[i].num_tuples = tuples;
       (*array)[i].num_columns = columns;

       (*array)[i].index = malloc((*array)[i].num_columns * sizeof(int));
       if((*array)[i].index == NULL){
	        printf("Error in malloc array[%d]->index \n", i);
	        exit(1);
       }

       (*array)[i].relation_array = malloc( ((*array)[i].num_tuples * (*array)[i].num_columns) * sizeof(uint64_t));
       if((*array)[i].relation_array == NULL){
	        printf("Error in malloc array[%d]->relation_array \n", i);
	        exit(1);
       }
       //////////////////////////////////////////////////

       int counter=0,index=0;
       (*stats_array)[i].stats[0].Ia=1000000000;
       (*stats_array)[i].stats[0].Ua=0;
       (*stats_array)[i].stats[0].max_case=0;
       (*stats_array)[i].stats[0].Fa=0;
       (*stats_array)[i].stats[0].Da=0;

       (*stats_array)[i].tuples=(*array)[i].num_tuples;
       (*stats_array)[i].columns=(*array)[i].num_columns;
       for(int j = 0 ; j < ((*array)[i].num_tuples * (*array)[i].num_columns) ; j++){
          if(counter == (*array)[i].num_tuples ){
            index++;
            counter=0;
            (*stats_array)[i].stats[index].Ia=1000000000;
            (*stats_array)[i].stats[index].Ua=0;
            (*stats_array)[i].stats[index].max_case=0;
            (*stats_array)[i].stats[index].Fa=0;
            (*stats_array)[i].stats[index].Da=0;

          }
          counter++;

	        fread(&x, sizeof(x), 1, fp);

          if( (*stats_array)[i].stats[index].Ia > x){ //min
            (*stats_array)[i].stats[index].Ia=x;
          }
          if( (*stats_array)[i].stats[index].Ua < x){//max
            (*stats_array)[i].stats[index].Ua=x;
          }
          (*stats_array)[i].stats[index].Fa=(*array)[i].num_tuples; //pli8os

	        (*array)[i].relation_array[j] = x;
       }
       for(int j=0;j<(*stats_array)[i].columns;j++){
         if(((*stats_array)[i].stats[j].Ua-(*stats_array)[i].stats[j].Ia +1) > MAX_N){
          (*stats_array)[i].stats[j].max_case=1;
          (*stats_array)[i].stats[j].Da_array= malloc(MAX_N * sizeof(bool));
         }
        else
         (*stats_array)[i].stats[j].Da_array= malloc(((*stats_array)[i].stats[j].Ua-(*stats_array)[i].stats[j].Ia +1)* sizeof(bool));

         memset((*stats_array)[i].stats[j].Da_array,false,sizeof(bool)* sizeof(*(*stats_array)[i].stats[j].Da_array));
       }
       index=0;counter=0;
       uint64_t pos;


       for(int j = 0 ; j < ((*array)[i].num_tuples * (*array)[i].num_columns) ; j++){
          if(counter == (*array)[i].num_tuples ){
            index++;
            counter=0;
          }
          counter++;
          x=(*array)[i].relation_array[j];
	       // fread(&x, sizeof(x), 1, fp);
          //printf("x==%lu\n",x);
          pos=(*stats_array)[i].stats[index].Ia;
          if((*stats_array)[i].stats[index].max_case==1) //true tin katallili 8esi tou pinaka,analoga to max_case
            (*stats_array)[i].stats[index].Da_array[(x-pos) % MAX_N]=true;
          else
            (*stats_array)[i].stats[index].Da_array[x-pos]=true;

       }



       int Da_array_size = 0;
       for(int j = 0; j < (*array)[i].num_columns; j++) {
          if((*stats_array)[i].stats[j].max_case == 0) {
              Da_array_size = ((*stats_array)[i].stats[j].Ua-(*stats_array)[i].stats[j].Ia +1);
          }
          else {
              Da_array_size = MAX_N;
          }

          for(int k = 0; k < Da_array_size; k++) {
            if((*stats_array)[i].stats[j].Da_array[k]==true) {
                (*stats_array)[i].stats[j].Da++;
            }
          }

       }



       (*array)[i].index[0] = 0;
       for(int j = 1 ; j < (*array)[i].num_columns ; j++)
          (*array)[i].index[j] = j * (*array)[i].num_tuples;
       ////////////////////////////////////////////////////

       fclose(fp);
   }
   fclose(f);

   return relation_number;
 }




 /*
  epistrefei pinaka me tis sxeseis
 */
 void take_relations(char *query, int *tables, int tables_size){
     char *relations;
     char *relation;

     relations  = strsep(&query,"|");

     int i = 0;
     while(1){
         relation = strsep(&relations," ");
         if(relation == NULL) break;
         tables[i] = atoi(relation);
         i++;
     }

     for(i = i ; i < tables_size ; i++) tables[i] = -1;

     return;
 }




 int take_number_of_predicates(char *query){

     int a = 0;
     char *token, c1 = '&';
     token = strtok(query, "|");
     token = strtok(NULL, "|");

     for(int i = 0 ; i < (int)strlen(token) ; i++){
         if(token[i] == c1) a++;
     }

     return a+1;
 }



 int take_tokens(char *str){
     int a = 0;
     for(int i = 0 ; i < (int)strlen(str) ; i++){
         if(str[i] == '.') a++;
     }
     return a;
 }



int find_checksum_number(char* query){
   strsep(&query,"|");
   strsep(&query,"|");

   return take_tokens(query);
}




void take_checksums(checksum_struct *checksums,int number_of_checksums,char* query){
  strsep(&query,"|");
  strsep(&query,"|");

  for(int i=0;i<number_of_checksums;i++){
    checksums[i].table = atoi(&query[0]);
    query+=2;

    checksums[i].row = atoi(&query[0]);
    query+=2;

  }
}



 void take_predicates(q *predicates, int number_of_predicates, char *query){
     strsep(&query,"|");
     char *preds  = strsep(&query,"|");

     for(int i = 0 ; i < number_of_predicates ; i++){
         char *predicate = strsep(&preds, "&");
         char A[100];
         strcpy(A,predicate);

         if(take_tokens(A) == 1){  // filtro

	     int  a1, a2;
	     uint64_t a3;
	     char c1, c2;
             sscanf(predicate, "%d %c %d %c %lu", &a1, &c1, &a2, &c2, &a3);

             predicates[i].flag = false;
	     predicates[i].join = false;
	     predicates[i].relationA = a1;
 	     predicates[i].columnA = a2;
             if(c2 == '=') 	predicates[i].relationB = 0;
	     else if(c2 == '>') predicates[i].relationB = 1;
             else 		predicates[i].relationB = 2;
	     predicates[i].columnB = a3;

	 }else{  // join

             int  a1, a2, a3;
	     uint64_t a4;
             char c1, c2, c3;
             sscanf(predicate, "%d %c %d %c %d %c %lu", &a1, &c1, &a2, &c2, &a3, &c3, &a4);

             predicates[i].flag = false;
	     predicates[i].join = true;
	     predicates[i].relationA = a1;
	     predicates[i].columnA = a2;
	     predicates[i].relationB = a3;
	     predicates[i].columnB = a4;

	 }
     }

     return;
 }




 void malloc_Rr_Ss(relation **Rr1, relation **Rr2){
     *Rr1 = malloc(sizeof(relation));
     if(Rr1 == NULL){
	 printf("Error malloc Rr1 \n");
	 exit(1);
     }

     *Rr2 = malloc(sizeof(relation));
     if(Rr2 == NULL){
         printf("Error malloc Rr2 \n");
         exit(1);
     }

     return;
 }




 void delete_Rr_Ss(relation **Rr1, relation **Rr2){
     free(*Rr1);
     free(*Rr2);

     return;
 }




 void make_Rr1_Rr2(main_array **array, int *tables, q *predicates, int number_of_predicates, int jj, relation **Rr1, relation **Rr2, int a){
     int r1;
     uint64_t c1;

     if( a == 1 ){
         r1 = predicates[jj].relationA;
         c1 = predicates[jj].columnA;
     }else{
         r1 = predicates[jj].relationB;
	 c1 = predicates[jj].columnB;
     }

     // elegxo an gia tin sxesi r1 exoume kapoio filtro
     int find_filter = 0;
     int col, telestis;
     uint64_t value;
     for(int i = 0 ; i < number_of_predicates ; i++){
	 if(predicates[i].join == false && predicates[i].flag == false){
	     if(predicates[i].relationA == r1){ //exoume kapoio filtro gia tin sxesi r1
	         find_filter = 1;
		 predicates[i].flag = true;
		 col = predicates[i].columnA;
		 telestis = predicates[i].relationB;
		 value = predicates[i].columnB;
                 break;
	     }
	 }
     }


     int lines = 0;
     if( find_filter ){ // einai 1 diladi exei filtro
	 // prepei na metrisw poses grammes tou arxikou ikanopoun to filtro

         for(int i = (*array)[tables[r1]].index[col] ; i < (*array)[tables[r1]].index[col] + (*array)[tables[r1]].num_tuples ; i++){
	     if(telestis == 0){
	         if( (*array)[tables[r1]].relation_array[i] == value) lines++;
	     }else if(telestis == 1){
		 if( (*array)[tables[r1]].relation_array[i] > value) lines++;
	     }else{
	         if( (*array)[tables[r1]].relation_array[i] < value) lines++;
	     }
	 }


	 /////////////////////////////////////////////////
	 (*Rr1)->num_tuples = lines;
         (*Rr1)->tuples = malloc((*Rr1)->num_tuples * sizeof(tuple));
         if((*Rr1)->tuples == NULL){
	     printf("Error malloc Rr");
	     exit(1);
	 }
	 (*Rr2)->num_tuples = lines;
         (*Rr2)->tuples = malloc((*Rr2)->num_tuples * sizeof(tuple));
	 if((*Rr2)->tuples == NULL){
             printf("Error malloc Rr");
             exit(1);
         }
	 ////////////////////////////////////////////////////


         int j = 0;
	 for(int i = (*array)[tables[r1]].index[col] ; i < (*array)[tables[r1]].index[col] + (*array)[tables[r1]].num_tuples ; i++){
             if(telestis == 0){
                 if( (*array)[tables[r1]].relation_array[i] == value ) {
		     if(col > c1){
		         (*Rr1)->tuples[j].key = (*array)[tables[r1]].relation_array[i - ((col-c1) * (*array)[tables[r1]].num_tuples)];
		         (*Rr1)->tuples[j].payload = i - (*array)[tables[r1]].index[col] + 1;
		     }else{
			 (*Rr1)->tuples[j].key = (*array)[tables[r1]].relation_array[i + ((c1-col) * (*array)[tables[r1]].num_tuples)];
                         (*Rr1)->tuples[j].payload = i - (*array)[tables[r1]].index[col] + 1;
		     }
		     j++;
		 }
             }else if(telestis == 1){
                 if( (*array)[tables[r1]].relation_array[i] > value ){
		     if(col > c1){
                         (*Rr1)->tuples[j].key = (*array)[tables[r1]].relation_array[i - ((col-c1) * (*array)[tables[r1]].num_tuples)];
                         (*Rr1)->tuples[j].payload = i - (*array)[tables[r1]].index[col] + 1;
                     }else{
                         (*Rr1)->tuples[j].key = (*array)[tables[r1]].relation_array[i + ((c1-col) * (*array)[tables[r1]].num_tuples)];
                         (*Rr1)->tuples[j].payload = i - (*array)[tables[r1]].index[col] + 1;
                     }
                     j++;
		 }
             }else{
                 if((*array)[tables[r1]].relation_array[i] < value ){
		     if(col > c1){
                         (*Rr1)->tuples[j].key = (*array)[tables[r1]].relation_array[i - ((col-c1) * (*array)[tables[r1]].num_tuples)];
                         (*Rr1)->tuples[j].payload = i - (*array)[tables[r1]].index[col] + 1;
                     }else{
                         (*Rr1)->tuples[j].key = (*array)[tables[r1]].relation_array[i + ((c1-col) * (*array)[tables[r1]].num_tuples)];
                         (*Rr1)->tuples[j].payload = i - (*array)[tables[r1]].index[col] + 1;
                     }
                     j++;
		 }
             }
         }

     }else{ // einai 0 diladi den exei filtro
	 lines = (*array)[tables[r1]].num_tuples;


	 /////////////////////////////////////////////////////
         (*Rr1)->num_tuples = lines;
	 (*Rr1)->tuples = malloc((*Rr1)->num_tuples * sizeof(tuple));
         if((*Rr1)->tuples == NULL){
             printf("Error malloc Rr1");
             exit(1);
         }
	 (*Rr2)->num_tuples = lines;
         (*Rr2)->tuples = malloc((*Rr2)->num_tuples * sizeof(tuple));
         if((*Rr2)->tuples == NULL){
             printf("Error malloc R2");
             exit(1);
         }
	 //////////////////////////////////////////////////////////


         int k = (*array)[tables[r1]].index[c1];
         for(int i = 0 ; i < lines ; i++){
	     (*Rr1)->tuples[i].key = (*array)[tables[r1]].relation_array[k];
             k++;
	     (*Rr1)->tuples[i].payload = i + 1;
	 }
     }

 }




 void make_Rr1_Rr2__2(main_array **array, main_pointer *mid_result, int *tables, q *predicates, int number_of_predicates, int jj, relation **Rr1, relation **Rr2, int a,int *sort_needed){
     int r, c;
     *sort_needed=1;
     if(a == 1){
         r = predicates[jj].relationA;
	 c = predicates[jj].columnA;
     }else{
	 r = predicates[jj].relationB;
	 c = predicates[jj].columnB;
     }

     int find_imid_result = 0;
     for(int i = 0 ; i < take_columns(mid_result) ; i++){
	     if(r == take_relation(mid_result, i)){
	        find_imid_result = i+1;
          if(c == take_col(mid_result,i)){
             *sort_needed=0;
           }
        }
     }


     if(find_imid_result == 0){
         // kalese tin proigoumeni make rr1 rr2
       make_Rr1_Rr2(array, tables, predicates, number_of_predicates, jj, Rr1, Rr2, a);
	     return;
     }

     int k;
     if(find_imid_result == 1)      k = 0;
     else if(find_imid_result == 2) k = 1;
     else                           k = 2;


     ///////////////////////////////////////////////////////////////////////////////////////////////////
     // se periptwsi pou thelw akrivos mia stili kai pinaka apo to endiameso
     // ftiaxnw tin sxesi Rr h opoia ta rowId  einai akribos mia apo tis stiles tou endiamesou kai pairnw kai ta key tis analogis stilis
      int rep = take_columns(mid_result);
      if(*sort_needed==0){ //ama exoume tin stili tou pinaka ston endiameso
        (*Rr1)->num_tuples = take_crowd_results_mid(mid_result);
        (*Rr1)->tuples = malloc((*Rr1)->num_tuples * sizeof(tuple));
        if((*Rr1)->tuples == NULL){
          printf("Error malloc (*Rr1)->tuples \n");
          exit(1);
        }
        (*Rr2)->num_tuples = take_crowd_results_mid(mid_result);
        (*Rr2)->tuples = malloc((*Rr2)->num_tuples * sizeof(tuple));
        if((*Rr2)->tuples == NULL){
            printf("Error malloc (*Rr2)->tuples \n");
            exit(1);
        }
        for(int i=0;i<(*Rr1)->num_tuples;i++){
          (*Rr1)->tuples[i].payload =  take_rowid(mid_result, k);
          uint64_t rowid = (*Rr1)->tuples[i].payload;
          (*Rr1)->tuples[i].key = (*array)[tables[r]].relation_array[(*array)[tables[r]].index[c] + rowid - 1];
          k+=rep;
        }
        return;
      }


     // prepei na vrw posa diaforetika roid yparxoun ston endiameso
    /* uint64_t *aa = malloc(take_crowd_results_mid(mid_result) * sizeof(uint64_t));
     uint64_t *bb = malloc(take_crowd_results_mid(mid_result) * sizeof(uint64_t));

     if(aa == NULL){
	 printf("Error malloc a \n");
	 exit(1);
 }*/
     deiktis_ht *H_Table = malloc(HASH_TABLE_SIZE * sizeof(deiktis_ht));
     if(H_Table == NULL) { printf("Error malloc memory H_Table. \n\n"); exit(1); }

     for(int i = 0 ; i < HASH_TABLE_SIZE ; i++){
          H_Table[i] = HashTable_dimiourgia(&H_Table[i]);
          dimoiourgeia_arxikwn_bucket(&H_Table[i]);
      }
      for(int j = 0 ; j < take_crowd_results_mid(mid_result) ; j++){
           uint64_t row_id = take_rowid(mid_result, k);
           int hash_r = hash(row_id, HASH_TABLE_SIZE);
           eisagogi_rowId(&H_Table[hash_r], row_id, 0, 0, 1);
           k += rep;
      }

     ////////////////////////////////////////////////////////////
     // neos tropos gia na min pairnoume ta diplotipa

    /* for(int i = 0 ; i < take_crowd_results_mid(mid_result) ; i++){
         bb[i] = take_rowid(mid_result, k);
         k += rep;
     }
     quicksort2(bb, 0, take_crowd_results_mid(mid_result) - 1);	/////

     int j = 0;
     for(int i = 0 ; i < take_crowd_results_mid(mid_result) ; i++){
         if(i == 0){
	     aa[j] = bb[i];
	     j++;
	 }else{
	     if(bb[i] != aa[j-1]){
	        aa[j] = bb[i];
	        j++;
	     }
	 }
 }*/

     ///////////////////////////////////////////////////////////
     uint64_t ids_counter=0;
     for(int i=0;i<HASH_TABLE_SIZE;i++){
       ids_counter+=take_unique_ids(&H_Table[i]);
     }
     (*Rr1)->num_tuples = ids_counter;
     (*Rr1)->tuples = malloc((*Rr1)->num_tuples * sizeof(tuple));
     if((*Rr1)->tuples == NULL){
	 printf("Error malloc (*Rr1)->tuples \n");
	 exit(1);
     }
     (*Rr2)->num_tuples = ids_counter;
     (*Rr2)->tuples = malloc((*Rr2)->num_tuples * sizeof(tuple));
     if((*Rr2)->tuples == NULL){
         printf("Error malloc (*Rr2)->tuples \n");
         exit(1);
     }
     ////////////////////////////////////////////////////////////

     k=0;
     for(int i = 0 ; i < HASH_TABLE_SIZE ; i++){
       for(int j=0;j<take_unique_ids(&H_Table[i]);j++){
	      (*Rr1)->tuples[k].payload = emfanisi_ht(&H_Table[i],j);
        k++;
      }
     }
     // gia kathe roid prepei na paw ston katalilo pinaka kai stili kai na parw to key
     for(int i = 0 ; i < (*Rr1)->num_tuples ; i++){
	 uint64_t rowid = (*Rr1)->tuples[i].payload;
	 (*Rr1)->tuples[i].key = (*array)[tables[r]].relation_array[(*array)[tables[r]].index[c] + rowid - 1];	//////////////// sos thelei -1
     }
     for(int i = 0 ; i < HASH_TABLE_SIZE ; i++) HashTable_diagrafi(&H_Table[i]);
     free(H_Table);
    // free(aa);
    // free(bb);

     return;
 }





 void  make_second_intermid(info_deikti *join_list, main_pointer *imid_list, int size_imid_list, int rel,int rel2, int join_stil_A, int join_stil_B){
     int k=rel,k1,k2;
     // prepei na elegxoyme oxi tin k alla tin alli an einai idia me mia apo tis 2 stiles tou join
     int l;
     int join,index,val,val2;

     if(rel2==-1){
       if(k == 0) l = 1;
       else       l = 0;

       if(take_relation(&imid_list[0], l) == join_stil_A)
           join = 0;	// key
       else
           join = 1;	// payload





       deiktis_ht *H_Table = malloc(HASH_TABLE_SIZE * sizeof(deiktis_ht));
       if(H_Table == NULL) { printf("Error malloc memory H_Table. \n\n"); exit(1); }

       for(int i = 0 ; i < HASH_TABLE_SIZE ; i++){
            H_Table[i] = HashTable_dimiourgia(&H_Table[i]);
            dimoiourgeia_arxikwn_bucket(&H_Table[i]);
        }

       ///////////////////////////////////////////////////////////////////////////
	// deimiourgia   hash_table
       index = l;
       val = k;
       for(int j = 0 ; j < take_crowd_results_mid(&imid_list[0]) ; j++){
            uint64_t row_id = take_rowid(&imid_list[0], index);
            int hash_r = hash(row_id, HASH_TABLE_SIZE);
            eisagogi_rowId(&H_Table[hash_r], row_id, take_rowid(&imid_list[0], val), 0, 1);

            val += 2;
            index += 2;
       }

	//////////////////////////////////////////////////////////////////////////////


       for(int i = 0 ; i < take_crowd_results(join_list) ; i++){
            tuple tt = take_row(join_list, i);
            uint64_t row_id;

	    if(join == 0) row_id = tt.key;
	    else	  row_id = tt.payload;

	    int hash_r = hash(row_id, HASH_TABLE_SIZE);
            rows_node *t = take_list(&H_Table[hash_r], row_id);

            while(t != NULL){
                eisagogi_eggrafis_mid(&imid_list[1], t->row_id);
		eisagogi_eggrafis_mid(&imid_list[1], tt.key);
		eisagogi_eggrafis_mid(&imid_list[1], tt.payload);
	        t = t->next_row;
	    }
       }

       for(int i = 0 ; i < HASH_TABLE_SIZE ; i++) HashTable_diagrafi(&H_Table[i]);
       free(H_Table);

/*
       for(int i = 0 ; i < take_crowd_results(join_list) ; i++){
         tuple t = take_row(join_list, i);
          index = l;
          val = k;
          for(int j = 0 ; j < take_crowd_results_mid(&imid_list[0]) ; j++){
              if(join == 0){
                if( take_rowid(&imid_list[0], index) == t.key){
                    eisagogi_eggrafis_mid(&imid_list[1], take_rowid(&imid_list[0], val));
                    eisagogi_eggrafis_mid(&imid_list[1], t.key);
                    eisagogi_eggrafis_mid(&imid_list[1], t.payload);
                 }
               val += 2;
               index += 2;
             }else{
                 if( take_rowid(&imid_list[0], index) == t.payload){
                    eisagogi_eggrafis_mid(&imid_list[1], take_rowid(&imid_list[0], val));
                    eisagogi_eggrafis_mid(&imid_list[1], t.key);
                    eisagogi_eggrafis_mid(&imid_list[1], t.payload);
                 }
                 val +=  2;
                 index += 2;
             }
          }
        }
*/

   }else{

     k1=rel,k2=rel2;

     if((k1==0 && k2==1) || (k1==1 && k2==0))
        l=2;
     else if((k1==0 && k2==2) || (k1==2 && k2==0))
        l=1;
     else{
        l=0;
     }


     int thesi, thesi2, thesi3;
     for(int i = 0 ; i < take_columns(&imid_list[1]) ; i++){
        if(l == take_relation(&imid_list[1], i)) thesi = i;
        if(k1 == take_relation(&imid_list[1], i)) thesi2 = i;
        if(k2 == take_relation(&imid_list[1], i)) thesi3 = i;
     }


     if(take_relation(&imid_list[1], thesi) == join_stil_A)
         join = 0;	// key
     else
         join = 1;	// payload





       deiktis_ht *H_Table = malloc(HASH_TABLE_SIZE * sizeof(deiktis_ht));
       if(H_Table == NULL) { printf("Error malloc memory H_Table. \n\n"); exit(1); }

       for(int i = 0 ; i < HASH_TABLE_SIZE ; i++){
            H_Table[i] = HashTable_dimiourgia(&H_Table[i]);
            dimoiourgeia_arxikwn_bucket(&H_Table[i]);
        }

       ///////////////////////////////////////////////////////////////////////////
        // deimiourgia   hash_table

       index = thesi;   // 1
       val = thesi2;    // 0
       val2= thesi3;    // 2

       for(int j = 0 ; j < take_crowd_results_mid(&imid_list[1]) ; j++){
            uint64_t row_id = take_rowid(&imid_list[1], index);
            int hash_r = hash(row_id, HASH_TABLE_SIZE);
            eisagogi_rowId(&H_Table[hash_r], row_id, take_rowid(&imid_list[1], val), take_rowid(&imid_list[1], val2), 2);

            val += 3;
	    val2 += 3;
            index += 3;
       }
        //////////////////////////////////////////////////////////////////////////////

       for(int i = 0 ; i < take_crowd_results(join_list) ; i++){
            tuple tt = take_row(join_list, i);
            uint64_t row_id;

            if(join == 0) row_id = tt.key;
            else          row_id = tt.payload;

            int hash_r = hash(row_id, HASH_TABLE_SIZE);
            rows_node *t = take_list(&H_Table[hash_r], row_id);

            while(t != NULL){
                eisagogi_eggrafis_mid(&imid_list[0], t->row_id);
		eisagogi_eggrafis_mid(&imid_list[0], t->row_id2);
                eisagogi_eggrafis_mid(&imid_list[0], tt.key);
                eisagogi_eggrafis_mid(&imid_list[0], tt.payload);
                t = t->next_row;
            }
       }

       for(int i = 0 ; i < HASH_TABLE_SIZE ; i++) HashTable_diagrafi(&H_Table[i]);
       free(H_Table);

/*
    for(int i = 0 ; i < take_crowd_results(join_list) ; i++){
       tuple t = take_row(join_list, i);
       index = thesi;	// 1
       val = thesi2;	// 0
       val2= thesi3;	// 2
       for(int j = 0 ; j < take_crowd_results_mid(&imid_list[1]) ; j++){
           if(join == 0){
             if( take_rowid(&imid_list[1], index) == t.key){

                 eisagogi_eggrafis_mid(&imid_list[0], take_rowid(&imid_list[1], val));
                 eisagogi_eggrafis_mid(&imid_list[0], take_rowid(&imid_list[1], val2));
                 eisagogi_eggrafis_mid(&imid_list[0], t.key);
                 eisagogi_eggrafis_mid(&imid_list[0], t.payload);
              }
            val += 3;
            val2 += 3;
            index += 3;
          }else{
              if( take_rowid(&imid_list[1], index) == t.payload){
                 eisagogi_eggrafis_mid(&imid_list[0], take_rowid(&imid_list[1], val));
                 eisagogi_eggrafis_mid(&imid_list[0], take_rowid(&imid_list[1], val2));
                 eisagogi_eggrafis_mid(&imid_list[0], t.key);
                 eisagogi_eggrafis_mid(&imid_list[0], t.payload);
              }
              val += 3;
              val2 += 3;
              index += 3;
          }
       }
     }
*/
  }
     return;
 }




 void print_checksums(main_array **array, int *tables, checksum_struct *checksums, int number_of_checksums, main_pointer *imid_list, int imid_index, int headPosition){

        int rep = take_columns(&imid_list[imid_index]);
        for(int loop = 0 ; loop < number_of_checksums ; loop++){//gia ka8e checksum
            uint64_t sum = 0;
            int k = 0;
            int number_of_columns = take_columns(&imid_list[imid_index]);

            for(int index = 0 ; index < number_of_columns ; index++){//vriskoume gia to ka8e table se poia 8esi tou endiamesou apo8ikeuetai
                if(checksums[loop].table==take_relation(&imid_list[imid_index],index)){
                    k = index;
                    break;
                }
            }

            for(int i = 0 ; i < take_crowd_results_mid(&imid_list[imid_index]) ; i++){
                uint64_t row = take_rowid(&imid_list[imid_index], k);
                k += rep;
	        sum += (*array)[tables[checksums[loop].table]].relation_array[(*array)[tables[checksums[loop].table]].index[checksums[loop].row] + row -1];
            }

            if(sum==0) {
              //printf("NULL ");
	            queue[headPosition].results[loop] = 0;
            }
            else {
              //printf("%lu ",sum);
	            queue[headPosition].results[loop] = sum;
            }
        }

     return;
 }




 void edit_itermid(main_array **array, int *tables, main_pointer *imid_list, main_pointer *imid_list2, int relA, int colA, int relB, uint64_t colB, int ii){

  if(ii == 1){
     if(relA != take_relation(&imid_list[0], 0)){
         uint64_t temp = relA;
	 relA = relB;
	 relB = temp;

 	 temp = colA;
	 colA = colB;
	 colB = temp;
     }

     imid_list[1] = MID_dimiourgia(&imid_list[1], 2, take_relation(&imid_list[0], 0), take_col(&imid_list[0], 0), take_relation(&imid_list[0], 1), take_col(&imid_list[0], 1), -1, -1, -1, -1);

     int k = 0, row1, row2;
     for(int i = 0 ; i < take_crowd_results_mid(&imid_list[0]) ; i++){
         row1 = take_rowid(&imid_list[0], k);
         row2 = take_rowid(&imid_list[0], k+1);


         if( (*array)[tables[relA]].relation_array[(*array)[tables[relA]].index[colA] + row1 -1] ==
	     (*array)[tables[relB]].relation_array[(*array)[tables[relB]].index[colB] + row2 -1] ){

             eisagogi_eggrafis_mid(&imid_list[1], row1);
	     eisagogi_eggrafis_mid(&imid_list[1], row2);
	 }
	 k += 2;

     }
     lista_diagrafi_mid(&imid_list[0]);

     imid_list[0] = MID_dimiourgia(&imid_list[0], 2, take_relation(&imid_list[1], 0), take_col(&imid_list[1], 0), take_relation(&imid_list[1], 1), take_col(&imid_list[1],1), -1, -1, -1, -1);

     k = 0;
     for(int i = 0 ; i < take_crowd_results_mid(&imid_list[1]) ; i++){
         row1 = take_rowid(&imid_list[1], k);
	 row2 = take_rowid(&imid_list[1], k+1);

	 eisagogi_eggrafis_mid(&imid_list[0], row1);
	 eisagogi_eggrafis_mid(&imid_list[0], row2);
         k += 2;
     }
     lista_diagrafi_mid(&imid_list[1]);


  }else{

      int x, y;
      for(int i = 0 ; i < take_columns(&imid_list[0]) ; i++){
	  if(take_relation(&imid_list[0], i) == relA) x = i;
      }

      for(int i = 0 ; i < take_columns(&imid_list[0]) ; i++){
          if(take_relation(&imid_list[0], i) == relB) y = i;
      }

      imid_list2[0] = MID_dimiourgia(&imid_list2[0], 3, take_relation(&imid_list[0], 0), take_col(&imid_list[0], 0), take_relation(&imid_list[0], 1), take_col(&imid_list[0], 1), take_relation(&imid_list[0], 2), take_col(&imid_list[0], 2), -1, -1);

      int k = 0, row1, row2, row3;

      for(int i = 0 ; i < take_crowd_results_mid(imid_list) ; i++){
          row1 = take_rowid(imid_list, k);
          row2 = take_rowid(imid_list, k+1);
	  row3 = take_rowid(imid_list, k+2);


          int ok = 0;
          if(x == 0){
	      if(y == 1){
	          if( (*array)[tables[relA]].relation_array[(*array)[tables[relA]].index[colA] + row1 -1] ==
            	      (*array)[tables[relB]].relation_array[(*array)[tables[relB]].index[colB] + row2 -1] ){

		       ok = 1;
		   }
	      }else{
		   if( (*array)[tables[relA]].relation_array[(*array)[tables[relA]].index[colA] + row1 -1] ==
                      (*array)[tables[relB]].relation_array[(*array)[tables[relB]].index[colB] + row3 -1] ){

		       ok = 1;
                   }
	      }
	  }else if(x == 1){
	      if(y == 0){
	          if( (*array)[tables[relA]].relation_array[(*array)[tables[relA]].index[colA] + row2 -1] ==
                      (*array)[tables[relB]].relation_array[(*array)[tables[relB]].index[colB] + row1 -1] ){

		       ok = 1;
                   }
	      }else{
	 	  if( (*array)[tables[relA]].relation_array[(*array)[tables[relA]].index[colA] + row2 -1] ==
                      (*array)[tables[relB]].relation_array[(*array)[tables[relB]].index[colB] + row3 -1] ){

		       ok = 1;
                   }
	      }
	  }else{
	      if(y == 0){
		  if( (*array)[tables[relA]].relation_array[(*array)[tables[relA]].index[colA] + row3 -1] ==
                      (*array)[tables[relB]].relation_array[(*array)[tables[relB]].index[colB] + row1 -1] ){

		       ok = 1;
                   }
	      }else{
		  if( (*array)[tables[relA]].relation_array[(*array)[tables[relA]].index[colA] + row3 -1] ==
                      (*array)[tables[relB]].relation_array[(*array)[tables[relB]].index[colB] + row2 -1] ){

		       ok = 1;
                   }
	      }
	  }

	  if( ok ){
	      eisagogi_eggrafis_mid(imid_list2, row1);
	      eisagogi_eggrafis_mid(imid_list2, row2);
	      eisagogi_eggrafis_mid(imid_list2, row3);
	  }

          k += 3;
      }



      lista_diagrafi_mid(imid_list);
      imid_list[0] = MID_dimiourgia(imid_list, 3, take_relation(imid_list2, 0), take_col(imid_list2, 0), take_relation(imid_list2, 1), take_col(imid_list2, 1), take_relation(imid_list2, 2), take_col(imid_list2, 2), -1, -1);


      k = 0;
      for(int i = 0 ; i < take_crowd_results_mid(imid_list2) ; i++){
          row1 = take_rowid(imid_list2, k);
	  row2 = take_rowid(imid_list2, k+1);
  	  row3 = take_rowid(imid_list2, k+2);

	  eisagogi_eggrafis_mid(imid_list, row1);
	  eisagogi_eggrafis_mid(imid_list, row2);
	  eisagogi_eggrafis_mid(imid_list, row3);

	  k += 3;
      }

      lista_diagrafi_mid(imid_list2);
  }

     return;
 }



 void lets_go_for_predicates(main_array **array, int *tables, int relation_number, q *predicates, int number_of_predicates,checksum_struct *checksums,int number_of_checksums, int index){

     ////////////////////////////////////// printing
      //printf("\n");
    /*
     printf("LETS GO, num of preds is %d\n", number_of_predicates);
     printf("queue head is %d\n", queue_head);
     printf("queue tail is %d\n", queue_tail);
     */

    ////////////////////////////////


     int i, ii = 0,sort_needed=0,flag=0;
     relation *Rr1, *Rr2;
     relation *Ss1, *Ss2;

     malloc_Rr_Ss(&Rr1, &Rr2);
     malloc_Rr_Ss(&Ss1, &Ss2);


     info_deikti join_list = NULL;

     main_pointer *imid_list = NULL;
     imid_list = malloc(2 * sizeof(main_pointer));
     if(imid_list == NULL){
         printf("Error malloc imid_list \n");
	 exit(1);
     }
     imid_list[0] = NULL;
     imid_list[1] = NULL;


     for(i = 0 ; i < number_of_predicates ; i++){
         flag=0;

         if(predicates[i].join == false) continue; 	// trexoume to for mono gia ta join predicate

	 if(ii == 0){
	     ii++;

	     make_Rr1_Rr2(array, tables, predicates, number_of_predicates, i, &Rr1, &Rr2, 1);
	     make_Rr1_Rr2(array, tables, predicates, number_of_predicates, i, &Ss1, &Ss2, 2);

	     recurseFunc(&Rr1, &Rr2, 0, Rr1->num_tuples, BYTEPOS);
	     recurseFunc(&Ss1, &Ss2, 0, Ss1->num_tuples, BYTEPOS);

             imid_list[0] = MID_dimiourgia(&imid_list[0], 2, predicates[i].relationA, predicates[i].columnA, predicates[i].relationB, predicates[i].columnB, -1, -1, -1, -1);

	     Sort_Merge_Join(&Rr1, &Ss1, &join_list, &imid_list[0],&imid_list[1],1,0);


	     free(Rr1->tuples);	free(Rr2->tuples);
	     free(Ss1->tuples);	free(Ss2->tuples);

	 }else if(ii == 1){

               if((predicates[i].relationA == take_relation(&imid_list[0], 0) && predicates[i].relationB == take_relation(&imid_list[0], 1)) ||
		  ((predicates[i].relationA == take_relation(&imid_list[0], 1) && predicates[i].relationB == take_relation(&imid_list[0], 0)))){

	           edit_itermid(array, tables, &imid_list[0], &imid_list[1], predicates[i].relationA, predicates[i].columnA, predicates[i].relationB, predicates[i].columnB, ii);

	       }else{

	           ii++;
	           join_list = LIST_dimiourgia(&join_list);


                   make_Rr1_Rr2__2(array, &imid_list[0], tables, predicates, number_of_predicates, i, &Rr1, &Rr2, 1,&sort_needed);
                   if(sort_needed) recurseFunc(&Rr1, &Rr2, 0, Rr1->num_tuples, BYTEPOS);
          	   else		   flag=1; //o R pairnetai apo ton endiameso etoimos


     	   	   make_Rr1_Rr2__2(array, &imid_list[0], tables, predicates, number_of_predicates, i, &Ss1, &Ss2, 2,&sort_needed);

         	   if(sort_needed) recurseFunc(&Ss1, &Ss2, 0, Ss1->num_tuples, BYTEPOS);
          	   else		   flag=2;//o S pairnetai apo ton endiameso etoimos


          	   if(flag==1)
            	       imid_list[1] = MID_dimiourgia(&imid_list[1], 3, take_relation(imid_list, 0), take_col(imid_list, 0), take_relation(imid_list, 1), take_col(imid_list, 1), predicates[i].relationB, predicates[i].columnB, -1, -1);
          	   if(flag==2)
            	       imid_list[1] = MID_dimiourgia(&imid_list[1], 3, take_relation(imid_list, 0), take_col(imid_list, 0), take_relation(imid_list, 1), take_col(imid_list, 1), predicates[i].relationA, predicates[i].columnA, -1, -1);


	           Sort_Merge_Join(&Rr1, &Ss1, &join_list,&imid_list[0],&imid_list[1],0,flag);

		   if(flag==0){//an flag=0 kanoume to klassiko
    	     	       if(take_relation(&imid_list[0], 0) != predicates[i].relationA && take_relation(&imid_list[0], 0) != predicates[i].relationB){
    	                   imid_list[1] = MID_dimiourgia(&imid_list[1], 3, take_relation(imid_list, 0), take_col(imid_list, 0), predicates[i].relationA, predicates[i].columnA, predicates[i].relationB, predicates[i].columnB, -1, -1);
    		           make_second_intermid(&join_list, imid_list, 2, take_relation(imid_list, 0), -1, predicates[i].relationA, predicates[i].relationB);
    	     	       }else{
    	                   imid_list[1] = MID_dimiourgia(&imid_list[1], 3, take_relation(imid_list, 1), take_col(imid_list, 1), predicates[i].relationA, predicates[i].columnA, predicates[i].relationB, predicates[i].columnB,-1, -1);
    	                   make_second_intermid(&join_list, imid_list, 2, take_relation(imid_list, 1), -1, predicates[i].relationA, predicates[i].relationB);
    	     	       }
           	   }



	     	   lista_diagrafi_mid(&imid_list[0]);
	     	   imid_list[0] = NULL;
	     	   lista_diagrafi(&join_list);
             	   free(Rr1->tuples);     free(Rr2->tuples);
             	   free(Ss1->tuples);     free(Ss2->tuples);
	     }
	 }else if(ii == 2){

	     if( (predicates[i].relationA == take_relation(&imid_list[1], 0) || predicates[i].relationA == take_relation(&imid_list[1], 1) || predicates[i].relationA == take_relation(&imid_list[1], 2)) &&
		  (predicates[i].relationB == take_relation(&imid_list[1], 0) || predicates[i].relationB == take_relation(&imid_list[1], 1) || predicates[i].relationB == take_relation(&imid_list[1], 2)) ){
	         edit_itermid(array, tables, &imid_list[1], &imid_list[0], predicates[i].relationA, predicates[i].columnA, predicates[i].relationB, predicates[i].columnB, ii);

	     }else{

	         ii++;
       	   	 join_list = LIST_dimiourgia(&join_list);
       	   	 make_Rr1_Rr2__2(array, &imid_list[1], tables, predicates, number_of_predicates, i, &Rr1, &Rr2, 1,&sort_needed);

           	 if(sort_needed) recurseFunc(&Rr1, &Rr2, 0, Rr1->num_tuples, BYTEPOS);
          	 else 		 flag=1;

           	 make_Rr1_Rr2__2(array, &imid_list[1], tables, predicates, number_of_predicates, i, &Ss1, &Ss2, 2,&sort_needed);

           	 if(sort_needed) recurseFunc(&Ss1, &Ss2, 0, Ss1->num_tuples, BYTEPOS);
            	 else 	         flag=2;


            	 if(flag==1)
                     imid_list[0] = MID_dimiourgia(&imid_list[1], 4, take_relation(&imid_list[1], 0), take_col(&imid_list[1], 0), take_relation(&imid_list[1], 1), take_col(&imid_list[1], 1),take_relation(&imid_list[1], 2), take_col(&imid_list[1], 2), predicates[i].relationB, predicates[i].columnB);
                 if(flag==2)
                     imid_list[0] = MID_dimiourgia(&imid_list[1], 4, take_relation(&imid_list[1], 0), take_col(&imid_list[1], 0), take_relation(&imid_list[1], 1), take_col(&imid_list[1], 1),take_relation(&imid_list[1], 2), take_col(&imid_list[1], 2), predicates[i].relationA, predicates[i].columnA);

	         Sort_Merge_Join(&Rr1, &Ss1, &join_list,&imid_list[1],&imid_list[0],0,flag);

                 if(flag==0){

    	             if(take_relation(&imid_list[1], 0) != predicates[i].relationA && take_relation(&imid_list[1], 0) != predicates[i].relationB &&
    		         take_relation(&imid_list[1], 1) != predicates[i].relationA && take_relation(&imid_list[1], 1) != predicates[i].relationB){

                         imid_list[0] = MID_dimiourgia(&imid_list[1], 4, take_relation(&imid_list[1], 0), take_col(&imid_list[1], 0), take_relation(&imid_list[1], 1), take_col(&imid_list[1], 1),
    		   	                               predicates[i].relationA, predicates[i].columnA, predicates[i].relationB, predicates[i].columnB);

    		         make_second_intermid(&join_list, imid_list, 2, take_relation(&imid_list[1], 0), take_relation(&imid_list[1], 1), predicates[i].relationA, predicates[i].relationB);

      	             }else if(take_relation(&imid_list[1], 0) != predicates[i].relationA && take_relation(&imid_list[1], 0) != predicates[i].relationB &&
    		               take_relation(&imid_list[1], 2) != predicates[i].relationA && take_relation(&imid_list[1], 2) != predicates[i].relationB){

    	                 imid_list[0] = MID_dimiourgia(&imid_list[1], 4, take_relation(&imid_list[1], 0), take_col(&imid_list[1], 0), take_relation(&imid_list[1], 2), take_col(&imid_list[1], 2),
    			                               predicates[i].relationA, predicates[i].columnA, predicates[i].relationB, predicates[i].columnB);

    		         make_second_intermid(&join_list, imid_list, 2, take_relation(&imid_list[1], 0), take_relation(&imid_list[1], 2), predicates[i].relationA, predicates[i].relationB);

    	             }else if(take_relation(&imid_list[1], 1) != predicates[i].relationA && take_relation(&imid_list[1], 1) != predicates[i].relationB &&
    		               take_relation(&imid_list[1], 2) != predicates[i].relationA && take_relation(&imid_list[1], 2) != predicates[i].relationB){

    	                 imid_list[0] = MID_dimiourgia(&imid_list[1], 4, take_relation(&imid_list[1], 1), take_col(&imid_list[1], 1), take_relation(&imid_list[1], 2), take_col(&imid_list[1], 2),
   	                                               predicates[i].relationA, predicates[i].columnA, predicates[i].relationB, predicates[i].columnB);

    	 	         make_second_intermid(&join_list, imid_list, 2, take_relation(&imid_list[1], 1), take_relation(&imid_list[1], 2), predicates[i].relationA, predicates[i].relationB);
                     }
                 }


                 lista_diagrafi_mid(&imid_list[1]);
	         imid_list[1] = NULL;
	         lista_diagrafi(&join_list);
	         free(Rr1->tuples);     free(Rr2->tuples);
                 free(Ss1->tuples);     free(Ss2->tuples);
	     }
	 }
     }
      
     if(ii==1 || ii==3)
         print_checksums(array,&tables[0],checksums,number_of_checksums,imid_list,0, index);
     else
         print_checksums(array,&tables[0],checksums,number_of_checksums,imid_list,1, index);



     delete_Rr_Ss(&Rr1, &Rr2);
     delete_Rr_Ss(&Ss1, &Ss2);

     if(imid_list[0] != NULL){
         lista_diagrafi_mid(&imid_list[0]);
     }else{
         lista_diagrafi_mid(&imid_list[1]);
     }
     free(imid_list);


     return;
 }



int* orderOfPredicates(q* predicates, int number_of_predicates, statistics_array **stats_array, int* tables, int relation_number) {



  statistics_array* stats_array_temp;

  int relationNum = 0;
  for(int i = 0; i < relation_number; i++) {
      if(tables[i] != -1) {
        relationNum++;
      }
  }

  stats_array_temp=malloc(relationNum * sizeof(statistics_array));

  for(int i = 0; i < relationNum; i++) {
    stats_array_temp[i].stats = malloc((*stats_array)[tables[i]].columns * sizeof(statistics));
    stats_array_temp[i].columns=(*stats_array)[tables[i]].columns;
    stats_array_temp[i].tuples=(*stats_array)[tables[i]].tuples;

    for(int j = 0; j < (*stats_array)[tables[i]].columns; j++) {
      stats_array_temp[i].stats[j].Ia = (*stats_array)[tables[i]].stats[j].Ia;
      stats_array_temp[i].stats[j].Ua = (*stats_array)[tables[i]].stats[j].Ua;
      stats_array_temp[i].stats[j].Fa = (*stats_array)[tables[i]].stats[j].Fa;
      stats_array_temp[i].stats[j].Da = (*stats_array)[tables[i]].stats[j].Da;
      stats_array_temp[i].stats[j].max_case = (*stats_array)[tables[i]].stats[j].max_case;


      int Da_array_size;

      if(stats_array_temp[i].stats[j].max_case == 0) {
        Da_array_size = stats_array_temp[i].stats[j].Ua - stats_array_temp[i].stats[j].Ia + 1;
      }
      else{
        Da_array_size = MAX_N;
      }

      stats_array_temp[i].stats[j].Da_array = malloc(Da_array_size * sizeof(bool));

      for(int k = 0; k < Da_array_size; k++) {
          stats_array_temp[i].stats[j].Da_array[k] = (*stats_array)[tables[i]].stats[j].Da_array[k];
      }
    }

  }
/*
    for(int i=0;i< relationNum;i++){
      for(int j=0;j<stats_array_temp[i].columns;j++){
        printf("Ia==%lu  ",stats_array_temp[i].stats[j].Ia);
        printf("Ua==%lu  ",stats_array_temp[i].stats[j].Ua);
        printf("Fa==%lu  ",stats_array_temp[i].stats[j].Fa);
        printf("Da==%lu  ",stats_array_temp[i].stats[j].Da);
      }
      printf("\n\n");
    }
*/

  for(int i = number_of_predicates - 1; i >= 0; i--) {
//    printf("i===%d\n",i);
    if(predicates[i].join == false) {     // it is a filter

        if(predicates[i].relationB == 0) {   // filtro =


            // gia thn stili A
            int rel = predicates[i].relationA;
            int filter = predicates[i].columnB;


            stats_array_temp[rel].stats[predicates[i].columnA].Ia = filter;
            stats_array_temp[rel].stats[predicates[i].columnA].Ua = filter;


            int pos = predicates[i].columnB - stats_array_temp[rel].stats[predicates[i].columnA].Ia;

            if(stats_array_temp[rel].stats[predicates[i].columnA].max_case == 1) {
                pos = pos % MAX_N;
            }

            uint64_t Da = stats_array_temp[rel].stats[predicates[i].columnA].Da;

            if(stats_array_temp[rel].stats[predicates[i].columnA].Da_array[pos] == true) {
              stats_array_temp[rel].stats[predicates[i].columnA].Da = 1;
              stats_array_temp[rel].stats[predicates[i].columnA].Fa /= Da;
            }
            else {
              stats_array_temp[rel].stats[predicates[i].columnA].Da = 0;
              stats_array_temp[rel].stats[predicates[i].columnA].Fa = 0;
            }

            // gia opoiadhpote allh stili C

            for(int j = 0; j < stats_array_temp[rel].columns; j++) {


                uint64_t Fa1 = stats_array_temp[rel].stats[predicates[i].columnA].Fa;
                if(j != predicates[i].columnA) {    // prepei na einai diaforetiki stili apo thn A
                  stats_array_temp[rel].stats[j].Fa = stats_array_temp[rel].stats[predicates[i].columnA].Fa;

                  uint64_t Dc = stats_array_temp[rel].stats[j].Da;
                  uint64_t Fa1 = stats_array_temp[rel].stats[predicates[i].columnA].Fa;
                  uint64_t Fa = (*stats_array)[rel].stats[predicates[i].columnA].Fa;
                  uint64_t Fc = stats_array_temp[rel].stats[j].Fa;

                  stats_array_temp[rel].stats[j].Da = ( Dc * (1 - (1 - Fa1 / Fa) ^ ( Fc / Dc ) ));
                }


            }


        }else if(predicates[i].relationB == 1) {   // filtro >
	    int rel = predicates[i].relationA;
	    uint64_t Ua = stats_array_temp[rel].stats[predicates[i].columnA].Ua;
	    uint64_t Ia = stats_array_temp[rel].stats[predicates[i].columnA].Ia;
	    uint64_t Da = stats_array_temp[rel].stats[predicates[i].columnA].Da;
	    uint64_t Fa = stats_array_temp[rel].stats[predicates[i].columnA].Fa;
	    uint64_t k1 = predicates[i].columnB;
	    uint64_t k2 = stats_array_temp[rel].stats[predicates[i].columnA].Ua;

	    stats_array_temp[rel].stats[predicates[i].columnA].Ia = k1;
	    stats_array_temp[rel].stats[predicates[i].columnA].Da = ((k2-k1) / (Ua - Ia)) * Da;
  	    stats_array_temp[rel].stats[predicates[i].columnA].Fa = ((k2-k1) / (Ua - Ia)) * Fa;

	    for(int j = 0; j < stats_array_temp[rel].columns; j++){

	        if(j != predicates[i].columnA) {
		    uint64_t Dc = stats_array_temp[rel].stats[j].Da;
                    uint64_t Fa1 = stats_array_temp[rel].stats[predicates[i].columnA].Fa;
                    uint64_t Fa = (*stats_array)[rel].stats[predicates[i].columnA].Fa;
                    uint64_t Fc = stats_array_temp[rel].stats[j].Fa;

                    stats_array_temp[rel].stats[j].Da = ( Dc * (1 - (1 - Fa1 / Fa) ^ ( Fc / Dc ) ));
		    stats_array_temp[rel].stats[j].Fa = Fa1;
		}
	    }
        }else if(predicates[i].relationB == 2) {     // filtro <
	    int rel = predicates[i].relationA;
            uint64_t Ua = stats_array_temp[rel].stats[predicates[i].columnA].Ua;
            uint64_t Ia = stats_array_temp[rel].stats[predicates[i].columnA].Ia;
            uint64_t Da = stats_array_temp[rel].stats[predicates[i].columnA].Da;
            uint64_t Fa = stats_array_temp[rel].stats[predicates[i].columnA].Fa;
            uint64_t k1 = stats_array_temp[rel].stats[predicates[i].columnA].Ia;
            uint64_t k2 = predicates[i].columnB;

            stats_array_temp[rel].stats[predicates[i].columnA].Ua = k2;
            stats_array_temp[rel].stats[predicates[i].columnA].Da = ((k2-k1) / (Ua - Ia)) * Da;
            stats_array_temp[rel].stats[predicates[i].columnA].Fa = ((k2-k1) / (Ua - Ia)) * Fa;

            for(int j = 0; j < stats_array_temp[rel].columns; j++){

                if(j != predicates[i].columnA) {
                    uint64_t Dc = stats_array_temp[rel].stats[j].Da;
                    uint64_t Fa1 = stats_array_temp[rel].stats[predicates[i].columnA].Fa;
                    uint64_t Fa = (*stats_array)[rel].stats[predicates[i].columnA].Fa;
                    uint64_t Fc = stats_array_temp[rel].stats[j].Fa;

                    stats_array_temp[rel].stats[j].Da = ( Dc * (1 - (1 - Fa1 / Fa) ^ ( Fc / Dc ) ));
                    stats_array_temp[rel].stats[j].Fa = Fa1;
                }
            }
        }

    }
    else {     // an ginetai join
        int relA = predicates[i].relationA;
        int relB = predicates[i].relationB;
        int colA = predicates[i].columnA;
        int colB = predicates[i].columnB;

        uint64_t Iaa = stats_array_temp[relA].stats[colA].Ia;
        uint64_t Ibb = stats_array_temp[relB].stats[colB].Ia;

        if(Iaa >= Ibb) {
          stats_array_temp[relA].stats[colA].Ia = Iaa;
          stats_array_temp[relB].stats[colB].Ia = Iaa;
        }
        else {
          stats_array_temp[relA].stats[colA].Ia = Ibb;
          stats_array_temp[relB].stats[colB].Ia = Ibb;
        }


        uint64_t Uaa = stats_array_temp[relA].stats[colA].Ua;
        uint64_t Ubb = stats_array_temp[relB].stats[colB].Ua;



      if(Uaa <= Ubb) {
          stats_array_temp[relA].stats[colA].Ua = Uaa;
          stats_array_temp[relB].stats[colB].Ua = Uaa;
      }
      else {
          stats_array_temp[relA].stats[colA].Ua = Ubb;
          stats_array_temp[relB].stats[colB].Ua = Ubb;
      }


      uint64_t n = stats_array_temp[relA].stats[colA].Ua - stats_array_temp[relA].stats[colA].Ia + 1;

      stats_array_temp[relA].stats[colA].Fa = (stats_array_temp[relA].stats[colA].Fa * stats_array_temp[relB].stats[colB].Fa) / n;
      stats_array_temp[relB].stats[colB].Fa = stats_array_temp[relA].stats[colA].Fa;

      stats_array_temp[relA].stats[colA].Da = (stats_array_temp[relA].stats[colA].Da * stats_array_temp[relB].stats[colB].Da) / n;
      stats_array_temp[relB].stats[colB].Da = stats_array_temp[relA].stats[colA].Da;




      // gia opoiadhpote allh stili C tou pinaka A

      for(int j = 0; j < stats_array_temp[relA].columns; j++) {

        //printf("j1==%d,relA=%d,columnA=%d\n",j,relA,predicates[i].columnA);


          if(j != predicates[i].columnA) {    // prepei na einai diaforetiki stili apo thn A
             uint64_t Fa1 = stats_array_temp[relA].stats[colA].Fa;

            stats_array_temp[relA].stats[j].Fa = Fa1;          // Fc'

            uint64_t Dc = stats_array_temp[relA].stats[j].Da;
            //printf("dc==%lu\n",Dc);
            Fa1 = stats_array_temp[relA].stats[colA].Fa;  // Fa'
            uint64_t Fa = (*stats_array)[tables[relA]].stats[predicates[i].columnA].Fa;
          //  printf("da==%lu\n",Da);

            uint64_t Fc = (*stats_array)[tables[relA]].stats[j].Fa;

          //  printf("1111\n");
	    if(Dc == 0) Dc = 1;
            stats_array_temp[relA].stats[j].Da =  Dc * (1 - (1 - Fa1 / Fa) ^ ( Fc / Dc ) );	/////////////

          }


      }
            // gia opoiadhpote allh stili C tou pinaka B
      for(int j = 0; j < stats_array_temp[relB].columns; j++) {
        //printf("j2==%d,relB=%d,columnB=%lu\n",j,relB,predicates[i].columnB);

          if(j != predicates[i].columnB) {    // prepei na einai diaforetiki stili apo thn A
             uint64_t Fa1 = stats_array_temp[relB].stats[predicates[i].columnB].Fa;

            stats_array_temp[relB].stats[j].Fa = Fa1;          // Fc'

            uint64_t Dc = stats_array_temp[relB].stats[j].Da;

            Fa1 = stats_array_temp[relB].stats[predicates[i].columnB].Fa;  // Fa'

            uint64_t Fa = (*stats_array)[tables[relB]].stats[predicates[i].columnB].Fa;

            uint64_t Fc = (*stats_array)[tables[relB]].stats[j].Fa;

          //  printf("--------Dc=%lu,Da=%lu,Da1=%lu,Fc=%lu\n",Dc,Da,Da1,Fc);
            if(Dc == 0) Dc = 1;
            stats_array_temp[relB].stats[j].Da =  Dc * (1 - (1 - Fa1 / Fa) ^ ( Fc / Dc ) );	/////   Dc

          }
      }

    }

  }



hash_node *hash_table;
  hash_table = malloc(relationNum * sizeof(hash_node));
  if(hash_table == NULL){
      printf("Error malloc hash_t \n");
      exit(1);
  }
  for(int i=0;i < relationNum;i++){
    hash_table[i].order=malloc(relationNum * sizeof(int));
    if(hash_table[i].order == NULL){
      printf("error order malloc\n");
      exit(1);
    }
    hash_table[i].cost=0;
    hash_table[i].index=0;
  }
  for(int i=0;i < relationNum;i++){
    int index = hash_table[i].index;
    hash_table[i].order[index] = i;
    hash_table[i].index++;
    hash_table[i].cost += stats_array_temp[i].stats[1].Fa;
  }


 int flag;
  for(int i = 1 ; i < relationNum ; i++){
      for(int s = 0 ; s < relationNum ; s++){
          flag=0;
          for(int j = 0 ; j < relationNum ; j++){
              if( is_in_hash_t(hash_table, s, j) || !connected(hash_table, s, j,predicates,number_of_predicates,flag)){//an uparxei to Rj sto S i den einai connected,continue
                continue;
              }
              hash_node t; //besttree+Rj(currtree)
              t.order = malloc( (i+1) * sizeof(int));
              if(hash_table[s].index< i+1){//besttree(S')==NULL
                t.cost = hash_table[s].cost;
                t.index = hash_table[s].index;
                for(int q = 0 ; q < t.index ; q++) t.order[q] = hash_table[s].order[q];//t.index=i ara q paei mexri i

                t.order[t.index] = j;
                t.index++;
                t.cost += stats_array_temp[j].stats[1].Fa;
              }
              if(hash_table[s].index== i+1){
                t.index=hash_table[s].index;
                t.cost=0;
                for(int q=0;q< t.index-1;q++){
                  t.order[q]= hash_table[s].order[q];
                  //printf("t.order[%d]=%d\n",q,t.order[q]);
                  t.cost+=stats_array_temp[t.order[q]].stats[1].Fa;
                }
                t.order[t.index-1] = j;
                t.cost+=stats_array_temp[j].stats[1].Fa;
              }

              if(t.index>hash_table[s].index){//besttree(S')==NULL
                hash_table[s].cost= t.cost;
                hash_table[s].index++;
                for(int q = 0 ; q <= t.index ; q++) {
                  hash_table[s].order[q]=t.order[q];
                flag=1;
                }
              }
              else if(t.index==hash_table[s].index){//elegxos kostous
                if(t.cost < hash_table[s].cost){
                  hash_table[s].cost= t.cost;
                  for(int q=0;q<= t.index;q++){
                    hash_table[s].order[q]=t.order[q];
                  }
                }

              }
          free(t.order);
         }
      }
   }
   int min_index=0;
   int min=hash_table[0].cost;
   for(int i=1;i<relationNum;i++){//return besttree
     if(hash_table[i].cost<min)
      min_index=i;
   }

   int* predicatesOrder = malloc(relationNum * sizeof(int));

   //printf("cost=%d\n",hash_table[min_index].cost);
   for(int i=0;i<relationNum;i++){
     //printf("hash_table[%d].order[%d]=%d\n",min_index,i,hash_table[min_index].order[i]);
     predicatesOrder[i] = hash_table[min_index].order[i];
   }



  for(int i=0;i< relationNum;i++){
      for(int j=0;j< stats_array_temp[i].columns;j++){
          free(stats_array_temp[i].stats[j].Da_array);
      }
      free(stats_array_temp[i].stats);
      free(hash_table[i].order);
   }
   free(stats_array_temp);
   free(hash_table);



   return predicatesOrder;
}




 void read_queries(char *query_file,main_array **array,int relation_number, statistics_array **stats_array){

   char *query, query2[100];
   size_t len = 0;
   clock_t time;

   FILE *f=fopen(query_file,"r");
   if(f==NULL){
       printf("error in opening query_file\n");
       exit(1);
   }

   int xx =  0;
   int yy = 0;
   while(getline(&query,&len,f)!= -1) {
        xx++;

        if(!strcmp(query,"F\n")){
            xx--;
            printf("\nEnd of batch.\n\n");
            continue;
        }
        //if(xx < 30 && xx==33) continue;

        yy++;
        int *tables = malloc(relation_number * sizeof(int));
        if(tables == NULL){
            printf("Error malloc tables \n");
	    exit(1);
        }
        strcpy(query2, query);

        take_relations(query2, &tables[0], relation_number); 	     // exoume tis sxeseis ston pinaka tables
        strcpy(query2, query);


        int number_of_predicates = take_number_of_predicates(query2);
        strcpy(query2, query);
        q *predicates = malloc(number_of_predicates * sizeof(q));
        if(predicates == NULL){
            printf("Error malloc queries \n");
            exit(1);
        }

        take_predicates(predicates, number_of_predicates, query2);  // edw exoume ena pinaka apo ta predicate tou query
        strcpy(query2, query);


        int relationNum = 0;
        for(int a = 0; a < relation_number; a++) {
            if(tables[a] != -1) {
              relationNum++;
            }
        }

        int* predicatesOrder = malloc(relationNum * sizeof(int));
        predicatesOrder = orderOfPredicates(predicates, number_of_predicates,stats_array, tables, relation_number );


        //checksum
        int number_of_checksums=find_checksum_number(query2);
        checksum_struct *checksums = malloc(number_of_checksums * sizeof(checksum_struct));
        strcpy(query2, query);

        take_checksums(checksums,number_of_checksums,query2);//edw exoume to struct me ta checksums

//        pthread_mutex_lock(&mtx);


//        while (queue_count >= queue_size) {
//          printf(">> Found Buffer Full \n");
//          pthread_cond_wait(&cond_nonfull, &mtx);
//        }

        queue[queue_tail].queryNum = xx;
        queue[queue_tail].number_of_predicates = number_of_predicates;
        queue[queue_tail].relation_number = relation_number;
        queue[queue_tail].number_of_checksums = number_of_checksums;
        queue[queue_tail].tables     = malloc(relation_number * sizeof(int));
        queue[queue_tail].checksums  = malloc(number_of_checksums * sizeof(checksum_struct));
        queue[queue_tail].predicates  = malloc(number_of_predicates * sizeof(q));
        queue[queue_tail].predicatesOrder  = malloc(relationNum * sizeof(int));

        queue[queue_tail].results = malloc(number_of_checksums * sizeof(uint64_t));


        for(int i = 0; i < relation_number; i++) {
            queue[queue_tail].tables[i] = tables[i];
        }

        for(int i = 0; i < number_of_checksums; i++) {
            queue[queue_tail].checksums[i] = checksums[i];
        }

        for(int i = 0; i < number_of_predicates; i++) {
            queue[queue_tail].predicates[i] = predicates[i];
        }

        for(int i = 0; i < relationNum; i++) {
            queue[queue_tail].predicatesOrder[i] = predicatesOrder[i];
        }

        add_queue();

//        pthread_mutex_unlock(&mtx);


//        pthread_cond_signal(&cond_nonempty);

        //lets_go_for_predicates(array, &tables[0], relation_number, predicates, number_of_predicates,checksums,number_of_checksums);

       free(checksums);
       free(predicates);
       free(tables);
       free(predicatesOrder);
   }

   initializeQueriesNumber(yy);


   fclose(f);
   free(query);

   return;
 }






 void delete_all_array(main_array **array, int relation_number, char **directory, char **file,char **query_file,statistics_array **stats_array){

   for(int i = 0 ; i < relation_number; i++){
       free( (*array)[i].index );
       free( (*array)[i].relation_array );
   }
   free( *array );

   for(int i=0;i< relation_number;i++){
     for(int j=0;j< (*stats_array)[i].columns;j++){
       free((*stats_array)[i].stats[j].Da_array);
     }
     free((*stats_array)[i].stats);
   }
   free(*stats_array);

   free(*directory);
   free(*file);
   free(*query_file);

   return;
 }



 void* threadFunction(void* args) {



    while(queue_count > 0) {

//      pthread_mutex_lock(&mtx);
       int queue_head2;
       int number_of_predicates;
       int relation_number;
       int number_of_checksums;
       clock_t time;

//      printf("queries num is %d\n", queriesNumber);
//      printf("queries checked is %d\n", queriesChecked);

//      if(queriesNumber == queriesChecked) {
//        pthread_mutex_unlock(&mtx);
//        break;
//      }

//      while (queue_count <= 0) {
//        printf(">> Found Buffer Empty \n");
//        pthread_cond_wait(&cond_nonempty, &mtx);
 //     }

        int aa = 0, xx;
        sem_wait(&semQueue);
            if(queue_count > 0){
                //queue_count--;


        number_of_predicates = queue[queue_head].number_of_predicates;
        relation_number = queue[queue_head].relation_number;
        number_of_checksums = queue[queue_head].number_of_checksums;


        time  = clock();

        q* newPredicates = malloc(number_of_predicates * sizeof(q));
        newPredicates = fixOrderOfPredicates(queue[queue_head].predicates, number_of_predicates, queue[queue_head].predicatesOrder);

        aa++;
        for(int d = 0; d < number_of_predicates; d++) {
            queue[queue_head].predicates[d].join = newPredicates[d].join;
            queue[queue_head].predicates[d].flag = newPredicates[d].flag;
            queue[queue_head].predicates[d].relationA = newPredicates[d].relationA;
            queue[queue_head].predicates[d].columnA = newPredicates[d].columnA;
            queue[queue_head].predicates[d].columnB = newPredicates[d].columnB;
            queue[queue_head].predicates[d].relationB = newPredicates[d].relationB;
//            printf("relA=%d,colA=%d,relB=%d,colB=%lu,join=%d,flag=%d   ", queue[queue_head].predicates[d].relationA,queue[queue_head].predicates[d].columnA,queue[queue_head].predicates[d].relationB,queue[queue_head].predicates[d].columnB,queue[queue_head].predicates[d].join,queue[queue_head].predicates[d].flag);
        }
        xx = queue[queue_head].queryNum;

        //printf("\n");
        free(newPredicates);
        queue_head2 = queue_head;

        delete_queue();
        queriesChecked++;

	}

	sem_post(&semQueue);


//      pthread_mutex_unlock(&mtx);
//      pthread_cond_signal(&cond_nonfull);
        if( aa )
            lets_go_for_predicates((main_array **)args, &queue[queue_head2].tables[0], relation_number, queue[queue_head2].predicates, number_of_predicates, queue[queue_head2].checksums , number_of_checksums, queue_head2);

        time = clock() - time;
        //printf(" xx is %d ........   time is: %lf\n", xx, (double) time / CLOCKS_PER_SEC);


    }


    pthread_exit(NULL);
 }





q* fixOrderOfPredicates(q *predicates, int number_of_predicates, int* orderOfPredicates) {
  int counter=0;
  q* newPredicates = malloc(number_of_predicates * sizeof(q));
  bool* predicateIsChecked = malloc(number_of_predicates * sizeof(bool));
  for(int i = 0; i < number_of_predicates; i++) {
    predicateIsChecked[i] = false;
  }

  int numOfFilters = 0;
  for(int i = 0; i < number_of_predicates; i++) {
    if(predicates[i].join == false) {
      numOfFilters++;
    }
  }

  for(int j = 0; j < number_of_predicates - numOfFilters; j++) {
    for(int i = 0; i < number_of_predicates - numOfFilters; i++) {

          if(j == 0) {

              if(predicateIsChecked[i] == false && predicates[i].relationA == orderOfPredicates[0] && predicates[i].relationB == orderOfPredicates[1]) {
                  newPredicates[0].join = predicates[i].join;
                  newPredicates[0].flag = predicates[i].flag;
                  newPredicates[0].relationA = predicates[i].relationA;
                  newPredicates[0].columnA = predicates[i].columnA;
                  newPredicates[0].columnB = predicates[i].columnB;
                  newPredicates[0].relationB = predicates[i].relationB;

                  predicateIsChecked[i] = true;
                  counter++;
                  break;
              }
              else if(predicateIsChecked[i] == false && predicates[i].relationA == orderOfPredicates[1] && predicates[i].relationB == orderOfPredicates[0]) {
                  newPredicates[0].join = predicates[i].join;
                  newPredicates[0].flag = predicates[i].flag;
                  newPredicates[0].relationA = predicates[i].relationA;
                  newPredicates[0].columnA = predicates[i].columnA;
                  newPredicates[0].columnB = predicates[i].columnB;
                  newPredicates[0].relationB = predicates[i].relationB;

                  predicateIsChecked[i] = true;
                  counter++;

                  break;
              }
          }
          else {
              int index = j + 1;
              if(predicateIsChecked[i] == false && predicates[i].relationA == orderOfPredicates[index]  ) {
                for(int count  = 0; count < index; count++) {
                  if(predicates[i].relationB == orderOfPredicates[count]) {
                      newPredicates[j].join = predicates[i].join;
                      newPredicates[j].flag = predicates[i].flag;
                      newPredicates[j].relationA = predicates[i].relationA;
                      newPredicates[j].columnA = predicates[i].columnA;
                      newPredicates[j].columnB = predicates[i].columnB;
                      newPredicates[j].relationB = predicates[i].relationB;
                      predicateIsChecked[i] = true;
                      counter++;

                  }

                }
              }
              else if(predicateIsChecked[i] == false && predicates[i].relationB == orderOfPredicates[index]  ) {
                for(int count  = 0; count < index; count++) {
                  if(predicates[i].relationA == orderOfPredicates[count]) {
                      newPredicates[j].join = predicates[i].join;
                      newPredicates[j].flag = predicates[i].flag;
                      newPredicates[j].relationA = predicates[i].relationA;
                      newPredicates[j].columnA = predicates[i].columnA;
                      newPredicates[j].columnB = predicates[i].columnB;
                      newPredicates[j].relationB = predicates[i].relationB;
                      predicateIsChecked[i] = true;
                      counter++;
                  }

                }
              }


          }

    }

  }
  for(int i=0;i<number_of_predicates-numOfFilters;i++){//pros8etoume ta extra predicates(periptwsi san to proteleutaio query)
    if(!predicateIsChecked[i]){
      newPredicates[counter].join = predicates[i].join;
      newPredicates[counter].flag = predicates[i].flag;
      newPredicates[counter].relationA = predicates[i].relationA;
      newPredicates[counter].columnA = predicates[i].columnA;
      newPredicates[counter].columnB = predicates[i].columnB;
      newPredicates[counter].relationB = predicates[i].relationB;
      counter++;
    }
  }


  /*for(int i=0;i<number_of_predicates-numOfFilters;i++){
    printf("checked=%d\n",predicateIsChecked[i]);
    printf("relA=%d,colA=%d,relB=%d,colB=%lu\n", newPredicates[i].relationA,newPredicates[i].columnA,newPredicates[i].relationB,newPredicates[i].columnB);
  }*/
  for(int i = 0; i < numOfFilters; i++) {
    int index = number_of_predicates + i - numOfFilters;
    newPredicates[index] = predicates[index];
  }
  free(predicateIsChecked);

  return newPredicates;
}
