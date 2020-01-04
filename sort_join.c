/* sort_join.c */
#include "sort_join.h"
//#include "mid_list.h"
#define HIST_SIZE  256
#define P_SUM_SIZE 256


/*
  make hist array for relation Rr_1
 */
 void make_hist(relation **Rr_1, int start, int end, int *hist, int hist_size, int bytePos){
    int i, a;

    for(i = 0 ; i < hist_size ; i++) hist[i] = 0;

    for(i = start ; i < end ; i++){
         a = ( (*Rr_1)->tuples[i].key >> (8*bytePos) ) & 0x00000000000000ff;
	 hist[a]++;
    }
 }




 /*
  make p_sum array
 */
 void make_p_sum(int *hist, int hist_size, int *p_sum, int p_sum_size, int start){
     int i, j;

     if(hist[0] != 0) p_sum[0] = start;
     else p_sum[0] = -1;
     for(i = 1 ; i < hist_size ; i++){

		if(hist[i] == 0){
			p_sum[i] = -1;
			continue;
		}
		int sum = 0;
		for(j = 0 ; j < i ; j++)
			sum += hist[j];

		p_sum[i] = sum + start;
     }
 }




 /*
  make Rr_2 relation
 */
 void make_Rr_2(relation **Rr_1, relation **Rr_2,  int start, int end, int *p_sum, int p_sum_size, int bytePos){
     int a, i, pos;

     for(i = start ; i < end ; i++){
         a = ((*Rr_1)->tuples[i].key >> (8*bytePos) ) & 0x00000000000000ff;
	       pos = p_sum[a];

	 (*Rr_2)->tuples[pos].key = (*Rr_1)->tuples[i].key;
	 (*Rr_2)->tuples[pos].payload = (*Rr_1)->tuples[i].payload;

	 p_sum[a]++;
     }
 }




 /*
  part of quicksort
 */
 /*
 int partition (relation **Rr, int low, int high) {
	uint64_t pivot = (*Rr)->tuples[high].key;
//	uint64_t pivot = (*Rr)->tuples[(low+high)/2].key;
	int i = (low - 1);
        uint64_t tempKey;
        uint64_t tempPayload;

	for (int j = low; j <= high- 1; j++) {
		if ((*Rr)->tuples[j].key < pivot) { // merikoi grafoun <=
			i++;
			 tempKey = (*Rr)->tuples[i].key;
		   tempPayload = (*Rr)->tuples[i].payload;
			(*Rr)->tuples[i].key = (*Rr)->tuples[j].key;
			(*Rr)->tuples[i].payload = (*Rr)->tuples[j].payload;

			(*Rr)->tuples[j].key = tempKey;
			(*Rr)->tuples[j].payload = tempPayload;
		}
	}
	tempKey = (*Rr)->tuples[i + 1].key;
        tempPayload = (*Rr)->tuples[i + 1].payload;
	(*Rr)->tuples[i + 1].key = (*Rr)->tuples[high].key;
	(*Rr)->tuples[i + 1].payload = (*Rr)->tuples[high].payload;
	(*Rr)->tuples[high].key = tempKey;
	(*Rr)->tuples[high].payload = tempPayload;
	return (i + 1);
 }
 */
 int partition(relation **Rr, int lo, int hi) {
     uint64_t pivot = (*Rr)->tuples[lo + (hi - lo) / 2].key;
     int i = lo - 1;
     int j = hi + 1;

     uint64_t tempKey;
     uint64_t tempPayload;

     while(1){
         do{
             i += 1;
	 }while((*Rr)->tuples[i].key < pivot);
         do{
             j -= 1;
	 }while((*Rr)->tuples[j].key > pivot);

  	 if( i >= j) return j;

	 tempKey = (*Rr)->tuples[i].key;
	 (*Rr)->tuples[i].key = (*Rr)->tuples[j].key;
	 (*Rr)->tuples[j].key = tempKey;

	 tempPayload = (*Rr)->tuples[i].payload;
	 (*Rr)->tuples[i].payload = (*Rr)->tuples[j].payload;
	 (*Rr)->tuples[j].payload = tempPayload;
     }
 }


 /*
  quicksort
 */
 /*
 void quickSort(relation **Rr, int low, int high) {
	if (low < high) {
		int pi = partition(Rr, low, high);
		quickSort(Rr, low, pi - 1);
		quickSort(Rr, pi + 1, high);
	}
 }
 */

 void quickSort(relation **Rr, int low, int high) {
    if(low < high){
        int pi = partition(Rr, low, high);
	quickSort(Rr, low, pi);
	quickSort(Rr, pi+1, high);
    }
 }



 /*
  recurse function for sorting relations
 */
 void recurseFunc(relation **Rr_1, relation **Rr_2, int start, int end, int bytePos) {

     if( end - start > 4096 ) {
	   int *hist, *p_sum;
	   hist = malloc(HIST_SIZE * sizeof(int));
	   if(hist == NULL){
		   printf("Error malloc hist \n");
		   exit(1);
	   }
	   p_sum = malloc(P_SUM_SIZE * sizeof(int));
	   if(p_sum == NULL){
		   printf("Error malloc p_sum \n");
		   exit(1);
	   }

	   make_hist(Rr_1, start, end, &hist[0], HIST_SIZE, bytePos);
	   make_p_sum(&hist[0], HIST_SIZE, &p_sum[0], P_SUM_SIZE, start);
	   make_Rr_2(Rr_1, Rr_2, start, end, &p_sum[0], P_SUM_SIZE, bytePos);

	   bytePos--;

	   for(int i = 0; i < HIST_SIZE ; i++) {
		if( hist[i] == 0)
		    continue;
		else{
		    if( bytePos > 0)
			recurseFunc( Rr_2, Rr_1, p_sum[i] - hist[i], p_sum[i], bytePos);
		    else{
			quickSort(Rr_2, start, end - 1);
			for(int k = start ; k < end ; k++){				///////
			   (*Rr_1)->tuples[k].key = (*Rr_2)->tuples[k].key; 		///////
			   (*Rr_1)->tuples[k].payload = (*Rr_2)->tuples[k].payload;	////////
			}								////////
		    }
		}
	   }

	   free(p_sum);
	   free(hist);
     }else{
	  quickSort(Rr_1, start, end - 1);
	  for(int k = start; k < end; k++){
              (*Rr_2)->tuples[k].key = (*Rr_1)->tuples[k].key;
              (*Rr_2)->tuples[k].payload = (*Rr_1)->tuples[k].payload;
          }
     }

     return;
 }




 /*
  function for join relation Rr - relation Ss
 */
 void Sort_Merge_Join(relation **Rr, relation **Ss, info_deikti *list,main_pointer *imid1,main_pointer *imid2,int first_join,int flag){
     int mark = -1;
     int r = 0, s = 0;

     do{
         if(mark == -1){
	     while( (*Rr)->tuples[r].key < (*Ss)->tuples[s].key && (r < (*Rr)->num_tuples) && (s < (*Ss)->num_tuples) )  { r++; }
	     while( (*Rr)->tuples[r].key > (*Ss)->tuples[s].key && (r < (*Rr)->num_tuples) && (s < (*Ss)->num_tuples) )  { s++; }
	     mark = s;
	 }

	 if((r > (*Rr)->num_tuples) || (s > (*Ss)->num_tuples)) break;

	 if( (*Rr)->tuples[r].key == (*Ss)->tuples[s].key ){
       if(first_join==1){//1o join,apotelesmata apeu8eias ston endiameso
         eisagogi_eggrafis_mid(imid1,(*Rr)->tuples[r].payload);
         eisagogi_eggrafis_mid(imid1,(*Ss)->tuples[s].payload);

       }
       else if(first_join==0 && flag==1){
         int cols=take_columns(imid1);
         for(int k=0;k<cols;k++){
           eisagogi_eggrafis_mid(imid2,take_rowid(imid1,r*cols+k));
         }
         eisagogi_eggrafis_mid(imid2,(*Ss)->tuples[s].payload);
       }
       else if(first_join==0 && flag==2){
         int cols=take_columns(imid1);
         for(int k=0;k<cols;k++){
           eisagogi_eggrafis_mid(imid2,take_rowid(imid1,s*cols+k));
         }
         eisagogi_eggrafis_mid(imid2,(*Rr)->tuples[r].payload);
       }
       else{
	      eisagogi_eggrafis(list, (*Rr)->tuples[r].payload, (*Ss)->tuples[s].payload);
       }
	     s++;
	 }else{
	     s = mark;
	     r++;
	     mark = -1;
	 }
     }while( (r < (*Rr)->num_tuples + 1) && (s < (*Ss)->num_tuples + 1) );



/*
     for(int i = 0 ; i < (*Rr)->num_tuples ; i++){
         for(int j = 0 ; j < (*Ss)->num_tuples ; j++){
             if((*Rr)->tuples[i].key == (*Ss)->tuples[j].key){
	         eisagogi_eggrafis(list, (*Rr)->tuples[i].payload, (*Ss)->tuples[j].payload);
	     }
	 }
     }
*/
     return;
 }





 int partition2(uint64_t *A, int lo, int hi){
     uint64_t pivot = A[lo + (hi - lo) / 2];
     int i = lo - 1;
     int j = hi + 1;

     while(1){
         do{
             i  = i + 1;
         }while(A[i] < pivot);
         do{
             j = j - 1;
         }while(A[j] > pivot);

         if( i >= j ) return j;

         uint64_t temp = A[i];
         A[i] = A[j];
         A[j] = temp;
     }
 }

 void quicksort2(uint64_t *A, int low, int high){
     if(low < high){
         int p = partition2(A, low, high);
         quicksort2(A, low, p);
         quicksort2(A, p+1, high);
     }
 }
