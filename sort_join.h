#ifndef __SORT_JOIN__
#define __SORT_JOIN__

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "join_list.h"
#include "mid_list.h"



void make_hist(relation **Rr_1, int start, int end, int *hist, int hist_size, int bytePos);
void make_p_sum(int *hist, int hist_size, int *p_sum, int p_sum_size, int start);
void make_Rr_2(relation **Rr_1, relation **Rr_2, int start, int end, int *p_sum, int p_sum_size, int bytePos);


int partition (relation **Rr, int low, int high);
void quickSort(relation **Rr, int low, int high);
void recurseFunc(relation **Rr_1, relation **Rr_2, int start, int end, int bytePos);


void Sort_Merge_Join(relation **Rr, relation **Ss, info_deikti *list,main_pointer *imid1,main_pointer *imid2,int first_join,int flag);


int partition2(uint64_t *A, int lo, int hi);
void quicksort2(uint64_t *A, int low, int high);

#endif
