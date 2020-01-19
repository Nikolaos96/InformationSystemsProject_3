#ifndef __FUNCTION__
#define __FUNCTION__

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include "structs.h"
#include "mid_list.h"
#include "join_list.h"
#include "stats.h"
#include "queue.h"
#include <pthread.h>

void take_arguments(int argc,char *argv[],char **file, char **dir,char **query_file);
int find_relation_number(char *init_file);
int create_init_relations(char *directory, char *workload_file, main_array **array,statistics_array **stats_array);
void take_relations(char *query,int *tables, int tables_size);

int take_number_of_predicates(char *query);
int take_tokens(char *str);

void take_predicates(q *predicates, int number_of_predicates, char *query);
int take_checksum_number(char* query);
void take_checksums(checksum_struct *checksums,int number_of_checksums,char* query);

void malloc_Rr_Ss(relation **Rr1, relation **Rr2);
void delete_Rr_Ss(relation **Rr1, relation **Rr2);


void make_Rr1_Rr2(main_array **array, int *tables, q *predicates, int number_of_predicates, int jj, relation **Rr1, relation **Rr2, int a);
void make_Rr1_Rr2__2(main_array **array, main_pointer *mid_result, int *tables, q *predicates, int number_of_predicates, int jj, relation **Rr1, relation **Rr2, int a,int *sort_needed);

void  make_second_intermid(info_deikti *join_list, main_pointer *imid_list, int size_imid_list, int rel,int rel2, int join_stil_A, int join_stil_B);
void print_checksums(main_array **array, int *tables,checksum_struct *checksums,int number_of_checksums,main_pointer *imid_list,int imid_index, int index);

void edit_itermid(main_array **array, int *tables, main_pointer *imid_list, main_pointer *imid_list2, int relA, int colA, int relB, uint64_t colB, int ii);

int* orderOfPredicates(q* predicates, int number_of_predicates, statistics_array **stats_array, int* tables, int relation_number);

void lets_go_for_predicates(main_array **array, int *tables, int relation_number, q *predicates, int number_of_predicates,checksum_struct *checksums,int number_of_checksums, int index);
void read_queries(char *query_file,main_array **array,int relation_number, statistics_array **stats_array);

void delete_all_array(main_array **array, int relation_number, char **directory, char **file,char **query_file,statistics_array **stats_array);

void* threadFunction(void* args);

q* fixOrderOfPredicates(q *predicates, int number_of_predicates, int* orderOfPredicates);

#endif
