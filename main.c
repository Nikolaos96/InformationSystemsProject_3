/* main.c */
#include "function.h"
#include "structs.h"
#include "stats.h"
#include <pthread.h>
#include <semaphore.h>
#define DONE "Done"
#define NUM_OF_THREADS 8

int queriesNumber = 10000;

int main(int argc, char *argv[]){

  char *workload_file, *directory,*query_file, done[20];
  int relation_number;
  main_array *array;
  statistics_array *stats_array;

  take_arguments(argc, argv, &workload_file, &directory,&query_file);

  relation_number = create_init_relations(directory, workload_file, &array,&stats_array);

  

  int rep = 0;
  do{
      if( !rep ){
	  printf("Give me str. \n");
	  scanf("%s", done);
      }else{
	  printf("False.Give again the str. \n");
	  scanf("%s", done);
      }
      rep++;
  }while(strcmp(DONE, done));


////////////////////////

  create_queue(100);

  read_queries(query_file,&array,relation_number, &stats_array);

  usleep(100000);

  clock_t time;
  time = clock();



  pthread_t *tids;
  if((tids = malloc(NUM_OF_THREADS * sizeof(pthread_t))) == NULL) {
      printf("error in malloc\n");
      exit(1);
  }

  int err;

  for(int i = 0; i < NUM_OF_THREADS; i++) {
    if(err = pthread_create(tids+i, NULL, threadFunction, (void*)&array)) {
      printf("Error in pthread_create\n");
      exit(1);
    }
  }




for(int i = 0; i < NUM_OF_THREADS; i++){
  if(err = pthread_join(*(tids+i), NULL)) {
    printf("Error in pthread_join\n");
    exit(1);
  }
}

//pthread_exit(NULL);

for(int i = 0; i < queue_tail; i++) {
  //printf("queuequeryNum is %d\n", queue[queue_tail - 1].queryNum);
  //printf("relationA is %d, column A is %d\n", queue[queue_tail - 1].predicates[0].relationA, queue[queue_tail - 1].number_of_predicates);
   free(queue[i].checksums);
   free(queue[i].predicates);
   free(queue[i].tables);
}



remove_queue();

time = clock() - time ;
printf("Total time taken by CPU: %lf\n", (double) time / CLOCKS_PER_SEC);

/////////////////////////////

  delete_all_array(&array, relation_number, &directory, &workload_file,&query_file,&stats_array);
  
  return 0;
	
}
