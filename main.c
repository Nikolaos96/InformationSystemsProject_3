/* main.c */
#include "function.h"
#include "structs.h"
#include "stats.h"
#define DONE "Done"



int main(int argc, char *argv[]){
  char *workload_file, *directory,*query_file, done[20];
  int relation_number;
  main_array *array;
  statistics_array *stats_array;

  take_arguments(argc, argv, &workload_file, &directory,&query_file);

  relation_number = create_init_relations(directory, workload_file, &array,&stats_array);
  for(int i=0;i < 1;i++){
    for(int j=0;j<1;j++){//stats_array[i].columns
    //  printf("min=%lu,max=%lu,pli8os=%lu\n",stats_array[i].stats[j].Ia,stats_array[i].stats[j].Ua,stats_array[i].stats[j].Fa);
      for(int k=0;k<(stats_array[i].stats[j].Ua-stats_array[i].stats[j].Ia +1);k++)
        printf("%d  ", stats_array[i].stats[j].Da_array[k]);
    }
  }
  exit(0);

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

  read_queries(query_file,&array,relation_number);

  delete_all_array(&array, relation_number, &directory, &workload_file,&query_file);
  return 0;
}
