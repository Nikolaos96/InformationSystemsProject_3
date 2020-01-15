#include "hash_t.h"


int is_in_hash_t(hash_node *table, int bucket, int stoixeio){
    for(int i = 0 ; i < table[bucket].index ; i++){
        if( table[bucket].order[i] == stoixeio ) return 1;
    }
    return 0;
}


int connected(hash_node* table, int s, int j,q* predicates,int number_of_predicates,int flag){
    int barrier;
    if(flag)
      barrier=table[s].index-1;
    else
      barrier=table[s].index;
    for(int i=0;i<barrier;i++){//gia ka8e sxesi pou uparxei sto besttree
      for(int n=0;n<number_of_predicates;n++){//gia ka8e predicate tou query
        if((predicates[n].relationA==j && predicates[n].relationB==table[s].order[i]) || (predicates[n].relationA==table[s].order[i] && predicates[n].relationB==j)){
          if(predicates[n].join)
            return 1; //connected ama uparxei join kapoias sxesis me to j
        }
      }
    }
    return 0;
}
