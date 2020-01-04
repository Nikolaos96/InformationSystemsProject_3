#ifndef __STATS__
#define __STATS__
#include <stdint.h>
#include <stdbool.h>

typedef struct statistics{
  uint64_t Ia; //min
  uint64_t Ua;  //max
  uint64_t Fa; //plithos
  uint64_t Da; //distinct
  bool *Da_array; 
  int max_case;

}statistics;

typedef struct statistics_array{
  statistics *stats;
  int columns;
  int tuples;
}statistics_array;

#endif
