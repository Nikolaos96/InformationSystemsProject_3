#ifndef __STRUCTS__
#define __STRUCTS__
#include <stdint.h>
#include <stdbool.h>


typedef struct tuple{
        uint64_t key;
        uint64_t payload;
}tuple;

typedef struct relation{
        tuple *tuples;
        uint64_t num_tuples;
}relation;

typedef struct main_array{
        uint64_t num_tuples;
        uint64_t num_columns;
        uint64_t *relation_array;
        int      *index;
}main_array;


typedef struct q{
	bool join; // join = TRUE

	bool flag;
	int relationA;
	int columnA;
	int relationB; 	      // gia ta filtra to   0 - (=)    1 - (>)     2 - (<)
	uint64_t columnB;
}q;

typedef struct checksum_struct{
  int table;
  int row;
}checksum_struct;



#endif
