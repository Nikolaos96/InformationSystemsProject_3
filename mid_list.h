#ifndef __MID_LIST__
#define __MID_LIST__
#include <stdint.h>

typedef struct main_node *  main_pointer;
typedef struct node      *  node_pointer;



main_pointer MID_dimiourgia(main_pointer *lista, int col, int r1, int c1, int r2, int c2, int r3, int c3, int r4, int c4);

void eisagogi_komvou_mid(main_pointer* linfo);
void eisagogi_eggrafis_mid(main_pointer* linfo, uint64_t a);

int take_crowd_results_mid(main_pointer *linfo);

int take_columns(main_pointer *linfo);
int take_relation(main_pointer *linfo, int r);
int take_col(main_pointer *linfo, int r);

uint64_t take_rowid(main_pointer *linfo, int r);
//void emfanisi_mid(main_pointer* linfo);


void lista_diagrafi_mid(main_pointer* linfo);
#endif
