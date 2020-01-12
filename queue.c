#include "queue.h"
#include <stdio.h>



void create_queue(int size){
     queue_size = size; 
     queue_head = 0;
     queue_tail = 0;

     queue = malloc(size * sizeof(queueElement));

 }



 void remove_queue(void){
     free(queue);
 }



 int queue_empty(void){
     if(queue_head == queue_tail) return 1;
     else return 0;
 }



 int queue_full(void){
     if(queue_head == ((queue_tail + 1) % queue_size)) return 1;
     else return 0;
 }



 void add_queue(void) {
     queue_tail = (queue_tail + 1) % queue_size;
 }



 void delete_queue(void){
     queue_head = (queue_head + 1) % queue_size;
 }