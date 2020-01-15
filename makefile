main: main.o function.o sort_join.o join_list.o mid_list.o HashTable.o hash_t.o queue.o
	gcc -O2  main.o function.o sort_join.o join_list.o mid_list.o HashTable.o hash_t.o queue.o -o main -pthread -lrt

main.o: main.c
	gcc -O2 -c main.c

function.o: function.c
	gcc -O2 -c function.c

sort_join.o: sort_join.c
	gcc -O2 -c sort_join.c

join_list.o: join_list.c
	gcc -O2 -c join_list.c

mid_list.o: mid_list.c
	gcc -O2 -c mid_list.c

HashTable.o: HashTable.c
	gcc -O2 -c HashTable.c

hash_t.o: hash_t.c
	gcc -O2 -c hash_t.c

queue.o: queue.c
	gcc -O2 -c queue.c

clean:
	rm -f main main.o function.o sort_join.o join_list.o mid_list.o HashTable.o hash_t.o queue.o

run:
	./main -D files -F small.init -Q files/small.work

run2:
	./main -D /tmp/workloads/medium/ -F medium.init -Q /tmp/workloads/medium/medium.work
