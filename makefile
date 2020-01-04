main: main.o function.o sort_join.o join_list.o mid_list.o HashTable.o
	gcc -O2 main.o function.o sort_join.o join_list.o mid_list.o HashTable.o -o main

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

clean:
	rm -f main main.o function.o sort_join.o join_list.o mid_list.o HashTable.o

run:
	./main -D files -F small.init -Q files/small.work

run2:
	./main -D /tmp/workloads/medium/ -F medium.init -Q /tmp/workloads/medium/medium.work
