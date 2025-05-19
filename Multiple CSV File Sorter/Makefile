CCFLAGS = -Wall -Werror -fsanitize=address -lpthread

compile: multiThreadCSVSorter.c csvFileParser.c mergesort.c helperFunctions.c MultiThreadCSVSorter.h Mergesort.h HelperFunctions.h
	gcc $(CCFLAGS) multiThreadCSVSorter.c csvFileParser.c mergesort.c helperFunctions.c -o csvThreadSorter
clean:
	rm -rf csvThreadSorter
