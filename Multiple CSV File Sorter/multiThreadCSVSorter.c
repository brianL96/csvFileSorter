#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<unistd.h>
#include<sys/wait.h>
#include<dirent.h>
#include<pthread.h>
#include"MultiThreadCSVSorter.h"
#include"Mergesort.h"

	int maxNumThreads = 256;
	int currentTID = 0;
	int numThreadsCreated = 0;

	char *outputDirectory;
	char *columnValue;

	pthread_t tid[256];

	pthread_mutex_t lock;
	pthread_mutex_t headerLock;

	NodeListRoot *listRoot;


	int main(int argc, char **argv){

		int pathSize = 1024;
		char *inputDirPath = (char*)malloc(sizeof(char) * pathSize);
		int columnIndex = -1;
		int sortDirIndex = -1;
		int outputDirIndex = -1;
		int parentPID = getpid();

		int threadIndex = 0;

		int numSearchThreads = 0;
		int numSortThreads = 0;
		int numWriteThreads = 0;
	
		void *args;

		pthread_attr_t threadAttr;
		pthread_attr_init(&threadAttr);
		outputDirectory = (char*)malloc(sizeof(char) * pathSize);

		//check to make sure we have minimum # of arguments
		if(argc < 3){
			printf("ERROR Not enough arguments, -c followed by a column name to sort are necessary\n");
			free(inputDirPath);
			free(outputDirectory);
			return 0;
		}

		//find the the index of the column flag
		columnIndex = findColumnArg(argc, argv);

		if(columnIndex == -1){
			printf("ERROR: There must be a -c argument followed by a column name to sort on\n");
			free(inputDirPath);
			free(outputDirectory);
			return 0;
		}

		//the column flag can't be the last argument
		if( (columnIndex + 1) == argc){
			printf("ERROR There must be a column name following -c\n");
			free(inputDirPath);
			free(outputDirectory);
			return 0;
		}

		//neither the input nor the output flag can follow the column flag
		if(  (strcmp(argv[columnIndex + 1], "-d") == 0) || (strcmp(argv[columnIndex + 1], "-o") == 0) ){
			printf("ERROR: There must be a valid column name following the -c flag. The flags -d or -o are not valid\n");
			free(inputDirPath);
			free(outputDirectory);
			return 0;
		}
	
		columnIndex++;

		sortDirIndex = findInputArg(argc, argv);

		if ( sortDirIndex == -1){
			//Sort Default Directory
			getcwd(inputDirPath, sizeof(char) * pathSize);
		}

		else{
			if( validateDirectoryPath(argv[sortDirIndex + 1], inputDirPath) == 0){
				free(inputDirPath);
				free(outputDirectory);
				return 0;
			}
			
		}
	
		outputDirIndex = findOutputArg(argc, argv);

		if(outputDirIndex == -1){		
			//Output files into default directory
			getcwd(outputDirectory, sizeof(char) * pathSize);
		}
	
		else if( validateDirectoryPath(argv[outputDirIndex + 1], outputDirectory) == 0){
			free(inputDirPath);
			free(outputDirectory);	
			return 0;
		}

		columnValue = (char*)malloc(sizeof(char) * (strlen(argv[columnIndex]) + 1) );
		strcpy(columnValue, argv[columnIndex]);

		listRoot = (NodeListRoot*)malloc(sizeof(NodeListRoot)  * 1);
		listRoot->root = NULL;		
	
		pthread_mutex_init(&lock, NULL);

		pthread_mutex_init(&headerLock, NULL);

		args = getReadDirPtr(inputDirPath, argv[columnIndex]);
		
		printf("Initial PID: %d\n", parentPID);
		printf("TIDs of Spawned Threads:\n");

		createThread(1, args);

		for(threadIndex = 0; threadIndex < numThreadsCreated; threadIndex++){
			pthread_join(tid[threadIndex], NULL);
		}


		numSearchThreads = numThreadsCreated;
		printf("Number of threads for finding and reading files: %d\n", numSearchThreads);

		numSortThreads = sortBigListEntries();
		printf("Number of threads for sorting: %d\n", numSortThreads);

		numWriteThreads = writeBigListEntries();
		printf("Number of threads for writing to file(s): %d\n", numWriteThreads);

		printf("Total number of spawned threads: %d\n", numSearchThreads + numSortThreads + numWriteThreads);

		freeBigList();
		free(listRoot);
		return 1;
	}

	int findColumnArg(int numArg, char **args){

		// returns the index of the -c argument
		// returns -1 if no -c argument

		int index = 1;

		while(index < numArg){
		
			if( strcmp(args[index], "-c" ) == 0){
				return index;
			}

			index++;
		}

		return -1;
	}
	
	int findInputArg(int numArg, char **args){

		//returns the index of the -d argument if there
		//is a valid input directory argument
		//returns -1 if no valid input directory argument

		int index = 1;
		int found = -1;
		
		while(index < numArg){

			if( strcmp(args[index], "-d" ) == 0){
				found = index;
				break;
			}
			index++;
		}

		if(found == -1){
			return -1;
		}

		if( (found + 1) == numArg ){
			return -1;
		}

		if( (strcmp(args[found + 1], "-c") == 0) || (strcmp(args[found + 1], "-o") == 0) ){
			return -1;
		}

		return found;
	}

	int findOutputArg(int numArg, char **args){

		//returns the index of the -o argument
		//if there is a valid output directory argument
		//returns -1 if no valid output directory argument

		int index = 1;
		int found = -1;
		
		while(index < numArg){
			
			if( strcmp(args[index], "-o" ) == 0){
				found = index;
				break;
			}
			index++;
		}
		
		if(found == -1){
			return -1;
		}
		
		if( (found + 1) == numArg ){
			return -1;
		}

		if( (strcmp(args[found + 1], "-c") == 0) || (strcmp(args[found + 1], "-d") == 0) ) {
			return -1;
		}

		return found;

	}

	


	void *readDirectory(void *arguments){

		char **args = (char**)arguments;
		char *inputDirectory = args[0];
		char *column = args[1];
	
		struct dirent *de;
		DIR *dr = opendir(inputDirectory);
		char newPath[1024];

		void *argsPTR;

		printf("TID: %lu reads directory: %s\n", pthread_self(), inputDirectory);

		pthread_attr_t threadAttr;
		pthread_attr_init(&threadAttr);
		
		if(dr == NULL){
			fprintf(stderr, "ERROR Could not open this directory: %s\n", inputDirectory);
			free(inputDirectory);
			free(column);
			return (void*)0;
		}

		//read first two entries in a directory
		de = readdir(dr);
		de = readdir(dr);	
	
		while( (de = readdir(dr)) != NULL){
			
			//directory entry is a directory
			if(de->d_type == DT_DIR){

				strcpy(newPath, inputDirectory);
				strcat(newPath, "/");
				strcat(newPath, de->d_name);

				argsPTR = getReadDirPtr(newPath, column);
	
				createThread(1, argsPTR);
	
			}

			//directory entry is a file
			else if(de->d_type == DT_REG){


				strcpy(newPath, inputDirectory);
				strcat(newPath, "/");
				strcat(newPath, de->d_name);

				argsPTR = getProcessFilePtr(newPath, column, de->d_name);
						
				createThread(2, argsPTR);
								
			}		
		
		}

		closedir(dr);
		
		free(inputDirectory);
		free(column);

		return (void*)1; 
	} 


	int checkFileName(char *fileName){

		//checkFileName returns 1 if fileName has a .csv extension
		//returns 0 otherwise

		int length = strlen(fileName);

		if(length < 5){
			return 0;
		}
	
		if( (fileName[length - 4] != '.')  || (fileName[length - 3] != 'c') || (fileName[length - 2] != 's') || (fileName[length - 1] != 'v') ){
			return 0;
		}
	
		return 1;
	}

	void *fileEncounter(void *arguments){
	
		char **args = (char**)arguments;
		char *fileName = args[2];
	
		printf("TID : %lu; encounters file: %s\n", pthread_self(), fileName);
	
		if(checkFileName(fileName) != 1){
			fprintf(stderr, "ERROR This file is not a .csv file: %s\n", fileName);
			free(args[0]);
			free(args[1]);
			free(args[2]);
			return (void*)0;
		}
		
		if( checkSortedFileName(fileName) == 1){
			fprintf(stderr, "This file is already sorted: %s\n", fileName);
			free(args[0]);
			free(args[1]);
			free(args[2]);
			return (void*)0;
		}

		processFile(arguments);

		free(args[0]);
		free(args[1]);
		free(args[2]);

		return (void*)1;

			
	}

	int checkSortedFileName(char *fileName){

		//returns 1 if file name begins with "AllFiles-sorted-" : file is a sorted aggragate file
		//returns 0 otherwise
		
		int nameLength = strlen(fileName);
		char strCheck[] = "AllFiles-sorted-";
		int checkLength = strlen(strCheck);
		int index = 0;
		
		if( nameLength < 20){
			return 0;
		}
		
		for(index = 0; index < checkLength; index++){
			
			if(fileName[index] != strCheck[index]){
				return 0;
			}	
			
		}	

		return 1;
	} 

	
	int validateDirectoryPath(char *directoryPath, char *validPath){
	
		//accepts a relative or absolute path passed by user and a char pointer
		//returns 1 if directoryPath is valid and writes absolute path into validPath
		//returns 0 if directoryPath is not valid

		DIR *dr;
		dr = opendir(directoryPath);

		if(dr != NULL){
			//Absolute Path Passed
			closedir(dr);
			strcpy(validPath, directoryPath);
			return 1;
		}

		else if (dr == NULL){
			//Relative Path Passed
			closedir(dr);

			getcwd(validPath, sizeof(validPath));
			strcat(validPath, "/");
			strcat(validPath, directoryPath);

			dr = opendir(validPath);

			if(dr == NULL){
				printf("ERROR Neither Relative OR Absolute Path Passed\n");
				return 0;
			}		
			
			closedir(dr);		
		}
		

		return 1;

	}

	

	void *getReadDirPtr(char *inputDirectory, char *column){

		char **arguments = (char**)malloc(sizeof(char*) * 2);
	
		arguments[0] = (char*)malloc( (strlen(inputDirectory) + 1) * sizeof(char) );
		arguments[1] = (char*)malloc( (strlen(column) + 1) * sizeof(char) );

		strcpy(arguments[0], inputDirectory);
		strcpy(arguments[1], column);

		return (void*)arguments;	

	}


	void *getProcessFilePtr(char *fileName, char *columnToSort, char *pureFileName){

		char **arguments = (char**)malloc(sizeof(char*) * 3);

		arguments[0] = (char*)malloc(sizeof(char) * (strlen(fileName) + 1) );
		arguments[1] = (char*)malloc(sizeof(char) * (strlen(columnToSort) + 1));
		arguments[2] = (char*)malloc(sizeof(char) * (strlen(pureFileName) + 1));

		strcpy(arguments[0], fileName);
		strcpy(arguments[1], columnToSort);
		strcpy(arguments[2], pureFileName);

		return (void*)arguments;

	}

	void createThread(int threadFunction, void *args){

		
		pthread_mutex_lock(&lock);

			pthread_attr_t threadAttr;
			pthread_attr_init(&threadAttr);

			if(threadFunction == 1){

				pthread_create(&tid[currentTID], &threadAttr, readDirectory, args);
			}
			else if(threadFunction == 2){
				
				pthread_create(&tid[currentTID], &threadAttr, fileEncounter, args);
			}

			currentTID++;
			numThreadsCreated++;

		pthread_mutex_unlock(&lock);
	}


	int sortBigListEntries(){

		int threadIndex = 0;
		pthread_attr_t threadAttr;
		pthread_attr_init(&threadAttr);
	
		currentTID = 0;
		numThreadsCreated = 0;

		NodeList *ptr = listRoot->root;

		while(ptr != NULL){

			ptr->last = NULL;
			pthread_create(&tid[currentTID], &threadAttr, sortList, (void*)ptr);
			currentTID++;
			numThreadsCreated++;
			ptr = ptr->next;
		}

		for(threadIndex = 0; threadIndex < numThreadsCreated; threadIndex++){
			pthread_join(tid[threadIndex], NULL);
		}

		return numThreadsCreated;
		
	}


	int writeBigListEntries(){

		//spawns a thread to write each NodeList (each seperate header)
		//and all the nodes pointed to by NodeList->first to proper file

		int threadIndex = 0;
		pthread_attr_t threadAttr;
		pthread_attr_init(&threadAttr);

		currentTID = 0;
		numThreadsCreated = 0;

		NodeList *ptr = listRoot->root;
		
		//no csv files sorted
		if(ptr == NULL){
			return 0;
		}
		
		//one entry only: all files sorted had same header
		if(ptr->next == NULL){
			ptr->indexInList = -1;
		}

		while(ptr != NULL){

			pthread_create(&tid[currentTID], &threadAttr, writeSortedCSV, (void*)ptr); 
			currentTID++;
			numThreadsCreated++;
			ptr = ptr->next;
		}

		for(threadIndex = 0; threadIndex < numThreadsCreated; threadIndex++){
			pthread_join(tid[threadIndex], NULL);
		}

		return numThreadsCreated;
		
	}





