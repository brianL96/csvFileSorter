#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<unistd.h>
#include<ctype.h>
#include<pthread.h>
#include "Mergesort.h"
#include "MultiThreadCSVSorter.h"
#include "HelperFunctions.h"


	extern NodeListRoot *listRoot;
	extern pthread_mutex_t headerLock;
	
	extern char *outputDirectory;
	extern char *columnValue;
	
	pthread_mutex_t bigListLocks[256];

	int keyType(char *key, int keyLength){
		//returns 1 if its a string
		//returns 0 if its a number

		int index = 0;

		if( (keyLength > 1) && (key[index] == '+' || key[index] == '-') ){

			index = 1;
		}

		while(index < keyLength){

			if(isdigit(key[index]) == 0){
				return 1;
			}
			index++;
		}

		return 0;
	}


	void getKey(Node *node, int keyCol){

		//getKey takes a node and traverses its record until 
		//it gets to the column used for sorting and writes 
		//that key to node->key

		int length = strlen(node->record);
		node->key = (char*)malloc(sizeof(char) * (length + 1));

		int currentCol = 0;
		int index = 0;
		int wordIndex = 0;
	
		while(currentCol < keyCol && index < length){

			//a comma can exist in between quotation marks
			//and not be a 'column seperating comma, so
			//when an opening quotation mark is found code 
			//must traverse to the next closing quotation mark

			if(node->record[index] == '"'){

				index++;

				while( (index < length) && (node->record[index] != '"') ){

					index++;
				}

				index++;
			}

			if(node->record[index] == ','){
				currentCol++;
			}

			index++;
		
		}


		//at this point we are at the key column
		//and code must read in the characters until either
		//the end of the line(record) or we get to the next column
		//seperating comma

		while( (node->record[index] != ',') && index < length){

			if(node->record[index] == '"'){

			
				index++;
			

				if(index >= length){
					break;
				}

				while( (node->record[index] != '"') && index < length){
			
					if(node->record[index] == ' '){
						index++;
						continue;
					}
	
					node->key[wordIndex] = node->record[index];
					index++;
					wordIndex++;
				}

		

				if(index >= length){
					break;
				}

			}

			if(node->record[index] == '"' || node->record[index] == ' '){
		
				index++;
				continue;
			}


			node->key[wordIndex] = node->record[index];
			index++;
			wordIndex++;
		}

		node->key[wordIndex] = '\0';


		//after we get the key, must either set it
		//as a numeric type or as a word for sorting purposes

		if(keyType(node->key, strlen(node->key) ) == 0){

			node->keyNum = atoi(node->key);
			node->type = 0;
		
		
		}

		else{
			node->keyNum = -1;
			node->type = 1;
		
		}

	



	}


	int findKeyColumn(Node *node, char *header){

		//function finds and returns the location of the column to sort
		//returns -1 if column to sort does not exist in header

		int start = 0;
		int end = 0;
		int length = strlen(node->record);
		char *word;
		int size;
		int wordIndex = 0;
		int keyColumn = 0;
	

		while( end < length){

			while( (node->record[end] != ',')  && end < length){
				end++;
			}

			size = end - start;
			word = (char*)malloc(sizeof(char) * (size + 1));
			wordIndex = 0;

			while(wordIndex < size){

				word[wordIndex] = node->record[start];
				start++;
				wordIndex++;
			}
		
			word[wordIndex] = '\0';

			if(strcmp(header, word) == 0){
				free(word);
				return keyColumn;
			}

			end++;
			start = end;
			free(word);
			keyColumn++;
				
		
		}

		return -1;

	}

	void countColumns(Node *node){

		//countColumns counts the # of column seperating commas
		//and sets the number of columns for the node passed in

		int length = strlen(node->record);
		int numColumns = 1;
	
		int index = 0;

		while( index < length) {

			if(node->record[index] == ','){
				numColumns++;
			}

			//if code runs into opening quotation mark
			//code will suspend counting until it
			//gets to the closing quotation mark

			if(node->record[index] == '"'){

				index++;

				while( (index < length) && (node->record[index] != '"') ){

					index++;
				}
			}

			index++;
		}	

		node->colnum = numColumns;

	}


	Node* getRecord(FILE *fptr, Node *front, char *header){


		Node *node;
		node = (Node*)malloc(sizeof(Node) * 1); 
		node->record = NULL;
		node->key = NULL;
		node->next = NULL;
		int c;
		char* line;
		int size = 10;
		int index = 0;
		int num = 0;

				
		c = fgetc(fptr);

		//need to consume space in between records
		if((char)c == '\n' || (char)c == '\r'){

			c = fgetc(fptr);

			while ( ((char)c == '\n' || (char)c == '\r') && feof(fptr) == 0){

				c = fgetc(fptr);
			}
		}
	
	
		//record is empty
		if( feof(fptr) != 0){
			free(node);
			return NULL;
		}


		node->record = (char*)malloc(sizeof(char) * size);

		while( feof(fptr) == 0 && (char)c != '\r' && (char)c != '\n'){

			node->record[index] = (char)c;
			index++;
			num++;
			c = fgetc(fptr);

			//need to allocate additional space to finish reading record
			//realloc is not used

			if(num == size){

				size = size + 5;
				line = (char*)malloc(sizeof(char) * size);
				memcpy(line, node->record, num);
				free(node->record);

				node->record = (char*)malloc(sizeof(char) * size);
				memcpy(node->record, line, num);
				free(line);
			
			}
		

		}

		node->record[index] = '\0';

		return node;

	
	}

	
	char *createFileName(int fileIndex){
		
		int directoryLength = strlen(outputDirectory);
		int headerLength = strlen(columnValue);
		int numLength = 0;
		int newFileLength = 0;
		char *fileNum;
		char *newFilePath; 

		if(fileIndex > -1){
			fileIndex++;
			numLength = intLength(fileIndex) + 1;
		}

		newFileLength = directoryLength + headerLength + numLength + 22;

		newFilePath = (char*)malloc(sizeof(char) * newFileLength );

		strcpy(newFilePath, outputDirectory);
		strcat(newFilePath, "/");
		strcat(newFilePath, "AllFiles-sorted-"); 
		strcat(newFilePath, columnValue);

		if(fileIndex > -1){

			fileNum = intToString(fileIndex);
			strcat(newFilePath, "-");
			strcat(newFilePath, fileNum);
		}

		strcat(newFilePath, ".csv");

		newFilePath[newFileLength - 1] = '\0';

		return newFilePath;
			
	}


	void *processFile(void *arguments){

		/*
 		* processFile opens up the file pointed to by fileName
		* and will make sure the file is a properly formatted csv file:
		* -the file is not empty
		* -the header of the file contains the columnToSort
		* -all of the subsequent lines have the same number of columns as the header.
		* Will then call findHeaderInBigList() to find NodeList that has same
		* header as this file and then call addNodesToBigList() to add records 
		* under that NodeList
		* Returns 1 on success
		* Returns 0 if file is not properly formatted with an error message
		*/ 

		char **args = (char**)arguments;

		char *fileName = args[0];
		char *columnToSort = args[1];

		NodeList *bigListPtr;			

		FILE *fptr;
		int keyColumn;
		Node *front;
		Node *ptr1;
		front = NULL;
		front = (Node*)malloc(1 * sizeof(Node));
		front->next = NULL;
		front->record = NULL;
		front->key = NULL;

		//open file here, need to check if file opened successfully
		fptr = fopen(fileName, "r");
	
		if(fptr == NULL){
			fclose(fptr);
			fprintf(stderr, "ERROR Cannot open this file: %s\n", fileName);
			free(front);
			return (void*)0;
		}

		//front->next contains the header
		front->next = getRecord(fptr, front, columnToSort);

		if(front->next == NULL){
			fclose(fptr);
			fprintf(stderr, "ERROR This file is empty: %s\n", fileName);
			free(front);
			return (void*)0;
		}
	
	
		//get the # of columns in the header
		countColumns(front->next);

		keyColumn = findKeyColumn(front->next, columnToSort);

		if(keyColumn == -1){
		
			fprintf(stderr, "ERROR Column: %s Does Not Exist In This File: %s\n", columnToSort, fileName);
			fclose(fptr);
			freeNodes(front);
			return (void*)0;
		}


		ptr1 = front->next;

		while(ptr1 != NULL){

			ptr1->next = getRecord(fptr, front, columnToSort);
			ptr1 = ptr1->next;

			if(ptr1 != NULL){
			
				countColumns(ptr1);
				if(front->next->colnum != ptr1->colnum){
				
					fprintf(stderr, "ERROR The record:\n%s\nDoes Not Have The Right Amount of Columns: It Has: %d, The Header Has: %d\n", ptr1->record, ptr1->colnum, front->next->colnum);
					fclose(fptr);
					freeNodes(front);
					return (void*)0;
				}
			}

		}


		fclose(fptr);

		ptr1 = front->next;

		if(ptr1->next != NULL){

			ptr1 = ptr1->next;

			while(ptr1 != NULL){

				getKey(ptr1, keyColumn);
				ptr1 = ptr1->next;
			}
		}

		//front is just a root pointer, front->next points to header
	
		ptr1 = front->next;
		ptr1 = ptr1->next;

		bigListPtr = findHeaderInBigList(front->next->record);

		if(bigListPtr == NULL){
			fprintf(stderr, "ERROR Header not written from file: %s\n", fileName);
			freeNodes(front);
			return (void*)0;
		}

		
		if(ptr1 != NULL){
			addNodesToBigList(bigListPtr, ptr1);
		}

		return (void*)1;
	}


	void *writeSortedCSV(void *arguments){

		//writes sorted data to aggragate file in output directory

		FILE *fptr;
		NodeList *nodeList = (NodeList*)arguments;
		char *filePath = createFileName(nodeList->indexInList);	

		printf("TID: %lu writes file %s\n", pthread_self(), filePath);

		Node *ptr = nodeList->first;

		fptr = fopen(filePath, "w+");

		fputs(nodeList->header, fptr);
		fputs("\n", fptr);

		while(ptr != NULL){

			fputs(ptr->record, fptr);
			fputs("\n", fptr);
			ptr = ptr->next;
		}

		fclose(fptr);
		
		free(filePath);
		return (void*)1;
	}

	void printSortedCSV(){
		//prints all aggragate data under
		//each separate header
		
		NodeList *ptr1 = listRoot->root;
		Node *ptr2;

		while(ptr1 != NULL){
		
			printf("%s\n", ptr1->header);
			
			ptr2 = ptr1->first;
			while(ptr2 != NULL){

				printf("%s\n", ptr2->record);	
				ptr2 = ptr2->next;
			}

			printf("\n");
			ptr1 = ptr1->next;
		}

	}

	void freeBigList(){

		//frees all aggragate data pointed to
		//by listRoot->root

		//no valid records were added
		if(listRoot->root == NULL){
			return;
		}

		NodeList *prev = listRoot->root;
		NodeList *ptr = prev->next;

		while(prev != NULL){
			
			prev->next = NULL;
			prev->last = NULL;

			if(prev->first != NULL){
				freeNodes(prev->first);
				prev->first = NULL;
			}

			free(prev->header);
			free(prev);
			prev = ptr;
			
			if(ptr != NULL){
				ptr = ptr->next;
			}

		}

		
		listRoot->root = NULL;
		
	}

	void freeNodes(Node *front){
		
		//frees a list of nodes 
	
		if(front == NULL){
			return;
		}

		Node *prev = front;
		Node *ptr = front->next;

		while(prev != NULL){

			prev->next = NULL;

			if(prev->record != NULL){
				free(prev->record);
			}

			if(prev->key != NULL){
				free(prev->key);
			}

			free(prev);
			prev = ptr;

			if(ptr != NULL){
				ptr = ptr->next;
			}
		}

	
		
	}

	NodeList *findHeaderInBigList(char *header){

		//find and return NodeList pointed to by listRoot->root that has 
		//same header as the header argument
		//if there is no NodeList with same header the writeHeaderInBigList is called
		
		int headerLocation = 0;

		NodeList *nodeListPtr = listRoot->root;
		NodeList *nodeListPrev = listRoot->root;
		
		if(nodeListPtr == NULL){
			
			writeHeaderInBigList(header, listRoot->root, headerLocation);
		}

		else{
			while(nodeListPtr != NULL){

				if( strcmp(nodeListPtr->header, header) == 0){
				
					return nodeListPtr;
				}

				headerLocation++;
				nodeListPrev = nodeListPtr;
				nodeListPtr = nodeListPtr->next;
			}

			writeHeaderInBigList(header, nodeListPrev, headerLocation);

		}
		
		nodeListPtr = listRoot->root;

		while(nodeListPtr != NULL){
		
			if (strcmp(nodeListPtr->header, header) == 0){
				
				return nodeListPtr;
			}

			nodeListPtr = nodeListPtr->next;
		}

		return NULL;
	}

	void writeHeaderInBigList(char *header, NodeList *prev, int headerLocation){

		//adds a new NodeList to list of NodeLists pointed to by listRoot->root
		pthread_mutex_lock(&headerLock);

			NodeList *ptr;
			NodeList *newNodeList;

			//listRoot->root was NULL when this thread
			//called this function but another thread	
			//has added to the aggragate list of headers		
			if(prev == NULL && listRoot->root != NULL){
				prev = listRoot->root;
				
				if(strcmp(prev->header, header) == 0){
					//same header was written by another thread
					pthread_mutex_unlock(&headerLock);
					return;
				}

				headerLocation++;
				
			}
	
			ptr = prev;

			if(prev != NULL){
	
				ptr = ptr->next;
	
				while(ptr != NULL){
					
					//same header was written by another thread
					if( strcmp(ptr->header, header) == 0){
					
						pthread_mutex_unlock(&headerLock);
						return;
					}

					prev = ptr;
					ptr = ptr->next;
					headerLocation++;
				}
			}

			newNodeList = (NodeList*)malloc(sizeof(NodeList) * 1);
			newNodeList->header = (char*)malloc(sizeof(char) * (strlen(header) + 1));

			strcpy(newNodeList->header, header);
			newNodeList->indexInList = headerLocation;
			newNodeList->next = NULL;
			newNodeList->first = NULL;
			newNodeList->last = NULL;

			pthread_mutex_init(&bigListLocks[headerLocation], NULL);

			if(listRoot->root == NULL){
				listRoot->root = newNodeList;
			}

			else{
				prev->next = newNodeList;
			}
			
		pthread_mutex_unlock(&headerLock);
	}




	void addNodesToBigList(NodeList *bigListEntry, Node *node){

		
		int location = bigListEntry->indexInList;

		pthread_mutex_lock(&bigListLocks[location]);
			
			Node *ptr;
			Node *prev;

			if(bigListEntry->first == NULL){
				bigListEntry->first = node;
			}

			else{
				bigListEntry->last->next = node;
				
			}

			ptr = node;

			prev = ptr;
			ptr = ptr->next;

			while(ptr != NULL){
				prev = ptr;
				ptr = ptr->next;
			}

			bigListEntry->last = prev;
			
		pthread_mutex_unlock(&bigListLocks[location]);
		
	}


















