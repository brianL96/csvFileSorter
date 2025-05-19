
#include<stdlib.h>
#include<stdio.h>
#include<string.h>
#include<pthread.h>
#include "Mergesort.h"


	void Split(Node *source, Node **frontR, Node **backR){

		Node *fast;
		Node *slow;
		slow = source;
		fast = source->next;

		while(fast != NULL){
	
			fast = fast->next;
			if(fast != NULL){

				slow = slow->next;
				fast = fast->next;

			}
		}

		*frontR = source;
		*backR = slow->next;
		slow->next = NULL;

		

	}


	Node*  sortMerge(Node *a, Node *b){

		Node *result = NULL;
		int strdiff = -1;
	
		if(a == NULL){

			return b;
		}

		else if(b == NULL){
			return a;
		}

		//both a and b are ints
		if(a->type == 0 && b->type == 0){

			if(a->keyNum <= b->keyNum){

				result = a;
				result->next = sortMerge(a->next, b);

			}
			else{

				result = b;
				result->next = sortMerge(a, b->next);

			}
		}

		//if a is an int and b is a string
		if(a->type == 0 && b->type == 1){

			result = a;
			result->next = sortMerge(a->next, b);	
		}

		//if a is a string and b is an int
		if(a->type == 1 && b->type == 0){

			result = b;
			result->next = sortMerge(a, b->next);
		}

		//both a and b are strings
		if(a->type == 1 && b->type == 1){

			strdiff = strcmp(a->key, b->key);
			if(strdiff <= 0){

				result = a;
				result->next = sortMerge(a->next, b);

			}
			else{
				result = b;
				result->next = sortMerge(a, b->next);
			}
		}

		return result;
	}


	void Mergesort(Node **front){

		Node *head;
		head = *front;
		Node *a;
		Node *b;
	
		if(head == NULL ){

			return;
		}	 

		if(head->next == NULL){
		
			return;
		}

		//split head into a and b sublists
		Split(head, &a, &b);

		Mergesort(&a);
		Mergesort(&b);
	
		*front = sortMerge(a, b);
	

	}

	void *sortList(void *args){

		NodeList *nodeList = (NodeList*)args;
		
		printf("TID: %lu sorts records under header: %s\n", pthread_self(), nodeList->header);

		Mergesort(&nodeList->first);

		return (void*)1;
	}







