#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<unistd.h>


	char *intToString(int i){
	
		int base = 1;
		int index = 0;
		int value;
		int size = 1;
		char *intString;

		while( (base * 10) < i){
		
			base = base * 10;
			size++;
		}

		if( (base * 10) == i){
			base = base * 10;
			size++;
		}

		intString = (char*)malloc(sizeof(char) * (size + 1));

		while(size > 0){
			value = i/base;
			intString[index] = value + '0';
			i = i - (value * base);
			base = base/10;
			index++;
			size--;	
		}

		
		intString[index] = '\0';

		return intString;
					

	}


	int intLength(int i){

		int size = 1;
		int base = 1;

		while( (base * 10) < i){
			base = base * 10;
			size++;
		}

		if( (base * 10) == i){
			size++;
		}

		return size;
	}





