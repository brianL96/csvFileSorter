#ifndef MERGE_H
#define MERGE_H


	typedef struct NodeInfo Node;

	struct NodeInfo{

		char* record;
		char* key;
		int colnum;
		Node *next;
		int keyNum;
		int type;

	};

	typedef struct nodeList NodeList;
	
	typedef struct nodeListRoot NodeListRoot;

	struct nodeList{
		Node *first;
		Node *last;
		char *header;
		int indexInList;
		NodeList *next;
	};

	struct nodeListRoot{
		NodeList *root;
	};

	void Mergesort(Node **front);
	void *processFile(void *arguments);
	char *createFileName(int fileNum);
	Node* getRecord(FILE *fptr, Node *front, char *header);
	void countColumns(Node *node);
	int findKeyColumn(Node *node, char *header);
	void getKey(Node *node, int keyCol);
	int keyType(char *key, int keyLength);
	void printSortedCSV();
	void *writeSortedCSV(void *arguments);
	NodeList *findHeaderInBigList(char *header);
	void writeHeaderInBigList(char *header, NodeList *prev, int headerLocation);
	void addNodesToBigList(NodeList *bigListEntry, Node *node);
	void *sortList(void *args);
	void freeNodes(Node *front);
	void freeBigList();

#endif
