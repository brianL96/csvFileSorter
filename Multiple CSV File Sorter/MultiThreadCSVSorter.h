#ifndef DIR_H
#define DIR_H


	void *getReadDirPtr(char *inputDirectory, char *column);
	void *readDirectory(void *arguments);
	int checkFileName(char *fileName);
	int findColumnArg(int numArg, char **args);
	int findInputArg(int numArg, char **args);
	int findOutputArg(int numArg, char **args);
	int checkSortedFileName(char *fileName);
	int validateDirectoryPath(char *directoryPath, char *validPath);
	void createThread(int threadFuntion, void* args);
	void *fileEncounter(void *arguments); 
	void *getProcessFilePtr(char *fileName, char *columnToSort, char *pureFileName);
	int sortBigListEntries();
	int writeBigListEntries();

#endif 
