#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "SortedFile.h"
#include "Defs.h"

SortedFile::SortedFile () {
	inPipe = null;
	outPipe = null;
	readPageBuffer = new Page();
	pageToBeMerged = new Page();
	bq = null;
}

int SortedFile::Create (char *f_path, fType f_type, void *startup) {

	this->file->Open(0,f_path);
	
	return 1;

}

void SortedFile::Load (Schema &f_schema, char *loadpath) {

}

int SortedFile::Open (char *f_path) {
	char *fName = new char[20];
	sprintf(fileHeaderName, "%s.meta", fileName);
	FILE *f = fopen(fileHeaderName,"r");
	
	// to decide what to store in meta file
	// and parse it
	
	fclose(f);
	file->Open(1, fpath);
	
}

void SortedFile::MoveFirst () {

}

int SortedFile::Close () {
}

void SortedFile::Add (Record &rec) {
}

int SortedFile::GetNext (Record &fetchme) {
	return 0;
}

int SortedFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
}


Sorted::~Sorted() {
	delete readerPageBuffer;
	delete inPipe;
	delete outPipe;
}