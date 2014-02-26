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
	m = R;
}

int SortedFile::Create (char *f_path, fType f_type, void *startup) {

	file->Open(0,f_path);		
	// use startup to get runlength and ordermaker
	si = (SortInfo *) startup;
	return 1;

}

void SortedFile::Load (Schema &f_schema, char *loadpath) {		// requires BigQ instance
	
	if(m!=Mode.W){
		m = Mode.W;
		// create input, output pipe and BigQ instance
		if(inPipe!=null)inPipe = new Pipe();	// requires size ?
		if(outPipe!=null)outPipe = new Pipe();
		if(bq!=null)bq = new BigQ(inPipe,outPipe,si->myOrder,si->runlength);
	}
	
	FILE* tableFile = fopen (loadpath,"r");
	Record temp;//need reference see below, make a record

	while(temp.SuckNextRecord(&f_schema,tableFile)!=0)
		inPipe(&temp);

	fclose(tableFile);	
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
	
	if(m==Mode.R){
		// In read mode, so direct movefirst is possible
		file->GetPage(readPageBuffer,1); //TODO: check off_t type,  void GetPage (Page *putItHere, off_t whichPage)
		readPage->GetFirst(current);
	}
	else{
		// change mode to read
		
		// Merge contents if any from BigQ
		
		// bring the first page into readPageBuffer
		// Set curr Record to first record
		// 
	}
	
}

int SortedFile::Close () {
	file->Close();
	// write updated state to meta file
}

void SortedFile::Add (Record &rec) {	// requires BigQ instance
	
	if(m!=Mode.W){
		m = Mode.W;
		// create input, output pipe and BigQ instance
		if(inPipe!=null)inPipe = new Pipe();	// requires size ?
		if(outPipe!=null)outPipe = new Pipe();
		if(bq!=null)bq = new BigQ(inPipe,outPipe,si->myOrder,si->runlength);
	}
	inPipe->Insert(&rec);	// pipe blocks and record is consumed
	
}

int SortedFile::GetNext (Record &fetchme) {
	return 0;
}

int SortedFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
}

void SortedFile:: MergeFromOutpipe(){
	
	//get records from BigQ

	//Merge those with already present
	
	

}

Sorted::~Sorted() {
	delete readerPageBuffer;
	delete inPipe;
	delete outPipe;
}