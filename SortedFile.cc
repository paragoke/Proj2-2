#include <fstream>
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
	bq = null;
	readPageBuffer = new Page();
	tobeMerged = new Page();
	m = R;
}

int SortedFile::Create (char *f_path, fType f_type, void *startup) {	// done
	file->Open(0,f_path);		
	// use startup to get runlength and ordermaker
	si = (SortInfo *) startup;
	pageIndex=1;
	endOfFile=1;
	return 1;
}

void SortedFile::Load (Schema &f_schema, char *loadpath) {		// requires BigQ instance done
	
	if(m!=Mode.W){
		m = Mode.W;
		// create input, output pipe and BigQ instance
		if(inPipe!=null)inPipe = new Pipe(100);	// requires size ?
		if(outPipe!=null)outPipe = new Pipe(100);
		if(bq!=null)bq = new BigQ(inPipe,outPipe,si->myOrder,si->runlength);
	}
	
	FILE* tableFile = fopen (loadpath,"r");
	Record temp;//need reference see below, make a record

	while(temp.SuckNextRecord(&f_schema,tableFile)!=0)
		inPipe->Insert(&temp);	// pipe blocks and record is consumed or is buffering required ?

	fclose(tableFile);	
}

int SortedFile::Open (char *f_path) {
	char *fName = new char[20];
	sprintf(fileHeaderName, "%s.meta", fileName);
	FILE *f = fopen(fileHeaderName,"r");
	
	// to decide what to store in meta file
	// and parse and get sort order and run length
	// requires some kind of de serialization
	// initialize it
	
	ifstream ifs(filename.meta,ios::binary);
	
	ifs.read((char*)&si, sizeof(si)); 
	
	m = Mode.R;
	
	MoveFirst();
	// set to read mode
	// bring first page into read buffer and initialize first record
	
	fclose(f);
	
	file->Open(1, fpath);	// open the corresponding file
	pageIndex = 1;
	endOfFile = 0;
}

void SortedFile::MoveFirst () {			// requires MergeFromOuputPipe()
	
	if(m==Mode.R){
		// In read mode, so direct movefirst is possible
		file->GetPage(readPageBuffer,1); //TODO: check off_t type,  void GetPage (Page *putItHere, off_t whichPage)
		readPage->GetFirst(current);
	}
	else{
		// change mode to read
		m = Mode.R;
		// Merge contents if any from BigQ
		MergeFromOutpipe();
		file->GetPage(readPageBuffer,1); //TODO: check off_t type,  void GetPage (Page *putItHere, off_t whichPage)
		readPage->GetFirst(current);
		// bring the first page into readPageBuffer
		// Set curr Record to first record
		
	}
	
}

int SortedFile::Close () {			// requires MergeFromOuputPipe()	done
	file->Close();
	endOfFile = 1;
	// write updated state to meta file
	ofstream ofs(fileName.meta,ios::binary);
	
	ofs.write((char*) &si,sizeof(si));	
	
}

void SortedFile::Add (Record &rec) {	// requires BigQ instance		done
	
	if(m!=Mode.W){
		m = Mode.W;
		// create input, output pipe and BigQ instance
		if(inPipe!=null)inPipe = new Pipe(100);	// requires size ?
		if(outPipe!=null)outPipe = new Pipe(100);
		if(bq!=null)bq = new BigQ(inPipe,outPipe,si->myOrder,si->runlength);
	}
	inPipe->Insert(&rec);	// pipe blocks and record is consumed or is buffering required ?
	
}

int SortedFile::GetNext (Record &fetchme) {		// requires MergeFromOuputPipe()		done
	
	if(m!=Mode.R){
	
		m = Mode.R;
		readPageBuffer->EmptyItOut();		// requires flush
		MergeFromOutpipe();		// 
		MoveFirst();	// always start from first record
	
	}
	
	if(endofFile==1) return 0;
	while(!readPageBuffer->GetFirst(&fetchme)) {
		
		if(pageIndex>=this->file->GetLength()-1){
				endOfFile = 1;	
		}
		else {
			pageIndex++;
			file->GetPage(readPageBuffer,pageIndex);
			readPageBuffer->GetFirst(current);
			
		}
	}
	return 1;
}

int SortedFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {		// requires binary search // requires MergeFromOuputPipe()
	MoveFirst();
	
	// sort order matches cnf
	// binary search

}

void SortedFile:: MergeFromOutpipe(){		// requires both read and write modes
	
	// close input pipe
	inPipe.ShutDown();
	// get sorted records from output pipe
	CompareEngine *ce;
	
	// following four lines get the first record from those already present (not done)
	if(!tobeMerged){ tobeMerged = new Page(); }
	pagePtrForMerge = 0; 
	Record *rFromFile = new Record();
	GetNew(rFromFile);						// loads the first record from existing records
	
	Record *rtemp = new Record();		
	Page *ptowrite = new Page();			// new page that would be added
	File *newfile = new File();				// new file after merging
	newfile->Open(0,"mergedFile");				
	
	boolean nomore = false;

	while(true){
		
		if(outPipe.Remove(rtemp)==1){		// got the record from out pipe
			
			while(ce->Compare(rFromFile,rtemp,si->myOrder)<=0){ 		// merging this record with others
				
				if(ptowrite->Append(rFromFile)!=1){		// merge already existing record
						// page full
						int pageIndex = newFile->GetLength()==0? 0:newFile->GetLength()-1;
						//*
						// write this page to file
						newFile->Add(ptowrite,pageIndex);
						// empty this out
						ptowrite->EmptyItOut();
						// append the current record ?
						ptowrite->Append(rtemp);		// does this consume the record ?
				}
				
				if(!GetNew(rFromFile)){ nomore = true; break; }	// bring next rFromFile record ?// check if records already present are exhausted

			}
			if(ptowrite->Append(rtemp)!=1){				// copy record from pipe
						// page full
						int pageIndex = newFile->GetLength()==0? 0:newFile->GetLength()-1;
						//*
						// write this page to file
						newFile->Add(ptowrite,pageIndex);
						// empty this out
						ptowrite->EmptyItOut();
						// append the current record ?
						ptowrite->Append(temp);		// does this consume the record ?
			}
		
		}
		else{
			// pipe is empty now, copy rest of records to new file
			do{
				if(ptowrite->Append(rFromFile)!=1){			
					
					int pageIndex = newFile->GetLength()==0? 0:newFile->GetLength()-1;	// page full
					//*
					// write this page to file
					newFile->Add(ptowrite,pageIndex);
					// empty this out
					ptowrite->EmptyItOut();
					// append the current record ?
					ptowrite->Append(rFromFile);		// does this consume the record ?
				}
			}while(GetNew(rFromFile)!=0);
			break;
		}
	}
	if(nomore==true){									// file is empty
		do{
			if(ptowrite->Append(rtemp)!=1){				// copy record from pipe
						int pageIndex = newFile->GetLength()==0? 0:newFile->GetLength()-1;		// page full
						// write this page to file
						newFile->Add(ptowrite,pageIndex);
						// empty this out
						ptowrite->EmptyItOut();
						// append the current record ?
						ptowrite->Append(temp);		// does this consume the record ?
			}
		}while(outPipe.remove(rtemp)!=0);
	}
	
	newfile->Add(ptowrite,newfile->GetLength()-1);

	newfile->Close();
	
	myFile->Close();
	// delete resources that are not required
	
	if(rename("mergefile.tmp", fileName)) {				// making merged file the new file
		cerr <<"rename file error!"<<endl;
		return;
	}
	readerPageBuffer->EmptyItOut();
	
	myFile->Open(1, this->fileName);
	
}

SortedFile::~SortedFile() {
	delete readerPageBuffer;
	delete inPipe;
	delete outPipe;
}

int SortedFile:: GetNew(Record *r1){

	while(!this->tobeMerged->GetFirst(r1)) {
		if(pagePtrForMerge >= file->GetLength()-1)
			return 0;
		else {
			file->GetPage(tobeMerged, pagePtrForMerge);
			pagePtrForMerge++;
		}
	}
	
	return 1;
}