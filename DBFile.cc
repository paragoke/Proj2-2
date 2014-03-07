
#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"


// DBFILE

DBFile::DBFile () {
	gdbf = null;
}

int DBFile::Create (char *f_path, fType f_type, void *startup) {
	
	if(f_type==heap){
		gdbf = new Heap();
	}
	else{
		gdbf = new SortedFile();
	}

	if(gdbf->Create(fpath, fType, void *startup)){
		
		return 1;
	}
	return 0;
}

void DBFile::Load (Schema &f_schema, char *loadpath) {
	gdbf->Load(f_schema, loadpath);
}

int DBFile::Open (char *f_path) {
	
	char *metaFile = new char[20];
	sprintf(metaFile, "%s.meta", f_path);
	File *f = fopen(metaFile, "r");
	
	fType f;
	char *s1 = new char[30];
	fscanf(f,"%s\n",s1);	// read the file type
	
	f = (fType) s1;
	
	if(f==heap) gdbf =  new Heap();
	else gdbf = new SortedFile();
	
	fclose(f);
	
	return gdbf->Open(f_path);
}

void DBFile::MoveFirst () {
	gdbf->MoveFirst();
}

int DBFile::Close () {
	return gdbf->Close();
}

void DBFile::Add (Record &rec) {
	gdbf->Add(rec);
}

int DBFile::GetNext (Record &fetchme) {
	return gdbf->GetNext(fetchme);
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	return gdbf->GetNext(fetchme, cnf, literal);
}

DBFile::~DBFile(){
	delete gdbf;
}


