#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "Defs.h"
#include "TwoWayList.h"
#include "Record.h"
#include "DBFile.h"
// stub file .. replace it with your own DBFile.cc

DBFile::DBFile () {

}

int DBFile::Create (char *f_path, fType f_type, void *startup) {

	if(f_type==heap){

		this->gdbf= new HeapFile();

	}
	else if(f_type==sorted){

		this->gdbf= new SortedFile();	

	}

	if(gdbf!=NULL){

		gdbf->Create(f_path,f_type,startup);		

	}	

}

void DBFile::Load (Schema &f_schema, char *loadpath) {

	gdbf->Load(f_schema,loadpath);

}

int DBFile::Open (char *f_path) {

	char meta_path[20];

	sprintf(meta_path,"%s.meta",f_path);

	FILE *meta =  fopen(meta_path,"r");;
	char f_type[10];

	fscanf(meta,"%s",f_type);
	//read meta details from file

	if(f_type=="heap"){
	

		HeapFile *h = new HeapFile();
		this->gdbf= h;

	}
	else if(f_type=="sorted"){
	

		SortedFile *s = new SortedFile();
		this->gdbf= s;	

	}

	gdbf->Open(f_path);

	//Note:should we read the order maker at this level or let the sorted file handle it??



}

void DBFile::MoveFirst () {

	gdbf->MoveFirst();

}

int DBFile::Close () {

	return gdbf->Close();

}

void DBFile::Add (Record &rec) {

	return gdbf->Add(rec);

}

int DBFile::GetNext (Record &fetchme) {

	return gdbf->GetNext(fetchme);

}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {

	return gdbf->GetNext(fetchme,cnf,literal);

}
