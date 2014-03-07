
#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "BigQ.h"
//#include "GenericDBFile.h"
//#include "HeapFile.h"
//#include "SortedFile.h"



// stub DBFile header..replace it with your own DBFile.h 

enum fType {heap, sorted, tree} ;

class GenericDBFile{

protected:
	
	File *myFile;
	char *fileName;

public:
	
	//GenericDBFile();
	
	virtual int Create (char *fpath, fType file_type, void *startup) = 0;
	virtual int Open (char *fpath) = 0;
	virtual int Close () = 0;

	virtual void Load (Schema &myschema, char *loadpath) = 0;

	virtual void MoveFirst () = 0;
	virtual void Add (Record &addme) = 0;
	virtual int GetNext (Record &fetchme) = 0;
	virtual int GetNext (Record &fetchme, CNF &cnf, Record &literal) = 0;
	
	//`virtual ~GenericDBFile();

};




class DBFile {

private :
	GenericDBFile *gdbf;	

public:
	DBFile (); 
	

	int Create (char *fpath, fType file_type, void *startup);
	int Open (char *fpath);
	int Close ();

	void Load (Schema &myschema, char *loadpath);

	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);

};



class HeapFile : public GenericDBFile{

	char *file_path;
	Record* current;
	Page* readPage;
	Page* writePage;
	File* file;
	off_t pageIndex;
	off_t writeIndex;
	char* name;
	int writeIsDirty;
	int endOfFile;

public:
	HeapFile (); 
	~HeapFile();

	int Create (char *fpath, fType file_type, void *startup);
	int Open (char *fpath);
	int Close ();

	void Load (Schema &myschema, char *loadpath);

	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);


};





struct SortInfo { 
	OrderMaker *myOrder; 
	int runLength; 
}; 

enum Mode {R, W};

class SortedFile: public GenericDBFile{

private:
	Pipe *inPipe;
	Pipe *outPipe;
	BigQ *bq;
	Page *readPageBuffer;
	Page *tobeMerged;
	int pagePtrForMerge;
	File* file;
	Mode m;
	SortInfo *si;
	Record* current;
	off_t pageIndex;
	off_t writeIndex;
	int endOfFile;

public:
	SortedFile (); 

	int Create (char *fpath, fType file_type, void *startup);
	int Open (char *fpath);
	int Close ();

	void Load (Schema &myschema, char *loadpath);

	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);

	void MergeFromOutpipe();
	int GetNew(Record *r1);	

	~SortedFile();
};
