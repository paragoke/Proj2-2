#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
//#include "DBFile.h"


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
