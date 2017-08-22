#include <string>

#include "Config.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "DBFile.h"

using namespace std;


DBFile::DBFile () : fileName("") {
	pageNumb = 0;
}

DBFile::~DBFile () {
}

DBFile::DBFile(const DBFile& _copyMe) :
	file(_copyMe.file),	fileName(_copyMe.fileName), pageNumb(0) {}

DBFile& DBFile::operator=(const DBFile& _copyMe) {
	// handle self-assignment first
	if (this == &_copyMe) return *this;

	file = _copyMe.file;
	fileName = _copyMe.fileName;
	pageNumb = _copyMe.pageNumb;

	return *this;
}

int DBFile::Create (char* f_path, FileType f_type) {

	//if (f_type == Heap)
	{	
		ftype = f_type;
		fileName = f_path;
		return (file.Open(0,f_path));
	}
}

int DBFile::Open (char* f_path) {
	fileName = f_path;
	return file.Open(1,f_path);

}

void DBFile::Load (Schema& schema, char* textFile) {

	FILE * pFile;
	string str = textFile;
	pFile = fopen(&str[0],"r");
	int i = 0;
	while (1)
	{
		Record rec;
		if (rec.ExtractNextRecord (schema, *pFile) == 0) break;
		AppendRecord(rec);
	}

	file.AddPage(page, file.GetLength());
	fclose(pFile);
	cout << "Entities read: " << i << " pages: " << file.GetLength() << endl;
}

int DBFile::Close () {

	if(ftype == Sorted || ftype == Index)
	file.AddPage(page, file.GetLength());
	return file.Close();
}

void DBFile::MoveFirst () {
	pageNumb = 0;
	
}

void DBFile::AppendRecord (Record& rec) {

	if (page.Append(rec) == 0)
	{
		file.AddPage(page, file.GetLength());
		page.EmptyItOut();
		page.Append(rec);
		pageNumb++;
	}
}

int DBFile::GetNext (Record& rec) {
	if (page.GetFirst(rec) == 0)
	{
		if (file.GetLength() == pageNumb) return 0;
		if (file.GetPage(page, pageNumb) == -1) return 0;
		page.GetFirst(rec);
		pageNumb++;
		
	}
	return 1;	
}

void DBFile::GetPageNo (int number, Page& indexpage) {
	file.GetPage(indexpage, number);
	}


int DBFile::GetSpecificRecord(int pNumber, int rNumber, Record& rec) { 
	
	page.EmptyItOut(); 
	if (file.GetPage(page, pNumber) == -1) return 0; // return 0 for no page found 
		//page.GetFirst(rec); return 0; 
	if (page.GetRecordNumber(rNumber, rec) != 1) return 0; 
	return 1; // return 1 if record exists 
}


