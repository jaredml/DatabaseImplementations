#include <iostream>
#include <string>

#include "Catalog.h"
//#include "QueryParser.h"
#include "QueryOptimizer.h"
#include "QueryCompiler.h"
#include "RelOp.h"

using namespace std;
extern "C"{
	#include "QueryParser.h"
}
// these data structures hold the result of the parsing
extern struct FuncOperator* finalFunction; // the aggregate function
extern struct TableList* tables; // the list of tables in the query
extern struct AndList* predicate; // the predicate in WHERE
extern struct NameList* groupingAtts; // grouping attributes
extern struct NameList* attsToSelect; // the attributes in SELECT
extern int distinctAtts; // 1 if there is a DISTINCT in a non-aggregate query

extern struct AttList* attsToCreate; // the attributes in CREATE
extern int queryType; // 0 if SELECT, 1 if CREATE TABLE, 2 if LOAD, 3 if CREATE INDEX
extern "C" int yyparse();
extern "C" int yylex_destroy();
int main () {

	string dbFile = "catalog.sqlite";
	Catalog catalog(dbFile);

	DBFile db;

	string filename[] = {"customer", "lineitem", "nation","orders","part","partsupp","region","supplier"};

	vector <string> files;
	vector <string> tablez;
	catalog.GetTables(files);

	for (int i = 0; i< files.size(); i++)
	{
		//cout << files[i] << endl;
		tablez.push_back(files[i]);
		files[i]+= ".tbl";
		files[i].insert(0,"tablez/");
		//cout<<files[i]<<endl;
	}

	//int x = 8;
	while(true)
	{
	for (int i = 0; i < files.size(); i++)
	{
		//cout << &filename[0]<<endl;
		//cout << "Getting Schema from table: "<<tablez[i]<<endl;
		Schema sch;
		catalog.GetSchema(tablez[i],sch);

		//cout << "Creating HeapFile: "<<&filename[i][0]<<endl;
		//db.Create(&filename[i][0],(FileType) Heap);
		
		/*db.Open(&filename[0]);
		Schema sch;
		Page p;
		Record r;
		int records = 0;
		db.setPage();
		while (db.GetNext(r) != 0) records++;

		cout<<"\ntotal rec "<<records;
		cout<<"\ncounting again\n";
		
		records = 0;
		db.MoveFirst();
		db.setPage();
		while (db.GetNext(r) != 0) records++;
		cout<<"\ntotal rec "<<records;*/
		//cout<<"DB Open HeapFile: "<<&filename[i][0]<<endl;
		db.Open(&filename[i][0]);
		//cout<<"DB Load DataFile: "<<&files[i][0]<<endl; 
		//db.Load(sch, &files[i][0]);
		
		//cout<<"New CurrentLngth "<<
		//cout <<"Closing Database"<<endl;
		db.Close();//<<endl;
	}
	//cout << "Optimizing Catalog"<<endl;
	QueryOptimizer optimizer(catalog);
	//cout <<"Compiling Catalog"<<endl;
	QueryCompiler compiler(catalog, optimizer);
	cout<<"Finished Loading Files, Enter Query:" <<endl;
	int parse = -1, np=1000;
		if (yyparse () == 0) {
			cout << "OK!" << endl;
			parse = 0;
		}else{
			cout << "Error: Query is not correct!" << endl;
			parse = -1;
		}
		yylex_destroy();

		if (parse != 0) return -1;
		
		QueryExecutionTree queryTree;
		cout <<"Compiling Query"<<endl;
		compiler.Compile(np, attsToCreate, queryType, tables, attsToSelect, finalFunction, predicate,
			groupingAtts, distinctAtts, queryTree);

		if(queryType==0)cout << queryTree << endl;
	}
	return 0;
}