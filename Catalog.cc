#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <sqlite3.h>
#include <cstring>
#include <sstream>

#include "Schema.h"
#include "Catalog.h"

using namespace std;

static int callback(void *data, int argc, char **argv, char **azColName)
{
	vector<string>* temp = (vector<string>*) data;	
	for(int i=0; i<argc; i++)
	{
		temp->push_back(azColName[i]);
		temp->push_back(argv[i]);
	}
	return 0;
}


Catalog::Catalog(string& _fileName) {
	
	char *zErrMsg = 0;
	char *sql = new char [_fileName.size()+1];
	strcpy (sql, _fileName.c_str());

	sqlite3_open(sql, &db);
	
	vector<string> SavedTables;
	string sql1 = "SELECT * from Tables;";
	sql = new char [sql1.size()+1];
	strcpy (sql, sql1.c_str());
	sqlite3_exec(db, sql, callback, (void*)&SavedTables, &zErrMsg);
	
	for(int i =0;i < SavedTables.size()/6; i++)
	{
		vector<string> TableData;
		
		//6i+3 = no of tuples
		tabList.name.push_back(SavedTables[i*6+ 1]);
		tabList.noOfTup.push_back(atoi(SavedTables[i*6+ 3].c_str()));
		tabList.path.push_back(SavedTables[i*6+ 5]);
		
		
		sql1 = "SELECT * from " + SavedTables[6*i + 1];
		sql = new char [sql1.size()+1];
		strcpy (sql, sql1.c_str());
		
		sqlite3_exec(db, sql, callback, (void*)&TableData, &zErrMsg);
		
		vector<unsigned int> d;
		vector<string> _attributes, _attributeTypes;

		for (int j = 0; j < TableData.size()/6; j++)
		{
			d.push_back(atoi(TableData[j*6 + 5].c_str()));
			_attributes.push_back(TableData[j*6 + 1]);
			_attributeTypes.push_back(TableData[j*6 + 3]);	
		}

		Schema s(_attributes,_attributeTypes,d);
		record[SavedTables[6*i + 1]] = s;	
	}	
}

Catalog::~Catalog() {
	//Save();
	sqlite3_close(db);
}

bool Catalog::Save() {
	
	char *zErrMsg = 0;
	int  rc;	
	string sql1;
	char *sql = NULL;
	
	vector<string> tables;
	GetTables(tables);
	
	sql1 = 	"CREATE TABLE Tables( NAME STRING,NoOfTuples INTEGER, PATH STRING );"; 
	
	sql = new char [sql1.size()+1];
	strcpy (sql, sql1.c_str());
	sqlite3_exec(db, sql, 0, 0, &zErrMsg);
	
	sql1 = "DELETE FROM Tables";
	sql = new char [sql1.size()+1];
	strcpy (sql, sql1.c_str());
	sqlite3_exec(db, sql, 0, 0, &zErrMsg);



	for (int i = 0; i < tables.size(); i++)
	{
		string s;
		stringstream out;

		unsigned int nt;
		GetNoTuples(tables[i], nt);

		out << nt;
		s = out.str();
		
		sql1 = "INSERT INTO Tables VALUES( '" + tables[i] + "','" + s + "','" + tables[i] + "' );"; 
		
		sql = new char [sql1.size()+1];
		strcpy (sql, sql1.c_str());
		sqlite3_exec(db, sql, 0, 0, &zErrMsg);
	}
	
	string _table;
	vector<string> _attributes , _attributeTypes;
		
	for (std::map<string, Schema>::iterator it=record.begin(); it!=record.end(); ++it)
	{	
		vector<Attribute> atts;
		Schema s;
		_table = it->first;
		s = it->second;
		atts = s.GetAtts();
		
		int Size = s.GetNumAtts();
		
		sql1 = 	"CREATE TABLE " + _table + "( ATTS STRING, TYPE STRING, NoDistinct INTEGER );"; 
		sql = new char [sql1.size()+1];
		strcpy (sql, sql1.c_str());
		sqlite3_exec(db, sql, 0, 0, &zErrMsg);
		
		sql1 = "DELETE FROM " + _table;
		sql = new char [sql1.size()+1];
		strcpy (sql, sql1.c_str());
		sqlite3_exec(db, sql, 0, 0, &zErrMsg);
		
		for (int i = 0;i < Size; i++)
		{
			sql1 = 	"INSERT INTO " + _table + " VALUES('" + atts[i].name + "' ,'";
			
			switch(atts[i].type) {
			case Integer:	sql1+= "INTEGER'";	break;
			case Float:	sql1+= "FLOAT'";	break;
			case String:	sql1+= "STRING'";	break;
			default:	sql1+= "UNKNOWN";	break;
			}
	
			string s;
			stringstream out;
			out << atts[i].noDistinct;
			s = out.str();
			sql1 = sql1 + "," + s + " );";
			
			sql = new char [sql1.size()+1];
			strcpy (sql, sql1.c_str());
			sqlite3_exec(db, sql, 0, 0, &zErrMsg);
			
		}
	}
	
}

bool Catalog::GetNoTuples(string& _table, unsigned int& _noTuples) {

	int i=-1;
	while(1)
	{
		i++;
		if (tabList.name[i] == _table) break;
	}
	_noTuples = tabList.noOfTup[i];
	return true;
}

void Catalog::SetNoTuples(string& _table, unsigned int& _noTuples) {
}

bool Catalog::GetDataFile(string& _table, string& _path) {
	return true;
}

void Catalog::SetDataFile(string& _table, string& _path) {
}

bool Catalog::GetNoDistinct(string& _table, string& _attribute,
	unsigned int& _noDistinct) {
	return true;
}
void Catalog::SetNoDistinct(string& _table, string& _attribute,
	unsigned int& _noDistinct) {
}

void Catalog::GetTables(vector<string>& _tables) {
	
	for (std::map<string, Schema>::iterator it=record.begin(); it!=record.end(); ++it)
	{
		_tables.push_back(it->first);
	}
}

bool Catalog::GetAttributes(string& _table, vector<string>& _attributes) {
	return true;
}

bool Catalog::GetSchema(string& _table, Schema& _schema) {


	if(record.find(_table) == record.end()) return false;

	_schema = record[_table];
	return true;
}

bool Catalog::CreateTable(string& _table, vector<string>& _attributes,
	vector<string>& _attributeTypes) {	
	
	vector<unsigned int> d;
		
	for (int i = 0; i < _attributes.size(); i++)
		d.push_back(0);
	
	Schema s(_attributes,_attributeTypes,d);
	record[_table] = s;

	tabList.name.push_back(_table);
	tabList.noOfTup.push_back(0);
	tabList.path.push_back(_table);
	
	return true;
}

bool Catalog::DropTable(string& _table) {
	
	char *zErrMsg = 0;
	int  rc;
	char *sql = NULL;
	
	string sql1;
	sql1 = 	"DROP TABLE " + _table;
	sql = new char [sql1.size()+1];
	strcpy (sql, sql1.c_str());

	rc = sqlite3_exec(db, sql, 0, 0, &zErrMsg);

	if( rc != SQLITE_OK ){
		fprintf(stderr, "SQL error: %s\n", zErrMsg);
		sqlite3_free(zErrMsg);
	}else{
		fprintf(stdout, "Table dropped successfully\n");
	}
	record.erase(_table);
	
	return true;
}

ostream& operator<<(ostream& _os, Catalog& _c) {
	
	for (std::map<string, Schema>::iterator it=_c.record.begin(); it!=_c.record.end(); ++it)
	{
		cout<<endl<<"Table: "<<it->first<<endl<<"Schema: ";
		Schema s = it->second;
		cout<<s<<endl<<endl;	
	}
	return _os;
}
