#include "QueryCompiler.h"
#include "QueryOptimizer.h"
#include "Schema.h"
#include "ParseTree.h"
#include "Record.h"
#include "DBFile.h"
#include "Comparison.h"
#include "Function.h"
#include "RelOp.h"

#include <sstream>


using namespace std;


QueryCompiler::QueryCompiler(Catalog& _catalog, QueryOptimizer& _optimizer) :
	catalog(&_catalog), optimizer(&_optimizer) { checkindex=0;	
}

QueryCompiler::~QueryCompiler() {
}

void QueryCompiler::Compile(int& pNum, AttList* _attsToCreate, int& _queryType, TableList* _tables, NameList* _attsToSelect,
	FuncOperator* _finalFunction, AndList* _predicate,
	NameList* _groupingAtts, int& _distinctAtts,
	QueryExecutionTree& _queryTree) {


	if(_queryType == 0) {

		cout<<"num pages "<<pNum<<endl;

		// SCAN operator for each table in the query along with push-down selections

		TableList* tables = _tables;
		//AndList* indexattr = _predicate;

		DBFile db; 
	
		while (tables != NULL)
		{
			Schema sch;

			string tab = tables->tableName;
			catalog->GetSchema(tab, sch);
			db.Open(&tab[0]); db.MoveFirst();

			scanz.insert (make_pair(tab,Scan(sch,db)) );

			CNF cnf;
			Record rec;
			cnf.ExtractCNF (*_predicate, sch, rec);
			if (cnf.numAnds != 0)
			{
				
			//INDEX PHASE
					
				//GETTING THE ATTRIBUTE
				string attcomp;
				
				if(cnf.andList[0].operand1 == Left)
				{
					vector <Attribute> atri;
					atri = sch.GetAtts();
					attcomp = atri[cnf.andList[0].whichAtt1].name;
				}
				if(cnf.andList[0].operand2 == Left)
				{
					vector <Attribute> atri;
					atri = sch.GetAtts();
					attcomp = atri[cnf.andList[0].whichAtt2].name;
				}
				//cout<<"attribute is "<<attcomp;

				//FINDING ATTRIBUTE IN CATALOG
				Schema seema;
				if(catalog->GetSchema(attcomp, seema)) {
					checkindex=1;
					vector <Attribute> indexfilenames;
					indexfilenames=seema.GetAtts();
					string indexfile, indexheader;
					indexfile = indexfilenames[0].name;
					indexheader = indexfilenames[1].name;
					//cout<<"index file "<<indexfile<<" index header "<<indexheader<<endl;
					
					ScanIndex sitest(sch, cnf, rec, indexfile, indexheader, tab);
					si.push_back(sitest);
				}
				else {
					Select sel(sch, cnf , rec ,(RelationalOp*) & scanz.at(tab));
					selectz.insert (make_pair(tab,sel) );
				}


		        }

			tables = tables->next;
		}
		
		// Optimizer to compute the join order

		OptimizationTree* root;
		optimizer->Optimize(_tables, _predicate, root);
		OptimizationTree* rootCopy = root;
	
		// Creating join operators based on the optimal order computed by the optimizer

		RelationalOp* join = constTree(rootCopy, _predicate);

		join->SetNoPages(pNum);
		//cout <<"join"<<endl;
		// Creating the remaining operators based on the query


		if (_finalFunction == NULL) 
		{
			Schema projSch;
			join->returnSchema(projSch);
			vector<Attribute> projAtts = projSch.GetAtts();	

			NameList* attsToSelect = _attsToSelect;
			int numAttsInput = projSch.GetNumAtts(), numAttsOutput = 0; 
			Schema projSchOut = projSch;
			vector<int> keepMe;

			while (attsToSelect != NULL)
			{
				string str(attsToSelect->name);
				keepMe.push_back(projSch.Index(str));
				attsToSelect = attsToSelect->next;
				numAttsOutput++;
			}
			int* keepme = new int [keepMe.size()];
			for (int i = 0;i < keepMe.size(); i++) keepme[i] = keepMe[i]; 
	
			projSchOut.Project(keepMe);
			Project* project = new Project (projSch, projSchOut, numAttsInput, numAttsOutput, keepme, join);
		
			join = (RelationalOp*) project;

			if (_distinctAtts == 1)
			{
				Schema dupSch;	
				join->returnSchema(dupSch);	
				DuplicateRemoval* duplicateRemoval = new DuplicateRemoval(dupSch, join);
				join = (RelationalOp*) duplicateRemoval;
			}
	
		}
		else
		{
			if (_groupingAtts == NULL) 
			{
				Schema schIn, schIn_;
				join->returnSchema(schIn_);
				schIn = schIn_;

				Function compute;
				FuncOperator* finalFunction = _finalFunction;
				compute.GrowFromParseTree(finalFunction, schIn_);

				vector<string> attributes, attributeTypes;
				vector<unsigned int> distincts;
				attributes.push_back("Sum");
				attributeTypes.push_back("FLOAT");
				distincts.push_back(1);
				Schema schOutSum(attributes, attributeTypes, distincts);
		
				Sum* sum = new Sum (schIn, schOutSum, compute, join);
				join = (RelationalOp*) sum;
			}

			else
			{
				Schema schIn, schIn_;
				join->returnSchema(schIn_);
				schIn = schIn_;

				NameList* grouping = _groupingAtts;
				int numAtts = 0; 
				vector<int> keepMe;

				vector<string> attributes, attributeTypes;
				vector<unsigned int> distincts;
				attributes.push_back("Sum");
				attributeTypes.push_back("FLOAT");
				distincts.push_back(1);

				while (grouping != NULL)
				{
					string str(grouping->name);
					keepMe.push_back(schIn_.Index(str));
					attributes.push_back(str);

					Type type;
					type = schIn_.FindType(str);

					switch(type) 
					{
						case Integer:	attributeTypes.push_back("INTEGER");	break;
						case Float:	attributeTypes.push_back("FLOAT");	break;
						case String:	attributeTypes.push_back("STRING");	break;
						default:	attributeTypes.push_back("UNKNOWN");	break;
					}
				
					distincts.push_back(schIn_.GetDistincts(str));
			
					grouping = grouping->next;
					numAtts++;
				}

				int* keepme = new int [keepMe.size()];
				for (int i = 0; i < keepMe.size(); i++) keepme[i] = keepMe[i];
			
				Schema schOut(attributes, attributeTypes, distincts);
				OrderMaker groupingAtts(schIn_, keepme, numAtts);

				Function compute;
				FuncOperator* finalFunction = _finalFunction;
				compute.GrowFromParseTree(finalFunction, schIn);
	
				GroupBy* groupBy = new GroupBy (schIn, schOut, groupingAtts, compute, join);	
				join = (RelationalOp*) groupBy;
			}	
		}
	
				//c//out <<"here"<<endl;

		Schema finalSchema;
		join->returnSchema(finalSchema);
		string outFile = "Output.txt";

		WriteOut * writeout = new WriteOut(finalSchema, outFile, join);
		join = (RelationalOp*) writeout;

		// Connecting everything in the query execution tree

		_queryTree.SetRoot(*join);	

		// free the memory occupied by the parse tree since it is not necessary anymore

	}
	else if(_queryType == 1) {

		vector<string> attributes, types;

		while(_attsToCreate != NULL) {
			string attribute(_attsToCreate->attname);
			attributes.push_back(attribute);
			string type(_attsToCreate->atttype);
			types.push_back(type);
			_attsToCreate = _attsToCreate->next;
		}		

		string table(_tables->tableName);

		cout<<table<<endl;
		for(int i =0 ; i<types.size(); i++)
		cout<<"attribute "<<attributes[i]<<endl<<"type "<<types[i]<<endl;
				

		catalog->CreateTable(table, attributes, types);
		catalog->Save();

	}
	else if(_queryType == 2) {

		string tableStr = _tables->tableName;
		string txtfileStr = _attsToSelect->name;
		string tbltxtfile = txtfileStr + ".tbl";
		FileType ftype = Heap;
		DBFile dbfile;

		dbfile.Create(&tableStr[0],ftype);

		Schema sch;

		catalog->GetSchema(txtfileStr,sch);

		dbfile.Load(sch,&tbltxtfile[0]);
		dbfile.Close();
	}
	else if(_queryType == 3) {

		TableList* tabl = _tables;

		string indexName = tabl->tableName;
		tabl = tabl->next;
		string indexTable = tabl->tableName;
		string indexAtt = _attsToSelect->name;

		//cout<<indexName<<indexTable<<indexAtt<<endl;

		DBFile db, ind, childex;
		db.Open(&indexTable[0]);
		Record record;
		int counter=0;
		int counteri=0, p = 1, r=0;
		//Record rec;

		ind.Create(&indexName[0], Index);

		string childname = "internal_" + indexAtt ;

		vector<string> at; at.push_back(indexName) ; at.push_back(childname);
		vector<string> type; type.push_back("STRING"); type.push_back("STRING");

		catalog->CreateTable(indexAtt, at, type);
	
		
		childex.Create(&childname[0], Index);

		
		catalog->Save();

		at.clear();
		type.clear();
/*
		vector<string> at; at.push_back("key");at.push_back("page");at.push_back("record");
		vector<string> type; type.push_back("INTEGER");type.push_back("INTEGER");type.push_back("INTEGER");

		catalog->CreateTable(indexName, at, type);

		vector<string> lt; lt.push_back("key"); lt.push_back("child");
		vector<string> typ; typ.push_back("INTEGER"); typ.push_back("INTEGER");

		catalog->CreateTable(childname, lt, typ);
*/		

		//Schema sc;
		//catalog->GetSchema(indexName, sc);

		while(db.GetNext(record)) { 
		
			counter++;

			counteri+= record.GetSize();
			if(counteri > PAGE_SIZE) {p++; r=0; counteri=0;};
			Schema sch;
			catalog->GetSchema(indexTable, sch);
			unsigned int numAtts = sch.GetNumAtts();
			vector<int> attIndex;
			attIndex.push_back(sch.Index(indexAtt));	
			int* attToKeep = &attIndex[0];
			record.Project (attToKeep,1,numAtts);
			//cout<<sch<<endl;
			sch.Project(attIndex);
			//cout<<sch<<endl;
			stringstream ss;
			record.print(ss, sch);
			//cout<<"record after projection "; record.print(cout, sch); cout<<endl;

			string s = ss.str();
			size_t pos = s.find(":");
			string str = s.substr(pos+2, s.length()-pos-3);
			//cout<<str;
			
			
			r++;
			int key = stoi(str, nullptr, 10);
			//cout<<key;

			
			char* recSpace = new char[PAGE_SIZE];
			int currentPosInRec = sizeof (int) * (2);
			((int *) recSpace)[1] = currentPosInRec;
			*((int *) &(recSpace[currentPosInRec])) = key ;
			currentPosInRec += sizeof (int);
			((int *) recSpace)[0] = currentPosInRec;
			Record newrec;
			newrec.CopyBits( recSpace, currentPosInRec );

			recSpace = new char[PAGE_SIZE];
			currentPosInRec = sizeof (int) * (2);
			((int *) recSpace)[1] = currentPosInRec;
			*((int *) &(recSpace[currentPosInRec])) = p ;
			currentPosInRec += sizeof (int);
			((int *) recSpace)[0] = currentPosInRec;
			Record newrec2;
			newrec2.CopyBits( recSpace, currentPosInRec );

			recSpace = new char[PAGE_SIZE];
			currentPosInRec = sizeof (int) * (2);
			((int *) recSpace)[1] = currentPosInRec;
			*((int *) &(recSpace[currentPosInRec])) = r ;
			currentPosInRec += sizeof (int);
			((int *) recSpace)[0] = currentPosInRec;
			Record newrec3;
			newrec3.CopyBits( recSpace, currentPosInRec );

			Record temp;
			temp.AppendRecords (newrec,newrec2,1,1);
			newrec.AppendRecords (temp,newrec3,2,1);

			//newrec.print(cout, sc ); cout<<endl;
			
			auto it = mapindex.find(key);
			if(it == mapindex.end()) {
				vector<Record> vrec;
				vrec.push_back(newrec);
				mapindex[key]=vrec;
			}
			else {
				it->second.push_back(newrec);
			}			


			//ind.AppendRecord(newrec);
						
		}
		
		counteri=0;
		auto it = mapindex.begin();
		int newkey = it->first;
		int childnum = 1;

	/*	Schema sh;
		catalog->GetSchema(childname, sh);
	*/
		char* recSpace = new char[PAGE_SIZE];
		int currentPosInRec = sizeof (int) * (2);
		((int *) recSpace)[1] = currentPosInRec;
		*((int *) &(recSpace[currentPosInRec])) = newkey ;
		currentPosInRec += sizeof (int);
		((int *) recSpace)[0] = currentPosInRec;
		Record newrec;
		newrec.CopyBits( recSpace, currentPosInRec );

		recSpace = new char[PAGE_SIZE];
		currentPosInRec = sizeof (int) * (2);
		((int *) recSpace)[1] = currentPosInRec;
		*((int *) &(recSpace[currentPosInRec])) = childnum ;
		currentPosInRec += sizeof (int);
		((int *) recSpace)[0] = currentPosInRec;
		Record newrec2;
		newrec2.CopyBits( recSpace, currentPosInRec );

		Record temp;
		temp.AppendRecords (newrec,newrec2,1,1);

		//temp.print(cout, sh);

		childex.AppendRecord(temp);
		
		for(auto at:mapindex) {
			for(int i =0; i<at.second.size(); i++)
				{	
					counteri += at.second[i].GetSize();
					//at.second[i].print(cout, sc); cout<<endl; 
					ind.AppendRecord(at.second[i]);
					
					
					if(counteri >= PAGE_SIZE) {
						
						newkey = at.first;
						childnum++;
						counteri=0;

						recSpace = new char[PAGE_SIZE];
						currentPosInRec = sizeof (int) * (2);
						((int *) recSpace)[1] = currentPosInRec;
						*((int *) &(recSpace[currentPosInRec])) = newkey ;
						currentPosInRec += sizeof (int);
						((int *) recSpace)[0] = currentPosInRec;
						Record newrec;
						newrec.CopyBits( recSpace, currentPosInRec );

						recSpace = new char[PAGE_SIZE];
						currentPosInRec = sizeof (int) * (2);
						((int *) recSpace)[1] = currentPosInRec;
						*((int *) &(recSpace[currentPosInRec])) = childnum ;
						currentPosInRec += sizeof (int);
						((int *) recSpace)[0] = currentPosInRec;
						Record newrec2;
						newrec2.CopyBits( recSpace, currentPosInRec );

						temp.AppendRecords (newrec,newrec2,1,1);

						//temp.print(cout, sh);

						childex.AppendRecord(temp);
						
					}
				}
		}
		
		childex.Close();
		ind.Close();

		//cout<<childnum<<endl;
		
					
	}


}


RelationalOp* QueryCompiler::constTree(OptimizationTree* root, AndList* _predicate)
{
	if (root -> leftChild == NULL && root -> rightChild == NULL)
	{	
		if(checkindex == 1) return (RelationalOp*) & si[0]; 
		RelationalOp* op;
		auto it = selectz.find(root -> tables[0]);
		if(it != selectz.end())		op = (RelationalOp*) & it->second;
		else				op = (RelationalOp*) & scanz.at(it->first);
		
		
		return op;
	}

	if (root -> leftChild -> tables.size() == 1  && root -> rightChild -> tables.size() == 1) 
	{
		string left = root -> leftChild -> tables[0];
		string right = root -> rightChild -> tables[0];

		CNF cnf;
		Schema sch1, sch2;
		RelationalOp* lop, *rop;

		auto it = selectz.find(left);
		if(it != selectz.end())		lop = (RelationalOp*) & it->second;
		else				lop = (RelationalOp*) & scanz.at(left);

		it = selectz.find(right);
		if(it != selectz.end()) 	rop = (RelationalOp*) & it->second;
		else				rop = (RelationalOp*) & scanz.at(right);
	
		lop->returnSchema(sch1);
		rop->returnSchema(sch2);
		
		cnf.ExtractCNF (*_predicate, sch1, sch2);
		Schema schout = sch1;
		schout.Append(sch2);
		Join* join = new Join(sch1, sch2, schout, cnf, lop , rop);
		return ((RelationalOp*) join);
	
	}

	if (root -> leftChild -> tables.size() == 1)
	{	
		string left = root -> leftChild -> tables[0];
		Schema sch1,sch2;
		CNF cnf;		
		RelationalOp* lop;

		auto it = selectz.find(left);
		if(it != selectz.end())		lop = (RelationalOp*) & it->second;
		else				lop = (RelationalOp*) & scanz.at(left);

		lop->returnSchema(sch1);
		RelationalOp* rop = constTree(root -> rightChild, _predicate);
		rop->returnSchema(sch2);

		cnf.ExtractCNF (*_predicate, sch1, sch2);
		Schema schout = sch1;
		schout.Append(sch2);
		Join* join = new Join(sch1, sch2, schout, cnf, lop , rop);
		return ((RelationalOp*) join);
	}

	if (root -> rightChild -> tables.size() == 1)
	{	
		string right = root -> rightChild -> tables[0];
		Schema sch1,sch2;
		CNF cnf;
		RelationalOp* rop;

		auto it = selectz.find(right);
		if(it != selectz.end())		rop = (RelationalOp*) & it->second;
		else				rop = (RelationalOp*) & scanz.at(right);

		rop->returnSchema(sch2);
		RelationalOp* lop = constTree(root -> leftChild, _predicate);
		lop->returnSchema(sch1);
		
		cnf.ExtractCNF (*_predicate, sch1, sch2);
		Schema schout = sch1;
		schout.Append(sch2);
		Join* join = new Join(sch1, sch2, schout, cnf, lop , rop);
		return ((RelationalOp*) join);
	}

	Schema sch1,sch2;
	CNF cnf;
	RelationalOp* lop = constTree(root -> leftChild, _predicate);
	RelationalOp* rop = constTree(root -> rightChild, _predicate);

	lop->returnSchema(sch1);
	rop->returnSchema(sch2);

	cnf.ExtractCNF (*_predicate, sch1, sch2);
	Schema schout = sch1;
	schout.Append(sch2);
	Join* join = new Join(sch1, sch2, schout, cnf, lop , rop);
	return ((RelationalOp*) join);

}




