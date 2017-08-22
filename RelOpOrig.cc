#include <iostream>
#include <sstream>
#include "RelOp.h"

using namespace std;


ostream& operator<<(ostream& _os, RelationalOp& _op) {
	return _op.print(_os);
}


Scan::Scan(Schema& _schema, DBFile& _file) {
	schema = _schema;
	file = _file;
}

bool Scan::GetNext(Record& rec) {
	return file.GetNext (rec);	
}


Scan::~Scan() {
}

ostream& Scan::print(ostream& _os) {
	return _os << "SCAN";
}


Select::Select(Schema& _schema, CNF& _predicate, Record& _constants,
	RelationalOp* _producer) {
	
	schema = _schema;
	predicate = _predicate;
	constants = _constants;
	producer = _producer;

}

bool Select::GetNext(Record& rec) {
	if (!producer->GetNext(rec)) return false;
	while (!predicate.Run(rec,constants))
	{
		if (!producer->GetNext(rec)) return false;
	}
	return true;

}

Select::~Select() {

}

ostream& Select::print(ostream& _os) {
	return _os << "(SELECT <- " << *producer << ")";
}


Project::Project(Schema& _schemaIn, Schema& _schemaOut, int _numAttsInput,
	int _numAttsOutput, int* _keepMe, RelationalOp* _producer) {
	
	schemaIn = _schemaIn;
	schemaOut = _schemaOut;
	numAttsInput = _numAttsInput;
	numAttsOutput = _numAttsOutput;
	keepMe = _keepMe;
	producer = _producer;
	
}

bool Project::GetNext(Record& record) {
	if (producer->GetNext(record))
	{
		record.Project(keepMe, numAttsOutput, numAttsInput);		
		return true;
	}
	return false;
}

Project::~Project() {

}

ostream& Project::print(ostream& _os) {
	return _os << "PROJECT (" << *producer << ")";
}


Join::Join(Schema& _schemaLeft, Schema& _schemaRight, Schema& _schemaOut,
	CNF& _predicate, RelationalOp* _left, RelationalOp* _right) {

	cout<<"num pages(in relop join) "<<noPages<<endl;

	schemaLeft = _schemaLeft; 	
	schemaRight = _schemaRight;
	left = _left;
	right = _right;	
	schemaOut = _schemaOut;
	predicate = _predicate;
	countL = 0; countR = 0;
	vecInd = 0;

	vector<Attribute> attsLeft = _schemaLeft.GetAtts(), attsRight = _schemaRight.GetAtts();
	for (auto a:attsLeft)	
		if (_schemaLeft.GetDistincts(a.name) > countL)	countL = _schemaLeft.GetDistincts(a.name);

	for (auto a:attsRight)	
		if (_schemaRight.GetDistincts(a.name) > countR)	countR = _schemaRight.GetDistincts(a.name);

	if (predicate.andList[predicate.numAnds-1].operand1 == Left)
	{
		for (int i = 0; i < predicate.numAnds; i++)
		{		
			watt1.push_back(predicate.andList[i].whichAtt1);
			watt2.push_back(predicate.andList[i].whichAtt2);
		}		
	}

	else
	{

		for (int i = 0; i < predicate.numAnds; i++)
		{		
			watt1.push_back(predicate.andList[i].whichAtt2);
			watt2.push_back(predicate.andList[i].whichAtt1);
		}
	}

	Record record;

	if (countL > countR) 
	{
		while (1)
		{
			if (right -> GetNext(record)){
				Record copy = record;
				int* attsToKeep = &watt2[0];
				int numAttsToKeep = predicate.numAnds;
				int numAttsNow = schemaRight.GetNumAtts();
				copy.Project (attsToKeep, numAttsToKeep, numAttsNow);
				Schema sCopy = schemaRight;
				sCopy.Project(watt2);
				vector <Attribute> attsToErase = sCopy.GetAtts();
			
				stringstream s;
				copy.print(s, sCopy);
				string sc = "", ss = s.str();

				for (int i = 0; i < attsToErase.size(); i++) 
				{
					string insideLoop = attsToErase[i].name;
					ss.erase (ss.find(insideLoop), insideLoop.size()+2);
					sc+= ss;
				}

				auto it = List.find(s.str());
				if(it != List.end())	List[sc].push_back(record);
				else
				{
					vector <Record> v;
					v.push_back(record);
					List.insert (make_pair(sc, v));
				}
			
			}
			else break;
		}
	}

	else
	{
		while (1)
		{
			if (left -> GetNext(record)){
				Record copy = record;
				int* attsToKeep = &watt1[0];
				int numAttsToKeep = predicate.numAnds;
				int numAttsNow = schemaLeft.GetNumAtts();
				copy.Project (attsToKeep, numAttsToKeep, numAttsNow);
				Schema sCopy = schemaLeft;
				sCopy.Project(watt1);
				vector <Attribute> attsToErase = sCopy.GetAtts();
			
				stringstream s;
				copy.print(s, sCopy);
				string sc = "", ss = s.str();

				for (int i = 0; i < attsToErase.size(); i++) 
				{
					string insideLoop = attsToErase[i].name;
					ss.erase (ss.find(insideLoop), insideLoop.size()+2);
					sc+= ss;
				}

				auto it = List.find(s.str());
				if(it != List.end())	List[sc].push_back(record);
				else
				{
					vector <Record> v;
					v.push_back(record);
					List.insert (make_pair(sc, v));
				}
			
			}
			else break;
		}
	}

	//for (auto it:List) {cout<<it.first<<"\t";it.second[0].print(cout, schemaLeft);cout<<endl;}
	//cout<<List.size()<<endl;
}

bool Join::GetNext(Record& record)
{
	
	Record rLeft;

	if (countL > countR) 
	{
		while (1)
		{ 
			if (lastrec.GetSize() == 0) 
				if(!left -> GetNext(lastrec)) return false;
		
			Record copy = lastrec;
			int* attsToKeep = &watt1[0];
			int numAttsToKeep = predicate.numAnds;
			int numAttsNow = schemaLeft.GetNumAtts();
			copy.Project (attsToKeep, numAttsToKeep, numAttsNow);
			Schema sCopy = schemaLeft;
			sCopy.Project(watt1);
			
			vector <Attribute> attsToErase = sCopy.GetAtts();
			stringstream s;
			copy.print(s, sCopy);
			string sc = "", ss = s.str();

			for (int i = 0; i < attsToErase.size(); i++) 
			{
				string insideLoop = attsToErase[i].name;
				ss.erase (ss.find(insideLoop), insideLoop.size()+2);
				sc+= ss;
			}

			auto it = List.find(sc);
			if (it == List.end()) lastrec.Nullify();
			else
			{
				if (it->second.size() == vecInd)
				{
					vecInd = 0;
					if(!left -> GetNext(lastrec)) return false;
				}

				else
				{
					rLeft = it->second[vecInd];
					vecInd++;
					if (predicate.Run (lastrec, rLeft))
					{
						record.AppendRecords( lastrec, rLeft, schemaLeft.GetNumAtts(), schemaRight.GetNumAtts());
						return true;
					}
				}
			}
		}
	}

	else
	{
		while (1)
		{
			if (lastrec.GetSize() == 0) 
				if(!right -> GetNext(lastrec)) return false;

			Record copy = lastrec;
			int* attsToKeep = &watt2[0];
			int numAttsToKeep = predicate.numAnds;
			int numAttsNow = schemaRight.GetNumAtts();
			copy.Project (attsToKeep, numAttsToKeep, numAttsNow);
			Schema sCopy = schemaRight;
			sCopy.Project(watt2);
			
			vector <Attribute> attsToErase = sCopy.GetAtts();
			stringstream s;
			copy.print(s, sCopy);
			string sc = "", ss = s.str();

			for (int i = 0; i < attsToErase.size(); i++) 
			{
				string insideLoop = attsToErase[i].name;
				ss.erase (ss.find(insideLoop), insideLoop.size()+2);
				sc+= ss;
			}

			auto it = List.find(sc);
			if (it == List.end()) lastrec.Nullify();
			else
			{
				if (it->second.size() == vecInd)
				{
					vecInd = 0;
					if(!right -> GetNext(lastrec)) return false;
				}

				else
				{
					rLeft = it->second[vecInd];
					vecInd++;
					if (predicate.Run (rLeft, lastrec))
					{
						record.AppendRecords( rLeft, lastrec, schemaLeft.GetNumAtts(), schemaRight.GetNumAtts());
						return true;
					}
				}
			}
		}
	}
}

Join::~Join() {

}

ostream& Join::print(ostream& _os) {
	return _os << "JOIN (" << *left << " & " << *right << ")";
}


DuplicateRemoval::DuplicateRemoval(Schema& _schema, RelationalOp* _producer) {
	
	schema = _schema;
	producer = _producer;

}

bool DuplicateRemoval::GetNext(Record& record)
{
	while (1)
	{
		if (! producer->GetNext(record)) return false;
		stringstream s;
		record.print(s, schema);
		auto it = set.find(s.str());
		if(it == set.end()) 
		{
			set[s.str()] = record;
			return true;
		}
	}
}

DuplicateRemoval::~DuplicateRemoval() {

}

ostream& DuplicateRemoval::print(ostream& _os) {
	return _os << "DISTINCT (" << *producer << ")";
}


Sum::Sum(Schema& _schemaIn, Schema& _schemaOut, Function& _compute,
	RelationalOp* _producer) {

	schemaIn = _schemaIn;
	schemaOut = _schemaOut; 
	compute = _compute;
	producer = _producer;
	recSent = 0;

}

bool Sum::GetNext(Record& record)
{
	if (recSent) return false;
	int iSum = 0;
	double dSum = 0;
	while(producer->GetNext(record))
	{	
		int iResult = 0;
		double dResult = 0;
		Type t = compute.Apply(record, iResult, dResult);
		if (t == Integer)	iSum+= iResult;
		if (t == Float)		dSum+= dResult;
	}

	double val = dSum + (double)iSum;

        char* recSpace = new char[PAGE_SIZE];
        int currentPosInRec = sizeof (int) * (2);
	((int *) recSpace)[1] = currentPosInRec;
        *((double *) &(recSpace[currentPosInRec])) = val;
        currentPosInRec += sizeof (double);
	((int *) recSpace)[0] = currentPosInRec;
        Record sumRec;
        sumRec.CopyBits( recSpace, currentPosInRec );
        delete [] recSpace;
	record = sumRec;
	recSent = 1;
	return true;
}

Sum::~Sum() {

}

ostream& Sum::print(ostream& _os) {
	return _os << "SUM (" << *producer << ")";
}


GroupBy::GroupBy(Schema& _schemaIn, Schema& _schemaOut, OrderMaker& _groupingAtts,
	Function& _compute,	RelationalOp* _producer) : groupingAtts(_groupingAtts) {

	schemaIn =  _schemaIn;
	schemaOut = _schemaOut;
	//groupingAtts = _groupingAtts;
	compute = _compute;	
	producer = _producer;
	phase = 0;

}

bool GroupBy::GetNext(Record& record)
{
	vector<int> attsToKeep, attsToKeep1;
	for (int i = 1; i < schemaOut.GetNumAtts(); i++)
		attsToKeep.push_back(i);

	copy = schemaOut;
	copy.Project(attsToKeep);

	attsToKeep1.push_back(0);
	sum = schemaOut;
	sum.Project(attsToKeep1);

	if (phase == 0)
	{
		while (producer->GetNext(record))	
		{	
			stringstream s;
			int iResult = 0;
			double dResult = 0;
			compute.Apply(record, iResult, dResult);
			double val = dResult + (double)iResult;
			
			record.Project(&groupingAtts.whichAtts[0], groupingAtts.numAtts , copy.GetNumAtts());
			record.print(s, copy);
			auto it = set.find(s.str());

			if(it != set.end())	set[s.str()]+= val;
			else
			{
				set[s.str()] = val;
				recMap[s.str()] = record;
			}
		
		}
		phase = 1;
	}

	if (phase == 1)
	{
		if (set.empty()) return false;

		Record temp = recMap.begin()->second;
		string strr = set.begin()->first;

		char* recSpace = new char[PAGE_SIZE];
		int currentPosInRec = sizeof (int) * (2);
		((int *) recSpace)[1] = currentPosInRec;
		*((double *) &(recSpace[currentPosInRec])) = set.begin()->second;
		currentPosInRec += sizeof (double);
		((int *) recSpace)[0] = currentPosInRec;
		Record sumRec;
		sumRec.CopyBits( recSpace, currentPosInRec );
		delete [] recSpace;
		
		Record newRec;
		newRec.AppendRecords(sumRec, temp, 1, schemaOut.GetNumAtts()-1);
		recMap.erase(strr);
		set.erase(strr);
		record = newRec;
		return true;
	}
}

GroupBy::~GroupBy() {

}

ostream& GroupBy::print(ostream& _os) {
	return _os << "GROUP BY (" << *producer << ")";
}


WriteOut::WriteOut(Schema& _schema, string& _outFile, RelationalOp* _producer) {
	schema = _schema;
	outFile = _outFile;
	producer = _producer;
  	myfile.open (&outFile[0]);
}

bool WriteOut::GetNext(Record& record) {

	bool writeout = producer->GetNext(record);
	if (!writeout)
	{
		myfile.close();
		return false;
	}
	record.print(myfile,schema);
	myfile<<endl;
	return writeout;

}


WriteOut::~WriteOut() {

}

ostream& WriteOut::print(ostream& _os) {
	return _os << "OUTPUT:\n{\n\t" << *producer <<"\n}\n";
}


ostream& operator<<(ostream& _os, QueryExecutionTree& _op) {

	Record r;
	unsigned long recs = 0;
	while (_op.root->GetNext(r)) recs++;

	return _os << "{\n\tWritten " << recs <<" records in the file 'Output_File.txt'.\n}\n";

}
