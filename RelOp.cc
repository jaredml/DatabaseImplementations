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


// SCAN INDEX

ScanIndex::ScanIndex(Schema& _schema, CNF& _predicate, Record& _constants, 
		     string& _indexfile, string& _indexheader, string& _table) {

	schema = _schema;
	predicate = _predicate;
	constants = _constants;
	leaf = _indexfile;
	internal = _indexheader;
	table = _table;
	once =0;
	veccount=0;
	//Record fals;
	//finalrec.push_back(fals);
}

bool ScanIndex::GetNext(Record& rec) {

	//vector<Record>finalrec;
	if(once==0) { //cout<<"here"<<endl;
		DBFile mainfile;
		mainfile.Open(&table[0]);

		DBFile f_leaf, f_internal;
		f_leaf.Open(&leaf[0]);
		f_internal.Open(&internal[0]);

		vector<string> at; at.push_back("value") ;
		vector<string> type; type.push_back("INTEGER");
		vector<unsigned int> dis; dis.push_back(0);
		Schema sche(at, type, dis);

		stringstream ss;	

		constants.print(ss, sche); //cout<<endl;
		string s = ss.str();
		size_t pos = s.find(":");
		string str = s.substr(pos+2, s.length()-pos-3);
		//cout<<endl<<str<<endl;

		Record internalrecord;
		f_internal.MoveFirst();
		int counter=1;
		int childcount=0;

		while(f_internal.GetNext(internalrecord)){
		
		
			at.clear();
			type.clear();
			dis.clear();

			at.push_back("key") ; at.push_back("child");
			type.push_back("INTEGER"); type.push_back("INTEGER");
			dis.push_back(0); dis.push_back(0);
			Schema sc(at, type, dis);

			stringstream ssindex;

			internalrecord.print(ssindex, sc); cout<<endl;
			s = ssindex.str();
			//cout<<s<<endl;
			pos = s.find(":");
			size_t pos1 = s.find(",");
			string internalkey = s.substr(pos+1, pos1-pos-1);
			//cout<<endl<<internalkey<<endl;
		
			if(stoi(str)<stoi(internalkey)) { childcount = counter-1; break;}
			else childcount = counter;
			counter++;
		}
		//cout<<"child "<<childcount<<" counter "<<counter;
	
		Page p;
		Record r;
		f_leaf.GetPageNo(childcount-1, p);
		int count=0, count1=0;
		vector<Record> indexrecvec;
	
		at.clear();
		type.clear();
		dis.clear();	
		at.push_back("key");at.push_back("page");at.push_back("record");
		type.push_back("INTEGER");type.push_back("INTEGER");type.push_back("INTEGER");
		dis.push_back(0); dis.push_back(0); dis.push_back(0);

		Schema leafschema(at, type, dis);


		while(p.GetFirst(r) != 0) {

			count++;
	
			stringstream leafrec;
			r.print(leafrec, leafschema);

			string a, keynum, c, pagenum, e, recnum;
			int kn=0, pn=0, rn=0;

			leafrec>>a>>keynum>>c>>pagenum>>e>>recnum;

			keynum.pop_back();
			pagenum.pop_back();
			recnum.pop_back();
			kn=stoi(keynum, nullptr,10); pn=stoi(pagenum, nullptr, 10); rn=stoi(recnum, nullptr, 10);

			int valTocompare = stoi(str, nullptr, 10);

			if(valTocompare == kn) 
			{

				count1++;

				cout<<"page"<<pn<<" ";

				cout<<"record"<<rn<<" "<<"count "<<count1 <<endl;
	
				Record mainrec;
			
				mainfile.GetSpecificRecord(pn-1, rn, mainrec);
				//cout<<"here";
		
				finalrec.push_back(mainrec);
				//cout<<"here";
				//mainrec.print(cout, schema); cout<<endl;

			
			}

			//cout<<kn<<endl<<pn<<endl<<rn<<endl;
			 
		}
		
		
		//cout<<"vector size "<<finalrec.size();
	
		once=1;
		cout<<endl<<"number total records "<<count<<" number applicable records "<<count1<<endl;
		//cout<<"HERE in Scan Index"<<endl;
		//return false;
	
	}
		
	if(veccount<finalrec.size()) {
		rec = finalrec[veccount];
		veccount++;
		return true;
	}
	else return false;
			

}


ScanIndex::~ScanIndex() {
}

ostream& ScanIndex::print(ostream& _os) {
	return _os << "SCAN INDEX";
}



//SCAN INDEX END

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
	
	schemaLeft = _schemaLeft; 	
	schemaRight = _schemaRight;
	left = _left;
	right = _right;	
	schemaOut = _schemaOut;
	predicate = _predicate;
	countL = 0; countR = 0;
	vecInd = 0; l=1;
	phase = 4;
	fcount = 0;												//p5

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

	
	//for (auto it:List) {cout<<it.first<<"\t";it.second[0].print(cout, schemaLeft);cout<<endl;}
	//cout<<List.size()<<endl;
	
}

bool Join::GetNext(Record& record){
	
	if(l==1){

		//cout<<"num pages(in relop join) "<<noPages<<endl;
		right->SetNoPages(noPages);
		left->SetNoPages(noPages);
	
		Record rrecord;

		if (countL > countR) 
		{
			//cout<<"\nright is smaller\n";
			int recSize = 0;
			int count = 0;	
			while (1)
			{
				if (right -> GetNext(rrecord)){

					Record copy = rrecord;
					int* attsToKeep = &watt2[0];
					int numAttsToKeep = predicate.numAnds;
					int numAttsNow = schemaRight.GetNumAtts();
					copy.Project (attsToKeep, numAttsToKeep, numAttsNow);
					Schema sCopy = schemaRight;
					sCopy.Project(watt2);
					vector <Attribute> attsToErase = sCopy.GetAtts();
					
								
					recSize += rrecord.GetSize();

					int c=0;
					if(recSize >= noPages * PAGE_SIZE){

						phase = 5;
						//cout<<"creating dbfile"<<endl;
						count++;
						string s = "Rrun"; s += to_string(count);
						//cout<<s<<endl;
						DBFile db;
						
						db.Create(&s[0], Sorted);

						
						for(auto it:List) {
							for(int i =0; i<it.second.size(); i++)
							{
								c++;
								db.AppendRecord(it.second[i]);
							}
						}
						//Record bekaar = rrecord;
						//db.AppendRecord(bekaar);
						db.Close();
						rdbvec.push_back(db);

						//cout<<"records appended "<<c<<endl;

						//Record aik;
						//db.GetNext(aik);
						//aik.print(cout,schemaLeft); cout<<endl;
						
						recSize=0;
						Fmap.insert(List.begin(), List.end());			//p5
						List.clear();
					}	


			
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
					if(it != List.end())	List[sc].push_back(rrecord);
					else
					{
						vector <Record> v;
						v.push_back(rrecord);
						List.insert (make_pair(sc, v));
					}
			
				}
				else {
					if(phase == 4) break;
					DBFile db;
					int c=0;
						
					count++;
					string s = "Rrun"; s += to_string(count);
					//cout<<s<<endl;
					db.Create(&s[0], Sorted);
					
					
					for(auto it:List) {
						for(int i =0; i<it.second.size(); i++)
						{
							c++;
							db.AppendRecord(it.second[i]);
						}
					}
					//Record bekaar = rrecord;
					//db.AppendRecord(bekaar);
					db.Close();
					rdbvec.push_back(db);
					Fmap.insert(List.begin(), List.end());					//p5

					List.clear();

					//cout<<"records appended "<<c<<endl;
					break;
				}
			}
		}

		else
		{
			//cout<<"\ncount left is less than count right\n";
			int recSize = 0;
			int count = 0;
			
			while (1)
			{
				if (left -> GetNext(rrecord)){
					Record copy = rrecord;
					int* attsToKeep = &watt1[0];
					int numAttsToKeep = predicate.numAnds;
					int numAttsNow = schemaLeft.GetNumAtts();
					copy.Project (attsToKeep, numAttsToKeep, numAttsNow);
					Schema sCopy = schemaLeft;
					sCopy.Project(watt1);
					vector <Attribute> attsToErase = sCopy.GetAtts();

					
					
					recSize += rrecord.GetSize();

					int c=0;
					if(recSize >= noPages * PAGE_SIZE){

						phase = 5;
						//cout<<"creating dbfile"<<endl;
						count++;
						string s = "Lrun"; s += to_string(count);
						//cout<<s<<endl;
						DBFile db;
						
						db.Create(&s[0], Sorted);

						
						for(auto it:List) {
							for(int i =0; i<it.second.size(); i++)
							{
								c++;
								db.AppendRecord(it.second[i]);
							}
						}
						//Record bekaar = rrecord;
						//db.AppendRecord(bekaar);
						db.Close();
						ldbvec.push_back(db);

						//cout<<"records appended "<<c<<endl;

						//Record aik;
						//db.GetNext(aik);
						//aik.print(cout,schemaLeft); cout<<endl;
						
						recSize=0;
						
						Fmap.insert(List.begin(), List.end());				//p5
						List.clear();
						recSize += rrecord.GetSize();
					}	

			
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
					if(it != List.end())	List[sc].push_back(rrecord);
					else
					{
						vector <Record> v;
						v.push_back(rrecord);
						List.insert (make_pair(sc, v));
					}
			
				}
				else {
					if(phase == 4) break;

					DBFile db;
					int c = 0;
					count++;
					string s = "Lrun"; s += to_string(count);
					//cout<<s<<endl;
					db.Create(&s[0], Sorted);

					
					for(auto it:List) {
						for(int i =0; i<it.second.size(); i++)
						{
							c++;
							db.AppendRecord(it.second[i]);
						}
					}
					//Record bekaar = rrecord;
					//db.AppendRecord(bekaar);
					db.Close();
					ldbvec.push_back(db);

					Fmap.insert(List.begin(), List.end());					//p5

					List.clear();

					//cout<<"records appended "<<c<<endl;
					break;
				}
			}
		}
		l=0;
	}
	

if(phase == 4) {

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
//else 
if (phase == 5) {
	
	if(l==0) {

	Record rrecord;

		if (countL < countR) 
		{
			//cout<<"\nright is greater\n";
			int recSize = 0;
			int count = 0;	
			while (1)
			{
				if (right -> GetNext(rrecord)){

					recright.push_back(rrecord);						//p5

					Record copy = rrecord;
					int* attsToKeep = &watt2[0];
					int numAttsToKeep = predicate.numAnds;
					int numAttsNow = schemaRight.GetNumAtts();
					copy.Project (attsToKeep, numAttsToKeep, numAttsNow);
					Schema sCopy = schemaRight;
					sCopy.Project(watt2);
					vector <Attribute> attsToErase = sCopy.GetAtts();
					
								
					recSize += rrecord.GetSize();

					int c=0;
					if(recSize >= noPages * PAGE_SIZE){

						phase = 5;
						//cout<<"creating dbfile"<<endl;
						count++;
						string s = "Rrun"; s += to_string(count);
						//cout<<s<<endl;
						DBFile db;
						
						db.Create(&s[0], Sorted);

						
						for(auto it:List) {
							for(int i =0; i<it.second.size(); i++)
							{
								c++;
								db.AppendRecord(it.second[i]);
							}
						}
						//Record bekaar = rrecord;
						//db.AppendRecord(bekaar);
						db.Close();
						rdbvec.push_back(db);

						//cout<<"records appended "<<c<<endl;

						//Record aik;
						//db.GetNext(aik);
						//aik.print(cout,schemaLeft); cout<<endl;
						
						recSize=0;

						List.clear();
					}	


			
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
					if(it != List.end())	List[sc].push_back(rrecord);
					else
					{
						vector <Record> v;
						v.push_back(rrecord);
						List.insert (make_pair(sc, v));
					}
			
				}
				else {
					DBFile db;
					int c=0;
						
					count++;
					string s = "Rrun"; s += to_string(count);
					//cout<<s<<endl;
					db.Create(&s[0], Sorted);
					
					
					for(auto it:List) {
						for(int i =0; i<it.second.size(); i++)
						{
							c++;
							db.AppendRecord(it.second[i]);
						}
					}
					//Record bekaar = rrecord;
					//db.AppendRecord(bekaar);
					db.Close();
					rdbvec.push_back(db);
					List.clear();

					//cout<<"records appended "<<c<<endl;
					break;
				}
			}
		}

		else
		{
			//cout<<"\ncount left is more than count right\n";
			int recSize = 0;
			int count = 0;
			
			while (1)
			{
				if (left -> GetNext(rrecord)){

					recleft.push_back(rrecord);						//p5

					Record copy = rrecord;
					int* attsToKeep = &watt1[0];
					int numAttsToKeep = predicate.numAnds;
					int numAttsNow = schemaLeft.GetNumAtts();
					copy.Project (attsToKeep, numAttsToKeep, numAttsNow);
					Schema sCopy = schemaLeft;
					sCopy.Project(watt1);
					vector <Attribute> attsToErase = sCopy.GetAtts();

					
					
					recSize += rrecord.GetSize();

					int c=0;
					if(recSize >= noPages * PAGE_SIZE){

						phase = 5;
						//cout<<"creating dbfile"<<endl;
						count++;
						string s = "Lrun"; s += to_string(count);
						//cout<<s<<endl;
						DBFile db;
						
						db.Create(&s[0], Sorted);

						
						for(auto it:List) {
							for(int i =0; i<it.second.size(); i++)
							{
								c++;
								db.AppendRecord(it.second[i]);
							}
						}
						//Record bekaar = rrecord;
						//db.AppendRecord(bekaar);
						db.Close();
						ldbvec.push_back(db);

						//cout<<"records appended "<<c<<endl;

						//Record aik;
						//db.GetNext(aik);
						//aik.print(cout,schemaLeft); cout<<endl;
						
						recSize=0;

						List.clear();
						recSize += rrecord.GetSize();
					}	

			
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
					if(it != List.end())	List[sc].push_back(rrecord);
					else
					{
						vector <Record> v;
						v.push_back(rrecord);
						List.insert (make_pair(sc, v));
					}
			
				}
				else {

					DBFile db;
					int c = 0;
					count++;
					string s = "Lrun"; s += to_string(count);
					//cout<<s<<endl;
					db.Create(&s[0], Sorted);

					
					for(auto it:List) {
						for(int i =0; i<it.second.size(); i++)
						{
							c++;
							db.AppendRecord(it.second[i]);
						}
					}
					//Record bekaar = rrecord;
					//db.AppendRecord(bekaar);
					db.Close();
					ldbvec.push_back(db);
					List.clear();

					//cout<<"records appended "<<c<<endl;
					break;
				}
			}

	}
	l=2;
	}
	
//	return false;										fp5starts

	
	Record rLeft;

	if (countL > countR) 
	{
		while (1)
		{ 
			if (lastrec.GetSize() == 0) {
				//if(!left -> GetNext(lastrec)) return false;
				  if(fcount > recleft.size()-1) return false;
				  else lastrec = recleft[fcount];
			}
	
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

			//auto it = List.find(sc);
			//if (it == List.end()) lastrec.Nullify();
			auto it = Fmap.find(sc);
			if (it == Fmap.end()) lastrec.Nullify();
			else
			{
				if (it->second.size() == vecInd)
				{
					vecInd = 0;
					//if(!left -> GetNext(lastrec)) return false;
					++fcount;
					if(fcount > recleft.size()-1) return false;
				  	else lastrec = recleft[fcount];
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
		fcount++;
		}
	}

	else
	{	//cout<<"in here 1"<<endl;
		while (1)
		{
			if (lastrec.GetSize() == 0) {
				//if(!right -> GetNext(lastrec)) return false;
				  if(fcount > recright.size()-1) return false;
				  else lastrec = recright[fcount];
			}
			//cout<<"in here 2"<<endl;
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

			//auto it = List.find(sc);
			//if (it == List.end()) lastrec.Nullify();
			auto it = Fmap.find(sc);
			if (it == Fmap.end()) lastrec.Nullify();
			else
			{
				if (it->second.size() == vecInd)
				{
					vecInd = 0;
					//if(!right -> GetNext(lastrec)) return false;
					++fcount;
					if(fcount > recright.size()-1) return false;
				  	else lastrec = recright[fcount];
					//cout<<"in here 3"<<endl;
				}

				else
				{
					rLeft = it->second[vecInd];
					vecInd++;
					if (predicate.Run (rLeft, lastrec))
					{
						//cout<<"in here 4"<<endl;
						record.AppendRecords( rLeft, lastrec, schemaLeft.GetNumAtts(), schemaRight.GetNumAtts());
						return true;
					}
				}
			}
		fcount++; //cout<<"in here 5"<<endl;
		}
	}



//												fp5ends
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
	cout << "Was I here"<<endl;
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

	return _os << "---------Records in output file : " << recs <<"---------\n";

}
