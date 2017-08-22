#include <string>
#include <vector>
#include <iostream>
#include <algorithm>
#include <sstream>
#include <climits>

#include "Schema.h"
#include "Comparison.h"
#include "QueryOptimizer.h"

using namespace std;


QueryOptimizer::QueryOptimizer(Catalog& _catalog) : catalog(&_catalog) {
}

QueryOptimizer::~QueryOptimizer() {
}

void QueryOptimizer::Optimize(TableList* _tables, AndList* _predicate,
	OptimizationTree*& _root) {
	// Computing the optimal join order

	TableList * tables = _tables;
	vector <string> tablez;		// 012 etc values after maping original tables names
	vector <string> Origtablez;	// original mapped table names
	vector <string> tablezNames;	// Original tables names
	int indx = 0;

	while (tables != NULL)
	{
		unsigned int tups;

		string s1( tables->tableName );
		catalog->GetNoTuples(s1, tups);
		Schema temp;
		catalog -> GetSchema(s1, temp);
		
		string s = to_string(indx);
				
		Map[s].size = tups;
		Map[s].cost = 0;
		Map[s].order = s;
		Map[s].sch = temp;

		tablez.push_back(s);
		Origtablez.push_back(s);
		tablezNames.push_back(s1);
		mapping[s] = s1;
		indx++;
		tables = tables->next;

	}

	/*cout<<"\nTables encoding:\n\n";
	for (int i=0; i<tablez.size(); i++) cout<<tablez[i]<<"\t"<<tablezNames[i]<<endl;
	cout<<endl;
	cout<<"\n==============\nPREPROCESSING:\n==============\n";
	cout<<"\n\t=====================\n\tSize\tCost\tOrder\n\t=====================\n";
	cout<<"\nMap fillled for individual tables:\n---------------------\n";

	for (std::map<string, plan>::iterator it=Map.begin(); it!=Map.end(); ++it)
	{
		cout<<it->first<<"\t"<<it->second.size<<"\t"<<it->second.cost<<"\t"<<it->second.order<<endl;
	}

	cout<<"---------------------\n";*/

	CNF cnf;
	tables = _tables;
	
	while (tables != NULL)
	{
		Schema sch;
		Record rec;
		int div=1;
	
		string tab(tables->tableName);
		catalog->GetSchema(tab, sch);
	
		cnf.ExtractCNF (*_predicate, sch, rec);
	
		if (cnf.numAnds >0)
		{
			for (int i = 0 ; i < cnf.numAnds; i++)
			{
				if (cnf.andList[i].operand1 == Left || cnf.andList[i].operand1 == Right)
				{
					if(cnf.andList[i].op == LessThan || cnf.andList[i].op == GreaterThan)
					{
						div = 3;
						vector <Attribute> atts;
						atts = sch.GetAtts();
					}
					else
					{
						vector <Attribute> atts;
						atts = sch.GetAtts();
						div = atts[cnf.andList[i].whichAtt1].noDistinct;
					}
					
				}

				if (cnf.andList[i].operand2 == Left || cnf.andList[i].operand2 == Right)
				{
					if(cnf.andList[i].op == LessThan || cnf.andList[i].op == GreaterThan)
					{
						div = 3;
						vector <Attribute> atts;
						atts = sch.GetAtts();
					}
					else
					{
						vector <Attribute> atts;
						atts = sch.GetAtts();
						div = atts[cnf.andList[i].whichAtt2].noDistinct;
					}
					
				}

			}

		for(int i=0;;i++){if(tablezNames[i]== tab) {Map[tablez[i]].size /= div; break;} }

		}
		
		tables = tables->next;
	
	}

	/*cout<<"\nMap fillled after push down selections:\n---------------------\n";

	for (std::map<string, plan>::iterator it=Map.begin(); it!=Map.end(); ++it)
	{
		cout<<it->first<<"\t"<<it->second.size<<"\t"<<it->second.cost<<"\t"<<it->second.order<<endl;
	}
	cout<<"---------------------\n";*/

	tables = _tables;
	int start = 0;

	while (tables->next != NULL)
	{
		if (start == 0 && tables->next == NULL) break;
		start = 1;

		Schema sch1;
		int div;
	
		string tab1(tables->tableName);
		catalog->GetSchema(tab1, sch1);

		TableList * tablesIn = tables->next;
		while (tablesIn != NULL)
		{
			
			Schema sch2;
			unsigned long long tups1,tups2;
			unsigned long long Va,Vb;
			string Name="";
	
			string tab2(tablesIn->tableName);
			catalog->GetSchema(tab2, sch2);
	
			cnf.ExtractCNF (*_predicate, sch1, sch2);

			vector <Attribute> atts1;
			atts1 = sch1.GetAtts();					

			vector <Attribute> atts2;
			atts2 = sch2.GetAtts();	
			
			
			for(int i=0;;i++)
				if (tablezNames[i] == tab1){
					Name += tablez[i];
					tups1 = Map[tablez[i]].size;
					break;
				}

			for(int i=0;;i++)
				if (tablezNames[i] == tab2){
					Name += tablez[i];
					tups2 = Map[tablez[i]].size;
					break;
				}

			if (cnf.numAnds >0)
			{ 
				for (int i = 0 ; i < cnf.numAnds; i++)
				{
					if (cnf.andList[i].operand1 == Left)
					{
						Va = atts1[cnf.andList[i].whichAtt1].noDistinct;
						Vb = atts2[cnf.andList[i].whichAtt2].noDistinct;
					}

					if (cnf.andList[i].operand1 == Right)
					{
						Va = atts1[cnf.andList[i].whichAtt2].noDistinct;
						Vb = atts2[cnf.andList[i].whichAtt1].noDistinct;
					}
					
					if (Va > Vb) div = Va; else div = Vb;	// Max [Va,Vb]
				}
			}

			if (cnf.numAnds == 0)	div = 1;	
		
			Map[Name].size = (tups1*tups2)/div;	// (Ta.Tb)/(Max(Va,Vb))
			Map[Name].cost = 0;
			Map[Name].order = Name;
			Schema temp = sch1;
			temp.Append(sch2);
			Map[Name].sch = temp;
			
			tablez.push_back(Name);

			tablesIn = tablesIn->next;			
		}
	
		tables = tables->next;
	}

	/*cout<<"\nMap fillled after pairing:\n---------------------\n";
	for (std::map<string, plan>::iterator it=Map.begin(); it!=Map.end(); ++it)
	{
		cout<<it->first<<"\t"<<it->second.size<<"\t"<<it->second.cost<<"\t"<<it->second.order<<endl;
	}
	cout<<"---------------------\n";*/

	string tabList = "";
	for (int i=0 ; i<Origtablez.size(); i++) tabList+= Origtablez[i];

	Partition(tabList, _predicate);

	/*cout<<"\nMap fillled after all permutations:\n---------------------\n";
	for (std::map<string, plan>::iterator it=Map.begin(); it!=Map.end(); ++it)
	{
		cout<<it->first<<"\t"<<it->second.size<<"\t"<<it->second.cost<<"\t"<<it->second.order<<endl;
	}
	cout<<"---------------------\n";*/

	_root = new OptimizationTree;
	_root -> leftChild = NULL;
	_root -> rightChild = NULL;
	for (int i = 0; i < tabList.size(); i++) 
	{
		_root -> tables.push_back(mapping[ {tabList[i]} ] );
	}

	cout<<endl<<endl;
	_root -> noTuples = Map[tabList].size;
	tabList = Map[tabList].order;
	treeGenerator(tabList, _root);

}


void QueryOptimizer::treeDisp(OptimizationTree* & root)
{
	if (root -> leftChild == NULL && root -> rightChild == NULL) 
	{
		cout<<root -> tables[0]<<endl;
		cout<<root -> noTuples<<endl;		
		cout<<endl;
		return;
	}
	treeDisp(root -> leftChild);
	treeDisp(root -> rightChild);
}
	

void QueryOptimizer::treeGenerator(string tabList, OptimizationTree* & root)
{
	string left,right;
	int pos = tabList.find(",");

	if (pos != string::npos)
	{
		left = tabList.substr(0,pos);
		right = tabList.substr(tabList.find(",")+1);

		root -> leftChild = new OptimizationTree;
		treeGenerator(left, root -> leftChild);
		root -> rightChild = new OptimizationTree;
		treeGenerator(right, root -> rightChild);
		return;	
	}
		
	for (int i=0; i<tabList.size(); i++)
		root -> tables.push_back(mapping[ {tabList[i]} ]);
	
	if (tabList.size() == 1)
	{
		root -> noTuples = Map[tabList].size;
		root -> leftChild = NULL;
		root -> rightChild = NULL;
		return;
	}
	
	root -> leftChild = new OptimizationTree;
	root -> leftChild -> leftChild = NULL;
	root -> leftChild -> rightChild = NULL;
	root -> leftChild -> tables.push_back(root -> tables[0]);
	root -> leftChild -> noTuples = Map[{tabList[0]}].size;
	
	root -> rightChild = new OptimizationTree;
	root -> rightChild -> leftChild = NULL;
	root -> rightChild -> rightChild = NULL;
	root -> rightChild -> tables.push_back(root -> tables[1]);
	root -> rightChild -> noTuples = Map[{tabList[1]}].size;

}

	
void QueryOptimizer::Partition(string tables, AndList* _predicate)
{
	string copy = tables;
	unsigned long long min_cost = LLONG_MAX,cost,size;
	string order;
	Schema sch1,sch2, temp;

	sort(copy.begin(), copy.end());

	auto it = Map.find(copy);
	if(it != Map.end()) return;

	int N = tables.size();
	bool lastPerm = true;

	while (lastPerm)
	{	
		for (int j = 0; j <= N - 2; j++)
		{
			string left="";

			for (int ind = 0; ind <= j; ind++) left+= tables[ind];
			Partition(left, _predicate);
			
			string right="";
			
			for (int ind = j+1; ind < N; ind++) right+= tables[ind];
			Partition(right, _predicate);

			sort(left.begin(), left.end());
			sort(right.begin(), right.end());

			cost = Map[left].cost + Map[right].cost;
			
			if (j!=0) cost += Map[left].size;
			if (j!=N-2) cost += Map[right].size;

			if (cost < min_cost){

				sch1 = Map[left].sch;
				sch2 = Map[right].sch;
				unsigned long long Va,Vb, div;
				CNF cnf;
				
				cnf.ExtractCNF (*_predicate, sch1, sch2);

				vector <Attribute> atts1 = sch1.GetAtts();					
				vector <Attribute> atts2 = sch2.GetAtts();	
				
				if (cnf.numAnds >0)
				{ 
					for (int i = 0 ; i < cnf.numAnds; i++)
					{
						if (cnf.andList[i].operand1 == Left)
						{
							Va = atts1[cnf.andList[i].whichAtt1].noDistinct;
							Vb = atts2[cnf.andList[i].whichAtt2].noDistinct;
						}

						if (cnf.andList[i].operand1 == Right)
						{
							Va = atts1[cnf.andList[i].whichAtt2].noDistinct;
							Vb = atts2[cnf.andList[i].whichAtt1].noDistinct;
						}
					
						if (Va > Vb) div = Va; else div = Vb;	// Max [Va,Vb]
					}
				}

				if (cnf.numAnds == 0)	div = 1;

				size = Map[left].size*Map[right].size/div;
				//order = "(" + Map[left].order + "," + Map[right].order + ")";
				order = Map[left].order + "," + Map[right].order;
				min_cost = cost;

				temp = sch1;
				temp.Append(sch2);
			}
		}
	
		lastPerm = permutation(tables);
	}

	Map[copy].order = order;
	Map[copy].size = size;
	Map[copy].cost = min_cost;
	Map[copy].sch = temp;
	
}

bool QueryOptimizer::permutation(string& array)
{
	int k = -1, l = 0;
	for (int i = 0; i < array.size()-1; i++)
		if(array[i]<array[i+1]) k = i;	
	
	if (k == -1) return false;		
	
	for (int i = k+1; i < array.size(); i++)
		if(array[k]<array[i]) l = i;	
	
	swap(array[k],array[l]);

	int count = 1, temp = k+1;
	for (int i=0; i<(array.size()-k-1)/2; i++,count++,temp++)
		swap(array[temp],array[array.size()-count]);

	return true;
}


