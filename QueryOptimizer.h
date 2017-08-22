#ifndef _QUERY_OPTIMIZER_H
#define _QUERY_OPTIMIZER_H

#include "Schema.h"
#include "Catalog.h"
#include "ParseTree.h"
#include "RelOp.h"

#include <string>
#include <vector>

using namespace std;


// data structure used by the optimizer to compute join ordering
struct OptimizationTree {
	// list of tables joined up to this node
	vector<string> tables;
	// number of tuples in each of the tables (after selection predicates)
	vector<int> tuples;
	// number of tuples at this node
	unsigned long long noTuples;

	// connections to children and parent
	OptimizationTree* parent;
	OptimizationTree* leftChild;
	OptimizationTree* rightChild;
};

class QueryOptimizer {
private:
	Catalog* catalog;
	
	struct plan{
		unsigned long long size;
		unsigned long long cost;
		string order;
		Schema sch;
	};
	
	map <string, plan> Map;
	map <string, string> mapping;

public:
	QueryOptimizer(Catalog& _catalog);
	virtual ~QueryOptimizer();

	void Optimize(TableList* _tables, AndList* _predicate, OptimizationTree*& _root);
	bool permutation(string& out);
	void Partition(string tables, AndList* _predicate);
	void treeGenerator(string tabList, OptimizationTree* & root);
	void treeDisp(OptimizationTree* & root);
};

#endif // _QUERY_OPTIMIZER_H
