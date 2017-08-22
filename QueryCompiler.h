#ifndef _QUERY_COMPILER_H
#define _QUERY_COMPILER_H

/* Take as input the query tokens produced by the query parser and generate
 * the query execution tree. This requires instantiating relational operators
 * with the correct parameters, based on the query.
 * Two issues have to be addressed:
 *  1) Identify the schema(s) for each operator.
 *  2) Identify the parameters of the operation the operator is executing.
 *     For example, identify the predicate in a SELECT. Or the JOIN PREDICATE.
 */
#include "Catalog.h"
#include "ParseTree.h"
#include "QueryOptimizer.h"
#include "RelOp.h"

using namespace std;


class QueryCompiler {
private:
	Catalog* catalog;
	QueryOptimizer* optimizer;

	map <string, Scan> scanz;	
	map <string, Select> selectz;

	map <int, vector<Record>> mapindex;

	vector<ScanIndex> si;

	int checkindex;

public:
	QueryCompiler(Catalog& _catalog, QueryOptimizer& _optimizer);
	virtual ~QueryCompiler();

	void Compile(int& pNum, AttList* _attsToCreate, int& _queryType, TableList* _tables, NameList* _attsToSelect,
		FuncOperator* _finalFunction, AndList* _predicate,
		NameList* _groupingAtts, int& _distinctAtts,
		QueryExecutionTree& _queryTree);

	RelationalOp* constTree(OptimizationTree* root, AndList* _predicate);
};

#endif // _QUERY_COMPILER_H
