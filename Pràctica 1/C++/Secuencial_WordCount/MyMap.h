#ifndef MYMAP_H_
#define MYMAP_H_

#include "Map.h"
#include "Reduce.h"

#include <functional>
#include <string>



class MyMap 
{
	char *InputPath;
	char *OutputPath;
	TMapFunction MapFunction;
	TReduceFunction ReduceFunction;
		
	vector<PtrMap> Mappers;
	vector<PtrReduce> Reducers;


	public:
		MyMap(char * input, char *output, TMapFunction map, TReduceFunction reduce, int nreducers=1);
		TError Run();

	private:
		TError Split(char *input);
		TError Map();
		TError Suffle();
		TError Reduce();
		
		inline void AddMap(PtrMap map) { Mappers.push_back(map); };
		inline void AddReduce(PtrReduce reducer) { Reducers.push_back(reducer); };
};
typedef class MyMap TMyMap, *PtrMyMap;


#endif /* MYMAP_H_ */
