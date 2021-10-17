#ifndef MAPREDUCE_H_
#define MAPREDUCE_H_

#include "Map.h"
#include "Reduce.h"

#include <functional>
#include <string>



class MapReduce 
{
	char *InputPath;
	char *OutputPath;
	TMapFunction MapFunction;
	TReduceFunction ReduceFunction;
		
	vector<PtrMap> Mappers;
	vector<PtrReduce> Reducers;


	public:
		MapReduce(char * input, char *output, TMapFunction map, TReduceFunction reduce, int nreducers=1);
		TError Run();

	private:
		TError Split(char *input);
		TError Map();
		TError Suffle();
		TError Reduce();
		
		inline void AddMap(PtrMap map) { Mappers.push_back(map); };
		inline void AddReduce(PtrReduce reducer) { Reducers.push_back(reducer); };
};
typedef class MapReduce TMapReduce, *PtrMapReduce;


#endif /* MAPREDUCE_H_ */
