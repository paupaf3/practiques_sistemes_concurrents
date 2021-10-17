#ifndef REDUCE_H_
#define REDUCE_H_

#include "Map.h"
#include "Types.h"

#include <string>
#include <fstream> 
#include <queue>
#include <map>


using namespace std;

typedef string TReduceInputKey, *PtrReduceInputKey;
typedef int TReduceInputValue, *PtrReduceInputValue;

typedef pair<string, int> TReduceInputTuple;
typedef multimap<TReduceInputKey, TReduceInputValue>::const_iterator TReduceInputIterator;

typedef string TReduceOutputKey, *PtrReduceOutputKey;
typedef int TReduceOutputValue, *PtrReduceOutputValue;


class Reduce
{
	TError (*ReduceFunction) (class Reduce*, TReduceInputKey, TReduceInputIterator, TReduceInputIterator);
	ofstream OutputFile;

	multimap<TReduceInputKey, TReduceInputValue> Input;

	public:
		Reduce(TError (*reduceFunction) (class Reduce*, TReduceInputKey, TReduceInputIterator, TReduceInputIterator), string OutputPath);
		~Reduce();

		void AddInputKeys(TMapOuputIterator begin, TMapOuputIterator end);
		void AddInput(TReduceInputKey key, TReduceInputValue value);
		TError Run(); 
		void EmitResult(TReduceOutputKey key, TReduceOutputValue value);
};
typedef class Reduce TReduce, *PtrReduce;

typedef TError (*TReduceFunction) (PtrReduce, TReduceInputKey, TReduceInputIterator, TReduceInputIterator);

#endif /* REDUCE_H_ */

