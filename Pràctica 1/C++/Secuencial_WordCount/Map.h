#ifndef MAP_H_
#define MAP_H_

#include "Types.h"

#include <string>
#include "MyQueue.h"
#include <map>

using namespace std;


typedef long int TMapInputKey, *PtrMapInputKey;
typedef string TMapInputValue, *PtrMapInputValue;

class MapInputTuple
{
	TMapInputKey Key;
	TMapInputValue Value;

	public:
    	MapInputTuple(TMapInputKey key, TMapInputValue value):Key(key),Value(value) {}
		
        inline TMapInputKey getKey() { return(Key); };
		inline TMapInputValue getValue() { return(Value); };
};
typedef class MapInputTuple TMapInputTuple, *PtrMapInputTuple;


typedef string TMapOutputKey, *PtrMapOutputKey;
typedef int TMapOutputValue, *PtrMapOutputValue;

typedef pair<TMapOutputKey, TMapOutputValue> TMapOuptTuple;



class Map
{
	TError (*MapFunction) (class Map *, TMapInputTuple);

	MyQueue<PtrMapInputTuple> Input;
	multimap<string, int> Output;

	public:
		Map(TError (*mapFunction) (class Map *, TMapInputTuple) ):MapFunction(mapFunction){};
		
		inline multimap<TMapOutputKey, TMapOutputValue> getOutput() { return(Output); };

		TError ReadFileTuples(char * file);
		TError Run(); 
		void EmitResult(TMapOutputKey key, TMapOutputValue value);

	private:
		void AddInput(PtrMapInputTuple tuple);
};
typedef class Map TMap, *PtrMap;


typedef multimap<TMapOutputKey, TMapOutputValue>::const_iterator TMapOuputIterator;

typedef TError (*TMapFunction)(class Map*, TMapInputTuple) ;

#endif /* MAP_H_ */


