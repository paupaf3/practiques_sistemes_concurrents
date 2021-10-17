#include "Types.h"
#include "MapReduce.h"

#include <sstream> 
#include <stdlib.h>

using namespace std;

TError MapWordCount(PtrMap, TMapInputTuple tuple);
TError ReduceWordCount(PtrReduce, TReduceInputKey key, TReduceInputIterator begin, TReduceInputIterator end);

int main(int argc, char* argv[])
{
	char *input_dir, *output_dir;
	
	// Procesar argumentos.
	if (argc!=3)
		error("Error in arguments: WordCount <input dir> <ouput dir>.\n");
	input_dir=argv[1];
	output_dir=argv[2];

	PtrMapReduce mapr = new TMapReduce(input_dir, output_dir, MapWordCount, ReduceWordCount);
	if (mapr==NULL)
		error("Error new MapReduce.\n");
	
	mapr->Run();
	
	exit(0);
}

// Word Count Map.
TError MapWordCount(PtrMap map, TMapInputTuple tuple)
{
	string value = tuple.getValue();

	if (debug) printf ("DEBUG::MapWordCount procesing tuple %ld->%s\n",tuple.getKey(), tuple.getValue().c_str());
		
	// Convertir todos los posibles separadores de palabras a espacios.
	for (int i=0; i<value.length(); i++)
	{
    	if (value[i] == ':' || value[i] == '.' || value[i] == ';' || value[i] == ',' || 
			value[i] == '"' || value[i] == '\'' || value[i] == '(' || value[i] == ')' || 
			value[i] == '[' || value[i] == ']' || value[i] == '?' || value[i] == '!' || 
			value[i] == '%' || value[i] == '<' || value[i] == '>' || value[i] == '-' || 
			value[i] == '_' || value[i] == '#' || value[i] == '*' || value[i] == '/')
        	value[i] = ' ';
	}

	stringstream ss;
	string temp;
	ss.str(value);
	// Emit map result (word,'1').
	while (ss >> temp) 
	    map->EmitResult(temp,1);

	return(COk);
}


// Word Count Reduce.
TError ReduceWordCount(PtrReduce reduce, TReduceInputKey key, TReduceInputIterator begin, TReduceInputIterator end)
{
	TReduceInputIterator it;
	int totalCount=0;

	if (debug) printf ("DEBUG::ReduceWordCount key %s ->",key.c_str());

	// Procesar todas los valores para esta clave.
	for (it=begin; it!=end; it++) {
		if (debug) printf (" %d",it->second);
		totalCount += it->second;
	}

	if (debug) printf (".\n");

	reduce->EmitResult(key, totalCount);

	return(COk);
}


