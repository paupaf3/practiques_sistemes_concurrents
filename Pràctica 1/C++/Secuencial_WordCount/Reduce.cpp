#include "Reduce.h"
#include "Types.h"

#include <fstream>      // std::ifstream


// Constructor para una tarea Reduce, se le pasa la función que reducción que tiene que 
// ejecutar para cada tupla de entrada y el nombre del fichero de salida en donde generará 
// los resultados.
Reduce::Reduce(TReduceFunction reduceFunction, string OutputPath)
{
	ReduceFunction = reduceFunction;
	if (debug) printf ("DEBUG::Creating output file %s\n",OutputPath.c_str());

	OutputFile.open(OutputPath, std::ofstream::out | std::ofstream::trunc);
	if (!OutputFile.is_open()) 
		error("Reduce::Reduce Error opening " + OutputPath + " output file.");
}


// Destructor tareas Reduce: cierra fichero salida.
Reduce::~Reduce()
{
	OutputFile.close();
}


// Función para añadir las tuplas de entrada para la función de redución en forma de lista de 
// tuplas (key,value).
void
Reduce::AddInputKeys(TMapOuputIterator begin, TMapOuputIterator end)
{
	TMapOuputIterator it;

	for (it = begin;  it != end; it++)
	{
        AddInput(it->first, it->second);
	}
}


void
Reduce::AddInput(TReduceInputKey key, TReduceInputValue value)
{
	if (debug) printf ("DEBUG::Reduce add input %s -> %d\n",key.c_str(), value);
	Input.insert(TReduceInputTuple(key,value));
}


// Función de ejecución de la tarea Reduce: por cada tupla de entrada invoca a la función 
// especificada por el programador, pasandolo el objeto Reduce, la clave y la lista de 
// valores.
TError 
Reduce::Run() 
{
	TError err;
	TReduceInputIterator it2;

	// Process all reducer inputs
	for (TReduceInputIterator it1=Input.begin(); it1!=Input.end(); it1=it2)
	{
		TReduceInputKey key = (*it1).first;
		pair<TReduceInputIterator, TReduceInputIterator> keyRange = Input.equal_range(key);

		err = ReduceFunction(this, key, keyRange.first, keyRange.second);
		if (err!=COk)
			return(err);

        //for (it2 = keyRange.first;  it2!=keyRange.second;  ++it2)
        //   Input.erase(it2);
	Input.erase(keyRange.first,keyRange.second);
	it2=keyRange.second;
	  
	}	


	return(COk);
}


// Función para escribir un resulta en el fichero de salida.
void
Reduce::EmitResult(TReduceOutputKey key, TReduceOutputValue value)
{
	OutputFile << key << " " << value << endl;
}

