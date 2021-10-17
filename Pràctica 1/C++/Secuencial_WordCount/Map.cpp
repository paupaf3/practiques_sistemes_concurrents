#include "Map.h"
#include "Types.h"

#include <fstream>      // std::ifstream


// Lee fichero de entrada (split) línea a línea y lo guarda en una cola del Map en forma de
// tuplas (key,value).
TError 
Map::ReadFileTuples(char * fileName)
{
	ifstream file(fileName);
	string str;
    streampos Offset=0;

    if(!file.is_open())
        return(CErrorOpenInputFile);
    
    while (std::getline(file, str))
	{
		if (debug) printf("DEBUG::Map input %d -> %s\n",(int)Offset, str.c_str());
        AddInput(new TMapInputTuple((TMapInputKey)Offset, str));
        Offset=file.tellg();
	}

	file.close();

	return(COk);
}


void
Map::AddInput(PtrMapInputTuple tuple)
{
	Input.push(tuple);
}


// Ejecuta la tarea de Map: recorre la cola de tuplas de entrada y para cada una de ellas
// invoca a la función de Map especificada por el programador.
TError 
Map::Run() 
{
	TError err;

	while (!Input.empty())
	{

		if (debug) printf ("DEBUG::Map process input tuple %ld -> %s\n",(Input.front())->getKey(), (Input.front())->getValue().c_str());
		err = MapFunction(this, *(Input.front()));
		if (err!=COk)
			return(err);

		Input.pop();
	}

	return(COk);
}



// Función para escribir un resultado parcial del Map en forma de tupla (key,value)
void
Map::EmitResult(TMapOutputKey key, TMapOutputValue value)
{
	if (debug) printf ("DEBUG::Map emit result %s -> %d\n", key.c_str(), value);
	Output.insert(TMapOuptTuple(key,value));
}
