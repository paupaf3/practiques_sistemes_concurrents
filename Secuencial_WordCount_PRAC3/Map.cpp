/* ---------------------------------------------------------------
Práctica 3.
Código fuente: MapReduce.cpp
Grau Informàtica
49258834X - Pau Agustí Fernandez
48053637J - Dand Marbà Sera
------------------------------------------------------------------ */

#include "Map.h"
#include "Types.h"

#include <fstream>      // std::ifstream


// Lee fichero de entrada (split) línea a línea y lo guarda en una cola del Map en forma de
// tuplas (key,value).



struct returnDades Map::ReadFileTuples(char * fileName)
{
	ifstream file(fileName);
	string str;
    streampos Offset=0;
	struct returnDades d;

    if(!file.is_open()){
		d.nLineasLocal=0;
		d.nBytesLocal=0;
		d.nTuplasEntradaLocal=0;
		d.error=CErrorOpenInputFile;
		return(d);
	}
    
    while (std::getline(file, str))
	{
		if (debug) printf("DEBUG::Map input %d -> %s\n",(int)Offset, str.c_str());
        AddInput(new TMapInputTuple((TMapInputKey)Offset, str));
        Offset=file.tellg();
		d.nLineasLocal=d.nLineasLocal+1;
		d.nTuplasEntradaLocal=d.nTuplasEntradaLocal+1;
		d.nBytesLocal = d.nBytesLocal + (Input.front())->getValue().size();
		
	}
	

	file.close();
	d.error=COk;
	

	return(d);
}


void
Map::AddInput(PtrMapInputTuple tuple)
{
	Input.push(tuple);
}


// Ejecuta la tarea de Map: recorre la cola de tuplas de entrada y para cada una de ellas
// invoca a la función de Map especificada por el programador.
struct returnDadesMap Map::Run() 
{
	TError err;
	struct returnDadesMap d; 
	while (!Input.empty())
	{

		if (debug) printf ("DEBUG::Map process input tuple %ld -> %s\n",(Input.front())->getKey(), (Input.front())->getValue().c_str());
		d.nTuplasEntradaProcesadas=d.nTuplasEntradaProcesadas+1;
		d.nBytesGeneradas = d.nBytesGeneradas + (Input.front())->getValue().size();
		err = MapFunction(this, *(Input.front()));
		if (err!=COk){
			d.nTuplasSalidaGeneradas=Output.size();
			d.error=err;
			return(d);
		}

		Input.pop();
	}
	d.nTuplasSalidaGeneradas=Output.size();
	return(d);
}



// Función para escribir un resultado parcial del Map en forma de tupla (key,value)
void
Map::EmitResult(TMapOutputKey key, TMapOutputValue value)
{
	if (debug) printf ("DEBUG::Map emit result %s -> %d\n", key.c_str(), value);
	Output.insert(TMapOuptTuple(key,value));
}
