#include "MapReduce.h"
#include "Types.h"

#include <dirent.h>
#include <string.h>

using namespace std;

// Constructor MapReduce: directorio/fichero entrada, directorio salida, función Map, función reduce y número de reducers a utilizar.
MapReduce::MapReduce(char * input, char *output, TMapFunction mapf, TReduceFunction reducef, int nreducers)
{
	MapFunction=mapf;
	ReduceFunction=reducef;
	InputPath = input;
	OutputPath = output;

	for(int x=0;x<nreducers;x++)
	{
		char filename[256];

		sprintf(filename, "%s/result.r%d", OutputPath, x+1);
		AddReduce(new TReduce(ReduceFunction, filename));
	}
}


// Procesa diferentes fases del framework mapreduc: split, map, suffle/merge, reduce.
TError 
MapReduce::Run()
{
	if (Split(InputPath)!=COk)
		error("MapReduce::Run-Error Split");

	if (Map()!=COk)
		error("MapReduce::Run-Error Map");

	if (Suffle()!=COk)
		error("MapReduce::Run-Error Merge");

	if (Reduce()!=COk)
		error("MapReduce::Run-Error Reduce");

	return(COk);
}


// Genera y lee diferentes splits: 1 split por fichero.
// Versión secuencial: asume que un único Map va a procesar todos los splits.
TError 
MapReduce::Split(char *input)
{
	DIR *dir;
	struct dirent *entry;
	unsigned char isFile =0x8;
	char input_path[256];

	PtrMap map = new TMap(MapFunction);
	AddMap(map);

	if ((dir=opendir(input))!=NULL) 
	{
  		/* Read all the files and directories within directory */
  		while ((entry=readdir(dir))!=NULL) 
		{
			if( strcmp(entry->d_name, ".")!=0 && strcmp(entry->d_name, "..")!=0 && entry->d_type == isFile ) 
			{
		    	printf ("Processing input file %s\n", entry->d_name);
				sprintf(input_path,"%s/%s",input, entry->d_name);
				map->ReadFileTuples(input_path);
			}
  		}
  		closedir(dir);
	} 
	else 
	{
		if (errno==ENOTDIR)
		{	// Read only a File
			if (map->ReadFileTuples(input)!=COk)
			{
				error("MapReduce::Split - Error could not open file");
				return(CErrorOpenInputDir);
			}	
		}
		else 
		{
			error("MapReduce::Split - Error could not open directory");
			return(CErrorOpenInputDir);
		}
	}

	return(COk);
}


// Ejecuta cada uno de los Maps.
TError 
MapReduce::Map()
{
	for(vector<TMap>::size_type m = 0; m != Mappers.size(); m++) 
	{
		if (debug) printf ("DEBUG::Running Map %d\n", (int)m+1);
		if (Mappers[m]->Run()!=COk)
			error("MapReduce::Map Run error.\n");
	}
	return(COk);
}

// Ordena y junta todas las tuplas de salida de los maps. Utiliza una función de hash como 
// función de partición, para distribuir las claves entre los posibles reducers.
// Utiliza un multimap para realizar la ordenación/unión.
TError 
MapReduce::Suffle()
{
	TMapOuputIterator it2;

	for(vector<TMap>::size_type m = 0; m != Mappers.size(); m++) 
	{
		 multimap<string, int> output = Mappers[m]->getOutput();

		// Process all mapper outputs
		for (TMapOuputIterator it1=output.begin(); it1!=output.end(); it1=it2)
		{
			TMapOutputKey key = (*it1).first;
			pair<TMapOuputIterator, TMapOuputIterator> keyRange = output.equal_range(key);

			// Calcular a que reducer le corresponde está clave:
			int r = std::hash<TMapOutputKey>{}(key)%Reducers.size();

			if (debug) printf ("DEBUG::MapReduce::Suffle merge key %s to reduce %d.\n", key.c_str(), r);

			// Añadir todas las tuplas de la clave al reducer correspondiente.
			Reducers[r]->AddInputKeys(keyRange.first, keyRange.second);

			// Eliminar todas las entradas correspondientes a esta clave.
	        //for (it2 = keyRange.first;  it2!=keyRange.second;  ++it2)
	        //   output.erase(it2);
		output.erase(keyRange.first,keyRange.second);
		it2=keyRange.second;
		}	
	}
	return(COk);
}


// Ejecuta cada uno de los Reducers.
TError 
MapReduce::Reduce()
{
	for(vector<TReduce>::size_type m = 0; m != Reducers.size(); m++) 
	{
		if (Reducers[m]->Run()!=COk)
			error("MapReduce::Reduce Run error.\n");
	}
	return(COk);
}

