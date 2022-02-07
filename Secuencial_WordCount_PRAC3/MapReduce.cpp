/* ---------------------------------------------------------------
Práctica 3.
Código fuente: MapReduce.cpp
Grau Informàtica
49258834X - Pau Agustí Fernandez
48053637J - Dand Marbà Sera
------------------------------------------------------------------ */
#include "MapReduce.h"
#include "Types.h"

#include <dirent.h>
#include <string.h>
#include <pthread.h>

using namespace std;

struct Data{
		int id;
		dirent *entry;
		PtrMap map;
		char input[256];
		vector<PtrReduce> reducers;

}data;

struct DataReduce{
		int id;
		PtrReduce reducer;
}dataReduce;

// crear el struc para estadisticas 
struct structSplit {
	long int nFicheros=0;
	long int  nBytes=0;
	long int  nLineas=0;
	long int  nTuplasEntradaGeneradas=0;
};


struct structMap {
	long int  nBytesGeneradas;
	long int  nTuplasEntradaProcesadas=0;
	long int  nTuplasSalidaGeneradas=0;
};

struct structSuffle {
	long int  nTuplasEntradaProcesadas=0;
	long int  nClavesProcesadas=0;
};

struct structReduce {
	long int nClavesDiferentesProcesadas=0;
	long int nOcurrenciasProcesadas=0;
	long int valorMedio=0;
	long int bytes=0;
};
struct Estadisticas {
	structSplit splitEstadisticas;
	structMap mapEstadisticas;
	structSuffle suffleEstadisticas;
	structReduce reduceEstadisticas;
};

struct Estadisticas e_global; 

// crear el Mutex 
pthread_mutex_t mutex;
pthread_cond_t Join;
pthread_barrier_t Barrera;
//contador global para hacer el "join"
int contJoin= 0;


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
	pthread_mutex_init(&mutex, NULL);
	pthread_cond_init(&Join,NULL);

	if (Split(InputPath)!=COk)
		error("MapReduce::Run-Error Split");

	if (Reduce()!=COk)
		error("MapReduce::Run-Error Reduce");


	pthread_cond_destroy(&Join);
	pthread_mutex_destroy(&mutex);

	return(COk);
}


// Thread  de  split
void * splitThread ( void * arg ) {
	// convertir
	struct Data *d;
	d = ( struct Data *)arg;
	// variables
	char input_path[256];

	
	// pthread_barrier_wait(&Barrera);
	if(d->id ==0 ){
		pthread_mutex_lock(&mutex);
		printf("+++++++++++++++++++++++++++++++ ETAPA SPLIT ++++++++++++++++++++++++++++++++\n");

		pthread_mutex_unlock( &mutex);
	}
	pthread_barrier_wait(&Barrera);	


	struct returnDades dadesLocales;			
	sprintf(input_path,"%s/%s",d->input, d->entry->d_name);

	dadesLocales = d->map->ReadFileTuples(input_path);

	
	pthread_mutex_lock(&mutex);
		e_global.splitEstadisticas.nFicheros+=1;
		e_global.splitEstadisticas.nBytes=e_global.splitEstadisticas.nBytes+dadesLocales.nBytesLocal;
		e_global.splitEstadisticas.nTuplasEntradaGeneradas=e_global.splitEstadisticas.nTuplasEntradaGeneradas+dadesLocales.nTuplasEntradaLocal;	
		e_global.splitEstadisticas.nLineas=e_global.splitEstadisticas.nLineas+dadesLocales.nLineasLocal;
		printf("\n+++++++++++++++++++++++++++ Thread %d+++++++++++++++++++++++++++\nNúmero de ficheros leídos: 1 de %d\nNúmero total de bytes leídos:%d",d->id, e_global.splitEstadisticas.nFicheros, dadesLocales.nBytesLocal);
		printf("\nNúmero de líneas:%ld\nNúmero de tuplas de entrada generadas:%ld\n",dadesLocales.nTuplasEntradaLocal,dadesLocales.nLineasLocal);
	pthread_mutex_unlock( &mutex);
		
	pthread_barrier_wait(&Barrera);
		if(d->id == 0 ){
		pthread_mutex_lock(&mutex);
				printf("\n+++++++++++++++++++++++++++ Global +++++++++++++++++++++++++++\nNúmero de ficheros leídos: %d\nNúmero total de bytes leídos:%d \nNúmero de líneas:%d \nNúmero de tuplas de entrada generadas:%d \n",e_global.splitEstadisticas.nFicheros, e_global.splitEstadisticas.nBytes, e_global.splitEstadisticas.nLineas, e_global.splitEstadisticas.nTuplasEntradaGeneradas);	
		pthread_mutex_unlock( &mutex);
		}

	
		if(d->id ==0 ){
		pthread_mutex_lock(&mutex);
		printf("\n\n+++++++++++++++++++++++++++++++ ETAPA MAP ++++++++++++++++++++++++++++++++\n");

		pthread_mutex_unlock( &mutex);
	}
	pthread_barrier_wait(&Barrera);	

	struct returnDadesMap dadesMapLocales;
	dadesMapLocales=d->map->Run();

	pthread_mutex_lock(&mutex);
		e_global.mapEstadisticas.nBytesGeneradas=e_global.mapEstadisticas.nBytesGeneradas+dadesMapLocales.nBytesGeneradas;
		e_global.mapEstadisticas.nTuplasEntradaProcesadas=e_global.mapEstadisticas.nTuplasEntradaProcesadas+dadesMapLocales.nTuplasEntradaProcesadas;
		e_global.mapEstadisticas.nTuplasSalidaGeneradas=e_global.mapEstadisticas.nTuplasSalidaGeneradas+dadesMapLocales.nTuplasSalidaGeneradas;
		printf("\n+++++++++++++++++++++++++++ Thread %d +++++++++++++++++++++++++++\nNúmero de Tuplas de entrada procesadas: %d\nNúmero de bytes procesados:%d \nNúmero de tuplass de salida generadas:%d \n",d->id,dadesMapLocales.nTuplasEntradaProcesadas,dadesMapLocales.nBytesGeneradas,dadesMapLocales.nTuplasSalidaGeneradas);
	pthread_mutex_unlock( &mutex);


	pthread_barrier_wait(&Barrera);
		if(d->id ==0 ){
		pthread_mutex_lock(&mutex);
		printf("\n+++++++++++++++++++++++++++ Global +++++++++++++++++++++++++++\nNúmero de Tuplas de entrada procesadas: %d\nNúmero de bytes procesados:%d \nNúmero de tuplass de salida generadas:%d \n",e_global.mapEstadisticas.nTuplasEntradaProcesadas,e_global.mapEstadisticas.nBytesGeneradas,e_global.mapEstadisticas.nTuplasSalidaGeneradas);
		pthread_mutex_unlock( &mutex);
	}

	
		if(d->id ==0 ){
			pthread_mutex_lock(&mutex);
			printf("\n\n+++++++++++++++++++++++++++++++ ETAPA SUFFLE ++++++++++++++++++++++++++++++++\n");
			pthread_mutex_unlock( &mutex);
		}
	pthread_barrier_wait(&Barrera);
		
	TMapOuputIterator it2;

	multimap<string, int> output = d->map->getOutput();
		int tuplalocal = 0;
		int clavelocal = 0;
		// Process all mapper outputs
		for (TMapOuputIterator it1=output.begin(); it1!=output.end(); it1=it2)
		{
			TMapOutputKey key = (*it1).first;
			pair<TMapOuputIterator, TMapOuputIterator> keyRange = output.equal_range(key);

			// Calcular a que reducer le corresponde está clave:
			int r = std::hash<TMapOutputKey>{}(key)%d->reducers.size();

			//printf ("DEBUG::MapReduce::Suffle merge key %s to reduce %d.\n", key.c_str(), r);

			// Añadir todas las tuplas de la clave al reducer correspondiente.
			//boqueo aqui	
			pthread_mutex_lock( &mutex );
			d->reducers[r]->AddInputKeys(keyRange.first, keyRange.second); // TODO mirar que sea global Reducers
			//desbloqueo aaqui
			e_global.suffleEstadisticas.nTuplasEntradaProcesadas+=1;
			tuplalocal++;
			pthread_mutex_unlock( &mutex);
			e_global.suffleEstadisticas.nClavesProcesadas+=1;
			clavelocal++;
			// Eliminar todas las entradas correspondientes a esta clave.
	        //for (it2 = keyRange.first;  it2!=keyRange.second;  ++it2)
	        //   output.erase(it2);
			output.erase(keyRange.first,keyRange.second);
			it2=keyRange.second;
		}
		printf("\n+++++++++++++++++++++++++++ Thread %d +++++++++++++++++++++++++++\nNúmero de tuplas de salida procesadas: %d\nNúmero total de claves procesadas: %d\n",d->id, tuplalocal,clavelocal);

		pthread_barrier_wait(&Barrera);
		if(d->id ==0 ){
			pthread_mutex_lock(&mutex);
			printf("\n+++++++++++++++++++++++++++ Global +++++++++++++++++++++++++++\nNúmero de tuplas de salida procesadas: %d\nNúmero total de claves procesadas: %d\n",e_global.suffleEstadisticas.nTuplasEntradaProcesadas,e_global.suffleEstadisticas.nClavesProcesadas);
			pthread_mutex_unlock( &mutex);
		}
		
	pthread_mutex_lock(&mutex);
	contJoin++; //manda la señal una vez el thread ha incrementado el valor de la variable
	pthread_cond_signal(&Join);
	pthread_mutex_unlock(&mutex);
 
}

int getNumberFiles(char* input){
	int count=0;
	DIR *dir;
	struct dirent *entry;
	if ((dir=opendir(input))!=NULL)
	{
		while ((entry=readdir(dir))!=NULL){
			if( strcmp(entry->d_name, ".")!=0 && strcmp(entry->d_name, "..")!=0 && entry->d_type == 0x8 ) 
			{
				count++;
			}
		}
		closedir(dir);
	}else{
		perror("Error leer directorio\n");
		return 1;
	}
	return count;
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

	int nFiles=getNumberFiles(input);
	struct Data data[nFiles];
	pthread_t split[nFiles];
	int n_thread = 0;
	pthread_barrier_init(&Barrera,NULL,nFiles);
	if ((dir=opendir(input))!=NULL) 
	{
  		/* Read all the files and directories within directory */
  		while ((entry=readdir(dir))!=NULL) 
		{
			if( strcmp(entry->d_name, ".")!=0 && strcmp(entry->d_name, "..")!=0 && entry->d_type == isFile ) 
			{
				
				data[n_thread].entry= entry;
				strcpy( data[n_thread].input, input );
				PtrMap map = new TMap(MapFunction);
				//AddMap(map);
				data[n_thread].map=map;
				data[n_thread].reducers=Reducers;
				data[n_thread].id = n_thread;
				pthread_create (& split[n_thread] , NULL , splitThread , (void *)&data[n_thread] );
				n_thread++;
				
			}
  		}
  		closedir(dir);
	} 
	else 
	{
		if (errno==ENOTDIR)
		{	// Read only a File

			data[0].map = new TMap(MapFunction);
			pthread_create (& split[0] , NULL , splitThread , (void *)&data[0] );
				
		}
		else 
		{
			error("MapReduce::Split - Error could not open directory");
			return(CErrorOpenInputDir);
		}
	}

	

	pthread_mutex_lock(&mutex);
	while(contJoin < nFiles){
		pthread_cond_wait(&Join,&mutex);
	}
	pthread_mutex_unlock(&mutex);
	contJoin=0;

	pthread_barrier_destroy(&Barrera);

	return(COk);
}


//Función para llamar al Run de cada reducer en el thread.
void * runReducer(void * arg ){

	struct DataReduce *dR;
	dR = ( struct DataReduce *)arg;

	
	if(dR->id ==0 ){
		pthread_mutex_lock(&mutex);
		printf("\n\n+++++++++++++++++++++++++++++++ ETAPA REDUCE ++++++++++++++++++++++++++++++++\n");
		pthread_mutex_unlock( &mutex);
	}
	pthread_barrier_wait(&Barrera);	
	
	
	struct returnDadesReduce dadesLocalesReducers;			

	dadesLocalesReducers = dR->reducer->Run();
	
	
	
	pthread_mutex_lock(&mutex);
		printf("\n+++++++++++++++++++++++++++ Thread %d+++++++++++++++++++++++++++\nNúmero de claves diferentes procesadas: %d\nNúmero de ocurrencias procesadas:%d \nValor medio ocurrencias/clave:%ld \nNúmero bytes escritos de salida:%d \n",dR->id, dadesLocalesReducers.nClaverLocal, dadesLocalesReducers.nOcurrenciaLocal, dadesLocalesReducers.nMitjanaLocal, dadesLocalesReducers.nBytesLocal);
		e_global.reduceEstadisticas.nClavesDiferentesProcesadas=e_global.reduceEstadisticas.nClavesDiferentesProcesadas+dadesLocalesReducers.nClaverLocal;
		e_global.reduceEstadisticas.nOcurrenciasProcesadas=e_global.reduceEstadisticas.nOcurrenciasProcesadas+dadesLocalesReducers.nOcurrenciaLocal;
		e_global.reduceEstadisticas.bytes=e_global.reduceEstadisticas.bytes+dadesLocalesReducers.nBytesLocal;
		e_global.reduceEstadisticas.valorMedio=e_global.reduceEstadisticas.valorMedio+dadesLocalesReducers.nMitjanaLocal;
	pthread_mutex_unlock( &mutex);
	

	pthread_barrier_wait(&Barrera);
		if(dR->id ==0 ){
			pthread_mutex_lock(&mutex);
			printf("\n+++++++++++++++++++++++++++ Global +++++++++++++++++++++++++++\nNúmero de claves diferentes procesadas: %d\nNúmero de ocurrencias procesadas:%d \nValor medio ocurrencias/clave:%ld \nNúmero bytes escritos de salida.:%d \n",e_global.reduceEstadisticas.nClavesDiferentesProcesadas, e_global.reduceEstadisticas.nOcurrenciasProcesadas,e_global.reduceEstadisticas.valorMedio, e_global.reduceEstadisticas.bytes);
			pthread_mutex_unlock( &mutex);
		}
		
		


	//Realiza la funcion del join
	pthread_mutex_lock(&mutex);
	contJoin++; //manda la señal una vez el thread ha incrementado el valor de la variable
	pthread_cond_signal(&Join);
	pthread_mutex_unlock(&mutex);
	
}

// Ejecuta cada uno de los Reducers.
TError 
MapReduce::Reduce()
{	
	struct DataReduce dataReduce[Reducers.size()];
	pthread_t reducersT [Reducers.size()];//tantos threads como reducers.
	pthread_barrier_init(&Barrera,NULL,Reducers.size());
	for(vector<TReduce>::size_type m = 0; m != Reducers.size(); m++) 
	{
		dataReduce[(int)m].reducer=Reducers[m];
		dataReduce[(int)m].id=(int)m;
		pthread_create(&reducersT[m], NULL,  runReducer, (void *)&dataReduce[m]);		
	}

	pthread_mutex_lock(&mutex);
	while(contJoin < Reducers.size()){
		pthread_cond_wait(&Join,&mutex);
	}
	pthread_mutex_unlock(&mutex);

	pthread_barrier_destroy(&Barrera);
	return(COk);
			
}

