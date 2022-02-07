/* ---------------------------------------------------------------
Práctica 2.
Código fuente: Reduce.java
Grau Informàtica
49258834X - Pau Agustí Fernandez
48053637J - Dand Marbà Sera
--------------------------------------------------------------- */

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Iterator;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.locks.Lock;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;
import statistics.StatisticsReduce;


class Reduce
{
	private MapReduce 	mapReduce;
	PrintWriter 		OutputFile;
	private ListMultimap<String, Integer> Input = Multimaps.synchronizedListMultimap(ArrayListMultimap.<String, Integer> create());

	// Constructor para una tarea Reduce, se le pasa la función que reducción que tiene que 
	// ejecutar para cada tupla de entrada y el nombre del fichero de salida en donde generará 
	// los resultados.
	public Reduce(MapReduce mapr, String OutputPath)
	{
		mapReduce = mapr;
		try
		{
			OutputFile = new PrintWriter(OutputPath);
		} 
		catch (FileNotFoundException e) 
		{
			System.err.println("Reduce::ERROR Open output file " + OutputPath + ".");
			e.printStackTrace();
		} 
	}


	// Finish: cierra fichero salida.
	public void Finish()
	{
		OutputFile.close();
	}


	// Función para añadir las tuplas de entrada para la función de redución en forma de lista de 
	// tuplas (key,value).
	public void AddInputKeys(String key, Integer value)
	{
		AddInput(key, value);
	}


	private synchronized void AddInput(String key, Integer value)
	{
		if (MapReduce.DEBUG)
			System.err.println("DEBUG::Reduce add input "+ key + "-> " + value);

		Input.put(key,value);
	}


	// Función de ejecución de la tarea Reduce: por cada tupla de entrada invoca a la función 
	// especificada por el programador, pasandolo el objeto Reduce, la clave y la lista de 
	// valores.
	public Error Run(CyclicBarrier barrier, StatisticsReduce globalStatisticsReduce, Lock lock)
	{
		StatisticsReduce localStatisticReduce = new StatisticsReduce();
		Iterator<String> keyIterator = Input.keySet().iterator();
		while(keyIterator.hasNext())
		{
			String key = keyIterator.next();

			// Evitem strings buits
			if(!key.trim().isEmpty())
			{
				globalStatisticsReduce.addToNumKeysSync();
				globalStatisticsReduce.addToNumBytesSync(key);
				localStatisticReduce.addToNumKeys();
				localStatisticReduce.addToNumBytes(key);

				Error err = mapReduce.Reduce(this, key, Input.get(key), globalStatisticsReduce, localStatisticReduce);
				if (err!=Error.COk)
					return(err);
			}

			keyIterator.remove();
		}

		lock.lock();
		localStatisticReduce.printStatistics("Estadísticas Locales Reduce");
		lock.unlock();

		Finish();

		try
		{
			barrier.await();
		}
		catch(InterruptedException | BrokenBarrierException e)
		{
			e.printStackTrace();
		}

		return(Error.COk);
	}


	// Función para escribir un resulta en el fichero de salida.
	public void EmitResult(String key, int value)
	{
		OutputFile.write(key + " " + value + "\n");
	}
		
}
