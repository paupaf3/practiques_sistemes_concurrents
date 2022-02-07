/* ---------------------------------------------------------------
Práctica 2.
Código fuente: Map.java
Grau Informàtica
49258834X - Pau Agustí Fernandez
48053637J - Dand Marbà Sera
--------------------------------------------------------------- */

import statistics.StatisticsMap;
import statistics.StatisticsSplit;

import java.io.*;
import java.util.Queue;
import java.util.Vector;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Lock;

class MapInputTuple
{
	long 	Key;
	String	Value;

	public MapInputTuple(long key, String value) 
	{
		setKey(key);
		setValue(value);
	}
		
	public long getKey()
	{
		return(Key);
	}

	public void setKey(long key)
	{
		Key = key;
	}
	
	public String getValue()
	{
		return(Value);
	}

	public void setValue(String value)
	{
		Value=value;
	}
}

public class Map
{
	public static String END_OF_SPLIT = "END_OF_SPLIT";

	private MapReduce mapReduce;
	public Vector<MapInputTuple> Input = new Vector<MapInputTuple>();
	public Queue<String> Output = new ConcurrentLinkedQueue<String>();

	public Map(MapReduce mapr)
	{
		mapReduce = mapr;
	}

	// Lee fichero de entrada (split) línea a línea y lo guarda en una cola del Map en forma de
	// tuplas (key,value).
	public Error ReadFileTuples(String fileName, StatisticsSplit globalStatisticsSplit, Lock lock)
	{
		StatisticsSplit localStatisticsSplit = new StatisticsSplit();
		localStatisticsSplit.addToNumFiles();

		long Offset = 0;
		FileInputStream fis;

		try
		{
			fis = new FileInputStream(fileName);
		}
		catch (FileNotFoundException e)
		{
			System.err.println("Map::ERROR File " + fileName + " not found.");
			e.printStackTrace();
			return (Error.CErrorOpenInputFile);
		}

		//Construct BufferedReader from InputStreamReader
		BufferedReader br = new BufferedReader(new InputStreamReader(fis));

		String line = null;
		try
		{
			while ((line = br.readLine()) != null)
			{
				if (MapReduce.DEBUG)
					System.err.println("DEBUG::Map input " + Offset + " -> " + line);

				globalStatisticsSplit.addToNumBytesSync(line);
				globalStatisticsSplit.addToNumLinesSync();

				localStatisticsSplit.addToNumBytes(line);
				localStatisticsSplit.addToNumLines();

				AddInput(new MapInputTuple(Offset, line));
				Offset += line.length();
			}
			// Indiquem el final de fitxer per poder controla la lectura a la tasca de Map.
			AddInput(new MapInputTuple(Offset + 1, Map.END_OF_SPLIT));

		}
		catch (IOException e)
		{
			System.err.println("Map::ERROR Reading file " + fileName + ".");
			e.printStackTrace();
			return (Error.CErrorReadingFile);
		}

		lock.lock();
		localStatisticsSplit.printStatistics("Estadísticas Locales Split");
		lock.unlock();

		try
		{
			br.close();
		}
		catch (IOException e)
		{
			e.printStackTrace();
			return (Error.CErrorReadingFile);
		}

		return (Error.COk);
	}

	// Ejecuta la tarea de Map: recorre la cola de tuplas de entrada y para cada una de ellas
	// invoca a la función de Map especificada por el programador.
	public Error Run(StatisticsMap globalStatisticsMap, Lock lock)
	{
		StatisticsMap localStatisticsMap = new StatisticsMap();
		Error err;

		this.threadSleepWhileInputIsEmpty();

		MapInputTuple inputTuple = getFirstIndexFromInput();

		while (!inputTuple.getValue().equals(Map.END_OF_SPLIT))
		{
			if (MapReduce.DEBUG)
				System.err.println("DEBUG::Map process input tuple " + inputTuple.getKey() + " -> " + inputTuple.getValue());


			globalStatisticsMap.addToNumBytesSync(inputTuple.getValue());
			localStatisticsMap.addToNumBytes(inputTuple.getValue());


			globalStatisticsMap.addToNumExitTuplesSync();
			localStatisticsMap.addToNumEntryTuples();

			err = mapReduce.Map(this, inputTuple, globalStatisticsMap, localStatisticsMap);

			if (err != Error.COk)
				return (err);

			removeFirstIndexFromInput();

			threadSleepWhileInputIsEmpty();

			inputTuple = getFirstIndexFromInput();
		}

		// Eliminar el END_OF_SPLIT de Input
		EmitResult(END_OF_SPLIT, 1);
		removeFirstIndexFromInput();

		lock.lock();
		localStatisticsMap.printStatistics("Estadísticas Locales Map");
		lock.unlock();

		return (Error.COk);
	}

	// Función para escribir un resultado parcial del Map en forma de tupla (key,value)
	public void EmitResult(String key, int value)
	{
		if (MapReduce.DEBUG)
			System.err.println("DEBUG::Map emit result " + key + " -> " + value);

		//Output.put(key,new Integer(value));
		this.Output.add(key);
	}

	// Mantener el hilo en espera mientras la Queue se encuentra vacía
	public void threadSleepWhileQueueIsEmpty()
	{
		while (this.Output.isEmpty())
		{
			try
			{
				Thread.sleep(100);
			}
			catch (InterruptedException e)
			{
				Error.showError("MapReduce:: Map Sleep Thread Error");
			}
		}
	}

	// Mantener el hilo en espera mientras Input se encuentra vacío
	public void threadSleepWhileInputIsEmpty()
	{
		while (this.Input.isEmpty())
		{
			try
			{
				Thread.sleep(100);
			}
			catch (InterruptedException e)
			{
				Error.showError("MapReduce:: Map Sleep Thread Error");
			}
		}
	}

	public synchronized void AddInput(MapInputTuple tuple)
	{
		Input.add(tuple);
	}

	public synchronized void removeFirstIndexFromInput()
	{
		Input.remove(0);
	}

	public synchronized MapInputTuple getFirstIndexFromInput()
	{
		return this.Input.get(0);
	}
}



