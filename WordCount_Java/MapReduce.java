/* ---------------------------------------------------------------
Práctica 2.
Código fuente: MapReduce.java
Grau Informàtica
49258834X - Pau Agustí Fernandez
48053637J - Dand Marbà Sera
--------------------------------------------------------------- */

import statistics.StatisticsMap;
import statistics.StatisticsReduce;
import statistics.StatisticsSplit;
import statistics.StatisticsSuffle;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


abstract class MapReduce 
{
	public static final boolean DEBUG = false;
	private static final Integer ONE = new Integer(1);

	private Thread[] splitThreads;
	private Thread[] mapThreads;
	private Thread[] suffleThreads;
	private Thread[] reduceThreads;

	private String 	InputPath;
	private String 	OutputPath;
		
	private Vector<Map> Mappers =  new Vector<Map>();
	private Vector<Reduce> Reducers =  new Vector<Reduce>();

	private StatisticsSplit globalStatisticsSplit = new StatisticsSplit();
	private StatisticsMap globalStatisticsMap = new StatisticsMap();
	private StatisticsSuffle globalStatisticsSuffle = new StatisticsSuffle();
	private StatisticsReduce globalStatisticsReduce = new StatisticsReduce();

	private Lock lock = new ReentrantLock();

	private List<String> processedKeys = new ArrayList<>();

	// Clase encargada de encapsular el código que ejecutará cada hilo de Split.
	public class ConcurrentSplit implements Runnable
	{
		Map map;
		String filePath;
		StatisticsSplit globalStatisticsSplit;
		Lock lock;

		public ConcurrentSplit(Map map, String filePath, StatisticsSplit globalStatisticsSplit, Lock lock)
		{
			this.map = map;
			this.filePath = filePath;
			this.globalStatisticsSplit = globalStatisticsSplit;
			this.lock = lock;
		}

		@Override
		public void run()
		{
			if(this.map.ReadFileTuples(this.filePath, this.globalStatisticsSplit, this.lock)!=Error.COk)
				Error.showError("MapReduce::Run-Error Concurrent Split");
		}
	}

	// Clase encargada de encapsular el código que ejecutará cada hilo de Map.
	public class ConcurrentMap implements Runnable
	{
		Map map;
		StatisticsMap globalStatisticsMap;
		Lock lock;

		public ConcurrentMap(Map map, StatisticsMap globalStatisticsMap, Lock lock)
		{
			this.map = map;
			this.lock = lock;
			this.globalStatisticsMap = globalStatisticsMap;
		}

		public void run()
		{
			if (this.map.Run(this.globalStatisticsMap, this.lock)!=Error.COk)
				Error.showError("MapReduce::Run-Error Concurrent Map");
		}
	}

	// Clase encargada de encapsular el código que ejecutará cada hilo de Suffle.
	public class ConcurrentSuffle implements Runnable
	{
		Map map;
		CyclicBarrier barrier;
		StatisticsSuffle globalStatisticsSuffle;
		Lock lock;

		public ConcurrentSuffle(Map map, CyclicBarrier barrier, StatisticsSuffle globalStatisticsSuffle, Lock lock)
		{
			this.map = map;
			this.barrier = barrier;
			this.globalStatisticsSuffle = globalStatisticsSuffle;
			this.lock = lock;
		}

		@Override
		public void run()
		{
			StatisticsSuffle localStatisticsSuffle = new StatisticsSuffle();
			List<String> localProcessedKeys = new ArrayList<>();

			// Esperar a que la cola no se encuentre vacía
			this.map.threadSleepWhileQueueIsEmpty();

			String key = queuePoll();

			while(!key.equals(Map.END_OF_SPLIT))
			{
				// Calcular a que reducer le corresponde está clave:
				int r = key.hashCode() % Reducers.size();
				if (MapReduce.DEBUG)
					System.err.println("DEBUG::MapReduce::Suffle merge key " + key + " to reduce " + r);

				if(!processedKeys.contains(key))
				{
					processedKeys.add(key);
					globalStatisticsSuffle.addToNumKeysSync();
				}

				if(!localProcessedKeys.contains(key))
				{
					localProcessedKeys.add(key);
					localStatisticsSuffle.addToNumKeys();
				}

				globalStatisticsSuffle.addToNumTuplesSync();
				localStatisticsSuffle.addToNumTuples();

				// Añadir todas las tuplas de la clave al reducer correspondiente.
				Reducers.get(r < 0 ? r + Reducers.size() : r).AddInputKeys(key, ONE);

				this.map.threadSleepWhileQueueIsEmpty();
				key = queuePoll();
			}

			lock.lock();
			localStatisticsSuffle.printStatistics("Estadísticas Locales Suffle");
			lock.unlock();

			try
			{
				barrier.await();
			}
			catch(InterruptedException | BrokenBarrierException e)
			{
				e.printStackTrace();

			}
		}

		public synchronized String queuePoll()
		{
			return this.map.Output.poll();
		}
	}

	// Clase encargada de encapsular el código que ejecutará cada hilo de Reduce.
	public class ConcurrentRecude implements Runnable
	{
		Reduce reduce;
		CyclicBarrier barrier;
		StatisticsReduce globalStatisticsReduce;
		Lock lock;

		private ConcurrentRecude(Reduce reduce, CyclicBarrier barrier, StatisticsReduce globalStatisticsReduce, Lock lock)
		{
			this.reduce = reduce;
			this.barrier = barrier;
			this.globalStatisticsReduce = globalStatisticsReduce;
			this.lock = lock;
		}

		@Override
		public void run()
		{
			if(this.reduce.Run(this.barrier, this.globalStatisticsReduce, this.lock)!=Error.COk)
				Error.showError("MapReduce::Run-Error Concurrent Reduce");
		}
	}

	public MapReduce()
	{
		SetInputPath("");
		SetOutputPath("");
	}
		
	// Constructor MapReduce: número de reducers a utilizar. Los parámetros de directorio/fichero entrada 
	// y directorio salida se inicilizan mediante Set* y las funciones Map y reduce sobreescribiendo los
	// métodos abstractos.
	public MapReduce(String input, String output, int nReducers)
	{
		SetInputPath(input);
		SetOutputPath(output);
		SetReducers(nReducers);
	}
	
	private void AddMap(Map map) 
	{ 
		Mappers.add(map); 
	}
	
	private void AddReduce(Reduce reducer) 
	{ 
		Reducers.add(reducer); 
	}
	
	public void SetInputPath(String path)
	{
		InputPath = path;
	}
	
	public void SetOutputPath(String path)
	{
		OutputPath = path;
	}
	
	public void SetReducers(int nReducers)
	{
		for(int x=0;x<nReducers;x++)
		{
			AddReduce(new Reduce(this, OutputPath+"/result.r"+(x+1)));
		}
	}

	// Procesa diferentes fases del framework mapreduc: split, map, suffle/merge, reduce.
	public Error Run()
	{
		if (Split(InputPath)!=Error.COk)
			Error.showError("MapReduce::Run-Error Split");

		if (Maps()!=Error.COk)
			Error.showError("MapReduce::Run-Error Map");

		if (Suffle()!=Error.COk)
			Error.showError("MapReduce::Run-Error Merge");

		if (Reduces()!=Error.COk)
			Error.showError("MapReduce::Run-Error Reduce");

		return(Error.COk);
	}

	// Genera y lee diferentes splits: 1 split por fichero.
	// Versión secuencial: asume que un único Map va a procesar todos los splits.
	// Versión concurrente: va a crear un Mapper por fichero, un Mapper por cada Split.
	private Error Split(String input)
	{
		File folder = new File(input);

		if (folder.isDirectory())
		{
			File[] listOfFiles = folder.listFiles();

			Map[] map = new Map[listOfFiles.length];
			splitThreads = new Thread[listOfFiles.length];

			/* Read all the files and directories within directory */
			for (int i = 0; i < listOfFiles.length; i++)
			{
				if (listOfFiles[i].isFile())
				{
					map[i] = new Map(this);
					AddMap(map[i]);
					splitThreads[i] = new Thread(new ConcurrentSplit(map[i], listOfFiles[i].getAbsolutePath(), globalStatisticsSplit, lock));
					System.out.println("Processing input file " + listOfFiles[i].getAbsolutePath() + ".");
					globalStatisticsSplit.addToNumFilesSync();
					splitThreads[i].start();
				}
				else if (listOfFiles[i].isDirectory())
				{
					System.out.println("Directory " + listOfFiles[i].getName());
				}
			}
		}
		else
		{
			Map map = new Map(this);
			Thread thread = new Thread(new ConcurrentSplit(map, folder.getAbsolutePath(), globalStatisticsSplit, lock));
			System.out.println("Processing input file " + folder.getAbsolutePath() + ".");
			globalStatisticsSplit.addToNumFilesSync();
			thread.start();
		}

		return(Error.COk);
	}

	// Ejecuta cada uno de los Maps.
	private Error Maps()
	{
		mapThreads = new Thread[Mappers.size()];

		for(int i = 0; i < Mappers.size(); i++)
		{
			if (MapReduce.DEBUG) System.err.println("DEBUG::Running Map "+ Mappers.get(i));
			mapThreads[i] = new Thread(new ConcurrentMap(Mappers.get(i), globalStatisticsMap, lock));
			mapThreads[i].start();
		}

		return(Error.COk);
	}

	public Error Map(Map map, MapInputTuple tuple, StatisticsMap globalStatisticsMap, StatisticsMap localStatisticsMap)
	{
		System.err.println("MapReduce::Map -> ERROR map must be override.");
		return(Error.CError);
	}

	// Ordena y junta todas las tuplas de salida de los maps. Utiliza una función de hash como
	// función de partición, para distribuir las claves entre los posibles reducers.
	// Utiliza un multimap para realizar la ordenación/unión.
	private Error Suffle()
	{
		CyclicBarrier barrier = new CyclicBarrier(Mappers.size()+1);
		suffleThreads = new Thread[Mappers.size()];

		for(int i = 0; i < Mappers.size(); i++)
		{
			if (MapReduce.DEBUG) System.err.println("DEBUG::Running Suffle "+ Mappers.get(i));
			suffleThreads[i] = new Thread(new ConcurrentSuffle(Mappers.get(i), barrier, globalStatisticsSuffle, lock));
			suffleThreads[i].start();
		}

		try
		{
			barrier.await();
		}
		catch (InterruptedException | BrokenBarrierException e)
		{
			for(Thread thread : suffleThreads)
				thread.interrupt();

			Error.showError("MapReduce:: Error joining Suffle Threads");
		}

		globalStatisticsSplit.printStatistics("Estadísticas Globales Split");
		globalStatisticsMap.printStatistics("Estadísticas Globales Map");
		globalStatisticsSuffle.printStatistics("Estadísticas Globales Suffle");

		return (Error.COk);
	}

	// Ejecuta cada uno de los Reducers.
	private Error Reduces()
	{
		CyclicBarrier barrier = new CyclicBarrier(Reducers.size() + 1);
		reduceThreads = new Thread[Reducers.size()];

		for(int i = 0; i<Reducers.size(); i++)
		{
			reduceThreads[i] = new Thread(new ConcurrentRecude(Reducers.get(i), barrier, globalStatisticsReduce, lock));
			reduceThreads[i].start();
		}

		try
		{
			barrier.await();
		}
		catch (InterruptedException | BrokenBarrierException e)
		{
			for(Thread thread : reduceThreads)
				thread.interrupt();
			Error.showError("MapReduce:: Error joining Reduce Threads");
		}

		globalStatisticsReduce.printStatistics("Estadísticas Globales Reduce");

		return(Error.COk);
	}


	public Error Reduce(Reduce reduce, String key, Collection<Integer> values, StatisticsReduce globalStatisticsReduce, StatisticsReduce localStatisticsReduce)
	{
		System.err.println("MapReduce::Reduce  -> ERROR Reduce must be override.");
		return(Error.CError);
	}
}


