/* ---------------------------------------------------------------
Práctica 2.
Código fuente: MapReduce.java
Grau Informàtica
49258834X - Pau Agustí Fernandez
48053637J - Dand Marbà Sera
--------------------------------------------------------------- */

import java.io.File;
import java.util.Collection;
import java.util.Vector;

 
abstract class MapReduce 
{
	public static final boolean DEBUG = false;
	private static final Integer ONE = new Integer(1);

	private Thread[] splitMapSuffleThreads;
	private Thread[] reduceThreads;

	private String 	InputPath;
	private String 	OutputPath;
		
	private Vector<Map> Mappers =  new Vector<Map>();
	private Vector<Reduce> Reducers =  new Vector<Reduce>();


	// Clase encargada de encapsular el código que ejecutará cada hilo de Split.
	public class ConcurrentSplitMapSuffle implements Runnable
	{
		Map map;
		String filePath;

		public ConcurrentSplitMapSuffle(Map map, String filePath)
		{
			this.map = map;
			this.filePath = filePath;
		}

		@Override
		public void run()
		{
			if(this.map.ReadFileTuples(this.filePath)!=Error.COk)
				Error.showError("MapReduce::Run-Error Concurrent Split");

			if (this.map.Run()!=Error.COk)
				Error.showError("MapReduce::Run-Error Concurrent Map");

			// Esperar a que la cola no se encuentre vacía
			this.map.threadSleepWhileQueueIsEmpty();

			String key = this.map.queue.poll();

			while(!key.equals(Map.END_OF_SPLIT))
			{
				// Calcular a que reducer le corresponde está clave:
				int r = key.hashCode() % Reducers.size();

				if (MapReduce.DEBUG)
					System.err.println("DEBUG::MapReduce::Suffle merge key " + key + " to reduce " + r);

				// Añadir todas las tuplas de la clave al reducer correspondiente.
				// Todo añadir de la queue no del output
				Reducers.get(r < 0 ? r + Reducers.size() : r).AddInputKeys(key, ONE);

				this.map.threadSleepWhileQueueIsEmpty();
				key = this.map.queue.poll();
			}
		}
	}



	// Clase encargada de encapsular el código que ejecutará cada hilo de Suffle.
	public class ConcurrentSuffle implements Runnable
	{
		Map map;

		public ConcurrentSuffle(Map map)
		{
			this.map = map;
		}

		@Override
		public void run()
		{
			// Esperar a que la cola no se encuentre vacía
			this.map.threadSleepWhileQueueIsEmpty();

			String key = this.map.queue.poll();

			while(!key.equals(Map.END_OF_SPLIT))
			{
				// Calcular a que reducer le corresponde está clave:
				int r = key.hashCode() % Reducers.size();

				if (MapReduce.DEBUG)
					System.err.println("DEBUG::MapReduce::Suffle merge key " + key + " to reduce " + r);

				// Añadir todas las tuplas de la clave al reducer correspondiente.
				// Todo añadir de la queue no del output
				Reducers.get(r < 0 ? r + Reducers.size() : r).AddInputKeys(key, ONE);

				this.map.threadSleepWhileQueueIsEmpty();
				key = this.map.queue.poll();
			}
		}
	}

	// Clase encargada de encapsular el código que ejecutará cada hilo de Reduce.
	public class ConcurrentRecude implements Runnable
	{
		Reduce reduce;

		private ConcurrentRecude(Reduce reduce)
		{
			this.reduce = reduce;
		}

		@Override
		public void run()
		{
			if(this.reduce.Run()!=Error.COk)
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
		if (SplitMapSuffle(InputPath)!=Error.COk)
			Error.showError("MapReduce::Run-Error SplitMapSuffle");

		if (Reduces()!=Error.COk)
			Error.showError("MapReduce::Run-Error Reduce");

		return(Error.COk);
	}

	// Genera y lee diferentes splits: 1 split por fichero.
	// Versión secuencial: asume que un único Map va a procesar todos los splits.
	// Versión concurrente: va a crear un Mapper por fichero, un Mapper por cada Split.
	private Error SplitMapSuffle(String input)
	{
		File folder = new File(input);
		
		if (folder.isDirectory()) 
		{
			File[] listOfFiles = folder.listFiles();

			Map[] map = new Map[listOfFiles.length];
			splitMapSuffleThreads = new Thread[listOfFiles.length];

			/* Read all the files and directories within directory */
		    for (int i = 0; i < listOfFiles.length; i++)
		    {
		    	if (listOfFiles[i].isFile()) 
		    	{
		    		map[i] = new Map(this);
		    		AddMap(map[i]);
		    		splitMapSuffleThreads[i] = new Thread(new ConcurrentSplitMapSuffle(map[i], listOfFiles[i].getAbsolutePath()));
		    		System.out.println("Processing input file " + listOfFiles[i].getAbsolutePath() + ".");
		    		splitMapSuffleThreads[i].start();
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
			Thread thread = new Thread(new ConcurrentSplitMapSuffle(map, folder.getAbsolutePath()));
			System.out.println("Processing input file " + folder.getAbsolutePath() + ".");
			thread.start();
		}

		try
		{
			for(Thread thread : splitMapSuffleThreads)
				thread.join();

		}
		catch (InterruptedException e)
		{
			for(Thread thread : splitMapSuffleThreads)
				thread.interrupt();
			Error.showError("MapReduce:: Error joining Split Threads");
		}
		return(Error.COk);
	}

	public Error Map(Map map, MapInputTuple tuple)
	{
		System.err.println("MapReduce::Map -> ERROR map must be override.");
		return(Error.CError);
	}
	
	// Ejecuta cada uno de los Reducers.
	private Error Reduces()
	{
		reduceThreads = new Thread[Reducers.size()];

		for(int i = 0; i<Reducers.size(); i++)
		{
			reduceThreads[i] = new Thread(new ConcurrentRecude(Reducers.get(i)));
			reduceThreads[i].start();
		}

		try
		{
			for(Thread thread : reduceThreads)
				thread.join();
		}
		catch (InterruptedException e)
		{
			for(Thread thread : reduceThreads)
				thread.interrupt();
			Error.showError("MapReduce:: Error joining Reduce Threads");
		}

		return(Error.COk);
	}


	public Error Reduce(Reduce reduce, String key, Collection<Integer> values)
	{
		System.err.println("MapReduce::Reduce  -> ERROR Reduce must be override.");
		return(Error.CError);
	}
}


