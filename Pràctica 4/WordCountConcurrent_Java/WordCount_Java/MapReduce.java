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

	private Thread[] splitThreads;
	private Thread[] mapThreads;
	private Thread[] reduceThreads;

	private String 	InputPath;
	private String 	OutputPath;
		
	private Vector<Map> Mappers =  new Vector<Map>();
	private Vector<Reduce> Reducers =  new Vector<Reduce>();


	// Clase encargada de encapsular el código que ejecutará cada hilo de Split.
	public class ConcurrentSplit implements Runnable
	{
		Map map;
		String filePath;

		public ConcurrentSplit(Map map, String filePath)
		{
			this.map = map;
			this.filePath = filePath;
		}

		@Override
		public void run()
		{
			if(this.map.ReadFileTuples(this.filePath)!=Error.COk)
				Error.showError("MapReduce::Run-Error Concurrent Split");
		}
	}

	// Clase encargada de encapsular el código que ejecutará cada hilo de Map.
	public class ConcurrentMap implements Runnable
	{
		Map map;

		public ConcurrentMap(Map map)
		{
			this.map = map;
		}

		public void run()
		{
			if (this.map.Run()!=Error.COk)
				Error.showError("MapReduce::Run-Error Concurrent Map");
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
		    		splitThreads[i] = new Thread(new ConcurrentSplit(map[i], listOfFiles[i].getAbsolutePath()));
		    		System.out.println("Processing input file " + listOfFiles[i].getAbsolutePath() + ".");
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
			Thread thread = new Thread(new ConcurrentSplit(map, folder.getAbsolutePath()));
			System.out.println("Processing input file " + folder.getAbsolutePath() + ".");
			thread.start();
		}

		try
		{
			for(Thread thread : splitThreads)
				thread.join();

		}
		catch (InterruptedException e)
		{
			for(Thread thread : splitThreads)
				thread.interrupt();
			Error.showError("MapReduce:: Error joining Split Threads");
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
			mapThreads[i] = new Thread(new ConcurrentMap(Mappers.get(i)));
			mapThreads[i].start();
		}

		try
		{
			for(Thread thread : mapThreads)
				thread.join();
		}
		catch (InterruptedException e)
		{
			for(Thread thread : mapThreads)
				thread.interrupt();
			Error.showError("MapReduce:: Error joining Map Threads");
		}
		return(Error.COk);
	}
	
	
	public Error Map(Map map, MapInputTuple tuple)
	{
		System.err.println("MapReduce::Map -> ERROR map must be override.");
		return(Error.CError);
	}
	
	
	// Ordena y junta todas las tuplas de salida de los maps. Utiliza una función de hash como 
	// función de partición, para distribuir las claves entre los posibles reducers.
	// Utiliza un multimap para realizar la ordenación/unión.
	private Error Suffle() {
		for (Map map : Mappers) {
			if (MapReduce.DEBUG) map.PrintOutputs();

			for (String key : map.GetOutput().keySet()) {
				// Calcular a que reducer le corresponde está clave:
				int r = key.hashCode() % Reducers.size();

				if (MapReduce.DEBUG)
					System.err.println("DEBUG::MapReduce::Suffle merge key " + key + " to reduce " + r);

				// Añadir todas las tuplas de la clave al reducer correspondiente.
				Reducers.get(r < 0 ? r + Reducers.size() : r).AddInputKeys(key, map.GetOutput().get(key));
			}

			// Eliminar todas las salidas.
			map.GetOutput().clear();
		}

		return (Error.COk);
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


