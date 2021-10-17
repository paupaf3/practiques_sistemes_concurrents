import java.io.File;
import java.util.Collection;
import java.util.Vector;

 
abstract class MapReduce 
{
	public static final boolean DEBUG = false;	
	
	private String 	InputPath;
	private String 	OutputPath;
		
	private Vector<Map> Mappers =  new Vector<Map>();
	private Vector<Reduce> Reducers =  new Vector<Reduce>();

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
		
	
	public void SetInputPath(String path) {
		InputPath = path;
	}
	
	public void SetOutputPath(String path) {
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
	private Error Split(String input)
	{
		File folder = new File(input);
	
		Map map = new Map(this);
		AddMap(map);
		
		if (folder.isDirectory()) 
		{
			File[] listOfFiles = folder.listFiles();
		
			/* Read all the files and directories within directory */
		    for (int i = 0; i < listOfFiles.length; i++) 
		    {
		    	if (listOfFiles[i].isFile()) 
		    	{
		    		System.out.println("Processing input file " + listOfFiles[i].getAbsolutePath() + ".");
		    		map.ReadFileTuples(listOfFiles[i].getAbsolutePath());
		    	}
		    	else if (listOfFiles[i].isDirectory()) {
		    		System.out.println("Directory " + listOfFiles[i].getName());
		    	}
		    }
		}
		else 
		{
			System.out.println("Processing input file " + folder.getAbsolutePath() + ".");
    		map.ReadFileTuples(folder.getAbsolutePath());
		}
		
		return(Error.COk);
	}
	
	
	// Ejecuta cada uno de los Maps.
	private Error Maps()
	{	
		for(Map map : Mappers)
		{
			if (MapReduce.DEBUG) System.err.println("DEBUG::Running Map "+ map);
			if (map.Run()!=Error.COk)
				Error.showError("MapReduce::Map Run error.\n");
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
	private Error Suffle()
	{
		for(Map map : Mappers)
		{
			if (MapReduce.DEBUG) map.PrintOutputs();

			for (String key : map.GetOutput().keySet())
			{		    
			    // Calcular a que reducer le corresponde está clave:
				int r = key.hashCode()%Reducers.size();
	
				if (MapReduce.DEBUG) System.err.println("DEBUG::MapReduce::Suffle merge key " + key +" to reduce " + r);
	
				// Añadir todas las tuplas de la clave al reducer correspondiente.
				Reducers.get(r).AddInputKeys(key, map.GetOutput().get(key));			
			}
			
			// Eliminar todas las salidas.
			map.GetOutput().clear();
		}
		
		return(Error.COk);
	}

	
	// Ejecuta cada uno de los Reducers.
	private Error Reduces()
	{
		for(Reduce reduce : Reducers)
		{
			if (reduce.Run()!=Error.COk)
				Error.showError("MapReduce::Reduce Run error.\n");
		}
		return(Error.COk);
	}
	
		
	
	public Error Reduce(Reduce reduce, String key, Collection<Integer> values)
	{
		System.err.println("MapReduce::Reduce  -> ERROR Reduce must be override.");
		return(Error.CError);
	}
}


