
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.Iterator;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;


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
		try {
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
	public void AddInputKeys(String key, Collection<Integer> values)
	{
		for (Integer value : values)
			AddInput(key, value);
	}


	private void AddInput(String key, Integer value)
	{
		if (MapReduce.DEBUG) System.err.println("DEBUG::Reduce add input "+ key + "-> " + value);
		Input.put(key,value);
	}


	// Función de ejecución de la tarea Reduce: por cada tupla de entrada invoca a la función 
	// especificada por el programador, pasandolo el objeto Reduce, la clave y la lista de 
	// valores.
	public Error Run() 
	{
		Iterator<String> keyIterator = Input.keySet().iterator();
		while(keyIterator.hasNext())
		{
			String key = keyIterator.next();
			
			Error err = mapReduce.Reduce(this, key, Input.get(key));
			if (err!=Error.COk)
				return(err);
			
			keyIterator.remove();
		}
		
		Finish();
		
		return(Error.COk);
	}


	// Función para escribir un resulta en el fichero de salida.
	public void EmitResult(String key, int value)
	{
		OutputFile.write(key + " " + value + "\n");
	}

		
}
