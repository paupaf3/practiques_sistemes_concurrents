/* ---------------------------------------------------------------
Práctica 2.
Código fuente: Map.java
Grau Informàtica
49258834X - Pau Agustí Fernandez
48053637J - Dand Marbà Sera
--------------------------------------------------------------- */

import java.io.*;
import java.util.List;
import java.util.Vector;
import com.google.common.collect.*;

class MapInputTuple
{
	public static String END_OF_SPLIT = "END_OF_SPLIT";
	long 	Key;
	String	Value;

	public MapInputTuple(long key, String value) 
	{
		setKey(key);
		setValue(value);
	}
		
	public long getKey() { return(Key); }
	public void setKey(long key) { Key = key; }
	
	public String getValue() { return(Value); }
	public void setValue(String value) { Value=value; }
}

public class Map
{
	private MapReduce mapReduce;
	private Vector<MapInputTuple> Input = new Vector<MapInputTuple>();
	private ListMultimap<String, Integer> Output = Multimaps.synchronizedListMultimap(ArrayListMultimap.<String, Integer> create());

	public Map(MapReduce mapr)
	{	
		mapReduce=mapr;
	}


	public ListMultimap<String, Integer> GetOutput()
	{
		return(Output);
	}


	public void threadSleepWhileInputIsEmpty()
	{
		while(this.Input.isEmpty())
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


	public void PrintOutputs()
	{
		for (String key : Output.keySet())
		{
			List<Integer> ocurrences = Output.get(key);
			System.out.println("Map " + this + " Output: key: "+ key + " -> " + ocurrences);
		}
	}


	// Lee fichero de entrada (split) línea a línea y lo guarda en una cola del Map en forma de
	// tuplas (key,value).
	public Error ReadFileTuples(String fileName)
	{
		long Offset=0;
		FileInputStream fis;
		
		try {
			fis = new FileInputStream(fileName);
		} catch (FileNotFoundException e) {
			System.err.println("Map::ERROR File "+fileName+" not found.");
			e.printStackTrace();
			return(Error.CErrorOpenInputFile);
		}
		 
		//Construct BufferedReader from InputStreamReader
		BufferedReader br = new BufferedReader(new InputStreamReader(fis));
	 
		String line = null;
		try
		{
			while ((line = br.readLine())!=null) 
			{
				if (MapReduce.DEBUG) System.err.println("DEBUG::Map input " + Offset + " -> " + line);
				AddInput(new MapInputTuple(Offset, line));
			    Offset+=line.length();
			}
			// Indiquem el final de fitxer per poder controla la lectura a la tasca de Map.
			AddInput(new MapInputTuple(Offset + 1, MapInputTuple.END_OF_SPLIT));

		}
		catch (IOException e)
		{
			System.err.println("Map::ERROR Reading file "+fileName+".");
			e.printStackTrace();
			return(Error.CErrorReadingFile);
		}
		try
		{
			br.close();
		}
		catch (IOException e)
		{
			e.printStackTrace();
			return(Error.CErrorReadingFile);
		}

		return(Error.COk);
	}

	public void AddInput(MapInputTuple tuple)
	{
		Input.add(tuple);
	}


	// Ejecuta la tarea de Map: recorre la cola de tuplas de entrada y para cada una de ellas
	// invoca a la función de Map especificada por el programador.
	public Error Run()
	{
		Error err;

		this.threadSleepWhileInputIsEmpty();

		while (!this.Input.get(0).getValue().equals(MapInputTuple.END_OF_SPLIT))
		{
			if (MapReduce.DEBUG) System.err.println("DEBUG::Map process input tuple " + Input.get(0).getKey() +" -> " + Input.get(0).getValue());
			err = mapReduce.Map(this, Input.get(0));
			if (err!=Error.COk)
				return(err);

			Input.remove(0);

			this.threadSleepWhileInputIsEmpty();
		}
		// Eliminar el END_OF_SPLIT de Input
		Input.remove(0);

		return(Error.COk);
	}


	// Función para escribir un resultado parcial del Map en forma de tupla (key,value)
	public void EmitResult(String key, int value)
	{
		if (MapReduce.DEBUG) System.err.println("DEBUG::Map emit result " + key + " -> " + value);
		Output.put(key,new Integer(value));
	}
}



