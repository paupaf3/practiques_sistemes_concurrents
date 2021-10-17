
import java.util.Collection;


public class WordCountMR extends MapReduce 
{

	public WordCountMR(String[] args) 
	{
		// Procesar argumentos.
		if (args.length!=2)
			throw new IllegalArgumentException("Illegal command line argument: WordCount <input dir> <ouput dir>.\n");
	
		SetInputPath(args[0]);
		SetOutputPath(args[1]);
		SetReducers(1);
	}

	// Word Count Map.
	@Override
	public Error Map(Map map, MapInputTuple tuple)
	{
		String value = tuple.getValue();

		if (MapReduce.DEBUG) System.err.println("DEBUG::MapWordCount procesing tuple " + tuple.getKey() + " -> "+ tuple.getValue());
		
		
		// Convertir todos los posibles separadores de palabras a espacios.
		value = value.replace(":", " ");
		value = value.replace(".", " ");
		value = value.replace(";", " ");
		value = value.replace(",", " ");
		value = value.replace("\"", " ");
		value = value.replace("\\", " ");
		value = value.replace("(", " ");
		value = value.replace(")", " ");
		value = value.replace("[", " ");
		value = value.replace("]", " ");
		value = value.replace("?", " ");
		value = value.replace("¿", " ");
		value = value.replace("!", " ");
		value = value.replace("¡", " ");
		value = value.replace("%", " ");
		value = value.replace("<", " ");
		value = value.replace(">", " ");
		value = value.replace("-", " ");
		value = value.replace("_", " ");
		value = value.replace("#", " ");
		value = value.replace("*", " ");
		value = value.replace("/", " ");

		// Emit map result (word,'1').
		for (String word : value.split("\\s+")) 
	    	map.EmitResult(word,1);

		return(Error.COk);
	}


	// Word Count Reduce.
	@Override
	public Error Reduce(Reduce reduce, String key, Collection<Integer> values)
	{
		int totalCount=0;

		if (MapReduce.DEBUG) System.err.print("DEBUG::ReduceWordCount key " + key +"s -> ");

		// Procesar todas los valores para esta clave.
		for(int number : values)
		{
			if (MapReduce.DEBUG) System.err.print(" " + number);
			totalCount +=  number;
		}

		if (MapReduce.DEBUG) System.err.println(" ==> " + totalCount);

		reduce.EmitResult(key, totalCount);

		return(Error.COk);
	}

}
