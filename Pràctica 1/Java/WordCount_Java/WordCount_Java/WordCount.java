

public class WordCount 
{

	public static void main(String[] args) 
	{
	
		WordCountMR wc = new WordCountMR(args);

		wc.Run();
	
		System.exit(0);
	}

}
