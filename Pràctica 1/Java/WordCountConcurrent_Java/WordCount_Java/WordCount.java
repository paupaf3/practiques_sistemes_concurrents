

public class WordCount 
{

	public static void main(String[] args) 
	{
		long init = System.currentTimeMillis();
		WordCountMR wc = new WordCountMR(args);

		wc.Run();
		long end = System.currentTimeMillis();
		System.out.println("TIME: " + (end - init));
		System.exit(0);
	}

}
