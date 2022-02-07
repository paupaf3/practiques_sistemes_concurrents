/* ---------------------------------------------------------------
Práctica 2.
Código fuente: WordCount.java
Grau Informàtica
49258834X - Pau Agustí Fernandez
48053637J - Dand Marbà Sera
--------------------------------------------------------------- */

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
