/* ---------------------------------------------------------------
Práctica 2.
Código fuente: Error.java
Grau Informàtica
49258834X - Pau Agustí Fernandez
48053637J - Dand Marbà Sera
--------------------------------------------------------------- */

public enum Error {
	COk, CError, CErrorOpenInputDir, CErrorOpenInputFile, CErrorReadingFile, 
	CErrorOpenOutputFile;

	public static void showError(String message) 
	{ 
		System.err.println(message); 
		System.exit(1); 
	}
}