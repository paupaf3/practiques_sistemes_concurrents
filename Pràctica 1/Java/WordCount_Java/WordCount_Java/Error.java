

public enum Error {
	COk, CError, CErrorOpenInputDir, CErrorOpenInputFile, CErrorReadingFile, 
	CErrorOpenOutputFile;


	public static void showError(String message) 
	{ 
		System.err.println(message); 
		System.exit(1); 
	}
}