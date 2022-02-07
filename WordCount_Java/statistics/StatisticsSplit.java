package statistics;

import java.io.UnsupportedEncodingException;

public class StatisticsSplit
{
    // Número de ficheros leídos
    private int numFiles = 0;

    public void addToNumFiles()
    {
        numFiles++;
    }

    public synchronized void addToNumFilesSync()
    {
        numFiles++;
    }

    public int getNumFiles()
    {
        return this.numFiles;
    }

    // Número total de bytes leídos
    private long numBytes = 0;

    public void addToNumBytes(String string)
    {
        long numBytes = 0;

        try
        {
            numBytes = string.getBytes("UTF-8").length;
        }
        catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        this.numBytes = this.numBytes + numBytes;
    }

    public synchronized void addToNumBytesSync(String string)
    {
        long numBytes = 0;

        try
        {
            numBytes = string.getBytes("UTF-8").length;
        }
        catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        this.numBytes = this.numBytes + numBytes;
    }

    public long getNumBytes()
    {
        return this.numBytes;
    }

    // Número de líneas leídas
    private int numLines = 0;

    public void addToNumLines()
    {
        numLines++;
    }

    public synchronized void addToNumLinesSync()
    {
        numLines++;
    }

    public int getNumLines()
    {
        return this.numLines;
    }


    public void printStatistics(String title)
    {
        System.out.println("---------------------------------------------------------------");
        System.out.println(title);
        System.out.println("Ficheros leídos: 	                " + this.getNumFiles());
        System.out.println("Bytes leídos: 		                " + this.getNumBytes());
        System.out.println("Líneas leídas/Tuplas generadas: 	" + this.getNumLines());
        System.out.println("---------------------------------------------------------------");
    }
}
