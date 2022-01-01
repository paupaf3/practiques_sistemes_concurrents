package statistics;

import java.io.UnsupportedEncodingException;

public class StatisticsMap
{
    // Número de tuplas de entrada procesadas
    private int numEntryTuples = 0;

    public void addToNumEntryTuples()
    {
        numEntryTuples++;
    }

    public synchronized void addToNumEntryTuplesSync()
    {
        numEntryTuples++;
    }

    public int getNumEntryTuples()
    {
        return this.numEntryTuples;
    }

    // Número de bytes procesados
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

    // Número de tuplas de salida generadas
    private int numExitTuples = 0;

    public void addToNumExitTuples()
    {
        numExitTuples++;
    }

    public synchronized void addToNumExitTuplesSync()
    {
        numExitTuples++;
    }

    public int getNumExitTuples()
    {
        return this.numExitTuples;
    }

    public void printStatistics(String title)
    {
        System.out.println("---------------------------------------------------------------");
        System.out.println(title);
        System.out.println("Tuplas de entrada: 	" + this.getNumEntryTuples());
        System.out.println("Tuplas de salida: 	" + this.getNumExitTuples());
        System.out.println("Bytes procesados: 	" + this.getNumBytes());
        System.out.println("---------------------------------------------------------------");
    }
}
