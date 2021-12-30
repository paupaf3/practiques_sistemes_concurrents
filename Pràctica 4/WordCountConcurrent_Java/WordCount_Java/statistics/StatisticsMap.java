package statistics;

public class StatisticsMap
{
    // Número de tuplas de entrada procesadas
    private int numEntryTuples = 0;

    public void addToNumEntryTuples()
    {
        numEntryTuples++;
    }

    public int getNumEntryTuples()
    {
        return this.numEntryTuples;
    }

    // Número de bytes procesados
    private long numBytes = 0;

    public void addToNumBytes(long numBytes)
    {
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

    public int getNumExitTuples()
    {
        return this.numExitTuples;
    }
}
