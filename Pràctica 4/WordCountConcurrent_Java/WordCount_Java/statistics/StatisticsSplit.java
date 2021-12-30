package statistics;

public class StatisticsSplit
{
    // Número de ficheros leídos
    private int numFile = 0;

    public void addToNumFile()
    {
        numFile++;
    }

    public int getNumFile()
    {
        return this.numFile;
    }

    // Número total de bytes leídos
    private long numBytes = 0;

    public void addToNumBytes(long numBytes)
    {
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

    public int getNumLines()
    {
        return this.numLines;
    }

    // Número de tuplas de entrada generadas
    private int numTuples = 0;

    public void addToNumTuples()
    {
        numTuples++;
    }

    public int getNumTuples()
    {
        return this.numTuples;
    }

}
