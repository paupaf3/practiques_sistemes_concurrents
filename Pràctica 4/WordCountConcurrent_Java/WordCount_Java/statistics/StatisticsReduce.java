package statistics;

public class StatisticsReduce
{
    // Número de claves diferentes procesadas
    private int numKeys = 0;

    public void addToNumKeys()
    {
        numKeys++;
    }

    public int getNumkeys()
    {
        return this.numKeys;
    }

    // Número de ocurrencias procesadas
    private int numOcurrences = 0;

    public void addToNumOcurrences()
    {
        numOcurrences++;
    }

    public int getNumOcurrences()
    {
        return this.numOcurrences;
    }

    // Valor medio ocurrenias/clave
    public double getOcurrencesKeyAverage()
    {
        return numOcurrences/numKeys;
    }

    // Número de bytes escritos de salida
    private long numBytes = 0;

    public void addToNumBytes(long numBytes)
    {
        this.numBytes = this.numBytes + numBytes;
    }

    public long getNumBytes()
    {
        return this.numBytes;
    }
}
