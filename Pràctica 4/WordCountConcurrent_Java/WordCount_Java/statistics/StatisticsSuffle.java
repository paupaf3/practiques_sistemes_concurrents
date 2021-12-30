package statistics;

public class StatisticsSuffle
{
    // Número de tuplas de salida procesadas
    private int numTuples = 0;

    public void addToNumTuples()
    {
        numTuples++;
    }

    public int getNumTuples()
    {
        return this.numTuples;
    }

    // Número de claves procesadas
    private int numKeys = 0;

    public void addToNumKeys()
    {
        numKeys++;
    }

    public int getNumkeys()
    {
        return this.numKeys;
    }
}
