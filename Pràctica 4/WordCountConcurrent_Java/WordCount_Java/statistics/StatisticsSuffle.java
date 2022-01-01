package statistics;

public class StatisticsSuffle
{
    // Número de tuplas de salida procesadas
    private int numTuples = 0;

    public void addToNumTuples()
    {
        numTuples++;
    }

    public synchronized void addToNumTuplesSync()
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

    public synchronized void addToNumKeysSync()
    {
        numKeys++;
    }

    public int getNumkeys()
    {
        return this.numKeys;
    }

    public void printStatistics(String title)
    {
        System.out.println("---------------------------------------------------------------");
        System.out.println(title);
        System.out.println("Tuplas procesadas: 	" + this.getNumTuples());
        System.out.println("Claves procesadas: 	" + this.getNumkeys());
        System.out.println("---------------------------------------------------------------");
    }
}
