package statistics;

import java.io.UnsupportedEncodingException;

public class StatisticsReduce
{
    // Número de claves diferentes procesadas
    private int numKeys = 0;

    public void addToNumKeys()
    {
        numKeys++;
    }

    public synchronized void addToNumKeysSync()
    {
        numKeys++;
    }

    public int getNumKeys()
    {
        return this.numKeys;
    }

    // Número de ocurrencias procesadas
    private int numOcurrences = 0;

    public void addToNumOcurrences()
    {
        numOcurrences++;
    }

    public synchronized void addToNumOcurrencesSync()
    {
        numOcurrences++;
    }

    public int getNumOcurrences()
    {
        return this.numOcurrences;
    }

    // Valor medio ocurrenias/clave
    public float getOcurrencesKeyAverage()
    {
        return ((float) numOcurrences)/numKeys;
    }

    // Número de bytes escritos de salida
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

    public void printStatistics(String title)
    {
        System.out.println("---------------------------------------------------------------");
        System.out.println(title);
        System.out.println("Claves procesadas: 	        " + this.getNumKeys());
        System.out.println("Ocurrencias procesadas:     " + this.getNumOcurrences());
        System.out.println("Media Ocurrencias/Claves:   " + this.getOcurrencesKeyAverage());
        System.out.println("Bytes procesados: 	        " + this.getNumBytes());
        System.out.println("---------------------------------------------------------------");
    }
}
