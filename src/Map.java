/*
 * Practica 1 - Sistemes Concurrents i Paral·lels. Grau en Enginyeria Informatica.
 * Codi font: Map.java. Adaptacio del projecte sequencial proporcionat.
 * Autors: Oriol Llado Real i Edmon. Cognoms complets i NIF: pendents d'omplir.
 */
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

class MapInputTuple
{
    private final long Key;
    private final String Value;
    public MapInputTuple(long key, String value) { Key = key; Value = value; }
    public long getKey() { return Key; }
    public String getValue() { return Value; }
}

public class Map implements Callable<Long>
{
    private final MapReduce mapReduce;
    private final LinkedBlockingQueue<MapInputTuple> Input;
    private long totalWords;

    public Map(MapReduce mapReduce, Split split)
    {
        this.mapReduce = mapReduce;
        Input = split.getInput();
    }
    public long getTotalWords() { return totalWords; }

    public Error Run() throws InterruptedException
    {
        while (!mapReduce.HasError())
        {
            if (Thread.currentThread().isInterrupted()) throw new InterruptedException();
            MapInputTuple tuple = Input.poll(50, TimeUnit.MILLISECONDS);
            // Una cua temporalment buida no vol dir que el fitxer hagi acabat.
            if (tuple == null) continue;
            if (tuple.getKey() < 0) return Error.COk;
            Error result = mapReduce.Map(this, tuple);
            if (result != Error.COk) return result;
        }
        return Error.CError;
    }

    public void EmitResult(String key, int value) throws InterruptedException
    {
        mapReduce.AddShuffleInput(key, value);
        totalWords++;
    }

    @Override
    public Long call() throws InterruptedException
    {
        try
        {
            Error result = Run();
            if (result != Error.COk) throw new IllegalStateException("Resultat del Map: " + result);
            return totalWords;
        }
        catch (InterruptedException e)
        {
            mapReduce.ReportError("Map interromput.");
            Thread.currentThread().interrupt();
            throw e;
        }
        catch (RuntimeException e)
        {
            mapReduce.ReportError("Error al Map: " + e.getMessage());
            throw e;
        }
    }
}
