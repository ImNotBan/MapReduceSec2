/*
 * Practica 1 - Sistemes Concurrents i Paral·lels. Grau en Enginyeria Informatica.
 * Codi font: Map.java. Adaptacio del projecte sequencial proporcionat.
 * Autor 1: Oriol Lladó Real. NIF: 39507303L.
 * Autor 2: Edmon Alcoceba Carbón. NIF: 47135257T.
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

// callable perque el pool ens torni el recompte de paraules
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
            // cua buida no vol dir fi de fitxer: la fi la marca la tupla amb clau -1
            if (tuple == null) continue;
            if (tuple.getKey() < 0) return Error.COk;
            Error result = mapReduce.Map(this, tuple);
            if (result != Error.COk) return result;
        }
        return Error.CError;
    }

    public void EmitResult(String key, int value) throws InterruptedException
    {
        // envia directament al shuffle, sense guardar sortida local
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
