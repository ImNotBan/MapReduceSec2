/*
 * Practica 1 - Sistemes Concurrents i Paral·lels. Grau en Enginyeria Informatica.
 * Codi font: Split.java. Adaptacio del projecte sequencial proporcionat.
 * Autor 1: Oriol Lladó Real. NIF: 39507303L.
 * Autor 2: Edmon Alcoceba Carbón. NIF: 47135257T.
 */
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class Split implements Runnable
{
    private static final int INPUT_QUEUE_SIZE = 256;
    private final LinkedBlockingQueue<MapInputTuple> Input = new LinkedBlockingQueue<>(INPUT_QUEUE_SIZE);
    private final MapReduce mapReduce;
    private final String fileName;
    private Error result = Error.CError;

    public Split(MapReduce mapReduce, String fileName)
    {
        this.mapReduce = mapReduce;
        this.fileName = fileName;
    }
    public LinkedBlockingQueue<MapInputTuple> getInput() { return Input; }
    public Error getResult() { return result; }

    @Override
    public void run()
    {
        try
        {
            result = ReadFileTuples(fileName);
            if (result == Error.COk)
            {
                // El productor posa el marcador despres de l'ultima linia.
                if (!AddInput(new MapInputTuple(-1, ""))) result = Error.CError;
            }
            if (result != Error.COk) mapReduce.ReportError("Error llegint el fitxer: " + fileName);
        }
        catch (InterruptedException e)
        {
            result = Error.CError;
            mapReduce.ReportError("Lectura interrompuda: " + fileName);
            Thread.currentThread().interrupt();
        }
        catch (RuntimeException e)
        {
            result = Error.CErrorReadingFile;
            mapReduce.ReportError("Error llegint " + fileName + ": " + e.getMessage());
        }
    }

    public Error ReadFileTuples(String fileName) throws InterruptedException
    {
        long offset = 0;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                new FileInputStream(fileName), StandardCharsets.UTF_8)))
        {
            String line;
            while ((line = reader.readLine()) != null)
            {
                if (!AddInput(new MapInputTuple(offset, line))) return Error.CError;
                offset += line.length();
            }
            return Error.COk;
        }
        catch (FileNotFoundException e) { return Error.CErrorOpenInputFile; }
        catch (IOException e) { return Error.CErrorReadingFile; }
    }

    private boolean AddInput(MapInputTuple tuple) throws InterruptedException
    {
        // L'espera limitada permet detectar un error d'una altra etapa.
        while (!mapReduce.HasError())
        {
            if (Thread.currentThread().isInterrupted()) throw new InterruptedException();
            if (Input.offer(tuple, 50, TimeUnit.MILLISECONDS)) return true;
        }
        return false;
    }
}
