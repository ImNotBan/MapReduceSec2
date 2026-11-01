/*
 * Practica 1 - Sistemes Concurrents i Paral·lels. Grau en Enginyeria Informatica.
 * Codi font: Reduce.java. Adaptacio del projecte sequencial proporcionat.
 * Autor 1: Oriol Lladó Real. NIF: 39507303L.
 * Autor 2: Edmon Alcoceba Carbón. NIF: 47135257T.
 */
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.CancellationException;

class Reduce implements Runnable
{
    private final MapReduce mapReduce;
    private final String OutputPath;
    private TreeMap<String, List<Integer>> Input = new TreeMap<>();
    private PrintWriter OutputFile;
    private long differentWords;
    private Error result = Error.CError;

    public Reduce(MapReduce mapReduce, String outputPath)
    {
        this.mapReduce = mapReduce;
        OutputPath = outputPath;
    }
    public long getDifferentWords() { return differentWords; }
    public Error getResult() { return result; }

    // Nomes el Shuffle propietari l'invoca, abans de crear el fil reducer.
    public void SetInput(TreeMap<String, List<Integer>> input) { Input = input; }

    @Override
    public void run()
    {
        try
        {
            result = Run();
            if (result != Error.COk) mapReduce.ReportError("Error escrivint la sortida: " + OutputPath);
        }
        catch (RuntimeException e)
        {
            result = Error.CError;
            mapReduce.ReportError("Error al reducer: " + e.getMessage());
        }
    }

    public Error Run()
    {
        // Cada fil es propietari del seu mapa i del seu fitxer de sortida.
        try (PrintWriter writer = new PrintWriter(Files.newBufferedWriter(
                Path.of(OutputPath), StandardCharsets.UTF_8)))
        {
            OutputFile = writer;
            for (String key : Input.keySet())
            {
                if (Thread.currentThread().isInterrupted() || mapReduce.HasError()) return Error.CError;
                Error error = mapReduce.Reduce(this, key, Input.get(key));
                if (error != Error.COk) return error;
            }
            writer.flush();
            // PrintWriter no llanca IOException durant write: cal comprovar-ho.
            return writer.checkError() ? Error.CErrorOpenOutputFile : Error.COk;
        }
        catch (IOException e) { return Error.CErrorOpenOutputFile; }
    }

    public void EmitResult(String key, int value)
    {
        if (Thread.currentThread().isInterrupted() || mapReduce.HasError())
            throw new CancellationException("Reduccio cancel·lada.");
        OutputFile.write(key + " " + value + "\n");
        differentWords++;
    }
}
