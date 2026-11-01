/*
 * Practica 1 - Sistemes Concurrents i Paral·lels. Grau en Enginyeria Informatica.
 * Codi font: WordCount.java. Adaptacio del projecte sequencial proporcionat.
 * Autor 1: Oriol Lladó Real. NIF: 39507303L.
 * Autor 2: Edmon Alcoceba Carbón. NIF: 47135257T.
 */
import java.util.Locale;

public class WordCount
{
    private static final String DOutputFiles = "./result";
    public static final double NANOSECONDS_IN_ONE_SECOND = 1_000_000_000.0;

    public static void main(String[] args)
    {
        int exitCode = execute(args);
        // En cas d'exit deixem que la JVM acabi sola: no amaguem fils pendents.
        if (exitCode != 0) System.exit(exitCode);
    }

    private static int execute(String[] args)
    {
        if (args.length < 1 || args.length > 4)
        {
            System.err.println("Uso: java WordCount <entrada> [<prefijo_salida>] [<reducers>] [<hilos>]");
            return 2;
        }
        try
        {
            String output = args.length > 1 ? args[1] : DOutputFiles;
            int reducers = args.length > 2 ? Integer.parseInt(args[2]) : 1;
            int threads = args.length > 3 ? Integer.parseInt(args[3]) : 1;
            WordCountMR wc = new WordCountMR(args[0], output, reducers, threads);
            long start = System.nanoTime();
            Error result = wc.Run();
            double seconds = (System.nanoTime() - start) / NANOSECONDS_IN_ONE_SECOND;
            if (result != Error.COk)
            {
                System.err.println("ERROR: " + wc.getErrorMessage());
                return 1;
            }
            System.out.printf(Locale.ROOT, "\n%d fichero/s procesado/s %.6f segs:%n",
                    wc.getProcessedFiles(), seconds);
            if (wc.ShowResults() != Error.COk)
            {
                System.err.println("ERROR: " + wc.getErrorMessage());
                return 1;
            }
            return 0;
        }
        catch (NumberFormatException e)
        {
            System.err.println("ERROR: reducers i fils han de ser nombres enters positius.");
            return 2;
        }
    }
}
