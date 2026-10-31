/*
 * Practica 1 - Sistemes Concurrents i Paral·lels. Grau en Enginyeria Informatica.
 * Codi font: MapReduce.java. Adaptacio del projecte sequencial proporcionat.
 * Autors: Oriol Llado Real i Edmon. Cognoms complets i NIF: pendents d'omplir.
 */
import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.stream.Stream;

abstract class MapReduce
{
    public static final boolean DEBUG = false;
    public static final int DTopN = 5;
    private static final int SHUFFLE_QUEUE_SIZE = 4096;
    private String InputPath = "";
    private String OutputFiles = "./result";
    private int NReducers = 1;
    private int NThreads = 1;
    private int ProcessedFiles;
    private long TotalWords;
    private long DifferentWords;

    // Inclou els Maps en execucio i els que encara esperen torn al pool.
    private final AtomicInteger MappersRunning = new AtomicInteger();
    private final Vector<Split> Splits = new Vector<>();
    private final Vector<Map> Mappers = new Vector<>();
    private final Vector<Reduce> Reducers = new Vector<>();
    private final Vector<Thread> splitThreads = new Vector<>();
    private final Vector<Thread> reduceThreads = new Vector<>();
    private final Vector<LinkedBlockingQueue<AbstractMap.SimpleEntry<String, Integer>>> ShuffleQueues =
            new Vector<>();
    private final Vector<Future<Long>> mapFutures = new Vector<>();
    private final Vector<Future<Error>> shuffleFutures = new Vector<>();
    // Canal d'errors entre tasques: no afegim locks ni semafors propis.
    private final ConcurrentLinkedQueue<String> errors = new ConcurrentLinkedQueue<>();
    private ExecutorService mapExecutor;
    private ExecutorService shuffleExecutor;
    private Path outputPrefix;
    private Pattern outputName;
    private boolean outputReady;

    public MapReduce() { }
    public MapReduce(String input, String output, int reducers, int threads)
    {
        SetInputPath(input);
        SetOutputFiles(output);
        SetReducers(reducers);
        SetThreads(threads);
    }
    public void SetInputPath(String path) { InputPath = path; }
    public void SetOutputFiles(String path) { OutputFiles = path; }
    public void SetThreads(int threads) { NThreads = threads; }
    public void SetReducers(int reducers) { NReducers = reducers; }
    public int getProcessedFiles() { return ProcessedFiles; }
    public long getTotalWords() { return TotalWords; }
    public long getDifferentWords() { return DifferentWords; }
    public boolean HasError() { return !errors.isEmpty(); }
    public String getErrorMessage() { return errors.peek(); }

    public void ReportError(String message)
    {
        if (errors.isEmpty()) errors.add(message);
    }

    public Error Init() throws IOException
    {
        Splits.clear(); Mappers.clear(); Reducers.clear(); ShuffleQueues.clear();
        splitThreads.clear(); reduceThreads.clear(); mapFutures.clear(); shuffleFutures.clear();
        errors.clear();
        mapExecutor = null; shuffleExecutor = null; outputReady = false;
        ProcessedFiles = 0; TotalWords = 0; DifferentWords = 0;
        MappersRunning.set(0);

        if (NThreads <= 0 || NReducers <= 0)
        {
            ReportError("El nombre de fils i de reducers ha de ser positiu.");
            return Error.CError;
        }
        if (InputPath == null || InputPath.isBlank() || !Files.exists(Path.of(InputPath)))
        {
            ReportError("No existeix l'entrada: " + InputPath);
            return Error.CErrorOpenInputFile;
        }
        if (OutputFiles == null || OutputFiles.isBlank())
        {
            ReportError("Cal indicar un prefix de sortida valid.");
            return Error.CErrorOpenOutputFile;
        }
        outputPrefix = Path.of(OutputFiles).toAbsolutePath().normalize();
        if (outputPrefix.getFileName() == null)
        {
            ReportError("La sortida ha de contenir una ruta i un prefix de fitxer.");
            return Error.CErrorOpenOutputFile;
        }
        outputName = Pattern.compile(Pattern.quote(outputPrefix.getFileName().toString()) + "\\.r[1-9][0-9]*");
        if (new File(InputPath).isFile() && IsOutputFile(Path.of(InputPath)))
        {
            ReportError("El fitxer d'entrada no pot ser un dels fitxers de sortida.");
            return Error.CErrorOpenOutputFile;
        }
        Files.createDirectories(outputPrefix.getParent());
        DeleteOutputFiles();
        outputReady = true;
        for (int r = 0; r < NReducers; r++)
        {
            // El constructor no obre el fitxer; nomes s'obre durant Reduce.
            Reducers.add(new Reduce(this, outputPrefix + ".r" + (r + 1)));
            ShuffleQueues.add(new LinkedBlockingQueue<>(SHUFFLE_QUEUE_SIZE));
        }
        return Error.COk;
    }

    public Error Run()
    {
        boolean success = false;
        try
        {
            Error result = Init();
            if (result != Error.COk) return result;
            result = ProcessInputFiles(new File(InputPath));
            if (result != Error.COk) return result;

            // Preparem els consumidors abans d'arrencar els lectors.
            StartSuffle();
            StartSplit();
            result = Maps();
            if (result != Error.COk) return result;
            result = WaitSplit();
            if (result != Error.COk) return result;
            result = WaitSuffle();
            if (result != Error.COk || HasError()) return Error.CError;

            // Els fils reducers nomes es creen quan les tres etapes han acabat.
            result = Reduces();
            if (result != Error.COk || HasError()) return Error.CError;
            success = true;
            return Error.COk;
        }
        catch (InterruptedException e)
        {
            ReportError("Execucio interrompuda.");
            Thread.currentThread().interrupt();
            return Error.CError;
        }
        catch (IOException | ExecutionException | RuntimeException e)
        {
            ReportError("Error durant l'execucio: " + e.getMessage());
            return Error.CError;
        }
        finally
        {
            StopAll();
            if (!success && outputReady)
            {
                try { DeleteOutputFiles(); }
                catch (IOException e) { ReportError("No s'han pogut eliminar les sortides parcials."); }
            }
        }
    }

    private Error ProcessInputFiles(File file) throws IOException
    {
        if (Files.isSymbolicLink(file.toPath()))
        {
            ReportError("No es processen enllaços simbolics: " + file);
            return Error.CErrorOpenInputFile;
        }
        if (file.isDirectory())
        {
            File[] files = file.listFiles();
            if (files == null)
            {
                ReportError("No es pot llegir la carpeta: " + file);
                return Error.CErrorOpenInputDir;
            }
            Arrays.sort(files, Comparator.comparing(File::getName));
            for (File child : files)
            {
                Error result = ProcessInputFiles(child);
                if (result != Error.COk) return result;
            }
        }
        else if (!file.isFile() || !file.canRead())
        {
            ReportError("No es pot llegir el fitxer: " + file);
            return Error.CErrorOpenInputFile;
        }
        else if (!IsOutputFile(file.toPath()))
        {
            Split split = new Split(this, file.getAbsolutePath());
            Splits.add(split);
            Mappers.add(new Map(this, split));
            System.out.println("Processing input file " + file.getName() + ".");
        }
        return Error.COk;
    }

    private void StartSplit() throws InterruptedException
    {
        for (int i = 0; i < Splits.size(); i++)
        {
            if (Thread.currentThread().isInterrupted()) throw new InterruptedException();
            Thread thread = Thread.ofVirtual().name("split-" + i).unstarted(Splits.get(i));
            splitThreads.add(thread);
            thread.start();
        }
    }

    private Error WaitSplit() throws InterruptedException
    {
        for (Thread thread : splitThreads) thread.join();
        for (Split split : Splits)
        {
            if (split.getResult() != Error.COk) return split.getResult();
            ProcessedFiles++;
        }
        return Error.COk;
    }

    private Error Maps() throws InterruptedException, ExecutionException
    {
        mapExecutor = Executors.newFixedThreadPool(NThreads);
        for (Map map : Mappers)
        {
            mapFutures.add(mapExecutor.submit(() -> {
                try { return map.call(); }
                finally { MappersRunning.decrementAndGet(); }
            }));
        }
        for (Future<Long> future : mapFutures) TotalWords += future.get();
        mapExecutor.shutdown();
        WaitExecutor(mapExecutor);
        System.out.println("\nMappers han procesado un total de " + TotalWords + " palabras.");
        return HasError() ? Error.CError : Error.COk;
    }

    public abstract Error Map(Map map, MapInputTuple tuple) throws InterruptedException;
    public abstract Error Reduce(Reduce reduce, String key, Collection<Integer> values);

    public void AddShuffleInput(String key, Integer value) throws InterruptedException
    {
        int r = Math.abs(key.hashCode() % NReducers);
        AbstractMap.SimpleEntry<String, Integer> tuple = new AbstractMap.SimpleEntry<>(key, value);
        while (!HasError())
        {
            if (Thread.currentThread().isInterrupted()) throw new InterruptedException();
            if (ShuffleQueues.get(r).offer(tuple, 50, TimeUnit.MILLISECONDS)) return;
        }
        throw new CancellationException("Una altra tasca ha fallat.");
    }

    private class SuffleTask implements Callable<Error>
    {
        private final int r;
        public SuffleTask(int r) { this.r = r; }

        @Override
        public Error call() throws InterruptedException
        {
            TreeMap<String, List<Integer>> keys = new TreeMap<>();
            try
            {
                while (MappersRunning.get() > 0 || !ShuffleQueues.get(r).isEmpty())
                {
                    if (HasError()) return Error.CError;
                    AbstractMap.SimpleEntry<String, Integer> tuple =
                            ShuffleQueues.get(r).poll(50, TimeUnit.MILLISECONDS);
                    if (tuple != null)
                        keys.computeIfAbsent(tuple.getKey(), k -> new ArrayList<>()).add(tuple.getValue());
                }
                // Transferencia de propietat: el reducer encara no s'ha iniciat.
                Reducers.get(r).SetInput(keys);
                return Error.COk;
            }
            catch (InterruptedException e)
            {
                ReportError("Shuffle interromput.");
                throw e;
            }
            catch (RuntimeException e)
            {
                ReportError("Error a Shuffle: " + e.getMessage());
                throw e;
            }
        }
    }

    private void StartSuffle()
    {
        MappersRunning.set(Mappers.size());
        shuffleExecutor = Executors.newFixedThreadPool(NReducers);
        for (int r = 0; r < NReducers; r++)
            shuffleFutures.add(shuffleExecutor.submit(new SuffleTask(r)));
    }

    private Error WaitSuffle() throws InterruptedException, ExecutionException
    {
        for (Future<Error> future : shuffleFutures)
            if (future.get() != Error.COk) return Error.CError;
        shuffleExecutor.shutdown();
        WaitExecutor(shuffleExecutor);
        return Error.COk;
    }

    private Error Reduces() throws InterruptedException
    {
        for (int i = 0; i < Reducers.size(); i++)
        {
            Thread thread = new Thread(Reducers.get(i), "reduce-" + i);
            reduceThreads.add(thread);
            thread.start();
        }
        for (Thread thread : reduceThreads) thread.join();
        for (Reduce reduce : Reducers)
        {
            if (reduce.getResult() != Error.COk) return reduce.getResult();
            DifferentWords += reduce.getDifferentWords();
        }
        System.out.println("Reduccers han procesado " + DifferentWords + " palabras diferentes.");
        return Error.COk;
    }

    private void WaitExecutor(ExecutorService executor) throws InterruptedException
    {
        while (!executor.awaitTermination(1, TimeUnit.SECONDS)) { }
    }

    // S'executa sempre: cap productor ni consumidor queda pendent en sortir.
    private void StopAll()
    {
        boolean interrupted = Thread.interrupted();
        for (Future<Long> future : mapFutures) future.cancel(true);
        for (Future<Error> future : shuffleFutures) future.cancel(true);
        if (mapExecutor != null) mapExecutor.shutdownNow();
        if (shuffleExecutor != null) shuffleExecutor.shutdownNow();
        Vector<Thread> threads = new Vector<>(splitThreads);
        threads.addAll(reduceThreads);
        for (Thread thread : threads)
            if (thread.isAlive()) thread.interrupt();
        for (Thread thread : threads)
        {
            while (thread.isAlive())
            {
                try { thread.join(); }
                catch (InterruptedException e) { interrupted = true; }
            }
        }
        for (ExecutorService executor : Arrays.asList(mapExecutor, shuffleExecutor))
        {
            if (executor == null) continue;
            while (!executor.isTerminated())
            {
                try { executor.awaitTermination(1, TimeUnit.SECONDS); }
                catch (InterruptedException e) { interrupted = true; }
            }
        }
        MappersRunning.set(0);
        if (interrupted) Thread.currentThread().interrupt();
    }

    private boolean IsOutputFile(Path file)
    {
        Path path = file.toAbsolutePath().normalize();
        return Objects.equals(path.getParent(), outputPrefix.getParent())
                && outputName.matcher(path.getFileName().toString()).matches();
    }

    public void DeleteOutputFiles() throws IOException
    {
        try (DirectoryStream<Path> files = Files.newDirectoryStream(outputPrefix.getParent()))
        {
            for (Path file : files)
                if (IsOutputFile(file) && !Files.isDirectory(file)) Files.delete(file);
        }
    }

    public Error ShowResults()
    {
        try
        {
            for (int r = 1; r <= NReducers; r++)
                processResultsFile(Path.of(outputPrefix + ".r" + r), DTopN);
            return Error.COk;
        }
        catch (IOException e)
        {
            ReportError("No es poden llegir els resultats: " + e.getMessage());
            return Error.CErrorOpenOutputFile;
        }
    }

    private record WC(String word, long count) { }

    private void processResultsFile(Path file, int topN) throws IOException
    {
        List<WC> entries;
        try (Stream<String> lines = Files.lines(file))
        {
            entries = lines.map(line -> line.trim().split("\\s+"))
                    .map(parts -> new WC(parts[0], Long.parseLong(parts[1]))).toList();
        }
        System.out.println("----------------------------------------------------------------------");
        System.out.println("Fichero: " + file.getFileName() + "\tTamaño: " + Files.size(file)
                + " bytes.\tNúmero palabras: " + entries.size());
        System.out.println("Top " + topN + " palabras:");
        entries.stream().sorted(Comparator.comparingLong(WC::count).reversed().thenComparing(WC::word))
                .limit(topN).forEach(wc -> System.out.printf("\t%-20s %d%n", wc.word(), wc.count()));
    }
}
