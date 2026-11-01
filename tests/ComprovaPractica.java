/*
 * Practica 1 - Sistemes Concurrents i Paral·lels. Grau en Enginyeria Informatica.
 * Codi font: tests/ComprovaPractica.java. Proves d'integracio sense JUnit ni biblioteques externes.
 * Autor 1: Oriol Lladó Real. NIF: 39507303L.
 * Autor 2: Edmon Alcoceba Carbón. NIF: 47135257T.
 */
import java.io.*;
import java.lang.reflect.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class ComprovaPractica
{
    private static final AtomicInteger checks = new AtomicInteger();
    private static void check(boolean condition, String message)
    {
        if (!condition) throw new AssertionError(message);
        checks.incrementAndGet();
    }

    private static TreeMap<String, Integer> readResults(Path prefix, int reducers) throws IOException
    {
        TreeMap<String, Integer> result = new TreeMap<>();
        for (int i = 1; i <= reducers; i++)
        {
            String previous = null;
            for (String line : Files.readAllLines(Path.of(prefix + ".r" + i)))
            {
                String[] parts = line.split(" ");
                check(previous == null || previous.compareTo(parts[0]) < 0, "Sortida no ordenada");
                check(result.put(parts[0], Integer.parseInt(parts[1])) == null, "Clau duplicada");
                previous = parts[0];
            }
        }
        return result;
    }

    private static void checkStopped(WordCountMR app) throws Exception
    {
        for (String fieldName : List.of("splitThreads", "reduceThreads"))
        {
            Field field = MapReduce.class.getDeclaredField(fieldName);
            field.setAccessible(true);
            for (Object value : (Vector<?>) field.get(app))
                check(!((Thread) value).isAlive(), "Ha quedat un fil viu: " + fieldName);
        }
        for (String fieldName : List.of("mapExecutor", "shuffleExecutor"))
        {
            Field field = MapReduce.class.getDeclaredField(fieldName);
            field.setAccessible(true);
            ExecutorService executor = (ExecutorService) field.get(app);
            check(executor == null || executor.isTerminated(), "Pool sense tancar");
        }
    }

    private static void basicTests(Path root) throws Exception
    {
        Path input = Files.createDirectory(root.resolve("entrada"));
        Files.createDirectories(input.resolve("subcarpeta"));
        Files.writeString(input.resolve("a.txt"), "Casa cotxe casa\nCOTXE, arbre!\n");
        Files.writeString(input.resolve("subcarpeta/b.txt"), "casa arbre\n");
        Files.writeString(input.resolve("buit.txt"), "");
        TreeMap<String, Integer> expected = new TreeMap<>();
        expected.put("arbre", 2); expected.put("casa", 3); expected.put("cotxe", 2);
        for (int threads : List.of(1, 2, 4))
        {
            for (int reducers : List.of(1, 3, 8))
            {
                Path prefix = root.resolve("resultat-" + threads + "-" + reducers);
                WordCountMR app = new WordCountMR(input.toString(), prefix.toString(), reducers, threads);
                check(app.Run() == Error.COk, app.getErrorMessage());
                check(app.getProcessedFiles() == 3, "Nombre de fitxers incorrecte");
                check(app.getTotalWords() == 7 && app.getDifferentWords() == 3, "Estadistiques incorrectes");
                check(readResults(prefix, reducers).equals(expected), "Recompte incorrecte");
                checkStopped(app);
                if (threads == 1 && reducers == 1)
                {
                    check(app.Run() == Error.COk, "No es pot reutilitzar l'objecte");
                    check(app.getTotalWords() == 7 && app.getDifferentWords() == 3, "Comptadors acumulats");
                    checkStopped(app);
                }
            }
        }
        Path unicode = root.resolve("unicode.txt");
        Files.writeString(unicode, "ÀRBRE àrbre cafè CAFÈ 123 --!\n");
        Path prefix = root.resolve("unicode-resultat");
        WordCountMR app = new WordCountMR(unicode.toString(), prefix.toString(), 3, 2);
        check(app.Run() == Error.COk, "Fallada Unicode");
        check(readResults(prefix, 3).equals(new TreeMap<>(java.util.Map.of("àrbre", 2, "cafè", 2))), "Accents incorrectes");
        Path minHash = root.resolve("hash-minim.txt");
        Files.writeString(minHash, "polygenelubricants polygenelubricants\n");
        check("polygenelubricants".hashCode() == Integer.MIN_VALUE, "Dada de prova del hash incorrecta");
        Path minPrefix = root.resolve("hash-minim");
        app = new WordCountMR(minHash.toString(), minPrefix.toString(), 3, 2);
        check(app.Run() == Error.COk, "Hash minim no suportat");
        check(readResults(minPrefix, 3).get("polygenelubricants") == 2, "Particio del hash incorrecta");
        Path empty = Files.createDirectory(root.resolve("carpeta-buida"));
        app = new WordCountMR(empty.toString(), root.resolve("buit").toString(), 2, 2);
        check(app.Run() == Error.COk && app.getTotalWords() == 0, "Carpeta buida incorrecta");
        checkStopped(app);
    }

    private static void errorTests(Path root) throws Exception
    {
        Path input = root.resolve("unicode.txt");
        for (int[] counts : List.of(new int[]{0, 2}, new int[]{2, 0}, new int[]{-1, 2}))
        {
            WordCountMR app = new WordCountMR(input.toString(), root.resolve("invalid").toString(), counts[0], counts[1]);
            check(app.Run() != Error.COk, "Arguments invalids acceptats");
            checkStopped(app);
        }
        WordCountMR missing = new WordCountMR(root.resolve("no-existeix").toString(), root.resolve("missing").toString(), 2, 2);
        check(missing.Run() != Error.COk, "Entrada inexistent acceptada");
        checkStopped(missing);
        Path collision = root.resolve("entrada-protegida.r1");
        Files.writeString(collision, "No eliminar aquesta entrada");
        WordCountMR collisionApp = new WordCountMR(collision.toString(), root.resolve("entrada-protegida").toString(), 1, 1);
        check(collisionApp.Run() != Error.COk, "Col·lisio entrada-sortida acceptada");
        check(Files.readString(collision).equals("No eliminar aquesta entrada"), "Entrada eliminada");

        Path prefix = root.resolve("sortida-no-valida");
        Files.createDirectory(Path.of(prefix + ".r1"));
        WordCountMR badOutput = new WordCountMR(input.toString(), prefix.toString(), 2, 2);
        check(badOutput.Run() != Error.COk, "Error de sortida no detectat");
        checkStopped(badOutput);
        check(!Files.exists(Path.of(prefix + ".r2")), "Sortida parcial sense eliminar");

        WordCountMR badMap = new WordCountMR(input.toString(), root.resolve("map-error").toString(), 2, 2) {
            @Override public Error Map(Map map, MapInputTuple tuple) { throw new IllegalStateException("Error de prova"); }
        };
        check(badMap.Run() != Error.COk, "Error de Map no detectat");
        checkStopped(badMap);
        WordCountMR badReduce = new WordCountMR(input.toString(), root.resolve("reduce-error").toString(), 2, 2) {
            @Override public Error Reduce(Reduce reduce, String key, Collection<Integer> values) {
                throw new IllegalStateException("Error de prova");
            }
        };
        check(badReduce.Run() != Error.COk, "Error de Reduce no detectat");
        checkStopped(badReduce);
        WordCountMR badShuffle = new WordCountMR(input.toString(), root.resolve("shuffle-error").toString(), 2, 2) {
            @Override public void AddShuffleInput(String key, Integer value) throws InterruptedException {
                try {
                    Field field = MapReduce.class.getDeclaredField("ShuffleQueues");
                    field.setAccessible(true);
                    @SuppressWarnings("unchecked")
                    Vector<LinkedBlockingQueue<AbstractMap.SimpleEntry<String, Integer>>> queues =
                            (Vector<LinkedBlockingQueue<AbstractMap.SimpleEntry<String, Integer>>>) field.get(this);
                    queues.get(0).put(new AbstractMap.SimpleEntry<>(null, value));
                } catch (ReflectiveOperationException e) { throw new IllegalStateException(e); }
            }
        };
        check(badShuffle.Run() != Error.COk, "Error de Shuffle no detectat");
        checkStopped(badShuffle);
    }

    private static void concurrencyTests(Path root) throws Exception
    {
        Path input = Files.createDirectory(root.resolve("concurrencia"));
        String lines = "casa cotxe\n".repeat(2000);
        for (int i = 0; i < 6; i++) Files.writeString(input.resolve(i + ".txt"), lines);
        CountDownLatch firstMaps = new CountDownLatch(3);
        AtomicInteger active = new AtomicInteger(), maximum = new AtomicInteger();
        Set<Thread> mapThreads = ConcurrentHashMap.newKeySet();
        Set<Map> seen = ConcurrentHashMap.newKeySet();
        AtomicBoolean pipelineSeen = new AtomicBoolean();
        WordCountMR app = new WordCountMR(input.toString(), root.resolve("concurrent-resultat").toString(), 3, 3) {
            @Override public Error Map(Map map, MapInputTuple tuple) throws InterruptedException {
                mapThreads.add(Thread.currentThread());
                if (seen.add(map)) {
                    int current = active.incrementAndGet();
                    maximum.accumulateAndGet(current, Math::max);
                    firstMaps.countDown();
                    if (!firstMaps.await(5, TimeUnit.SECONDS)) throw new IllegalStateException("Maps no concurrents");
                    active.decrementAndGet();
                    try {
                        Field splitField = MapReduce.class.getDeclaredField("splitThreads");
                        splitField.setAccessible(true);
                        boolean liveReader = false;
                        for (Object value : (Vector<?>) splitField.get(this)) {
                            Thread reader = (Thread) value;
                            check(reader.isVirtual(), "Split no virtual");
                            liveReader |= reader.isAlive();
                        }
                        Field queues = MapReduce.class.getDeclaredField("ShuffleQueues");
                        queues.setAccessible(true);
                        map.EmitResult("pipeline", 1);
                        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
                        boolean drained = false;
                        while (System.nanoTime() < deadline && !drained) {
                            drained = true;
                            for (Object queue : (Vector<?>) queues.get(this))
                                drained &= ((BlockingQueue<?>) queue).isEmpty();
                            if (!drained) Thread.sleep(1);
                        }
                        if (liveReader && drained) pipelineSeen.set(true);
                    } catch (ReflectiveOperationException e) { throw new IllegalStateException(e); }
                }
                return super.Map(map, tuple);
            }
        };
        check(app.Run() == Error.COk, app.getErrorMessage());
        check(maximum.get() == 3 && mapThreads.size() <= 3, "Limit del pool incorrecte");
        check(pipelineSeen.get(), "No s'ha observat solapament Split-Map-Shuffle");
        checkStopped(app);

        CountDownLatch started = new CountDownLatch(1);
        AtomicReference<Error> result = new AtomicReference<>();
        AtomicBoolean interrupted = new AtomicBoolean();
        WordCountMR cancelled = new WordCountMR(input.toString(), root.resolve("cancelled").toString(), 2, 1) {
            @Override public Error Map(Map map, MapInputTuple tuple) throws InterruptedException {
                started.countDown();
                Thread.sleep(10000);
                return super.Map(map, tuple);
            }
        };
        Thread coordinator = new Thread(() -> {
            result.set(cancelled.Run());
            interrupted.set(Thread.currentThread().isInterrupted());
        });
        coordinator.start();
        check(started.await(5, TimeUnit.SECONDS), "Map no iniciat");
        coordinator.interrupt();
        coordinator.join(5000);
        check(!coordinator.isAlive() && result.get() != Error.COk, "Cancel·lacio bloquejada");
        check(interrupted.get(), "Indicador d'interrupcio perdut");
        checkStopped(cancelled);
    }

    public static void main(String[] args) throws Exception
    {
        Path root = Files.createTempDirectory("scp-proves-");
        PrintStream console = System.out;
        try (PrintStream quiet = new PrintStream(OutputStream.nullOutputStream(), true, StandardCharsets.UTF_8))
        {
            System.setOut(quiet);
            basicTests(root);
            errorTests(root);
            concurrencyTests(root);
        }
        finally { System.setOut(console); }
        System.out.println("Proves correctes: " + checks.get() + " comprovacions. Dades temporals: " + root);
    }
}
