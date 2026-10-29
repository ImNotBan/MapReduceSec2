import java.io.File;
import java.util.Collection;
import java.util.Vector;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

 
abstract class MapReduce 
{
	public static final boolean DEBUG = false;
	public static final int DTopN = 5;

	private String 	InputPath;
	private String OutputFiles;
	private int NReducers;
	private int NThreads;

	private int ProcessedFiles =0;
	public static long TotalWords = 0;
	public static long DifferentWords = 0;
	public static AtomicInteger MappersRunning = new AtomicInteger(0);

	public int getProcessedFiles() { return ProcessedFiles; }
	public void incProcessedFiles() { ProcessedFiles++; }

	private Vector<Split> Splits =  new Vector<Split>();
	private Vector<Map> Mappers =  new Vector<Map>();
	private Vector<Reduce> Reducers =  new Vector<Reduce>();

	// una cua thread-safe per cada fil de shuffle, on els mappers deixen les tuples (clau, valor)
	private Vector<LinkedBlockingQueue<AbstractMap.SimpleEntry<String, Integer>>> ShuffleQueues =
			new Vector<LinkedBlockingQueue<AbstractMap.SimpleEntry<String, Integer>>>();


	// Set and Gets
	private void AddSplit(Split split) { Splits.add(split); }
	private void AddMapper(Map mapper) { Mappers.add(mapper); }
	private void AddReduce(Reduce reducer) { Reducers.add(reducer); }
	public void SetInputPath(String path) {
		InputPath = path;
	}
	public void SetOutputFiles(String path) {
		OutputFiles = path;
	}

	public void SetThreads(int nthreads) { NThreads=nthreads; }
	public void SetReducers(int nReducers) { NReducers = nReducers; }

	//  Constructores
	public MapReduce()
	{
		SetInputPath("");
		SetOutputFiles("");
		SetReducers(1);
		SetThreads(1);
	}
		
	// Constructor MapReduce: número de reducers a utilizar. Los parámetros de directorio/fichero entrada y salida,
	// número de reducers y threads se inicilizan mediante Set* y las funciones Map y reduce sobreescribiendo los
	// métodos abstractos.
	public MapReduce(String input, String output, int nReducers, int ntreads)
	{
		SetInputPath(input);
		SetOutputFiles(output);
		SetReducers(nReducers);
		SetThreads(ntreads);
	}

	public Error Init()
	{
		// Delete output reducer files
		DeleteOutputFiles();

		for(int x=0;x<NReducers;x++)
		{
			AddReduce(new Reduce(this, OutputFiles +".r"+(x+1)));
			ShuffleQueues.add(new LinkedBlockingQueue<AbstractMap.SimpleEntry<String, Integer>>());
		}

		return Error.COk;
	}

	
	// Procesa diferentes fases del framework mapreduc: split, map, suffle/merge, reduce.
	public Error Run()
	{
		if (Init()!=Error.COk)
			Error.showError("MapReduce::Run-Error Init");

		if (Split(InputPath)!=Error.COk)
			Error.showError("MapReduce::Run-Error Split");

		if (Maps()!=Error.COk)
			Error.showError("MapReduce::Run-Error Map");

		if (Suffle()!=Error.COk)
			Error.showError("MapReduce::Run-Error Merge");

		if (Reduces()!=Error.COk)
			Error.showError("MapReduce::Run-Error Reduce");

		return(Error.COk);
	}

	// Procesa los ficheros de entrada para generar los diferentes split y posteriormente crea un mapper para procesarlos
	// Versión secuencial: asume que un único Map va a procesar todos los splits.
	private Error Split(String inputDir)
	{
		Error cod;

		// Process input files
		cod = ProcessInputFiles(inputDir);

		if (cod==Error.COk)
		{
            // crear un map per cada split
			for(Split split : Splits){
                Map mapper = new Map(this, split);
                AddMapper(mapper);

            }

		}
		return cod;
	}

    private Error ProcessInputFiles(String input)
    {
        File folder = new File(input);

        // Si l'entrada és un fitxer, preparar el seu Split.
        if (!folder.isDirectory())
            return ProcessInputFile(folder);

        // Si és una carpeta, obtenir-ne el contingut.
        File[] files = folder.listFiles();
        if (files == null)
            return Error.CErrorOpenInputDir;

        for (File file : files)
        {
            // També permet entrar a les subcarpetes.
            Error result = ProcessInputFiles(file.getAbsolutePath());

            if (result != Error.COk)
                return result;
        }

        return Error.COk;
    }

	private Error ProcessInputFile(File file)
	{
        Split split = new Split();

        System.out.println("Processing  input file " + file.getName() + ".");


        // Llegir les línies i comprovar si hi ha hagut algun error.
        Error result = split.ReadFileTuples(file.getAbsolutePath());
        if (result != Error.COk) return result;

        split.AddInput(new MapInputTuple(-1, ""));

        AddSplit(split);
        incProcessedFiles();

        return Error.COk;
	}
	
	
	// Ejecuta cada uno de los Maps.
	private Error Maps()
	{
        TotalWords = 0;
		for(Map map : Mappers)
		{
			if (MapReduce.DEBUG) System.err.println("DEBUG::Running Map "+ map);
			MappersRunning.incrementAndGet();
			if (map.Run()!=Error.COk)
				Error.showError("MapReduce::Map Run error.\n");
			MappersRunning.decrementAndGet();
            TotalWords += map.getTotalWords();
		}
		System.out.println("\nMappers han procesado un total de "+TotalWords+" palabras.");
		return(Error.COk);
	}
	
	
	public Error Map(Map map, MapInputTuple tuple)
	{
		System.err.println("MapReduce::Map -> ERROR map must be override.");
		return(Error.CError);
	}
	
	
	
	// funcio de particio: el hash de la clau decideix a quina cua de shuffle va la tupla.
	// es pot cridar des de diversos fils map alhora perque les cues son thread-safe.
	public void AddShuffleInput(String key, Integer value)
	{
		int r = Math.abs(key.hashCode() % NReducers);

		if (MapReduce.DEBUG) System.err.println("DEBUG::MapReduce::Suffle merge key " + key +" to reduce " + r);

		ShuffleQueues.get(r).add(new AbstractMap.SimpleEntry<String, Integer>(key, value));
	}

	// tasca d'un fil de shuffle: llegeix les tuples de la seva cua, les junta i les ordena per clau.
	// acaba quan no queden mappers actius i la cua es buida. al final passa les claus al seu reducer.
	private class SuffleTask implements Callable<Error>
	{
		private int r;

		public SuffleTask(int r)
		{
			this.r = r;
		}

		@Override
		public Error call() throws InterruptedException
		{
			LinkedBlockingQueue<AbstractMap.SimpleEntry<String, Integer>> queue = ShuffleQueues.get(r);
			// el treemap junta els valors de cada clau i mante les claus ordenades
			TreeMap<String, List<Integer>> keys = new TreeMap<String, List<Integer>>();

			while (MappersRunning.get() > 0 || !queue.isEmpty())
			{
				// espera com a molt 10 ms perque no es quedi bloquejat si ja no arriben mes dades
				AbstractMap.SimpleEntry<String, Integer> tuple = queue.poll(10, TimeUnit.MILLISECONDS);

				if (tuple != null)
				{
					List<Integer> values = keys.get(tuple.getKey());
					if (values == null)
					{
						values = new ArrayList<Integer>();
						keys.put(tuple.getKey(), values);
					}
					values.add(tuple.getValue());
				}
			}

			// nomes aquest fil toca aquest reducer, per tant no hi ha condicions de carrera
			for (String key : keys.keySet())
				Reducers.get(r).AddInputKeys(key, keys.get(key));

			return(Error.COk);
		}
	}

	// Ordena y junta todas las tuplas de salida de los maps. Utiliza una función de hash como 
	// función de partición, para distribuir las claves entre los posibles reducers.
	private Error Suffle()
	{
		// temporal: mentre Map no cridi AddShuffleInput, passem aqui les sortides dels mappers a les cues.
		// s'ha de treure quan el map concurrent envii directament a les cues
		for(Map map : Mappers)
		{
			if (MapReduce.DEBUG) map.PrintOutputs();

			for (String key : map.GetOutput().keySet())
				for (Integer value : map.GetOutput().get(key))
					AddShuffleInput(key, value);

			// Eliminar todas las salidas.
			map.GetOutput().clear();
		}

		ExecutorService executor = Executors.newFixedThreadPool(NReducers);
		Vector<Future<Error>> futures = new Vector<Future<Error>>();

		// un fil de shuffle per cada reducer
		for (int r = 0; r < NReducers; r++)
			futures.add(executor.submit(new SuffleTask(r)));

		// esperar que acabin tots els fils de shuffle
		Error result = Error.COk;
		try
		{
			for (Future<Error> future : futures)
			{
				if (future.get() != Error.COk)
					result = Error.CError;
			}
		}
		catch (InterruptedException e)
		{
			System.err.println("MapReduce::Suffle interrupted.");
			result = Error.CError;
		}
		catch (ExecutionException e)
		{
			System.err.println("MapReduce::Suffle thread error: " + e.getCause());
			result = Error.CError;
		}
		finally
		{
			executor.shutdown();
		}

		return(result);
	}
	
	// Ejecuta cada uno de los Reducers.
	private Error Reduces()
	{
		Vector<Thread> threads = new Vector<Thread>();

		// un fil per cada reducer
		for(Reduce reduce : Reducers)
		{
			Thread thread = new Thread(reduce);
			threads.add(thread);
			thread.start();
		}

		// esperar que acabin tots els fils
		try
		{
			for(Thread thread : threads)
				thread.join();
		}
		catch (InterruptedException e)
		{
			System.err.println("MapReduce::Reduce interrupted.");
			return(Error.CError);
		}

		// comprovar el resultat de cada reducer i sumar les paraules diferents
		for(Reduce reduce : Reducers)
		{
			if (reduce.getResult()!=Error.COk)
				Error.showError("MapReduce::Reduce Run error.\n");
			DifferentWords += reduce.getDifferentWords();
		}
		System.out.println("Reduccers han procesado "+DifferentWords+" palabras diferentes.");
		return(Error.COk);
	}
		
	
	public Error Reduce(Reduce reduce, String key, Collection<Integer> values)
	{
		System.err.println("MapReduce::Reduce  -> ERROR Reduce must be override.");
		return(Error.CError);
	}

	public void DeleteOutputFiles()
	{
		Path outputPath = Paths.get(OutputFiles);
		Path directorio = outputPath.getParent();
		String nombreResultado = outputPath.getFileName().toString();

		try (DirectoryStream<Path> stream =  Files.newDirectoryStream(directorio, nombreResultado +".r*"))
		{
			for (Path file : stream) {
				Files.delete(file);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}


	public void ShowResults()
	{
		Path outputPath = Paths.get(OutputFiles);
		Path directorio = outputPath.getParent();
		String nombreResultado = outputPath.getFileName().toString();

		try (DirectoryStream<Path> stream =  Files.newDirectoryStream(directorio, nombreResultado +".r*"))
		{
			for (Path file : stream) {
				System.out.println("----------------------------------------------------------------------");
				System.out.print("Fichero: " + file.getFileName());
				processResultsFile(file, DTopN);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	record WC(String word, long count) {}

	private void processResultsFile(Path file, int topN) throws IOException
	{
		long sizeBytes = Files.size(file);

		List<WC> entries = 	Files.lines(file)
								.map(this::parseLine)
								.filter(Objects::nonNull)
								.collect(Collectors.toList());

		System.out.println(" \tTamaño: " + sizeBytes + " bytes.  \tNúmero palabras: " + entries.size());
		System.out.println("Top " + topN + " palabras:");
		entries.stream()
				.sorted( Comparator.comparingLong(WC::count).reversed())
				.limit(topN)
				.forEach(wc -> System.out.printf("\t%-20s %d%n", wc.word(), wc.count()));
	}

	private WC parseLine(String line)
	{
		String[] parts = line.trim().split("\\s+");

		if (parts.length != 2) {
			return null;
		}

		try {
			return new WC(parts[0], Long.parseLong(parts[1]));
		}
		catch (NumberFormatException e) {
			return null;
		}
	}

}


