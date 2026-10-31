
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.Callable;
//import com.google.common.collect.*;


class MapInputTuple
{
	long 	Key;
	String	Value;

	public MapInputTuple(long key, String value) 
	{
		setKey(key);
		setValue(value);
	}
		
	public long getKey() { return(Key); }
	public void setKey(long key) { Key = key; }
	
	public String getValue() { return(Value); }
	public void setValue(String value) { Value=value; }
}



public class Map implements Callable<Long>
{
	private MapReduce mapReduce;
	private Vector<MapInputTuple> Input = new Vector<MapInputTuple>();
	private HashMap<String, List<Integer>> Output = new HashMap<String, List<Integer>>();
    private long totalWords = 0;

    public long getTotalWords() {
        return totalWords;
    }

    public Map(MapReduce mapr)
	{	
		mapReduce=mapr;
	}
	public Map(MapReduce mapr, Split split)
	{
		mapReduce=mapr;
		Input = split.getInput();
	}

	public HashMap<String, List<Integer>> GetOutput()
	{
		return(Output);
	}


	public void addInputSplit(Vector<MapInputTuple> split)
	{
		Input.addAll(split);
	}
	
	public void PrintOutputs()
	{
		for (String key : Output.keySet())
		{
			List<Integer> ocurrences = Output.get(key);
			System.out.println("Map " + this + " Output: key: "+ key + " -> " + ocurrences);
		}
	}
		

	// Ejecuta la tarea de Map: recorre la cola de tuplas de entrada y para cada una de ellas
	// invoca a la función de Map especificada por el programador.
	public Error Run() 
	{
		Error err;

		while (!Input.isEmpty())
		{
			// Permetre que el coordinador cancel·li la tasca si alguna altra falla.
			if (Thread.currentThread().isInterrupted())
				return Error.CError;

			if (MapReduce.DEBUG) System.err.println("DEBUG::Map process input tuple " + Input.get(0).getKey() +" -> " + Input.get(0).getValue());

			if (Input.get(0).Key<0)
				return(Error.COk);

			err = mapReduce.Map(this, Input.get(0));
			if (err!=Error.COk)
				return(err);

			Input.remove(0);
		}

		return(Error.COk);
	}



	// Función para escribir un resultado parcial del Map en forma de tupla (key,value)
	public void EmitResult(String key, int value)
	{
		if (MapReduce.DEBUG) System.err.println("DEBUG::Map emit result " + key + " -> " + value);
		//Output.put(key,Integer.valueOf(value));
        mapReduce.AddShuffleInput(key, value);
		totalWords++;
	}

    @Override
    public Long call(){
        Error result = Run();
        if(result != Error.COk)
            throw new IllegalStateException("Error executant el Map: " + result);

        return totalWords;
    }

}



