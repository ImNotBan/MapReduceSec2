import java.io.*;
import java.util.Vector;

public class Split implements Runnable
{
    private Vector<MapInputTuple> Input = new Vector<MapInputTuple>();

    public Vector<MapInputTuple> getInput()
    {
        return(Input);
    }


    private final String fileName;

    public Split(String fileName){
        this.fileName = fileName;
    }

    private Error result = Error.CError;
    public Error getResult(){
        return result;
    }

    @Override
    public void run(){
        try {
            result = ReadFileTuples(fileName);
        } catch (RuntimeException e) {
            System.err.println("Split::ERROR Reading file " + fileName + ": " + e.getMessage());
            result = Error.CErrorReadingFile;
        }
    }


    // Lee fichero de entrada (split) línea a línea y lo guarda en un vector en forma de tuplas (key,value).
    public Error ReadFileTuples(String fileName)
    {
        long Offset=0;
        if (Thread.currentThread().isInterrupted())
            return Error.CError;

        // El lector es tanca tant si la lectura acaba be com si falla.
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(fileName)))) {
            String line;
            while ((line = br.readLine())!=null)
            {
                if (Thread.currentThread().isInterrupted())
                    return Error.CError;
                if (MapReduce.DEBUG) System.err.println("DEBUG::Map input " + Offset + " -> " + line);
                AddInput(new MapInputTuple(Offset, line));
                Offset+=line.length();
            }
        } catch (FileNotFoundException e) {
            System.err.println("Split::ERROR File "+fileName+" not found.");
            e.printStackTrace();
            return(Error.CErrorOpenInputFile);
        } catch (IOException e) {
            System.err.println("Split::ERROR Reading file "+fileName+".");
            e.printStackTrace();
            return(Error.CErrorReadingFile);
        }

        return(Error.COk);
    }

    public void AddInput(MapInputTuple tuple) { Input.add(tuple); }

}
