package suadb.record;

import suadb.file.Chunk;
import suadb.tx.Transaction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Created by ILHYUN on 2016-11-17.
 */
public class ArrayFile {
    private ArrayInfo ai;
    private Transaction tx;
    private String filename;
    private CellFile currentCFiles[];
    // total number of chunks of the array - IHSUH
    private int numberofchunks = 1;

    private Collection<String> dimensions;
    // total number of dimensions of the array -IHSUH
    private int numberofdimensions;
    // number of chunks per dimension - IHSUh
    private int nunberofchunksperdimension[];


    private Collection<String> attributes;
    private int numberofattributes;
    /**
     * Constructs an object to manage a suadb.file of records.
     * If the suadb.file does not exist, it is created.
     * @param ai the table suadb.metadata
     * @param tx the transaction
     */
    public ArrayFile(ArrayInfo ai, Transaction tx) {
        this.ai = ai;
        this.tx = tx;

        // set up array information from schema
        dimensions = ai.schema().dimensions();
        attributes = ai.schema().attributes();
        this.numberofdimensions = dimensions.size();
        this.numberofattributes = attributes.size();
        nunberofchunksperdimension = new int[numberofdimensions];
        int index = 0;
        for(String dimname: dimensions){
            // total number of chunks is multiplication of chunks of each dimension
            nunberofchunksperdimension[index] = (int)Math.ceil(((float)(ai.schema().end(dimname) - ai.schema().start(dimname)+1))/dimensions.size());
            index++;
        }
        for(int i = 0 ; i < numberofdimensions ; i++){
            numberofchunks *= nunberofchunksperdimension[i];
        }

        // set up CellFiles that corresponds to the array ( for each attribute )
        currentCFiles = new CellFile[numberofattributes];
        int j = 0;
        for(String attributename : attributes){
            currentCFiles[j] = new CellFile(ai,tx,0,attributename);
            j++;
        }
    }

    /**
     * Closes the suadb.record suadb.file.
     */
    public void close() {
        for( int j = 0 ; j < numberofattributes ; j++){
            currentCFiles[j].close();
        }

    }

}
