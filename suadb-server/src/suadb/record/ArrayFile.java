package suadb.record;

import java.util.ArrayList;
import java.util.List;

import suadb.tx.Transaction;

import static suadb.file.Page.BLOCK_SIZE;
import static suadb.file.Page.INT_SIZE;

/**
 * Created by ILHYUN on 2016-11-17.
 */
public class ArrayFile
{
    private ArrayInfo ai;
    private Transaction tx;
    private String filename;
    private CellFile currentCFiles[];
	
    // total number of chunks of the array - IHSUH
    private int numberofchunks = 1;

    private List<String> dimensions;
    // total number of dimensions of the array -IHSUH
    private int numberofdimensions;
    // number of chunks per dimension - IHSUh
    private int numberofchunksperdimension[];

    private List<String> attributes;
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
        dimensions = new ArrayList<String>(ai.schema().dimensions());
        attributes = new ArrayList<String>(ai.schema().attributes());
        this.numberofdimensions = dimensions.size();
        this.numberofattributes = attributes.size();
        numberofchunksperdimension = new int[numberofdimensions];
        int index = 0;
        for(String dimname: dimensions){
            // total number of chunks is multiplication of chunks of each dimension
            numberofchunksperdimension[index] = (int)Math.ceil(((float)(ai.schema().end(dimname) - ai.schema().start(dimname)+1))/ai.schema().chunkSize(dimname));
            index++;
        }
        for(int i = 0 ; i < numberofdimensions ; i++){
            numberofchunks *= numberofchunksperdimension[i];
        }

        // set up CellFiles that corresponds to the array ( for each attribute )
        currentCFiles = new CellFile[numberofattributes];
        int j = 0;
        for(String attributename : attributes){
            currentCFiles[j] = new CellFile(ai,tx,0,attributename,getNumberOfBlocksPerChunk(attributename),numberofchunks);
            j++;
        }
    }

    /*
    calculates the number of blocks in a chunk for a given attributename
     */
    private int getNumberOfBlocksPerChunk(String attributename){
        int numberofcellsinablock = 1;
        int numberofcellsinachunk = 1;

        int recsize = ai.recordLength(attributename) + INT_SIZE;

        numberofcellsinablock = (int)Math.floor((double)BLOCK_SIZE / recsize);

        for(String dimensionname : dimensions){
            numberofcellsinachunk*= ai.schema().chunkSize(dimensionname);
        }
        return (int) Math.ceil( (double)numberofcellsinachunk / numberofcellsinablock);
    }
    
    private int getChunknumber(CID cid){
        int chunkindex[] = new int [numberofdimensions];
        int chunkOffset = 0;
        for(int i = 0 ; i <numberofdimensions ; i++){
            chunkindex[i] = ( (cid.dimensionValues().get(i) - ai.schema().start(dimensions.get(i)) ) / ai.schema().chunkSize(dimensions.get(i)));
        }
        for(int i = 0  ; i < numberofdimensions ; i++){
            int numberchunksinfollowingdimension = 1;
            for (int j = i + 1; j < numberofdimensions ; j++){
                numberchunksinfollowingdimension *= numberofchunksperdimension[j];
            }
            chunkOffset += numberchunksinfollowingdimension * (chunkindex[i]);
        }
        return chunkOffset;
    }

    // calculate the offset of cell in C style order
    // for example, the offset of (2,3) in [0:3,0:9] array is ((9 + 1)  * (2) ) + 3  = 23
    // watch out for incrementing by 1 of the given index;
    // TODO ::needs test!! - IHSUh
    private int calculateCellOffsetInChunk(CID cid){

        int dimensionvalueinchunk[] = new int [numberofdimensions];
        int offset = 0;
        for(int i = 0 ; i < numberofdimensions ; i++){
            dimensionvalueinchunk[i] = cid.dimensionValues().get(i) - ai.schema().start(dimensions.get(i));
            dimensionvalueinchunk[i] %= ai.schema().chunkSize(dimensions.get(i));
         }

        for(int i = 0  ; i < numberofdimensions ; i++){
            int numbercellsinfollowingdimension = 1;
            for (int j = i+1; j < numberofdimensions ; j++){
                numbercellsinfollowingdimension *= ai.schema().chunkSize(dimensions.get(j));
            }
            offset += numbercellsinfollowingdimension * (dimensionvalueinchunk[i]);
        }
        return offset;
    }

    /**
     * Closes the suadb.record suadb.file.
     */
    public void close() {
        for( int j = 0 ; j < numberofattributes ; j++){
            currentCFiles[j].close();
        }
    }

    /**
     * Positions the current suadb.record so that a call to method next
     * will wind up at the first suadb.record.
     */
    public void beforeFirst() {
        for( int j = 0 ; j < numberofattributes ; j++){
            currentCFiles[j].beforeFirst();
   //         currentCFiles[j].moveTo(0);
    //        currentCFiles[j].moveToId(0);
        }
    }

    public void beforeFirst(String attributename) {
        int index = attributes.indexOf(attributename);
        if( index < 0)
            return;
        currentCFiles[index].beforeFirst();
   //     currentCFiles[index].moveTo(0);
   //     currentCFiles[index].moveToId(0);
    }

    /**
     * Moves to the next suadb.record. Returns false if there
     * is no next suadb.record.
     * @return false if there is no next suadb.record.
     */
    public boolean next()
    {
	    if(numberofattributes <= 0)
		    return false;
	
	    boolean result = true;
	    for( int j = 0 ; j < numberofattributes ; j++)
	    {
		    result &= currentCFiles[j].next();
	    }
	
	    return result;
    }

    public boolean next(String attributename) {
        int index = attributes.indexOf(attributename);
        if( index < 0)
            return false;
        return currentCFiles[index].next();
    }

    /**
     * Returns the value of the specified field
     * in the current suadb.record.
     * @return the integer value at that field
     */
    public int getInt(String attributename) {
        int index = attributes.indexOf(attributename);
        if( index < 0)
            return -1;
        return currentCFiles[index].getInt();
    }

    /**
     * Returns the value of the specified field
     * in the current suadb.record.
     * @return the string value at that field
     */
    public String getString(String attributename) {
        int index = attributes.indexOf(attributename);
        if (index < 0)
            return null;
        return currentCFiles[index].getString();
    }

    /**
     * Sets the value of the specified field
     * in the current suadb.record.
     * @param val the new value for the field
     */
    public void setInt(String attributename,int val) {
        int index = attributes.indexOf(attributename);
        if (index < 0)
            return ;
        currentCFiles[index].setInt(val);
    }

    /**
     * Sets the value of the specified field
     * in the current suadb.record.
     * @param val the new value for the field
     */
    public void setString(String attributename,String val) {
        int index = attributes.indexOf(attributename);
        if (index < 0)
            return ;
        currentCFiles[index].setString(val);
    }

    /**
     * Deletes the current suadb.record.
     * The client must call next() to move to
     * the next suadb.record.
     * Calls to methods on a deleted suadb.record
     * have unspecified behavior.
     */
    public void delete() {
        for( int j = 0 ; j < numberofattributes ; j++){
            currentCFiles[j].delete();
        }
    }

    /**
     * Positions the current suadb.record as indicated by the
     * specified CID.
     * @param cid a suadb.record identifier
     */
    public void moveToCid(CID cid) {

        for( int j = 0 ; j < numberofattributes ; j++) {
            currentCFiles[j].moveTo(getChunknumber(cid));
            currentCFiles[j].moveToId(calculateCellOffsetInChunk(cid));
        }
    }

    public void moveToCid(String attributename, CID cid) {
        int index = attributes.indexOf(attributename);
        if (index < 0)
            return ;
        currentCFiles[index].moveTo(getChunknumber(cid));
        currentCFiles[index].moveToId(calculateCellOffsetInChunk(cid));
    }

    /**
     * Returns the RID of the current suadb.record.
     * @return a suadb.record identifier
     */

    // TODO :: this method should be implemented - IHSUh
    /*
    public CID currentCid() {
        int id = cp.currentId();
        return new CID(currentchunknum, id);
    }
    */
    
    public int getDimension(String dimName)
    {
	    // TODO :: Return Dimension Value
	    return 0;
    }

	// TODO :: Insert()                     - RonyK
	// insert data sequentially.
	// TODO :: Insert(Dimension[] dim)      - RonyK
}
