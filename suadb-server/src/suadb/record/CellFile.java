package suadb.record;

import suadb.file.Chunk;
import suadb.file.FileMgr;
import suadb.server.SuaDB;
import suadb.tx.Transaction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * Manages a suadb.file of records.
 * There are methods for iterating through the records
 * and accessing their contents.
 * @author Edward Sciore
 */
public class CellFile {
    private ArrayInfo ai;
    private Transaction tx;
    private String filename;
    private String attributename;
    private CellPage cp;
    private int currentchunknum = -1;
    private int numberofblocks;
    private int numberofchunks;         // this variable is needed in order to find out if the current chunk is the last one

	/**
	 * Constructs an object to manage a suadb.file of records.
	 * If the suadb.file does not exist, it is created.
	 * @param ai the table suadb.metadata
	 * @param tx the transaction
	 */

    /**
     * Constructs an object to manage a suadb.file of records.
     * If the suadb.file does not exist, it is created.
     * @param ai the table suadb.metadata
     * @param tx the transaction
     * @param chunknum the chunk number of an array
     * @param attributename the name of attribute (SuaDB adopts separate storage for each attribute)
     */

    public CellFile(ArrayInfo ai, Transaction tx, int chunknum, String attributename, int numberofblocks,int numberofchunks) {
        this.ai = ai;
        this.tx = tx;
       // this.currentchunknum = chunknum;
        this.attributename = attributename;
        this.numberofblocks = numberofblocks;
        this.numberofchunks = numberofchunks;
        // move to the specified chunk , updates itself
        moveTo(chunknum);

    }

    private String assignName(ArrayInfo ai, String attributename, int currentchunknum){
        return ai.arrayName() + "_" + attributename + "_" + currentchunknum;
    }

    /**
     * Closes the suadb.record suadb.file.
     */
    public void close() throws IOException{
        cp.close();
        SuaDB.fileMgr().flushFile(filename);
    }

    /**
     * Positions the current suadb.record so that a call to method next
     * will wind up at the first suadb.record.
     */
    public void beforeFirst() {
        moveTo(0);
    }

    /**
     * Moves to the next suadb.record. Returns false if there
     * is no next suadb.record.
     * @return false if there is no next suadb.record.
     */
    public boolean next() {
        while (true) {
            if (cp.next())
                return true;
            if (atLaskChunk())
                return false;
            moveTo(currentchunknum + 1);
        }
    }

    /**
     * Returns the value of the specified field
     * in the current suadb.record.
     * @return the integer value at that field
     */
    public int getInt() {
        return cp.getInt();
    }

    /**
     * Returns the value of the specified field
     * in the current suadb.record.
     * @return the string value at that field
     */
    public String getString() {
        return cp.getString();
    }

    /**
     * Sets the value of the specified field
     * in the current suadb.record.
     * @param val the new value for the field
     */
    public void setInt(int val) {
        cp.setInt(val);
    }

    /**
     * Sets the value of the specified field
     * in the current suadb.record.
     * @param val the new value for the field
     */
    public void setString(String val) {
        cp.setString(val);
    }

    /**
     * Deletes the current suadb.record.
     * The client must call next() to move to
     * the next suadb.record.
     * Calls to methods on a deleted suadb.record
     * have unspecified behavior.
     */
    public void delete() {
        cp.delete();
    }


    /**
     * Positions the current suadb.record as indicated by the
     * specified RID.
     * @param offset a offset of the cell in this chunk
     */
    public void moveToId(int offset) {
       // moveTo(rid.blockNumber());
        cp.moveToId(offset);
    }

    public void moveToId(int offset, int record) {
        // moveTo(rid.blockNumber());
        cp.moveToId(offset);
    }

    /**
     * Returns the RID of the current suadb.record.
     * @return a suadb.record identifier
     */
    public RID currentRid() {
        int id = cp.currentId();
        return new RID(currentchunknum, id);
    }

    public void moveTo(int c) {
        if( currentchunknum == c) {
	        cp.setCurrentId(-1);//Initialize slot
	        return;
        }

        if (cp != null) {
            cp.close();
            try {
                SuaDB.fileMgr().flushFile(filename);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        currentchunknum = c;
        // Update filename  - ILHYUN
        this.filename = assignName(this.ai,this.attributename,currentchunknum);
        if (tx.size(filename) == 0)
            createChunk(c);
        Chunk blk = new Chunk(filename, currentchunknum,numberofblocks);
        cp = new CellPage(blk, ai, tx, ai.recordLength(attributename));
    }

    private boolean atLaskChunk() {
        return currentchunknum == (numberofchunks-1);
    }


    private void createChunk(int chunknum) {
        CellFormatter fmtr = new CellFormatter(ai,attributename);
        String filename =assignName(this.ai,this.attributename,chunknum);
        tx.createNewChunk(filename, fmtr,numberofblocks);

    }


	/**

     * Get current CID for identifying dimensions.
     * @return CID
     */
    public CID getCurrentCID(){
        return new CID(getCurrentDimensionValues(),ai);
    }

	/**
	 * Get current dimension values.
	 * @return List<Integer> the dimension.
	 */
    private List<Integer> getCurrentDimensionValues(){
        //현재 몇번째 chunk == currentchunknum
	    Schema schema = ai.schema();
	    List<String> dimensions =  new ArrayList<String>(ai.schema().dimensions());
	    int numOfDimensions = dimensions.size();
	    List<Integer> result = new ArrayList<Integer>();

		//Convert logical chunk number to the left bottom cell's coordinate of the chunk.
	    int chunkNum = currentchunknum;
	    int temp = 1;
	    for(int i=0;i<numOfDimensions;i++)
		    temp *= schema.getNumOfChunk(dimensions.get(i));

	    for(int i=0;i<numOfDimensions;i++){
			temp /= schema.getNumOfChunk(dimensions.get(i));

		    result.add(
				    (chunkNum/temp) * schema.chunkSize(dimensions.get(i)));
		    chunkNum %= temp;
	    }

	    //Calculate the coordinate of the slot in the chunk.
	    temp = 1;
	    for(int i=0;i<numOfDimensions;i++)
		    temp *= schema.chunkSize(dimensions.get(i));

	    int currentSlotNum = cp.currentId();
	    for(int i=0;i<numOfDimensions;i++){
		    temp /= schema.chunkSize(dimensions.get(i));

		    result.set(i,
				    result.get(i) + (currentSlotNum/temp));
		    currentSlotNum %= temp;
	    }

	    return result;
    }

	/**
	 * Get current chunk number.
	 * @return
	 */
	public int getCurrentchunknum() {
		return currentchunknum;
	}
}

