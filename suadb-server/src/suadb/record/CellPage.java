package suadb.record;

import suadb.file.Chunk;
import suadb.tx.Transaction;

import static suadb.file.Page.BLOCK_SIZE;
import static suadb.file.Page.INT_SIZE;

/**
 * Manages the placement and access of cells in a chunk.
 * @author ILHYUN
 */
public class CellPage {
    public static final int EMPTY = 0, INUSE = 1;

    private Chunk chunk;
    private ArrayInfo ai;
    private Transaction tx;
    private int slotSize;
    private int currentId = -1;
	
    // IHSUH blockPadding for block
    private int blockPadding = 0;
	private int cellFlag[];
	private int numCellsInBlock;
	private int numCellsInChunk;

    /** Creates the suadb.record manager for the specified chunk.
     * The current suadb.record is set to be prior to the first one.
     * @param chunk a reference to the disk chunk
     * @param ai the array's suadb.metadata
     * @param tx the transaction performing the operations
     * @param recordlen the lengthe of a record
     */
    public CellPage(Chunk chunk, ArrayInfo ai, Transaction tx, int recordlen) {
        this.chunk = chunk;
        this.ai = ai;
        this.tx = tx;
	
	    slotSize = recordlen + INT_SIZE;
	    blockPadding = BLOCK_SIZE % slotSize;
	    this.numCellsInBlock = (int)Math.floor((double)BLOCK_SIZE / slotSize);
	    this.numCellsInChunk = numCellsInBlock * chunk.getNumOfBlocks();
	
        tx.pin(chunk);
	
	    getAllFlags();
    }
    
    private void getAllFlags()
    {
	    cellFlag = new int[numCellsInChunk];
	
	    for(int i = 0; i < numCellsInChunk; i++)
	    {
		    int blockSeq = i / numCellsInBlock;
		    int position = i * slotSize + blockSeq * blockPadding;
		    cellFlag[i] = tx.getInt(chunk, position);
	    }
    }
    
    public int[] cellFlag()
    {
	    return cellFlag.clone();
    }

    /**
     * Closes the manager, by unpinning the chunk.
     */
    public void close() {
        if (chunk != null) {
            tx.unpin(chunk);
            chunk = null;
        }
    }

    /**
     * Moves to the next suadb.record in the chunk.
     * @return false if there is no next suadb.record.
     */
    public boolean next() {
	    if(isValidSlot())
	    {
		    return true;
	    }
	    
	    return false;
    }
//
//	public boolean next() {
//		return searchFor(INUSE);
//	}
//
	
	private boolean searchFor(int flag) {
		currentId++;
		while (isValidSlot())
		{
			if(cellFlag[currentId] == flag)
			{
				return true;
			}
			currentId++;
		}
		
		return false;
	}

    /**
     * Returns the integer value stored for the
     * specified field of the current suadb.record.
     * @return the integer stored in that field
     */
    public int getInt() {
        int position = currentpos() + INT_SIZE;
        return tx.getInt(chunk, position);
    }

    /**
     * Returns the string value stored for the
     * specified field of the current suadb.record.
     * @return the string stored in that field
     */
    public String getString() {
        int position = currentpos() + INT_SIZE;
        return tx.getString(chunk, position);
    }

    /**
     * Stores an integer at the specified field
     * of the current suadb.record.
     * @param val the integer value stored in that field
     */
    public void setInt(int val) {
        int position = currentpos();
        tx.setInt(chunk, position, INUSE);
	    cellFlag[currentId] = INUSE;
        position += INT_SIZE;
        tx.setInt(chunk, position, val);
    }

    /**
     * Stores a string at the specified field
     * of the current suadb.record.
     * @param val the string value stored in that field
     */
    public void setString( String val) {
        int position = currentpos();
        tx.setInt(chunk, position, INUSE);
	    cellFlag[currentId] = INUSE;
        position += INT_SIZE;
        tx.setString(chunk, position, val);
    }

    /**
     * Deletes the current suadb.record.
     * Deletion is performed by just marking the suadb.record
     * as "deleted"; the current suadb.record does not change.
     * To get to the next suadb.record, call next().
     */
    public void delete() {
        int position = currentpos();
        tx.setInt(chunk, position, EMPTY);
    }

    /**
     * Sets the current suadb.record to be the suadb.record having the
     * specified ID.
     * @param id the ID of the suadb.record within the page.
     */
    public void moveToId(int id) {
        currentId = id;
    }

    /**
     * Returns the ID of the current suadb.record.
     * @return the ID of the current suadb.record
     */
    public int currentId() {
        return currentId;
    }
    
    public void setCurrentId(int currentId){
        this.currentId = currentId;
    }
    
    private int currentpos() {
        int blockSeq = currentId / numCellsInBlock;
        return (currentId * slotSize) + blockSeq * blockPadding;
    }

    private boolean isValidSlot() {
        return currentpos() + slotSize <= BLOCK_SIZE * chunk.getNumOfBlocks();
    }

	/**
     * If current position is null, return true.
     * @return
     */
    public boolean isNull(){
	    if(cellFlag[currentId] == EMPTY)
	    {
		    return true;
	    }else
	    {
		    return false;
	    }
    }

	/**
	 * Get the chunk for identifying dimensions.
     * @return Chunk
     */
    public Chunk chunk(){
        return chunk;
    }
}
