package suadb.record;

import static suadb.file.Page.*;
import suadb.file.Chunk;
import suadb.file.Page;
import suadb.tx.Transaction;

/**
 * Manages the placement and access of cells in a chunk.
 * @author ILHYUN
 */
public class CellPage {
    public static final int EMPTY = 0, INUSE = 1;

    private Chunk chunk;
    private ArrayInfo ai;
    private Transaction tx;
    private int slotsize;
    private int currentslot = -1;
    // IHSUH blank for block
    private int blank = 0;


    /** Creates the suadb.record manager for the specified chunk.
     * The current suadb.record is set to be prior to the first one.
     * @param chunk a reference to the disk chunk
     * @param ti the table's suadb.metadata
     * @param tx the transaction performing the operations
     */
    /*
    public CellPage(Chunk chunk, ArrayInfo ai, Transaction tx) {
        this.chunk = chunk;
        this.ai = ai;
        this.tx = tx;
        slotsize = ai.recordLength() + INT_SIZE;
        tx.pin(chunk);
    }
    */

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
        slotsize = recordlen + INT_SIZE;
        blank = BLOCK_SIZE % slotsize;
        tx.pin(chunk);
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
        return searchFor(INUSE);
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
        currentslot = id;
    }

    /**
     * Returns the ID of the current suadb.record.
     * @return the ID of the current suadb.record
     */
    public int currentId() {
        return currentslot;
    }
    public void setCurrentId(int currentslot){
        this.currentslot = currentslot;
    }
    private int currentpos() {
        int numberofcellsinablock = (int)Math.floor((double)BLOCK_SIZE / slotsize);
        int blockseq = currentslot / numberofcellsinablock;
        return (currentslot * slotsize) + blockseq * blank;
    }
/*
    private int fieldpos() {
        int offset = INT_SIZE;
        return currentpos() + offset;
    }
*/
    private boolean isValidSlot() {
        return currentpos() + slotsize <= BLOCK_SIZE*chunk.getNumOfBlocks();
    }

    private boolean searchFor(int flag) {
        currentslot++;
        while (isValidSlot()) {
            int position = currentpos();
            if (tx.getInt(chunk, position) == flag)
                return true;
            currentslot++;
        }
        return false;
    }

	/**
     * If current position is null, return true.
     * @return
     */
    public boolean isNull(){
        int position = currentpos();
        if (tx.getInt(chunk, position) == EMPTY)
            return true;
        else
            return false;
    }

	/**
	 * Get the chunk for identifying dimensions.
     * @return Chunk
     */
    public Chunk getChunk(){
        return chunk;
    }
}
