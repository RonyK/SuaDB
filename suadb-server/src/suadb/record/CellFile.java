package suadb.record;

import suadb.file.Chunk;
import suadb.tx.Transaction;

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
    private int currentblknum;
    private int chunknum;

    /**
     * Constructs an object to manage a suadb.file of records.
     * If the suadb.file does not exist, it is created.
     * @param ai the table suadb.metadata
     * @param tx the transaction
     */
    /*
    public CellFile(ArrayInfo ai, Transaction tx) {
        this.ai = ai;
        this.tx = tx;
        filename = ai.fileName();
        if (tx.size(filename) == 0)
            appendBlock();
        moveTo(0);
    }

*/
    /**
     * Constructs an object to manage a suadb.file of records.
     * If the suadb.file does not exist, it is created.
     * @param ai the table suadb.metadata
     * @param tx the transaction
     * @param chunknum the chunk number of an array
     * @param attributename the name of attribute (SuaDB adopts separate storage for each attribute)
     */

    public CellFile(ArrayInfo ai, Transaction tx, int chunknum, String attributename) {
        this.ai = ai;
        this.tx = tx;
        this.chunknum = chunknum;
        this.attributename = attributename;
        filename = ai.arrayName() + "_" + attributename + "_" + chunknum;
        if (tx.size(filename) == 0)
            appendBlock();
        moveTo(0);
    }


    /**
     * Closes the suadb.record suadb.file.
     */
    public void close() {
        cp.close();
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
            if (atLastBlock())
                return false;
            moveTo(currentblknum + 1);
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
     * Inserts a new, blank suadb.record somewhere in the suadb.file
     * beginning at the current suadb.record.
     * If the new suadb.record does not fit into an existing chunk,
     * then a new chunk is appended to the suadb.file.
     */
    public void insert() {
        while (!cp.insert()) {
            if (atLastBlock())
                appendBlock();
            moveTo(currentblknum + 1);
        }
    }

    /**
     * Positions the current suadb.record as indicated by the
     * specified RID.
     * @param rid a suadb.record identifier
     */
    public void moveToRid(RID rid) {
        moveTo(rid.blockNumber());
        cp.moveToId(rid.id());
    }

    /**
     * Returns the RID of the current suadb.record.
     * @return a suadb.record identifier
     */
    public RID currentRid() {
        int id = cp.currentId();
        return new RID(currentblknum, id);
    }

    private void moveTo(int b) {
        if (cp != null)
            cp.close();
        currentblknum = b;
        Chunk blk = new Chunk(filename, currentblknum);
        cp = new CellPage(blk, ai, tx, ai.recordLength(attributename));
    }

    // TODO :: Change Block to chunk
    private boolean atLastBlock() {
        return currentblknum == tx.size(filename) - 1;
    }

    // TODO :: Change to appendChunk
    private void appendBlock() {
        CellFormatter fmtr = new CellFormatter(ai,attributename);
        tx.append(filename, fmtr);
    }
}