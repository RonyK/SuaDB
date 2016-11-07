package suadb.record;

import suadb.file.Block;
import suadb.tx.Transaction;

/**
 * Manages a suadb.file of records.
 * There are methods for iterating through the records
 * and accessing their contents.
 * @author Edward Sciore
 */
public class RecordFile {
	private TableInfo ti;
	private Transaction tx;
	private String filename;
	private RecordPage rp;
	private int currentblknum;

	/**
	 * Constructs an object to manage a suadb.file of records.
	 * If the suadb.file does not exist, it is created.
	 * @param ti the table suadb.metadata
	 * @param tx the transaction
	 */
	public RecordFile(TableInfo ti, Transaction tx) {
		this.ti = ti;
		this.tx = tx;
		filename = ti.fileName();
		if (tx.size(filename) == 0)
			appendBlock();
		moveTo(0);
	}

	/**
	 * Closes the suadb.record suadb.file.
	 */
	public void close() {
		rp.close();
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
			if (rp.next())
				return true;
			if (atLastBlock())
				return false;
			moveTo(currentblknum + 1);
		}
	}

	/**
	 * Returns the value of the specified field
	 * in the current suadb.record.
	 * @param fldname the name of the field
	 * @return the integer value at that field
	 */
	public int getInt(String fldname) {
		return rp.getInt(fldname);
	}

	/**
	 * Returns the value of the specified field
	 * in the current suadb.record.
	 * @param fldname the name of the field
	 * @return the string value at that field
	 */
	public String getString(String fldname) {
		return rp.getString(fldname);
	}

	/**
	 * Sets the value of the specified field
	 * in the current suadb.record.
	 * @param fldname the name of the field
	 * @param val the new value for the field
	 */
	public void setInt(String fldname, int val) {
		rp.setInt(fldname, val);
	}

	/**
	 * Sets the value of the specified field
	 * in the current suadb.record.
	 * @param fldname the name of the field
	 * @param val the new value for the field
	 */
	public void setString(String fldname, String val) {
		rp.setString(fldname, val);
	}

	/**
	 * Deletes the current suadb.record.
	 * The client must call next() to move to
	 * the next suadb.record.
	 * Calls to methods on a deleted suadb.record
	 * have unspecified behavior.
	 */
	public void delete() {
		rp.delete();
	}

	/**
	 * Inserts a new, blank suadb.record somewhere in the suadb.file
	 * beginning at the current suadb.record.
	 * If the new suadb.record does not fit into an existing block,
	 * then a new block is appended to the suadb.file.
	 */
	public void insert() {
		while (!rp.insert()) {
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
		rp.moveToId(rid.id());
	}

	/**
	 * Returns the RID of the current suadb.record.
	 * @return a suadb.record identifier
	 */
	public RID currentRid() {
		int id = rp.currentId();
		return new RID(currentblknum, id);
	}

	private void moveTo(int b) {
		if (rp != null)
			rp.close();
		currentblknum = b;
		Block blk = new Block(filename, currentblknum);
		rp = new RecordPage(blk, ti, tx);
	}

	private boolean atLastBlock() {
		return currentblknum == tx.size(filename) - 1;
	}

	private void appendBlock() {
		RecordFormatter fmtr = new RecordFormatter(ti);
		tx.append(filename, fmtr);
	}
}