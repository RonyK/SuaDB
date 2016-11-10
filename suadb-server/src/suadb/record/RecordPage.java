package suadb.record;

import static suadb.file.Page.*;
import suadb.file.Chunk;
import suadb.tx.Transaction;

/**
 * Manages the placement and access of records in a chunk.
 * @author Edward Sciore
 */
public class RecordPage {
	public static final int EMPTY = 0, INUSE = 1;

	private Chunk blk;
	private TableInfo ti;
	private Transaction tx;
	private int slotsize;
	private int currentslot = -1;

	/** Creates the suadb.record manager for the specified chunk.
	  * The current suadb.record is set to be prior to the first one.
	  * @param blk a reference to the disk chunk
	  * @param ti the table's suadb.metadata
	  * @param tx the transaction performing the operations
	  */
	public RecordPage(Chunk blk, TableInfo ti, Transaction tx) {
		this.blk = blk;
		this.ti = ti;
		this.tx = tx;
		slotsize = ti.recordLength() + INT_SIZE;
		tx.pin(blk);
  }

	/**
	 * Closes the manager, by unpinning the chunk.
	 */
	public void close() {
		if (blk != null) {
	 	  tx.unpin(blk);
	 	  blk = null;
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
	 * @param fldname the name of the field.
	 * @return the integer stored in that field
	 */
	public int getInt(String fldname) {
		int position = fieldpos(fldname);
		return tx.getInt(blk, position);
	}

	/**
	 * Returns the string value stored for the
	 * specified field of the current suadb.record.
	 * @param fldname the name of the field.
	 * @return the string stored in that field
	 */
	public String getString(String fldname) {
		int position = fieldpos(fldname);
		return tx.getString(blk, position);
	}

	/**
	 * Stores an integer at the specified field
	 * of the current suadb.record.
	 * @param fldname the name of the field
	 * @param val the integer value stored in that field
	 */
	public void setInt(String fldname, int val) {
		int position = fieldpos(fldname);
		tx.setInt(blk, position, val);
	}

	/**
	 * Stores a string at the specified field
	 * of the current suadb.record.
	 * @param fldname the name of the field
	 * @param val the string value stored in that field
	 */
	public void setString(String fldname, String val) {
		int position = fieldpos(fldname);
		tx.setString(blk, position, val);
	}

	/**
	 * Deletes the current suadb.record.
	 * Deletion is performed by just marking the suadb.record
	 * as "deleted"; the current suadb.record does not change.
	 * To get to the next suadb.record, call next().
	 */
	public void delete() {
		int position = currentpos();
		tx.setInt(blk, position, EMPTY);
	}

	/**
	 * Inserts a new, blank suadb.record somewhere in the page.
	 * Return false if there were no available slots.
	 * @return false if the insertion was not possible
	 */
	public boolean insert() {
		currentslot = -1;
		boolean found = searchFor(EMPTY);
		if (found) {
			int position = currentpos();
			tx.setInt(blk, position, INUSE);
		}
		return found;
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

	private int currentpos() {
		return currentslot * slotsize;
	}

	private int fieldpos(String fldname) {
		int offset = INT_SIZE + ti.offset(fldname);
		return currentpos() + offset;
	}

	private boolean isValidSlot() {
		return currentpos() + slotsize <= BLOCK_SIZE;
	}

	private boolean searchFor(int flag) {
		currentslot++;
		while (isValidSlot()) {
			int position = currentpos();
			if (tx.getInt(blk, position) == flag)
				return true;
			currentslot++;
		}
		return false;
	}
}
