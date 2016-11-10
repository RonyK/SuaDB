package suadb.tx.recovery;

import suadb.server.SuaDB;
import suadb.buffer.*;
import suadb.file.Chunk;
import suadb.log.BasicLogRecord;

class SetIntRecord implements LogRecord {
	private int txnum, offset, val;
	private Chunk blk;

	/**
	 * Creates a new setint log suadb.record.
	 * @param txnum the ID of the specified transaction
	 * @param blk the chunk containing the value
	 * @param offset the offset of the value in the chunk
	 * @param val the new value
	 */
	public SetIntRecord(int txnum, Chunk blk, int offset, int val) {
		this.txnum = txnum;
		this.blk = blk;
		this.offset = offset;
		this.val = val;
	}

	/**
	 * Creates a log suadb.record by reading five other values from the log.
	 * @param rec the basic log suadb.record
	 */
	public SetIntRecord(BasicLogRecord rec) {
		txnum = rec.nextInt();
		String filename = rec.nextString();
		int blknum = rec.nextInt();
		blk = new Chunk(filename, blknum);
		offset = rec.nextInt();
		val = rec.nextInt();
	}

	/**
	 * Writes a setInt suadb.record to the log.
	 * This log suadb.record contains the SETINT operator,
	 * followed by the transaction id, the filename, number,
	 * and offset of the modified chunk, and the previous
	 * integer value at that offset.
	 * @return the LSN of the last log value
	 */
	public int writeToLog() {
		Object[] rec = new Object[] {SETINT, txnum, blk.fileName(),
			blk.number(), offset, val};
		return logMgr.append(rec);
	}

	public int op() {
		return SETINT;
	}

	public int txNumber() {
		return txnum;
	}

	public String toString() {
		return "<SETINT " + txnum + " " + blk + " " + offset + " " + val + ">";
	}

	/**
	 * Replaces the specified data value with the value saved in the log suadb.record.
	 * The method pins a suadb.buffer to the specified chunk,
	 * calls setInt to restore the saved value
	 * (using a dummy LSN), and unpins the suadb.buffer.
	 * @see suadb.tx.recovery.LogRecord#undo(int)
	 */
	public void undo(int txnum) {
		BufferMgr buffMgr = SuaDB.bufferMgr();
		ChunkBuffer buff = buffMgr.pin(blk);
		buff.setInt(offset, val, txnum, -1);
		buffMgr.unpin(buff);
	}
}
