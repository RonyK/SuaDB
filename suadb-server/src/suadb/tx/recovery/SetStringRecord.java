package suadb.tx.recovery;

import suadb.file.Chunk;
import suadb.server.SuaDB;
import suadb.buffer.*;
import suadb.log.BasicLogRecord;

class SetStringRecord implements LogRecord {
	private int txnum, offset;
	private String val;
	private Chunk blk;

	/**
	 * Creates a new setstring log suadb.record.
	 * @param txnum the ID of the specified transaction
	 * @param blk the chunk containing the value
	 * @param offset the offset of the value in the chunk
	 * @param val the new value
	 */
	public SetStringRecord(int txnum, Chunk blk, int offset, String val) {
		this.txnum = txnum;
		this.blk = blk;
		this.offset = offset;
		this.val = val;
	}

	/**
	 * Creates a log suadb.record by reading five other values from the log.
	 * @param rec the basic log suadb.record
	 */
	public SetStringRecord(BasicLogRecord rec) {
		txnum = rec.nextInt();
		String filename = rec.nextString();
		int blknum = rec.nextInt();
		blk = new Chunk(filename, blknum);
		offset = rec.nextInt();
		val = rec.nextString();
	}

	/**
	 * Writes a setString suadb.record to the log.
	 * This log suadb.record contains the SETSTRING operator,
	 * followed by the transaction id, the filename, number,
	 * and offset of the modified chunk, and the previous
	 * string value at that offset.
	 * @return the LSN of the last log value
	 */
	public int writeToLog() {
		Object[] rec = new Object[] {SETSTRING, txnum, blk.fileName(),
			blk.number(), offset, val};
		return logMgr.append(rec);
	}

	public int op() {
		return SETSTRING;
	}

	public int txNumber() {
		return txnum;
	}

	public String toString() {
		return "<SETSTRING " + txnum + " " + blk + " " + offset + " " + val + ">";
	}

	/**
	 * Replaces the specified data value with the value saved in the log suadb.record.
	 * The method pins a suadb.buffer to the specified chunk,
	 * calls setString to restore the saved value
	 * (using a dummy LSN), and unpins the suadb.buffer.
	 * @see suadb.tx.recovery.LogRecord#undo(int)
	 */
	public void undo(int txnum) {
		BufferMgr buffMgr = SuaDB.bufferMgr();
		ChunkBuffer buff = buffMgr.pin(blk);
		buff.setString(offset, val, txnum, -1);
		buffMgr.unpin(buff);
	}
}
