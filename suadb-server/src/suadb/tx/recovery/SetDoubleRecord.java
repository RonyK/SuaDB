package suadb.tx.recovery;

import suadb.buffer.BufferMgr;
import suadb.buffer.ChunkBuffer;
import suadb.file.Chunk;
import suadb.log.BasicLogRecord;
import suadb.server.SuaDB;

/**
 * Created by CDS on 2016-11-25.
 */
public class SetDoubleRecord implements LogRecord{
	private int txnum, offset;
	private double val;
	private Chunk chunk;

	/**
	 * Creates a new setdouble log suadb.record.
	 * @param txnum the ID of the specified transaction
	 * @param chunk the chunk containing the value
	 * @param offset the offset of the value in the chunk
	 * @param val the new value
	 */
	public SetDoubleRecord(int txnum, Chunk chunk, int offset, double val) {
		this.txnum = txnum;
		this.chunk = chunk;
		this.offset = offset;
		this.val = val;
	}

	/**
	 * Creates a log suadb.record by reading five other values from the log.
	 * @param rec the basic log suadb.record
	 */
	public SetDoubleRecord(BasicLogRecord rec) {
		txnum = rec.nextInt();
		String filename = rec.nextString();
		int chunkNum = rec.nextInt();
		chunk = new Chunk(filename, chunkNum);
		offset = rec.nextInt();
		val = rec.nextDouble();
	}

	/**
	 * Writes a setDouble suadb.record to the log.
	 * This log suadb.record contains the SETDOUBLE operator,
	 * followed by the transaction id, the filename, number,
	 * and offset of the modified chunk, and the previous
	 * integer value at that offset.
	 * @return the LSN of the last log value
	 */
	public int writeToLog() {
		Object[] rec = new Object[] {SETDOUBLE, txnum, chunk.fileName(),
				chunk.number(), offset, val};
		return logMgr.append(rec);
	}

	public int op() {
		return SETDOUBLE;
	}

	public int txNumber() {
		return txnum;
	}

	public String toString() {
		return "<SETDOUBLE " + txnum + " " + chunk + " " + offset + " " + val + ">";
	}

	/**
	 * Replaces the specified data value with the value saved in the log suadb.record.
	 * The method pins a suadb.buffer to the specified chunk,
	 * calls setDouble to restore the saved value
	 * (using a dummy LSN), and unpins the suadb.buffer.
	 * @see suadb.tx.recovery.LogRecord#undo(int)
	 */
	public void undo(int txnum) {
		BufferMgr buffMgr = SuaDB.bufferMgr();
		ChunkBuffer buff = buffMgr.pin(chunk);
		buff.setDouble(offset, val, txnum, -1);
		buffMgr.unpin(buff);
	}
}
