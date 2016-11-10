package suadb.tx.recovery;

import static suadb.tx.recovery.LogRecord.*;
import suadb.file.Chunk;
import suadb.buffer.ChunkBuffer;
import suadb.server.SuaDB;

import java.util.*;

/**
 * The recovery manager.  Each transaction has its own recovery manager.
 * @author Edward Sciore
 */
public class RecoveryMgr {
	private int txnum;

	/**
	 * Creates a recovery manager for the specified transaction.
	 * @param txnum the ID of the specified transaction
	 */
	public RecoveryMgr(int txnum) {
		this.txnum = txnum;
		new StartRecord(txnum).writeToLog();
	}

	/**
	 * Writes a commit suadb.record to the log, and flushes it to disk.
	 */
	public void commit() {
		SuaDB.bufferMgr().flushAll(txnum);
		int lsn = new CommitRecord(txnum).writeToLog();
		SuaDB.logMgr().flush(lsn);
	}

	/**
	 * Writes a rollback suadb.record to the log, and flushes it to disk.
	 */
	public void rollback() {
		doRollback();
		SuaDB.bufferMgr().flushAll(txnum);
		int lsn = new RollbackRecord(txnum).writeToLog();
		SuaDB.logMgr().flush(lsn);
	}

	/**
	 * Recovers uncompleted transactions from the log,
	 * then writes a quiescent checkpoint suadb.record to the log and flushes it.
	 */
	public void recover() {
		doRecover();
		SuaDB.bufferMgr().flushAll(txnum);
		int lsn = new CheckpointRecord().writeToLog();
		SuaDB.logMgr().flush(lsn);

	}

	/**
	 * Writes a setint suadb.record to the log, and returns its lsn.
	 * Updates to temporary files are not logged; instead, a
	 * "dummy" negative lsn is returned.
	 * @param buff the suadb.buffer containing the page
	 * @param offset the offset of the value in the page
	 * @param newval the value to be written
	 */
	public int setInt(ChunkBuffer buff, int offset, int newval) {
		int oldval = buff.getInt(offset);
		Chunk blk = buff.chunk();
		if (isTempBlock(blk))
			return -1;
		else
			return new SetIntRecord(txnum, blk, offset, oldval).writeToLog();
	}

	/**
	 * Writes a setstring suadb.record to the log, and returns its lsn.
	 * Updates to temporary files are not logged; instead, a
	 * "dummy" negative lsn is returned.
	 * @param buff the suadb.buffer containing the page
	 * @param offset the offset of the value in the page
	 * @param newval the value to be written
	 */
	public int setString(ChunkBuffer buff, int offset, String newval) {
		String oldval = buff.getString(offset);
		Chunk blk = buff.chunk();
		if (isTempBlock(blk))
			return -1;
		else
			return new SetStringRecord(txnum, blk, offset, oldval).writeToLog();
	}

	/**
	 * Rolls back the transaction.
	 * The method iterates through the log records,
	 * calling undo() for each log suadb.record it finds
	 * for the transaction,
	 * until it finds the transaction's START suadb.record.
	 */
	private void doRollback() {
		Iterator<LogRecord> iter = new LogRecordIterator();
		while (iter.hasNext()) {
			LogRecord rec = iter.next();
			if (rec.txNumber() == txnum) {
				if (rec.op() == START)
					return;
				rec.undo(txnum);
			}
		}
	}

	/**
	 * Does a complete database recovery.
	 * The method iterates through the log records.
	 * Whenever it finds a log suadb.record for an unfinished
	 * transaction, it calls undo() on that suadb.record.
	 * The method stops when it encounters a CHECKPOINT suadb.record
	 * or the end of the log.
	 */
	private void doRecover() {
		Collection<Integer> finishedTxs = new ArrayList<Integer>();
		Iterator<LogRecord> iter = new LogRecordIterator();
		while (iter.hasNext()) {
			LogRecord rec = iter.next();
			if (rec.op() == CHECKPOINT)
				return;
			if (rec.op() == COMMIT || rec.op() == ROLLBACK)
				finishedTxs.add(rec.txNumber());
			else if (!finishedTxs.contains(rec.txNumber()))
				rec.undo(txnum);
		}
	}

	/**
	 * Determines whether a chunk comes from a temporary suadb.file or not.
	 */
	private boolean isTempBlock(Chunk blk) {
		return blk.fileName().startsWith("temp");
	}
}
