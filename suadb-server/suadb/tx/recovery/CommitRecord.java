package suadb.tx.recovery;

import suadb.log.BasicLogRecord;

/**
 * The COMMIT log suadb.record
 * @author Edward Sciore
 */
class CommitRecord implements LogRecord {
	private int txnum;

	/**
	 * Creates a new commit log suadb.record for the specified transaction.
	 * @param txnum the ID of the specified transaction
	 */
	public CommitRecord(int txnum) {
		this.txnum = txnum;
	}

	/**
	 * Creates a log suadb.record by reading one other value from the log.
	 * @param rec the basic log suadb.record
	 */
	public CommitRecord(BasicLogRecord rec) {
		txnum = rec.nextInt();
	}

	/**
	 * Writes a commit suadb.record to the log.
	 * This log suadb.record contains the COMMIT operator,
	 * followed by the transaction id.
	 * @return the LSN of the last log value
	 */
	public int writeToLog() {
		Object[] rec = new Object[] {COMMIT, txnum};
		return logMgr.append(rec);
	}

	public int op() {
		return COMMIT;
	}

	public int txNumber() {
		return txnum;
	}

	/**
	 * Does nothing, because a commit suadb.record
	 * contains no undo information.
	 */
	public void undo(int txnum) {}

	public String toString() {
		return "<COMMIT " + txnum + ">";
	}
}
