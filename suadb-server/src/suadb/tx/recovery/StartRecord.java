package suadb.tx.recovery;

import suadb.log.BasicLogRecord;

class StartRecord implements LogRecord {
	private int txnum;

	/**
	 * Creates a new start log suadb.record for the specified transaction.
	 * @param txnum the ID of the specified transaction
	 */
	public StartRecord(int txnum) {
		this.txnum = txnum;
	}

	/**
	 * Creates a log suadb.record by reading one other value from the log.
	 * @param rec the basic log suadb.record
	 */
	public StartRecord(BasicLogRecord rec) {
		txnum = rec.nextInt();
	}

	/**
	 * Writes a start suadb.record to the log.
	 * This log suadb.record contains the START operator,
	 * followed by the transaction id.
	 * @return the LSN of the last log value
	 */
	public int writeToLog() {
		Object[] rec = new Object[] {START, txnum};
		return logMgr.append(rec);
	}

	public int op() {
		return START;
	}

	public int txNumber() {
		return txnum;
	}

	/**
	 * Does nothing, because a start suadb.record
	 * contains no undo information.
	 */
	public void undo(int txnum) {}

	public String toString() {
		return "<START " + txnum + ">";
	}
}
