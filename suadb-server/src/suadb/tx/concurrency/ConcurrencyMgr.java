package suadb.tx.concurrency;

import suadb.file.Chunk;

import java.util.*;

/**
 * The concurrency manager for the transaction.
 * Each transaction has its own concurrency manager. 
 * The concurrency manager keeps track of which locks the 
 * transaction currently has, and interacts with the
 * global lock table as needed. 
 * @author Edward Sciore
 */
public class ConcurrencyMgr {

	/**
	 * The global lock table.  This variable is static because all transactions
	 * share the same table.
	 */
	private static LockTable locktbl = new LockTable();
	private Map<Chunk,String> locks  = new HashMap<Chunk,String>();

	/**
	 * Obtains an SLock on the chunk, if necessary.
	 * The method will ask the lock table for an SLock
	 * if the transaction currently has no locks on that chunk.
	 * @param chunk a reference to the disk chunk
	 */
	public void sLock(Chunk chunk) {
		if (locks.get(chunk) == null) {
			locktbl.sLock(chunk);
			locks.put(chunk, "S");
		}
	}

	/**
	 * Obtains an XLock on the chunk, if necessary.
	 * If the transaction does not have an XLock on that chunk,
	 * then the method first gets an SLock on that chunk
	 * (if necessary), and then upgrades it to an XLock.
	 * @param chunk a refrence to the disk chunk
	 */
	public void xLock(Chunk chunk) {
		if (!hasXLock(chunk)) {
			sLock(chunk);
			locktbl.xLock(chunk);
			locks.put(chunk, "X");
		}
	}

	/**
	 * Releases all locks by asking the lock table to
	 * unlock each one.
	 */
	public void release() {
		for (Chunk chunk : locks.keySet())
			locktbl.unlock(chunk);
		locks.clear();
	}

	private boolean hasXLock(Chunk chunk) {
		String locktype = locks.get(chunk);
		return locktype != null && locktype.equals("X");
	}
}
