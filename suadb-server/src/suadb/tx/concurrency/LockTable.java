package suadb.tx.concurrency;

import suadb.file.Chunk;

import java.util.*;

/**
 * The lock table, which provides methods to lock and unlock blocks.
 * If a transaction requests a lock that causes a conflict with an
 * existing lock, then that transaction is placed on a wait list.
 * There is only one wait list for all blocks.
 * When the last lock on a chunk is unlocked, then all transactions
 * are removed from the wait list and rescheduled.
 * If one of those transactions discovers that the lock it is waiting for
 * is still locked, it will place itself back on the wait list.
 * @author Edward Sciore
 */
class LockTable {
	private static final long MAX_TIME = 10000; // 10 seconds

	private Map<Chunk,Integer> locks = new HashMap<Chunk,Integer>();

	/**
	 * Grants an SLock on the specified chunk.
	 * If an XLock exists when the method is called,
	 * then the calling thread will be placed on a wait list
	 * until the lock is released.
	 * If the thread remains on the wait list for a certain
	 * amount of time (currently 10 seconds),
	 * then an exception is thrown.
	 * @param blk a reference to the disk chunk
	 */
	public synchronized void sLock(Chunk blk) {
		try {
			long timestamp = System.currentTimeMillis();
			while (hasXlock(blk) && !waitingTooLong(timestamp))
				wait(MAX_TIME);
			if (hasXlock(blk))
				throw new LockAbortException();
			int val = getLockVal(blk);  // will not be negative
			locks.put(blk, val+1);
		}
		catch(InterruptedException e) {
			throw new LockAbortException();
		}
	}

	/**
	 * Grants an XLock on the specified chunk.
	 * If a lock of any type exists when the method is called,
	 * then the calling thread will be placed on a wait list
	 * until the locks are released.
	 * If the thread remains on the wait list for a certain
	 * amount of time (currently 10 seconds),
	 * then an exception is thrown.
	 * @param blk a reference to the disk chunk
	 */
	synchronized void xLock(Chunk blk) {
		try {
			long timestamp = System.currentTimeMillis();
			while (hasOtherSLocks(blk) && !waitingTooLong(timestamp))
				wait(MAX_TIME);
			if (hasOtherSLocks(blk))
				throw new LockAbortException();
			locks.put(blk, -1);
		}
		catch(InterruptedException e) {
			throw new LockAbortException();
		}
	}

	/**
	 * Releases a lock on the specified chunk.
	 * If this lock is the last lock on that chunk,
	 * then the waiting transactions are notified.
	 * @param blk a reference to the disk chunk
	 */
	synchronized void unlock(Chunk blk) {
		int val = getLockVal(blk);
		if (val > 1)
			locks.put(blk, val-1);
		else {
			locks.remove(blk);
			notifyAll();
		}
	}

	private boolean hasXlock(Chunk blk) {
		return getLockVal(blk) < 0;
	}

	private boolean hasOtherSLocks(Chunk blk) {
		return getLockVal(blk) > 1;
	}

	private boolean waitingTooLong(long starttime) {
		return System.currentTimeMillis() - starttime > MAX_TIME;
	}

	private int getLockVal(Chunk blk) {
		Integer ival = locks.get(blk);
		return (ival == null) ? 0 : ival.intValue();
	}
}
