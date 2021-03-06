package suadb.buffer;

import suadb.file.*;
import suadb.server.SuaDB;

/**
 * The publicly-accessible suadb.buffer manager.
 * A suadb.buffer manager wraps a basic suadb.buffer manager, and
 * provides the same methods. The difference is that
 * the methods {@link #pin(Chunk) pin} and
 * {@link #pinNew(String, PageFormatter, int) pinNew}
 * will never return null.
 * If no buffers are currently available, then the
 * calling thread will be placed on a waiting list.
 * The waiting threads are removed from the list when 
 * a suadb.buffer becomes available.
 * If a thread has been waiting for a suadb.buffer for an
 * excessive amount of time (currently, 10 seconds)
 * then a {@link BufferAbortException} is thrown.
 * @author Edward Sciore
 */

public class BufferMgr {
	private static final long MAX_TIME = 10000; // 10 seconds
	private ChunkBufferMgr chunkBufferMgr;

	/**
	 * Creates a new suadb.buffer manager having the specified
	 * number of buffers.
	 * This constructor depends on both the {@link FileMgr} and
	 * {@link suadb.log.LogMgr LogMgr} objects
	 * that it gets from the class
	 * {@link SuaDB}.
	 * Those objects are created during system initialization.
	 * Thus this constructor cannot be called until
	 * {@link SuaDB#initFileAndLogMgr(String)} or
	 * is called first.
	 * @param numbuffers the number of suadb.buffer slots to allocate
	 */
	public BufferMgr(int numbuffers) {
		chunkBufferMgr = new ChunkBufferMgr(numbuffers);
	}

	/**
	 * Pins a suadb.buffer to the specified chunk, potentially
	 * waiting until a suadb.buffer becomes available.
	 * If no suadb.buffer becomes available within a fixed
	 * time period, then a {@link BufferAbortException} is thrown.
	 * @param chunk a reference to a disk chunk
	 * @return the suadb.buffer pinned to that chunk
	 */
	public synchronized ChunkBuffer pin(Chunk chunk) {
		try {
			long timestamp = System.currentTimeMillis();
			ChunkBuffer buff = chunkBufferMgr.pin(chunk);
			while (buff == null && !waitingTooLong(timestamp)) {//wait to allocate the chunkBuffer to chunk
				wait(MAX_TIME);
				buff = chunkBufferMgr.pin(chunk);
			}
			if (buff == null)
				throw new BufferAbortException();
			return buff;
		}
		catch(InterruptedException e) {
			throw new BufferAbortException();
		}
	}

	/**
	 * Pins a suadb.buffer to a new chunk in the specified suadb.file,
	 * potentially waiting until a suadb.buffer becomes available.
	 * If no suadb.buffer becomes available within a fixed
	 * time period, then a {@link BufferAbortException} is thrown.
	 * @param filename the name of the suadb.file
	 * @param fmtr the formatter used to initialize the page
	 * @param numberOfBlocks The number of blocks to create a chunk.
	 * @return the suadb.buffer pinned to that chunk
	 */
	public synchronized ChunkBuffer pinNew(String filename, PageFormatter fmtr, int numberOfBlocks) {
		try {
			long timestamp = System.currentTimeMillis();
			ChunkBuffer buff = chunkBufferMgr.pinNew(filename, fmtr, numberOfBlocks);
			while (buff == null && !waitingTooLong(timestamp)) {
				wait(MAX_TIME);
				buff = chunkBufferMgr.pinNew(filename, fmtr, numberOfBlocks);
			}
			if (buff == null)
				throw new BufferAbortException();

			return buff;
		}
		catch(InterruptedException e) {
			throw new BufferAbortException();
		}
	}

	/**
	 * Unpins the specified suadb.buffer.
	 * If the suadb.buffer's pin count becomes 0,
	 * then the threads on the wait list are notified.
	 * @param buff the suadb.buffer to be unpinned
	 */
	public synchronized void unpin(ChunkBuffer buff) {
		chunkBufferMgr.unpin(buff);
		if (!buff.isPinned())
			notifyAll();
	}

	/**
	 * Flushes the dirty buffers modified by the specified transaction.
	 * @param txnum the transaction's id number
	 */
	public void flushAll(int txnum) {
		chunkBufferMgr.flushAll(txnum);
	}

	public void flushAll()
	{
		chunkBufferMgr.flushAll();
	}
	
	/**
	 * Returns the number of available (ie unpinned) buffers.
	 * @return the number of available buffers
	 */
	public int available() {
		return chunkBufferMgr.available();
	}

	private boolean waitingTooLong(long starttime) {
		return System.currentTimeMillis() - starttime > MAX_TIME;
	}
}
