package suadb.buffer;

import suadb.file.*;
import suadb.server.SuaDB;

/**
 * Manages the pinning and unpinning of buffers to blocks.
 * @author Edward Sciore
 *
 */
class BasicBufferMgr {
	private BasicBuffer[] bufferpool;
	private int numAvailable;

	/**
	 * Creates a suadb.buffer manager having the specified number
	 * of suadb.buffer slots.
	 * This constructor depends on both the {@link FileMgr} and
	 * {@link suadb.log.LogMgr LogMgr} objects
	 * that it gets from the class
	 * {@link SuaDB}.
	 * Those objects are created during system initialization.
	 * Thus this constructor cannot be called until
	 * {@link SuaDB#initFileAndLogMgr(String)} or
	 * is called first.
	 * @param numbuffs the number of suadb.buffer slots to allocate
	 */
	BasicBufferMgr(int numbuffs) {
		bufferpool = new BasicBuffer[numbuffs];
		numAvailable = numbuffs;
		for (int i=0; i<numbuffs; i++)
			bufferpool[i] = new BasicBuffer();
	}

	/**
	 * Flushes the dirty buffers modified by the specified transaction.
	 * @param txnum the transaction's id number
	 */
	synchronized void flushAll(int txnum) {
		for (BasicBuffer buff : bufferpool)
			if (buff.isModifiedBy(txnum))
				buff.flush();
	}

	/**
	 * Pins a suadb.buffer to the specified chunk.
	 * If there is already a suadb.buffer assigned to that chunk
	 * then that suadb.buffer is used;
	 * otherwise, an unpinned suadb.buffer from the pool is chosen.
	 * Returns a null value if there are no available buffers.
	 * @param blk a reference to a disk chunk
	 * @return the pinned suadb.buffer
	 */
	synchronized BasicBuffer pin(Block blk) {
		BasicBuffer buff = findExistingBuffer(blk);
		if (buff == null) {
			buff = chooseUnpinnedBuffer();
			if (buff == null)
				return null;
			buff.assignToBlock(blk);
		}
		if (!buff.isPinned())
			numAvailable--;
		buff.pin();
		return buff;
	}

	/**
	 * Allocates a new chunk in the specified suadb.file, and
	 * pins a suadb.buffer to it.
	 * Returns null (without allocating the chunk) if
	 * there are no available buffers.
	 * @param filename the name of the suadb.file
	 * @param fmtr a pageformatter object, used to format the new chunk
	 * @return the pinned suadb.buffer
	 */
	synchronized BasicBuffer pinNew(String filename, PageFormatter fmtr) {
		BasicBuffer buff = chooseUnpinnedBuffer();
		if (buff == null)
			return null;
		buff.assignToNew(filename, fmtr);
		numAvailable--;
		buff.pin();
		return buff;
	}

	/**
	 * Unpins the specified suadb.buffer.
	 * @param buff the suadb.buffer to be unpinned
	 */
	synchronized void unpin(BasicBuffer buff) {
		buff.unpin();
		if (!buff.isPinned())
			numAvailable++;
	}

	/**
	 * Returns the number of available (i.e. unpinned) buffers.
	 * @return the number of available buffers
	 */
	int available() {
		return numAvailable;
	}

	private BasicBuffer findExistingBuffer(Block chunk) {
		for (BasicBuffer buff : bufferpool) {
			Block b = buff.block();
			if (b != null && b.equals(chunk))
				return buff;
		}
		return null;
	}

	private BasicBuffer chooseUnpinnedBuffer() {
		for (BasicBuffer buff : bufferpool)
			if (!buff.isPinned())
			return buff;
		return null;
	}
}
