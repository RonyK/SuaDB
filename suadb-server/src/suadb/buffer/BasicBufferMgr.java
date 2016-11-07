package suadb.buffer;

import suadb.file.*;
import suadb.server.SuaDB;

/**
 * Manages the pinning and unpinning of buffers to blocks.
 * @author Edward Sciore
 *
 */
class BasicBufferMgr {
	private Buffer[] bufferpool;
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
		bufferpool = new Buffer[numbuffs];
		numAvailable = numbuffs;
		for (int i=0; i<numbuffs; i++)
			bufferpool[i] = new Buffer();
	}

	/**
	 * Flushes the dirty buffers modified by the specified transaction.
	 * @param txnum the transaction's id number
	 */
	synchronized void flushAll(int txnum) {
		for (Buffer buff : bufferpool)
			if (buff.isModifiedBy(txnum))
			buff.flush();
	}

	/**
	 * Pins a suadb.buffer to the specified block.
	 * If there is already a suadb.buffer assigned to that block
	 * then that suadb.buffer is used;
	 * otherwise, an unpinned suadb.buffer from the pool is chosen.
	 * Returns a null value if there are no available buffers.
	 * @param blk a reference to a disk block
	 * @return the pinned suadb.buffer
	 */
	synchronized Buffer pin(Block blk) {
		Buffer buff = findExistingBuffer(blk);
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
	 * Allocates a new block in the specified suadb.file, and
	 * pins a suadb.buffer to it.
	 * Returns null (without allocating the block) if
	 * there are no available buffers.
	 * @param filename the name of the suadb.file
	 * @param fmtr a pageformatter object, used to format the new block
	 * @return the pinned suadb.buffer
	 */
	synchronized Buffer pinNew(String filename, PageFormatter fmtr) {
		Buffer buff = chooseUnpinnedBuffer();
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
	synchronized void unpin(Buffer buff) {
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

	private Buffer findExistingBuffer(Block blk) {
		for (Buffer buff : bufferpool) {
			Block b = buff.block();
			if (b != null && b.equals(blk))
				return buff;
		}
		return null;
	}

	private Buffer chooseUnpinnedBuffer() {
		for (Buffer buff : bufferpool)
			if (!buff.isPinned())
			return buff;
		return null;
	}
}
