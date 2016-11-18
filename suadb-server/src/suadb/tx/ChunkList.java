package suadb.tx;

import suadb.file.Chunk;
import suadb.server.SuaDB;
import suadb.buffer.*;

import java.util.*;

/**
 * Manages the transaction's currently-pinned chunks.
 * @author Edward Sciore
 */
class ChunkList {
	private Map<Chunk, ChunkBuffer> buffers = new HashMap<Chunk,ChunkBuffer>();//Chunk & ChunkBuffer Map of a transaction - CDS
	private List<Chunk> pins = new ArrayList<Chunk>();//Chunk list of a transaction - CDS
	private BufferMgr bufferMgr = SuaDB.bufferMgr();

	/**
	 * Returns the suadb.buffer pinned to the specified chunk.
	 * The method returns null if the transaction has not
	 * pinned the chunk.
	 * @param chunk a reference to the disk chunk
	 * @return the suadb.buffer pinned to that chunk
	 */
	ChunkBuffer getBuffer(Chunk chunk) {
		return buffers.get(chunk);
	}

	/**
	 * Pins the chunk and keeps track of the suadb.buffer internally.
	 * @param chunk a reference to the disk chunk
	 */
	void pin(Chunk chunk) {
		ChunkBuffer buff = bufferMgr.pin(chunk);
		buffers.put(chunk, buff);
		pins.add(chunk);
	}

	/**
	 * Appends a new chunk to the specified suadb.file
	 * and pins it.
	 * @param filename the name of the suadb.file
	 * @param fmtr the formatter used to initialize the new page
	 * @param numberOfBlocks The number of blocks to create a chunk.
	 * @return a reference to the newly-created chunk
	 */
	Chunk pinNew(String filename, PageFormatter fmtr, int numberOfBlocks) {
		ChunkBuffer buff = bufferMgr.pinNew(filename, fmtr, numberOfBlocks);
		Chunk chunk = buff.chunk();
		buffers.put(chunk, buff);
		pins.add(chunk);
		return chunk;
	}

	/**
	 * Unpins the specified chunk.
	 * @param chunk a reference to the disk chunk
	 */
	void unpin(Chunk chunk) {
		ChunkBuffer buff = buffers.get(chunk);
		bufferMgr.unpin(buff);
		pins.remove(chunk);
		if (!pins.contains(chunk))
			buffers.remove(chunk);
	}

	/**
	 * Unpins any buffers still pinned by this transaction.
	 */
	void unpinAll() {
		for (Chunk chunk : pins) {
			ChunkBuffer buff = buffers.get(chunk);
			bufferMgr.unpin(buff);
		}
		buffers.clear();
		pins.clear();
	}
}