package suadb.buffer;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import suadb.server.SuaDB;
import suadb.file.*;
import static suadb.file.Page.*;

/**
 * A suadb.ChunkBuffer contain all buffers in chunk
 * and store information about status of chunk,
 * such as the disk chunk associated with the page,
 * the number of times the chunk has been pinned,
 * whether the contents of the page have been modified,
 * and if so, the id of the modifying transaction and
 * the LSN of the corresponding log suadb.record.
 *
 * Created by Rony on 2016-11-10.
 */
public class ChunkBuffer
{
	private List<Buffer> buffers = new Vector<Buffer>();
	private Chunk chunk = null;
	private int pins = 0;
	private int modifiedBy = -1;  // negative means not modified
	private int logSequenceNumber = -1; // negative means no corresponding log suadb.record

	/**
	 * Creates a new suadb.buffer, wrapping a new
	 * {@link suadb.file.Page page}.
	 * This constructor is called exclusively by the
	 * class {@link BasicBufferMgr}.
	 * It depends on  the
	 * {@link suadb.log.LogMgr LogMgr} object
	 * that it gets from the class
	 * {@link SuaDB}.
	 * That object is created during system initialization.
	 * Thus this constructor cannot be called until
	 * {@link SuaDB#initFileAndLogMgr(String)} or
	 * is called first.
	 */
	public ChunkBuffer() {}

	private int blockSeq(int offset)
	{
		return (int)(offset / BLOCK_SIZE);
	}

	private Buffer buffer(int offset)
	{
		return buffers.get(blockSeq(offset));
	}

	private int blockOffset(int offset)
	{
		return (int)(offset % BLOCK_SIZE);
	}

	/**
	 * Returns the integer value at the specified offset of the
	 * suadb.buffer's page.
	 * If an integer was not stored at that location,
	 * the behavior of the method is unpredictable.
	 * @param offset the byte offset of the page
	 * @return the integer value at that offset
	 */
	public int getInt(int offset) {
		return buffer(offset).getInt(blockOffset(offset));
	}

	/**
	 * Returns the string value at the specified offset of the
	 * suadb.buffer's page.
	 * If a string was not stored at that location,
	 * the behavior of the method is unpredictable.
	 * @param offset the byte offset of the page
	 * @return the string value at that offset
	 */
	public String getString(int offset) {
		return buffer(offset).getString(blockOffset(offset));
	}

	/**
	 * Writes an integer to the specified offset of the
	 * suadb.buffer's page.
	 * This method assumes that the transaction has already
	 * written an appropriate log suadb.record.
	 * The suadb.buffer saves the id of the transaction
	 * and the LSN of the log suadb.record.
	 * A negative lsn value indicates that a log suadb.record
	 * was not necessary.
	 * @param offset the byte offset within the page
	 * @param val the new integer value to be written
	 * @param txnum the id of the transaction performing the modification
	 * @param lsn the LSN of the corresponding log suadb.record
	 */
	public void setInt(int offset, int val, int txnum, int lsn) {
		modifiedBy = txnum;
		if (lsn >= 0)
			logSequenceNumber = lsn;
		buffer(offset).setInt(blockOffset(offset), val);
	}

	/**
	 * Writes a string to the specified offset of the
	 * suadb.buffer's page.
	 * This method assumes that the transaction has already
	 * written an appropriate log suadb.record.
	 * A negative lsn value indicates that a log suadb.record
	 * was not necessary.
	 * The suadb.buffer saves the id of the transaction
	 * and the LSN of the log suadb.record.
	 * @param offset the byte offset within the page
	 * @param val the new string value to be written
	 * @param txnum the id of the transaction performing the modification
	 * @param lsn the LSN of the corresponding log suadb.record
	 */
	public void setString(int offset, String val, int txnum, int lsn) {
		modifiedBy = txnum;
		if (lsn >= 0)
			logSequenceNumber = lsn;
		buffer(offset).setString(blockOffset(offset), val);
	}

	/**
	 * Returns a reference to the disk chunk
	 * that the suadb.buffer is pinned to.
	 * @return a reference to a disk chunk
	 */
	public Chunk chunk() {
		return chunk;
	}

	/**
	 * Writes the page to its disk chunk if the
	 * page is dirty.
	 * The method ensures that the corresponding log
	 * suadb.record has been written to disk prior to writing
	 * the page to disk.
	 */
	void flush() {
		if (modifiedBy >= 0) {
			SuaDB.logMgr().flush(logSequenceNumber);
			for (Buffer b : buffers)
			{
				b.flush();
			}

			modifiedBy = -1;
		}
	}

	/**
	 * Increases the suadb.buffer's pin count.
	 */
	void pin() {
		pins++;
	}

	/**
	 * Decreases the suadb.buffer's pin count.
	 */
	void unpin() {
		pins--;
	}

	/**
	 * Returns true if the suadb.buffer is currently pinned
	 * (that is, if it has a nonzero pin count).
	 * @return true if the suadb.buffer is pinned
	 */
	boolean isPinned() {
		return pins > 0;
	}

	/**
	 * Returns true if the suadb.buffer is dirty
	 * due to a modification by the specified transaction.
	 * @param txnum the id of the transaction
	 * @return true if the transaction modified the suadb.buffer
	 */
	boolean isModifiedBy(int txnum) {
		return txnum == modifiedBy;
	}

	/**
	 * Reads the contents of the specified chunk into
	 * the suadb.buffer's page.
	 * If the suadb.buffer was dirty, then the contents
	 * of the previous page are first written to disk.
	 * @param c a reference to the data chunk
	 * @param buffers list of assigned buffers
	 */
	void assignToChunk(Chunk c, List<Buffer> buffers)
	{
		flush();
		chunk = c;
		this.buffers = buffers;

		int blockNum = c.number();
		for (Buffer buff : buffers)
		{
			Block block = new Block(c.fileName(), blockNum);
			buff.assignToBlock(block);
			blockNum++;
		}

		pins = 0;
	}

	/**
	 * Initializes the suadb.buffer's page according to the specified formatter,
	 * and appends the page to the specified suadb.file.
	 * If the suadb.buffer was dirty, then the contents
	 * of the previous page are first written to disk.
	 * @param fileName the name of the suadb.file
	 * @param fmtr a page formatter, used to initialize the page
	 */
	void assignToNew(String fileName, PageFormatter fmtr, List<Buffer> buffers)
	{
		flush();

		this.buffers = buffers;
		for(Buffer b : buffers)
		{
			b.assignToNew(fileName, fmtr);
		}

		pins = 0;
	}

	public List<Buffer> retrieveBuffer()
	{
		List<Buffer> retrieveBuffers = buffers;
		buffers = new Vector<Buffer>();

		return retrieveBuffers;
	}

	public int size()
	{
		return buffers.size();
	}
}