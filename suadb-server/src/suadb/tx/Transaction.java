package suadb.tx;

import suadb.file.Block;
import suadb.file.Chunk;
import suadb.server.SuaDB;
import suadb.buffer.*;
import suadb.tx.recovery.RecoveryMgr;
import suadb.tx.concurrency.ConcurrencyMgr;

/**
 * Provides transaction management for clients,
 * ensuring that all transactions are serializable, recoverable,
 * and in general satisfy the ACID properties.
 * @author Edward Sciore
 */
public class Transaction {
	private static int nextTxNum = 0;
	private static final int END_OF_FILE = -1;
	private RecoveryMgr	 recoveryMgr;
	private ConcurrencyMgr concurMgr;
	private int txnum;
	private ChunkList myChunks = new ChunkList();//Chunk list that this transaction has - CDS

	/**
	 * Creates a new transaction and its associated
	 * recovery and concurrency managers.
	 * This constructor depends on the suadb.file, log, and suadb.buffer
	 * managers that it gets from the class
	 * {@link SuaDB}.
	 * Those objects are created during system initialization.
	 * Thus this constructor cannot be called until either
	 * {@link SuaDB#init(String)} or
	 * {@link SuaDB#initFileLogAndBufferMgr(String)} or
	 * is called first.
	 */
	public Transaction() {
		txnum		 = nextTxNumber();
		recoveryMgr = new RecoveryMgr(txnum);
		concurMgr	= new ConcurrencyMgr();
	}

	/**
	 * Commits the current transaction.
	 * Flushes all modified buffers (and their log records),
	 * writes and flushes a commit suadb.record to the log,
	 * releases all locks, and unpins any pinned buffers.
	 */
	public void commit() {
		recoveryMgr.commit();
		concurMgr.release();
		myChunks.unpinAll();
		System.out.println("transaction " + txnum + " committed");
	}

	/**
	 * Rolls back the current transaction.
	 * Undoes any modified values,
	 * flushes those buffers,
	 * writes and flushes a rollback suadb.record to the log,
	 * releases all locks, and unpins any pinned buffers.
	 */
	public void rollback() {
		recoveryMgr.rollback();
		concurMgr.release();
		myChunks.unpinAll();
		System.out.println("transaction " + txnum + " rolled back");
	}

	/**
	 * Flushes all modified buffers.
	 * Then goes through the log, rolling back all
	 * uncommitted transactions.  Finally,
	 * writes a quiescent checkpoint suadb.record to the log.
	 * This method is called only during system startup,
	 * before user transactions begin.
	 */
	public void recover() {
		SuaDB.bufferMgr().flushAll(txnum);
		recoveryMgr.recover();
	}

	/**
	 * Pins the specified chunk.
	 * The transaction manages the suadb.buffer for the client.
	 * @param chunk a reference to the disk chunk
	 */
	public void pin(Chunk chunk) {
		myChunks.pin(chunk);
	}

	/**
	 * Unpins the specified chunk.
	 * The transaction looks up the suadb.buffer pinned to this chunk,
	 * and unpins it.
	 * @param chunk a reference to the disk chunk
	 */
	public void unpin(Chunk chunk) {
		myChunks.unpin(chunk);
	}

	/**
	 * Returns the integer value stored at the
	 * specified offset of the specified chunk.
	 * The method first obtains an SLock on the chunk,
	 * then it calls the suadb.buffer to retrieve the value.
	 * @param chunk a reference to a disk chunk
	 * @param offset the byte offset within the chunk
	 * @return the integer stored at that offset
	 */
	public int getInt(Chunk chunk, int offset) {
		concurMgr.sLock(chunk);
		ChunkBuffer buff = myChunks.getBuffer(chunk);
		return buff.getInt(offset);
	}

	/**
	 * Returns the string value stored at the
	 * specified offset of the specified chunk.
	 * The method first obtains an SLock on the chunk,
	 * then it calls the suadb.buffer to retrieve the value.
	 * @param chunk a reference to a disk chunk
	 * @param offset the byte offset within the chunk
	 * @return the string stored at that offset
	 */
	public String getString(Chunk chunk, int offset) {
		concurMgr.sLock(chunk);
		ChunkBuffer buff = myChunks.getBuffer(chunk);
		return buff.getString(offset);
	}

	/**
	 * Returns the double value stored at the
	 * specified offset of the specified chunk.
	 * The method first obtains an SLock on the chunk,
	 * then it calls the suadb.buffer to retrieve the value.
	 * @param chunk a reference to a disk chunk
	 * @param offset the byte offset within the chunk
	 * @return the double stored at that offset
	 */
	public double getDouble(Chunk chunk, int offset) {
		concurMgr.sLock(chunk);
		ChunkBuffer buff = myChunks.getBuffer(chunk);
		return buff.getDouble(offset);
	}

	/**
	 * Stores an integer at the specified offset
	 * of the specified chunk.
	 * The method first obtains an XLock on the chunk.
	 * It then reads the current value at that offset,
	 * puts it into an update log suadb.record, and
	 * writes that suadb.record to the log.
	 * Finally, it calls the suadb.buffer to store the value,
	 * passing in the LSN of the log suadb.record and the transaction's id.
	 * @param chunk a reference to the disk chunk
	 * @param offset a byte offset within that chunk
	 * @param val the value to be stored
	 */
	public void setInt(Chunk chunk, int offset, int val) {
		concurMgr.xLock(chunk);
		ChunkBuffer buff = myChunks.getBuffer(chunk);
		int lsn = recoveryMgr.setInt(buff, offset, val);
		buff.setInt(offset, val, txnum, lsn);
	}

	/**
	 * Stores a string at the specified offset
	 * of the specified chunk.
	 * The method first obtains an XLock on the chunk.
	 * It then reads the current value at that offset,
	 * puts it into an update log suadb.record, and
	 * writes that suadb.record to the log.
	 * Finally, it calls the suadb.buffer to store the value,
	 * passing in the LSN of the log suadb.record and the transaction's id.
	 * @param chunk a reference to the disk chunk
	 * @param offset a byte offset within that chunk
	 * @param val the value to be stored
	 */
	public void setString(Chunk chunk, int offset, String val) {
		concurMgr.xLock(chunk);
		ChunkBuffer buff = myChunks.getBuffer(chunk);
		int lsn = recoveryMgr.setString(buff, offset, val);
		buff.setString(offset, val, txnum, lsn);
	}

	/**
	 * Stores an double at the specified offset
	 * of the specified chunk.
	 * The method first obtains an XLock on the chunk.
	 * It then reads the current value at that offset,
	 * puts it into an update log suadb.record, and
	 * writes that suadb.record to the log.
	 * Finally, it calls the suadb.buffer to store the value,
	 * passing in the LSN of the log suadb.record and the transaction's id.
	 * @param chunk a reference to the disk chunk
	 * @param offset a byte offset within that chunk
	 * @param val the value to be stored
	 */
	public void setDouble(Chunk chunk, int offset, double val) {
		concurMgr.xLock(chunk);
		ChunkBuffer buff = myChunks.getBuffer(chunk);
		int lsn = recoveryMgr.setDouble(buff, offset, val);
		buff.setDouble(offset, val, txnum, lsn);
	}

	/**
	 * Returns the number of blocks in the specified suadb.file.
	 * This method first obtains an SLock on the
	 * "end of the suadb.file", before asking the suadb.file manager
	 * to return the suadb.file size.
	 * @param filename the name of the suadb.file
	 * @return the number of blocks in the suadb.file
	 */
	public int size(String filename,int numOfBlocks) { // For chunk
		Chunk dummyChunk = new Chunk(filename, END_OF_FILE,numOfBlocks);
		concurMgr.sLock(dummyChunk);
		return SuaDB.fileMgr().size(filename);
	}
	public int size(String filename) { //For existing block I/O
		Chunk dummyChunk = new Chunk(filename, END_OF_FILE,1);
		concurMgr.sLock(dummyChunk);
		return SuaDB.fileMgr().size(filename);
	}

	private static synchronized int nextTxNumber() {
		nextTxNum++;
		System.out.println("new transaction: " + nextTxNum);
		return nextTxNum;
	}

	/**
	 * Appends a new block(one-block-sized chunk) to the end of the specified file
	 * and returns a reference to it.
	 * This method first obtains an XLock on the
	 * "end of the file", before performing the append.
	 * @param filename the name of the file
	 * @param fmtr the formatter used to initialize the new page
	 * @return a reference to the newly-created disk block
	 */
	public Chunk append(String filename, PageFormatter fmtr) {
		Chunk dummyChunk = new Chunk(filename, END_OF_FILE, 1);
		concurMgr.xLock(dummyChunk);
		Chunk chunk = myChunks.pinNew(filename, fmtr, 1);//Appends a new block(one-block-sized chunk)
		unpin(chunk);
		return chunk;
	}
	/**
	 * create new chunk file.
	 *
	 * @param fileName
	 * @param fmtr
	 * @param numberOfBlocks
	 * @return
	 */
	public Chunk createNewChunk(String fileName, PageFormatter fmtr, int numberOfBlocks){
		Chunk dummyChunk = new Chunk(fileName, END_OF_FILE);
		concurMgr.xLock(dummyChunk);

		Chunk chunk = myChunks.pinNew(fileName, fmtr, numberOfBlocks);
		unpin(chunk);

		return chunk;
	}
}
