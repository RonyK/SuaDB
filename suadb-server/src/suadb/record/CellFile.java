package suadb.record;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import suadb.file.Chunk;
import suadb.server.SuaDB;
import suadb.tx.Transaction;


/**
 * Manages a suadb.file of records.
 * There are methods for iterating through the records
 * and accessing their contents.
 * @author Edward Sciore
 */
public class CellFile {
	private ArrayInfo ai;
	private Transaction tx;
	private String fileName;
	private String attributeName;
	private CellPage cp;
	private int currentChunkNum = -1;
	private int numBlocks;
	private int numChunks;         // this variable is needed in order to find out if the current chunk is the last one

	private int totalChunkNum = 1; //The total number of chunks in an array.
	private int numCellsInChunk = 1; //The number of cells in a chunk.
	private int numDimensions;
	private List<Integer> currentChunk;

	/**
	 * Constructs an object to manage a suadb.file of records.
	 * If the suadb.file does not exist, it is created.
	 * @param ai the table suadb.metadata
	 * @param tx the transaction
	 * @param chunknum the chunk number of an array
	 * @param attributeName the name of attribute (SuaDB adopts separate storage for each attribute)
	 */

	public CellFile(ArrayInfo ai, Transaction tx, int chunknum, String attributeName, int numBlocks, int numChunks) {
		this.ai = ai;
		this.tx = tx;
		this.attributeName = attributeName;
		this.numBlocks = numBlocks;
		this.numChunks = numChunks;
		
		List<String> dimensions = new ArrayList<String>(ai.schema().dimensions());
		numDimensions = dimensions.size();
		this.currentChunk = new ArrayList<>(numDimensions);
		
		for(int i = 0; i< numDimensions; i++) {
			totalChunkNum *= ai.schema().getNumOfChunk(dimensions.get(i));
			numCellsInChunk *= ai.schema().chunkSize(dimensions.get(i));
		}

		// move to the specified chunk, updates itself
		moveTo(chunknum);
	}

	private String assignName(ArrayInfo ai, String attributename, int currentchunknum){
		return ai.arrayName() + "_" + attributename + "_" + currentchunknum;
	}

	/**
	 * Closes the suadb.record suadb.file.
	 */
	public void close() throws IOException{
		cp.close();
		SuaDB.fileMgr().flushFile(fileName);
	}

	/**
	 * Positions the current suadb.record so that a call to method next
	 * will wind up at the first suadb.record.
	 */
	public void beforeFirst() {
		moveTo(0);
	}

	/**
	 * Moves to the next suadb.record. Returns false if there
	 * is no next suadb.record.
	 * @return false if there is no next suadb.record.
	 */
	public boolean next() {
		while (true)
		{
			if (cp.next()) {
				return true;
			}
			
			if (atLaskChunk())
			{
				return false;
			}
				
			moveTo(currentChunkNum + 1);
		}
	}

	/**
	 * Returns the value of the specified field
	 * in the current suadb.record.
	 * @return the integer value at that field
	 */
	public int getInt() {
		return cp.getInt();
	}

	/**
	 * Returns the value of the specified field
	 * in the current suadb.record.
	 * @return the string value at that field
	 */
	public String getString() {
		return cp.getString();
	}

	/**
	 * Sets the value of the specified field
	 * in the current suadb.record.
	 * @param val the new value for the field
	 */
	public void setInt(int val) {
		cp.setInt(val);
	}

	/**
	 * Sets the value of the specified field
	 * in the current suadb.record.
	 * @param val the new value for the field
	 */
	public void setString(String val) {
		cp.setString(val);
	}

	/**
	 * Deletes the current suadb.record.
	 * The client must call next() to move to
	 * the next suadb.record.
	 * Calls to methods on a deleted suadb.record
	 * have unspecified behavior.
	 */
	public void delete() {
		cp.delete();
	}

	/**
	 * Positions the current suadb.record as indicated by the
	 * specified RID.
	 * @param offset a offset of the cell in this chunk
	 */
	public void moveToId(int offset) {
		cp.moveToId(offset);
	}

	public void moveTo(int chunkNum) {
		if( currentChunkNum == chunkNum)
		{
			cp.setCurrentId(-1);//Initialize slot
			return;
		}

		if (cp != null)
		{
			cp.close();
			
			try {
				SuaDB.fileMgr().flushFile(fileName);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		currentChunkNum = chunkNum;
		
		// Update fileName  - ILHYUN
		this.fileName = assignName(this.ai, this.attributeName, currentChunkNum);
		
		if (tx.size(fileName, numBlocks) == 0)
		{
			createChunk(chunkNum);
		}
		
		Chunk chunk = new Chunk(fileName, currentChunkNum, numBlocks);
		cp = new CellPage(chunk, ai, tx, ai.recordLength(attributeName));
	}

	private boolean atLaskChunk() {
		return currentChunkNum >= (numChunks - 1);
	}

	private void createChunk(int chunkNum) {
		CellFormatter fmtr = new CellFormatter(ai, attributeName);
		String filename = assignName(this.ai, this.attributeName, chunkNum);
		tx.createNewChunk(filename, fmtr, numBlocks);
	}
	
	/**
	 * Get current chunk number.
	 * @return
	 */
	public int currentChunkNum() {
		return cp.chunk().getChunkNum();
	}
	
	public int currentId()
	{
		return cp.currentId();
	}

	/**
	 * If current position is null, return true.
	 */
	public boolean isNull()
	{
		return cp.isNull();
	}
}

