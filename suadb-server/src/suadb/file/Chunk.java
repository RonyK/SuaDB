package suadb.file;

/**
 * A reference to a disk chunk.
 * A Chunk object consists of a fileName and a chunk number.
 * It does not hold the contents of the chunk;
 *
 * Created by Rony on 2016-11-10.
 */
public class Chunk
{


	/**
	 * File name of the chunk.
	 * arrayName_attribute Name_chunkNumInTheAttribute
	 */
	private String fileName;
	/**
	 * The logical order in an array
	 */
	private int chunkNum;
	/**
	 * The number of cells in a chunk.
	 * numOfCells = Product of each dimension's chunkSize
	 */
	private int numOfCells;

	/**
	*   The number of required Blocks to make a chunk.
	*   numOfBlocks = Math.ceil(numOfCells/Math.floor(BLOCK_SIZE/(4Byte(empty/inuse flag) + (4Byte(INT) or 1BYTE*VARCHAR LENGTH))))
	 *   It must be less than BLOCK_SIZE.
	*/
	private int numOfBlocks;

	public Chunk(){}

	public Chunk(String fileName, int chunkNum) {
		this.fileName = fileName;
		this.chunkNum = chunkNum;
	}
	public Chunk(String fileName, int chunkNum,int numOfBlocks) {
		this(fileName, chunkNum);
		this.numOfBlocks = numOfBlocks;
	}
	public Chunk(String fileName, int chunkNum,int numOfBlocks, int numOfCells){
		this(fileName, chunkNum,numOfBlocks);
		this.numOfCells = numOfCells;
	}
	public Chunk(Block block){//For wrapping a block.
		this(block.fileName(),block.number(),1);//one-block-sized chunk.
	}

	/**
	 * Returns the name of the suadb.file where the chunk lives.
	 * @return the fileName
	 */
	public String fileName() {
		return fileName;
	}

	/**
	 * Returns the location of the chunk within the suadb.file.
	 * @return the chunk number
	 */
	public int number() {
		return chunkNum;
	}

	public int getNumOfBlocks(){
		return numOfBlocks;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public int getChunkNum() {
		return chunkNum;
	}

	public void setChunkNum(int chunkNum) {
		this.chunkNum = chunkNum;
	}

	public void setNumOfCells(int numOfCells) {
		this.numOfCells = numOfCells;
	}

	public void setNumOfBlocks(int numOfBlocks) {
		this.numOfBlocks = numOfBlocks;
	}

	public boolean equals(Object obj) {
		Chunk chunk = (Chunk) obj;
		return fileName.equals(chunk.fileName) && chunkNum == chunk.chunkNum;
	}

	public String toString() {
		return "[suadb.file " + fileName + ", chunk " + chunkNum + "]";
	}

	public int hashCode() {
		return toString().hashCode();
	}


}
