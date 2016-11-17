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
	 * arrayName_attribute Name_chunkNumInTheAttribute;
	 */
	private String fileName;
	/**
	 * The logical order in an array.
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
	*/
	private int numOfBlocks;

	/**
	 * Constructs a chunk reference
	 * for the specified fileName and chunk number.
	 * @param fileName the name of the suadb.file
	 * @param chunkNum the chunk number
	 */
	public Chunk(String fileName, int chunkNum) {
		this.fileName = fileName;
		this.chunkNum = chunkNum;
	}

	public Chunk(String fileName, int chunkNum, int numOfCells,int numOfBlocks){
		this(fileName, chunkNum);
		this.numOfCells = numOfCells;
		this.numOfCells = numOfCells;
		this.numOfBlocks = numOfBlocks;
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
