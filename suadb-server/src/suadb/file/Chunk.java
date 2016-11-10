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
	private String fileName;
	private int chunkNum;
	private int chunkSize = 1;

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

	public Chunk(String fileName, int chunkNum, int chunkSize)
	{
		this(fileName, chunkNum);
		this.chunkSize = chunkSize;
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

	public int chunkSize()
	{
		return chunkSize;
	}

	public boolean equals(Object obj) {
		Chunk blk = (Chunk) obj;
		return fileName.equals(blk.fileName) && chunkNum == blk.chunkNum;
	}

	public String toString() {
		return "[suadb.file " + fileName + ", chunk " + chunkNum + "]";
	}

	public int hashCode() {
		return toString().hashCode();
	}
}
