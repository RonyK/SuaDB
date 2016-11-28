package suadb.file;

/**
 * It does not hold the contents of the chunk;
 * instead, that is the job of a {@link Page} object.
 *
 * Created by Rony on 2016-11-07.
 */
public class Block
{
	private String fileName;
	private int blkNum;

	/**
	 * Constructs a chunk
	 *
	 * @param fileName
	 * @param blkNum
	 */
	public Block(String fileName, int blkNum)
	{
		this.fileName = fileName;
		this.blkNum = blkNum;
	}

	/**
	 * Returns the name of the suadb.file where the chunk lives.
	 * @return the filename
	 */
	public String fileName()
	{
		return fileName;
	}

	/**
	 * Returns the location of the chunk within the suadb.file.
	 * @return the chunk number
	 */
	public int number()
	{
		return blkNum;
	}

	public boolean equals(Object obj)
	{
		Block block = (Block)obj;
		return fileName.equals(block.fileName) && blkNum == block.blkNum;
	}

	public String toString()
	{
		return "[suadb.file " + fileName + ", chunk " + blkNum + "]";
	}

	public int hashCode()
	{
		return toString().hashCode();
	}
}
