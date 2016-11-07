package suadb.file;

/**
 * It does not hold the contents of the chunk;
 * instead, that is the job of a {@link MemChunk} object.
 *
 * Created by Rony on 2016-11-07.
 */
public class Chunk
{
	// TODO :: How to treat chunk size - RonyK
	// If we use compression chunk, size of each chunk would be different.
	private int chunkSize;

	private String fileName;
	private int chunkNum;

	/**
	 * Constructs a chunk
	 *
	 * @param fileName
	 * @param chunkNum
	 */
	public Chunk(String fileName, int chunkNum)
	{
		this.fileName = fileName;
		this.chunkNum = chunkNum;
	}

	public String getFileName()
	{
		return fileName;
	}

	public int getChunkNum()
	{
		return chunkNum;
	}

	public int getChunkSize()
	{
		return chunkSize;
	}

	public boolean equals(Object obj)
	{
		Chunk chunk = (Chunk)obj;
		return fileName.equals(chunk.fileName) && chunkNum == chunk.chunkNum;
	}

	public String toString()
	{
		return "[suadb.file " + fileName + ", chunk " + chunkNum + "]";
	}

	public int hashCode()
	{
		return toString().hashCode();
	}
}
