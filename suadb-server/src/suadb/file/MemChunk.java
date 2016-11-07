package suadb.file;

import java.nio.ByteBuffer;

/**
 * The content of a Chunk in memory.
 *
 * Created by Rony on 2016-11-07.
 */
public class MemChunk extends Page
{
	// TODO :: How to treat chunk size? - RonyK
	private int chunkSize;

	private ByteBuffer contents = null;

	public MemChunk(int chunkSize)
	{
		this.chunkSize = chunkSize;
		this.contents = ByteBuffer.allocateDirect(chunkSize);
	}

	public synchronized void read(Chunk chunk)
	{
		fileMgr.read(chunk, contents);
	}

	public synchronized void write(Chunk chunk)
	{
		fileMgr.write(chunk, contents);
	}

	public synchronized Chunk appendToChunk(String fileName)
	{
		return fileMgr.appendToChunk(fileName, contents);
	}

	public synchronized Block append(String fileName)
	{
		throw new UnsupportedOperationException("Chunk class don't support append to block operator");
	}
}
