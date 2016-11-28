package suadb.buffer;

import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Vector;

import suadb.file.Block;
import suadb.file.Chunk;
import suadb.file.FileMgr;
import suadb.server.SuaDB;
import suadb.server.SuaDBTestBase;

/**
 * Created by Rony on 2016-11-11.
 */
public class ChunkBufferMockTest extends SuaDBTestBase
{
	static ChunkBuffer chunkBuffer;
	
	static Field fBlock;
	static Field fBuffers;
	static Field fFileMgr;
	
	@BeforeClass
	public static void setup() throws Exception
	{
		FileMgr fileMgr = mock(FileMgr.class);
		
		chunkBuffer = new ChunkBuffer();
		
		// Set Access to private member field
		fBlock = Buffer.class.getDeclaredField("block");
		fBlock.setAccessible(true);
		
		fBuffers = ChunkBuffer.class.getDeclaredField("buffers");
		fBuffers.setAccessible(true);
		
		fFileMgr = SuaDB.class.getDeclaredField("fm");
		fFileMgr.setAccessible(true);
		fFileMgr.set(SuaDB.class, fileMgr);
	}
	
	@Test
	public void test_05_assignToChunk() throws Exception
	{
		int chunkSize = 5;
		int chunkNum = 0;
		Chunk chunk = new Chunk(arrayFileName, chunkNum);
		List<Buffer> buffers = makeBuffer(chunkSize);
		
		chunkBuffer.assignToChunk(chunk, buffers);
		List<Buffer> chunkBuffers = (List<Buffer>)fBuffers.get(chunkBuffer);
		
		// Check chunkBuffer has right number of block
		assertEquals(
				"Check chunkBuffer has right number of block",
				chunkBuffers.size(), chunkSize);
		
		for(int i = 0; i < chunkSize; i++)
		{
			Block block = (Block)fBlock.get(chunkBuffers.get(i));
			
			// Check block number
			assertEquals(
					"Check block number",
					block.number(), i);
			
			// Check block file path
			assertEquals(
					"Check block file path",
					block.fileName(), arrayFileName);
		}
	}
	
	@Test
	public void test_90_retrieveBuffer() throws Exception
	{
		int chunkSize = chunkBuffer.size();
		List<Buffer> buffers = (List<Buffer>)fBuffers.get(chunkBuffer);
		assertTrue(buffers.size() == chunkSize);

		List<Buffer> rBuffers = chunkBuffer.retrieveBuffer();
		buffers = (List<Buffer>)fBuffers.get(chunkBuffer);

		// Left Buffers are Zero after retrieve buffers
		assertTrue(
				"Left Buffers are Zero after retrieve buffers",
				buffers.size() == 0);
		
		// Retrieve Buffer size is same wiht chunksize
		assertTrue(
				"Retrieve Buffer size is same wiht chunksize",
				rBuffers.size() == chunkSize);
	}
	
	public List<Buffer> makeBuffer(int size)
	{
		List<Buffer> buffers = new Vector<Buffer>();
		for(int i = 0; i < size; i++)
		{
			buffers.add(new Buffer());
		}
		
		return buffers;
	}
}