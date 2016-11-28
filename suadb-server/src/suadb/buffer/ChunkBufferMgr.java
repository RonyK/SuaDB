package suadb.buffer;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

import suadb.file.Chunk;

/**
 * ChunkBufferMgr based on BasicBufferMgr in SimpleDB.
 * Created by Rony on 2016-11-10.
 */
public class ChunkBufferMgr
{
	private Buffer[] bufferPool;//Total buffers in SuaDB.
	private Queue<Buffer> freeBuffers = new LinkedBlockingQueue<Buffer>();//Available buffers.
	private List<ChunkBuffer> cBuffers;//List of currently allocated ChunkBuffers.
	private int numAvailable;//The number of available buffers. initially BUFFER_SIZE.

	ChunkBufferMgr(int numBuffs)
	{
		bufferPool = new Buffer[numBuffs];
		numAvailable = numBuffs;
		for (int i = 0; i < numBuffs; i++){
			bufferPool[i] = new Buffer();
			freeBuffers.add(bufferPool[i]);
		}

		cBuffers = Collections.synchronizedList(new LinkedList<ChunkBuffer>());
	}

	synchronized void flushAll(int txNum)
	{
		for (ChunkBuffer cBuff : cBuffers)
		{
			if (cBuff.isModifiedBy(txNum))
			{
				cBuff.flush();
			}
		}
	}

	synchronized ChunkBuffer pin(Chunk chunk){
		ChunkBuffer cBuff = findExistingBuffer(chunk);
		if(cBuff == null){ // if the chunk doesn't have buffers(chunkBuffer)
			List<Buffer> buffers = chooseUnpinnedBuffer(chunk.getNumOfBlocks());

			if (buffers == null){//fail to pin
				return null;
			}

			cBuff = new ChunkBuffer();
			cBuff.assignToChunk(chunk, buffers);//load chunk into buffers.
			cBuffers.add(cBuff);
		}

		if(!cBuff.isPinned())
			numAvailable -= cBuff.size();

		cBuff.pin();

		return cBuff;
	}

	/**
	 * Assign buffers to a new chunk in the specified array.
	 * pinNew is similar to pin,
	 * except that it immediately calls chooseUnpinnedBuffer(no findExistingBuffer).
	 * And it calls assignToNew on the buffers it finds.
	 * @param fileName the name of the suadb.file
	 * @param fmtr the formatter used to initialize the page
	 * @param requiredNumOfBlocks The number of blocks to create a chunk.
	 * @return
	 */
	synchronized ChunkBuffer pinNew(String fileName, PageFormatter fmtr, int requiredNumOfBlocks)
	{
		List<Buffer> buffers = chooseUnpinnedBuffer(requiredNumOfBlocks);
		if (buffers == null){
			return null;
		}
		ChunkBuffer cBuff = new ChunkBuffer();
		cBuff.setChunk(fileName,requiredNumOfBlocks);//Chunk initialization in ChunkBuffer.
		cBuff.assignToNew(fileName, fmtr, buffers);
		numAvailable -= cBuff.size();
		cBuff.pin();
		return cBuff;

	}

	synchronized void unpin(ChunkBuffer cBuff)
	{
		cBuff.unpin();
		if(!cBuff.isPinned())
		{
			numAvailable += cBuff.size();
		}
	}

	int available()
	{
		return numAvailable;
	}

	private ChunkBuffer findExistingBuffer(Chunk chunk)
	{
		for (ChunkBuffer cBuff : cBuffers)
		{
			Chunk c = cBuff.chunk();
			if( c != null && c.equals(chunk))
			{
				return cBuff;
			}
		}

		return null;
	}

	private List<Buffer> chooseUnpinnedBuffer(int requiredNumOfBlocks){
		boolean isAvailable=true;//Chunk can get enough buffers == TRUE
		if(numAvailable < requiredNumOfBlocks)
			return null;


		List<Buffer> result = new Vector<Buffer>();

		// fixed erroneous code -  java.util.ConcurrentModificationException -IHSUh
		if(freeBuffers.size() < requiredNumOfBlocks){
			for(Iterator<ChunkBuffer> iterator = cBuffers.iterator(); iterator.hasNext();){
				ChunkBuffer cBuff = iterator.next();
				if (!cBuff.isPinned()){
					iterator.remove();
					retriveBuffer(cBuff);
				}

				if (freeBuffers.size() >= requiredNumOfBlocks){
					isAvailable = true;
					break;
				}
			}
			isAvailable = false;
		}

		if(isAvailable) {
			for (int i = 0; i < requiredNumOfBlocks; i++) {
				result.add(freeBuffers.poll());
			}
			return result;
		}
		else
			return null;

	}

	private void retriveBuffer(ChunkBuffer cBuff)
	{
		freeBuffers.addAll(cBuff.retrieveBuffer());
	}
}
