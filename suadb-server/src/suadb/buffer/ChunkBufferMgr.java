package suadb.buffer;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Vector;
import java.util.concurrent.LinkedBlockingQueue;

import suadb.file.Chunk;

/**
 * Created by Rony on 2016-11-10.
 */
public class ChunkBufferMgr
{
	private Buffer[] bufferPool;
	private Queue<Buffer> freeBuffers = new LinkedBlockingQueue<Buffer>();
	private List<ChunkBuffer> cBuffers;
	private int numAvailable;

	ChunkBufferMgr(int numBuffs)
	{
		bufferPool = new Buffer[numBuffs];
		numAvailable = numBuffs;
		for (int i = 0; i < numBuffs; i++)
		{
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

	synchronized ChunkBuffer pin(Chunk chunk)
	{
		ChunkBuffer cBuff = findExistingBuffer(chunk);
		if(cBuff == null)
		{
			List<Buffer> buffers = chooseUnpinnedBuffer(chunk.chunkSize());

			if (buffers == null)
			{
				return null;
			}

			cBuff.assignToChunk(chunk, buffers);
		}else
		{
			cBuffers.remove(cBuff);
		}

		if(!cBuff.isPinned())
		{
			numAvailable -= cBuff.size();
		}

		cBuffers.add(cBuff);
		cBuff.pin();

		return cBuff;
	}

	synchronized ChunkBuffer pinNew(String fileName, PageFormatter fmtr, int chunkSize)
	{
		List<Buffer> buffers = chooseUnpinnedBuffer(chunkSize);
		if (buffers == null)
		{
			return null;
		}

		ChunkBuffer cBuff = new ChunkBuffer();
		cBuff.assignToNew(fileName, fmtr, buffers);
		cBuff.pin();

		numAvailable -= cBuff.size();

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

	private List<Buffer> chooseUnpinnedBuffer(int chunkSize)
	{
		if(numAvailable < chunkSize)
		{
			return null;
		}

		List<Buffer> result = new Vector<Buffer>();

		if(freeBuffers.size() < chunkSize)
		{
			for (ChunkBuffer cBuff : cBuffers)
			{
				if (!cBuff.isPinned())
				{
					cBuffers.remove(cBuff);
					retriveBuffer(cBuff);
				}

				if (freeBuffers.size() >= chunkSize)
				{
					break;
				}
			}
		}

		for (int i = 0; i < chunkSize; i++)
		{
			result.add(freeBuffers.poll());
		}

		return result;
	}

	private void retriveBuffer(ChunkBuffer cBuff)
	{
		freeBuffers.addAll(cBuff.retrieveBuffer());
	}
}
