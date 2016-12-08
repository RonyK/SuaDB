package suadb.buffer;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.mockito.BDDMockito;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import suadb.file.Chunk;
import suadb.file.FileMgr;
import suadb.log.LogMgr;
import suadb.metadata.MetadataMgr;
import suadb.server.SuaDB;
import suadb.test.SuaDBTestBase;
import suadb.tx.Transaction;

import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static suadb.server.SuaDB.BUFFER_SIZE;

/**
 * Created by Rony on 2016-11-14.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(SuaDB.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ChunkBufferMgrTest extends SuaDBTestBase
{
	@BeforeClass
	public static void beforeClass()
	{
		SuaDB.init(dbName);
	}

	@Test
	public void test_00_init()
	{
		assertTrue(SuaDB.bufferMgr().available() == BUFFER_SIZE);
	}
	@Test
	public void test_01_pin()
	{
		assertTrue(SuaDB.bufferMgr().available() == BUFFER_SIZE);
		
		Chunk chunk0 = new Chunk(arrayFileName, 0, 3);
		Chunk chunk1 = new Chunk(arrayFileName, 3, 2);
		Chunk chunk2 = new Chunk(arrayFileName, 5, 2);


		SuaDB.bufferMgr().pin(chunk0);

		assertTrue(
				"Check available chunk size",
				SuaDB.bufferMgr().available() == BUFFER_SIZE - chunk0.getNumOfBlocks());

		SuaDB.bufferMgr().pin(chunk1);

		assertTrue(
				"Check available chunk size",
				SuaDB.bufferMgr().available() == BUFFER_SIZE - chunk0.getNumOfBlocks() - chunk1.getNumOfBlocks());
	}
	
	public void test_05_unpin()
	{
		//TODO
	}
	
	@AfterClass
	public static void tearDown()
	{
		try
		{
			SuaDB.fileMgr().flushAllFiles();
		} catch (Exception e)
		{
			e.printStackTrace();
		}
	}
}