package suadb.file;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import suadb.server.SuaDBTestBase;

import static suadb.file.Page.*;
import static org.junit.Assert.*;

/**
 * Created by Rony on 2016-11-11.
 */

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class FileMgrTest extends SuaDBTestBase
{
	static FileMgr fileMgr;
	protected static String homeDir;
	static File dbDirectory;
	
	@BeforeClass
	public static void setup()
	{
		homeDir = System.getProperty("user.home");
		dbDirectory = new File(homeDir, dbName);
		fileMgr = new FileMgr(dbName);
	}

	@Test
	public void test_01_makeFile() throws Exception
	{
		// Check Directory exist.
		assertTrue(
				"Check Directory exist",
				dbDirectory.exists());

		// Check this directory is the new one.
		assertTrue(
				"Check this directory is the new one",
				fileMgr.isNew());
	}

	@Test
	public void test_05_appendPage() throws Exception
	{
		ByteBuffer bb = ByteBuffer.allocateDirect(BLOCK_SIZE);
		
		// Check this is a first block of db file
		assertEquals(
				"Check this is a first block of db file",
				fileMgr.append(arrayFileName, bb),
				new Block(arrayFileName, 0));
	}

	@AfterClass
	public static void tearDown()
	{
		try
		{
			fileMgr.flushAllFiles();
		} catch (IOException ioe)
		{
			System.out.println("Flush all files of file manager fail");
			System.out.println(ioe.getMessage());
			ioe.printStackTrace();
		}
		
		eraseAllTestFile(dbDirectory);
	}
}