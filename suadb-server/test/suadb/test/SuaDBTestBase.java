package suadb.test;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.File;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by Rony on 2016-11-16.
 */
public class SuaDBTestBase
{
	protected static String dbName = "testDB";
	protected static String arrayFileName = "testArray.arr";
	
	protected static String homeDir;
	protected static File dbDirectory;
	
	@BeforeClass
	final public static void SuaDBTestBaseSetup()
	{
		homeDir = System.getProperty("user.home");
		dbDirectory = new File(homeDir, dbName);
	}
	
	@AfterClass
	final public static void SuaDBTestBaseTearDown()
	{
		if(dbDirectory.exists())
		{
			eraseAllTestFile(dbDirectory);
		}
	}
	
	protected static void eraseFile(String fileName)
	{
		File file = new File(fileName);
		assertTrue(file.exists() == file.delete());
	}
	
	protected static void eraseAllTestFile(File file)
	{
		assertTrue(file.exists() == deleteDirectory(file));
		assertFalse(file.exists());
	}
	
	protected static boolean deleteDirectory(File path)
	{
		if(!path.exists())
		{
			return false;
		}
		
		File[] files = path.listFiles();
		for(File file : files)
		{
			if(file.isDirectory())
			{
				deleteDirectory(file);
			} else
			{
				assertTrue(
						"Delete " + file.getPath(),
						file.exists() == file.delete());
			}
		}
		
		return path.delete();
	}
}
