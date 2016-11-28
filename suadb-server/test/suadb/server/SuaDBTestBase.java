package suadb.server;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by Rony on 2016-11-11.
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
		eraseAllTestFile(dbDirectory);
	}
	
	static protected void eraseAllTestFile(File file)
	{
		assertTrue(file.exists() == deleteDirectory(file));
		assertFalse(file.exists());
	}
	
	static protected boolean deleteDirectory(File path)
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
				boolean b1 = file.exists();
				boolean b2 = file.delete();
				
				System.out.println(b1);
				System.out.println(b2);
				
				assertTrue(
						"Delete " + file.getPath(),
						file.exists() == file.delete());
				
			}
		}
		
		return path.delete();
	}
}