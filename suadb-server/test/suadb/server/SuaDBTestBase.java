package suadb.server;

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
	
	@BeforeClass
	public static void SuaDBTestBaseSetup()
	{

	}
	
	static protected void eraseAllTestFile(File file)
	{
		assertTrue(deleteDirectory(file));
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
				assertTrue("Delete " + file.getPath(), file.delete());
			}
		}
		
		return path.delete();
	}
}
