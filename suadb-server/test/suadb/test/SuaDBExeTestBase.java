package suadb.test;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import suadb.server.SuaDB;

/**
 * Created by Rony on 2016-12-06.
 */
public class SuaDBExeTestBase extends SuaDBTestBase
{
	@BeforeClass
	public final static void SuaDBExeTestBase_Init()
	{
		System.out.print("INIT");
		SuaDB.init(dbName);
	}
	
	@AfterClass
	public final static void SuaDBExeTestBase_TearDown()
	{
		System.out.print("SHUTDOWN");
		SuaDB.shutDown();
	}
}
