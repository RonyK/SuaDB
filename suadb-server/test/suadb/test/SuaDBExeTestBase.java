package suadb.test;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import suadb.query.Plan;
import suadb.query.Scan;
import suadb.server.SuaDB;
import suadb.tx.Transaction;

/**
 * Created by Rony on 2016-12-06.
 */
public abstract class SuaDBExeTestBase extends SuaDBTestBase
{
	@BeforeClass
	public final static void SuaDBExeTestBase_Init()
	{
		System.out.println("INIT");
		SuaDB.init(dbName);
	}
	
	@AfterClass
	public final static void SuaDBExeTestBase_TearDown()
	{
		System.out.print("SHUTDOWN");
		SuaDB.shutDown();
	}
	
	protected Scan list(Transaction tx)
	{
		String query = "LIST()";
		
		Plan plan = SuaDB.planner().createQueryPlan(query, tx);
		Scan s = plan.open();
		
		return s;
	}
}
