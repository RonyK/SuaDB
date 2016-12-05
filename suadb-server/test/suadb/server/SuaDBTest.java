package suadb.server;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import suadb.parse.Parser;
import suadb.query.Plan;
import suadb.query.Scan;
import suadb.test.SuaDBTestBase;
import suadb.tx.Transaction;

import static org.junit.Assert.*;

/**
 * Created by Rony on 2016-11-28.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SuaDBTest extends SuaDBTestBase
{
	@BeforeClass
	public static void init()
	{
		SuaDB.init(dbName);
	}
	
	@Test
	public void test_01_list()
	{
		Transaction tx = new Transaction();
		String query =
				"LIST(arrays)";
		
		Plan plan = SuaDB.planner().createQueryPlan(query, tx);
		Scan s = plan.open();
		
		assertFalse("Check empty database", s.next());
		tx.commit();
	}
	
	@Test
	public void test_10_create_1D_array()
	{
		Transaction tx = new Transaction();
		String query =
				"CREATE ARRAY TD1" +
				"<" +
				"   a : int," +
				"   b : int" +
				">" +
				"[x = 0:100,10, y = 0:30,6]";
		
		SuaDB.planner().executeUpdate(query, tx);
	}
	
	@Test
	public void test_11_create_1D_array()
	{
		Transaction tx = new Transaction();
		// TODO :: Underbar delimiter unacceptable
		// Create array fail
		String query =
				"CREATE ARRAY T_D1" +
						"<" +
						"   a : int," +
						"   b : int" +
						">" +
						"[x = 0:100,10, y = 0:30,6]";
		
		SuaDB.planner().executeUpdate(query, tx);
	}
	
	@AfterClass
	public static void tearDown()
	{
		SuaDB.shutDown();
	}
}