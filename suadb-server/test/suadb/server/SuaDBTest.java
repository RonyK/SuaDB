package suadb.server;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

import suadb.parse.BadSyntaxException;
import suadb.parse.InputArrayData;
import suadb.parse.Parser;
import suadb.planner.BasicUpdatePlanner;
import suadb.planner.Planner;
import suadb.query.Plan;
import suadb.query.Scan;
import suadb.record.ArrayInfo;
import suadb.record.Schema;
import suadb.test.DummyData;
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
				"[x = 0:100,10]";
		
		SuaDB.planner().executeUpdate(query, tx);
		
		ArrayInfo ai = SuaDB.mdMgr().getArrayInfo("TD1", tx);
		
		assertEquals(ai.arrayName(), "TD1");
		
		assertEquals(ai.schema().attributes().size(), 2);
		assertTrue(ai.schema().hasAttribute("a"));
		assertTrue(ai.schema().hasAttribute("b"));
		
		assertEquals(ai.schema().dimensions().size(), 1);
		assertTrue(ai.schema().hasDimension("x"));
		
		tx.commit();
	}

	@Test
	public void test_10_create_1D_array_with_underbar()
	{
		Transaction tx = new Transaction();
		// TODO :: Underbar delimiter unacceptable
		// Create array fail
		String query =
				"CREATE ARRAY T_D2" +
				"<" +
				"   a : int," +
				"   b : int" +
				">" +
				"[x = 0:100,10]";
		
		SuaDB.planner().executeUpdate(query, tx);
		
		tx.commit();
	}
	
	@Test(expected = BadSyntaxException.class)
	public void test_10_create_2D_array_with_duplicate_attributename()
	{
		Transaction tx = new Transaction();
		String query =
				"CREATE ARRAY TD3" +
				"<" +
				"   a : int," +
				"   b : int," +
				"   a : int" +
				">" +
				"[x = 0:100,10, y = 0:30,6]";
		
		SuaDB.planner().executeUpdate(query, tx);
		tx.commit();
	}
	
	@Test(expected = BadSyntaxException.class)
	public void test_10_create_3D_array_with_duplicate_dimensionname()
	{
		Transaction tx = new Transaction();
		String query =
				"CREATE ARRAY TD4" +
				"<" +
				"   a : int," +
				"   b : int" +
				">" +
				"[x = 0:100,10, y = 0:30,6, x = 0:50,2]";
		
		SuaDB.planner().executeUpdate(query, tx);
		tx.commit();
	}

	@Test
	public void test_80_remove_1D_array()
	{
		Transaction tx = new Transaction();
		String query = "REMOVE(TD1)";

		SuaDB.planner().executeUpdate(query, tx);
		tx.commit();
	}
	
	@AfterClass
	public static void tearDown()
	{
		SuaDB.shutDown();
	}
}