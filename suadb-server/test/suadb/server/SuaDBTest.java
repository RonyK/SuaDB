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
import suadb.test.SuaDBExeTestBase;
import suadb.test.SuaDBTestBase;
import suadb.tx.Transaction;

import static org.junit.Assert.*;

/**
 * Created by Rony on 2016-11-28.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SuaDBTest extends SuaDBExeTestBase
{
	@Test
	public void test_01_list()
	{
		Transaction tx = new Transaction();
		Scan s = list(tx);
		
		assertFalse("Check empty database", s.next());
		tx.commit();
	}
	
	@Test
	public void test_10_create_1D_array()
	{
		String ARRAY_NAME = "TD1";
		String ATTR_01 = "a";
		String ATTR_02 = "b";
		String DIM_01 = "x";
		
		Transaction tx = new Transaction();
		String query =
				"CREATE ARRAY TD1" +
				"<" +
				"   a : int," +
				"   b : int" +
				">" +
				"[x = 0:100,10]";
		
		SuaDB.planner().executeUpdate(query, tx);
		
		ArrayInfo ai = SuaDB.mdMgr().getArrayInfo(ARRAY_NAME, tx);
		assertEquals(ai.arrayName(), ARRAY_NAME);
		
		// Check Attributes
		assertEquals(ai.schema().attributes().size(), 2);
		assertTrue(ai.schema().hasAttribute(ATTR_01));
		assertTrue(ai.schema().hasAttribute(ATTR_02));
		
		// Check Dimensions
		assertEquals(ai.schema().dimensions().size(), 1);
		assertTrue(ai.schema().hasDimension(DIM_01));
		
		tx.commit();
	}

	@Test
	public void test_10_create_1D_array_with_underbar()
	{
		String ARRAY_NAME = "T_D2";
		String ATTR_01 = "c";
		String ATTR_02 = "d";
		String DIM_01 = "z";
		
		Transaction tx = new Transaction();
		String query = String.format(
				"CREATE ARRAY %s" +
				"<" +
				"   %s : int," +
				"   %s : int" +
				">" +
				"[%s = 0:100,10]",
				ARRAY_NAME, ATTR_01, ATTR_02, DIM_01);
		
		SuaDB.planner().executeUpdate(query, tx);
		
		ArrayInfo ai = SuaDB.mdMgr().getArrayInfo(ARRAY_NAME, tx);
		assertEquals(ai.arrayName(), ARRAY_NAME);
		
		// Check Attributes
		assertEquals(ai.schema().attributes().size(), 2);
		assertTrue(ai.schema().hasAttribute(ATTR_01));
		assertTrue(ai.schema().hasAttribute(ATTR_02));
		
		// Check Dimensions
		assertEquals(ai.schema().dimensions().size(), 1);
		assertTrue(ai.schema().hasDimension(DIM_01));
		
		tx.commit();
	}
	
	@Test
	public void test_10_create_1D_array_attributes_with_underbar()
	{
		String ARRAY_NAME = "T_D3";
		String ATTR_01 = "a_a";
		String ATTR_02 = "b_b";
		String DIM_01 = "x";
		
		Transaction tx = new Transaction();
		String query = String.format(
				"CREATE ARRAY %s" +
				"<" +
				"   %s : int," +
				"   %s : int" +
				">" +
				"[%s = 0:100,10]",
				ARRAY_NAME, ATTR_01, ATTR_02, DIM_01);
		
		SuaDB.planner().executeUpdate(query, tx);
		
		ArrayInfo ai = SuaDB.mdMgr().getArrayInfo(ARRAY_NAME, tx);
		assertEquals(ai.arrayName(), ARRAY_NAME);
		
		// Check Attributes
		assertEquals(ai.schema().attributes().size(), 2);
		assertTrue(ai.schema().hasAttribute(ATTR_01));
		assertTrue(ai.schema().hasAttribute(ATTR_02));
		
		// Check Dimensions
		assertEquals(ai.schema().dimensions().size(), 1);
		assertTrue(ai.schema().hasDimension(DIM_01));
		
		tx.commit();
	}
	
	@Test(expected = BadSyntaxException.class)
	public void test_10_create_2D_array_with_duplicate_attributename()
	{
		Transaction tx = new Transaction();
		
		try
		{
			String query =
					"CREATE ARRAY TD3" +
					"<" +
					"   a : int," +
					"   b : int," +
					"   a : int" +
					">" +
					"[x = 0:100,10, y = 0:30,6]";
			
			SuaDB.planner().executeUpdate(query, tx);
		}catch (Exception e)
		{
			throw e;
		}finally
		{
			tx.commit();
		}
	}
	
	@Test(expected = BadSyntaxException.class)
	public void test_10_create_3D_array_with_duplicate_dimensionname()
	{
		Transaction tx = new Transaction();
		
		try
		{
			String query =
					"CREATE ARRAY TD4" +
					"<" +
					"   a : int," +
					"   b : int" +
					">" +
					"[x = 0:100,10, y = 0:30,6, x = 0:50,2]";
			
			SuaDB.planner().executeUpdate(query, tx);
		}catch (Exception e)
		{
			throw e;
		}finally
		{
			tx.commit();
		}
	}

	@Test
	public void test_80_remove_1D_array()
	{
		Transaction tx = new Transaction();
		String query = "REMOVE(TD1)";

		SuaDB.planner().executeUpdate(query, tx);
		tx.commit();
	}
}