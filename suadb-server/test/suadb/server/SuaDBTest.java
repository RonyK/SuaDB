package suadb.server;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.FileWriter;
import java.io.IOException;

import suadb.parse.BadSyntaxException;
import suadb.parse.Parser;
import suadb.query.Plan;
import suadb.query.Scan;
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
	}
	
	@Test
	public void test_10_create_3D_array()
	{
		Transaction tx = new Transaction();
		String query =
				"CREATE ARRAY TARRAY_3D_ABC" +
				"<" +
				"   a : int," +
				"   b : int," +
				"   c : int" +
				">" +
				"[x = 0:10,2, y = 0:20,4, z = 0:5,1]";
		
		SuaDB.planner().executeUpdate(query, tx);
	}
	
	@Test
	public void test_20_insert_1D_array_input() throws IOException
	{
		String FILE_PATH = homeDir+"/test.txt";
		
		FileWriter fw = new FileWriter(FILE_PATH);
		fw.write(DummyData.InputDummy_3A_3D);
		fw.close();
		
		Transaction tx = new Transaction();
		String query = "INPUT(TARRAY_3D_ABC, " + FILE_PATH + ")";
		
		SuaDB.planner().executeUpdate(query, tx);
		
		eraseFile(FILE_PATH);
	}
	
	@Test
	public void test_80_remove_1D_array()
	{
		Transaction tx = new Transaction();
		String query = "REMOVE(TD1)";

		SuaDB.planner().executeUpdate(query, tx);
	}
	
	@AfterClass
	public static void tearDown()
	{
		SuaDB.shutDown();
	}
}