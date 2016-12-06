package suadb.server;

import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;

import suadb.parse.InputArrayData;
import suadb.planner.BasicUpdatePlanner;
import suadb.planner.Planner;
import suadb.record.ArrayInfo;
import suadb.server.SuaDB;
import suadb.test.DummyData;
import suadb.test.SuaDBExeTestBase;
import suadb.tx.Transaction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by Rony on 2016-12-06.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SuaDB_3D_ABC extends SuaDBExeTestBase
{
	static final String ARRAY_NAME = "TARRAY3DABC";
	static String FILE_PATH;
	
	@BeforeClass
	public static void init()
	{
		FILE_PATH = homeDir + "/" + ARRAY_NAME + ".txt";
	}
	
	@Test
	public void test_10_create_3D_array()
	{
		Transaction tx = new Transaction();
		String query =
				"CREATE ARRAY " + ARRAY_NAME +
				"<" +
				"   a : int," +
				"   b : int," +
				"   c : int" +
				">" +
				"[x = 0:1,2, y = 0:3,4, z = 0:1,2]";
		
		SuaDB.planner().executeUpdate(query, tx);
		
		ArrayInfo ai = SuaDB.mdMgr().getArrayInfo(ARRAY_NAME, tx);
		
		assertEquals(ai.arrayName(), ARRAY_NAME);
		
		assertEquals(ai.schema().attributes().size(), 3);
		assertTrue(ai.schema().hasAttribute("a"));
		assertTrue(ai.schema().hasAttribute("b"));
		assertTrue(ai.schema().hasAttribute("c"));
		assertFalse(ai.schema().hasAttribute("d"));
		
		assertEquals(ai.schema().dimensions().size(), 3);
		assertTrue(ai.schema().hasDimension("x"));
		assertTrue(ai.schema().hasDimension("y"));
		assertTrue(ai.schema().hasDimension("z"));
		assertFalse(ai.schema().hasDimension("t"));
		
		tx.commit();
	}
	
	@Test
	public void test_20_insert_3D_array_input_mannual() throws IOException
	{
		try
		{
			Field upField = Planner.class.getDeclaredField("uplanner");
			upField.setAccessible(true);
			
			FileWriter fw = new FileWriter(FILE_PATH);
			fw.write(DummyData.InputDummy_3A_3D);
			fw.close();
			
			Transaction tx = new Transaction();
			
			InputArrayData obj = new InputArrayData(ARRAY_NAME, FILE_PATH);
			BasicUpdatePlanner uplanner = (BasicUpdatePlanner)upField.get(SuaDB.planner());
			uplanner.executeInputArray((InputArrayData)obj, tx);
			
			tx.commit();
			
			eraseFile(FILE_PATH);
		}catch (NoSuchFieldException e)
		{
			e.printStackTrace();
		}catch (IllegalAccessException e)
		{
			e.printStackTrace();
		}
	}
	
	@Test
	public void test_20_insert_3D_array_input() throws IOException
	{
		FileWriter fw = new FileWriter(FILE_PATH);
		fw.write(DummyData.InputDummy_3A_3D);
		fw.close();
		
		Transaction tx = new Transaction();
		String query = "INPUT(" + ARRAY_NAME + ", \'" + FILE_PATH + "\')";
		
		SuaDB.planner().executeUpdate(query, tx);
		tx.commit();
		
		eraseFile(FILE_PATH);
	}
}
