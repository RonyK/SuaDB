package suadb.server;

import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import suadb.metadata.ArrayMgr;
import suadb.parse.Expression;
import suadb.parse.InputArrayData;
import suadb.planner.BasicUpdatePlanner;
import suadb.planner.Planner;
import suadb.query.Plan;
import suadb.query.Region;
import suadb.query.Scan;
import suadb.record.ArrayInfo;
import suadb.server.SuaDB;
import suadb.test.DummyData;
import suadb.test.SuaDBExeTestBase;
import suadb.test.T3A;
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
	static final String ATTR_01 = "a";
	static final String ATTR_02 = "b";
	static final String ATTR_03 = "c";
	
	static final String DIM_01 = "x";
	static final String DIM_02 = "y";
	static final String DIM_03 = "z";
	
	static final T3A<Integer, Integer, Integer>[][][] dummy = DummyData.getArrayDummy_3A_3D();
	static final int dummyCellNum = 16 - 1;     // (0, 1, 3) has null value for all attributes.
	
	@BeforeClass
	public static void init()
	{
		FILE_PATH = homeDir + "\\" + ARRAY_NAME + ".txt";
		
		System.out.println(DummyData.getInputDummy_3A_3D(dummy));
	}

	@Test
	public void test_09_list_empty_check()
	{
		Transaction tx = new Transaction();
		Scan s = list(tx);

		assertFalse("Check Empty Result", s.next());
		tx.commit();
	}
	
	@Test
	public void test_10_create_array()
	{
		Transaction tx = new Transaction();
		String query = String.format(
				"CREATE ARRAY %s" +
						"<" +
						"   %s : int," +
						"   %s : int," +
						"   %s : int" +
						">" +
						"[%s = 0:1,2, %s = 0:3,2, %s = 0:3,2]",
				ARRAY_NAME, ATTR_01, ATTR_02, ATTR_03,
				DIM_01, DIM_02, DIM_03);
		
		SuaDB.planner().executeUpdate(query, tx);
		
		ArrayInfo ai = SuaDB.mdMgr().getArrayInfo(ARRAY_NAME, tx);
		
		assertEquals(ai.arrayName(), ARRAY_NAME);
		
		assertEquals(ai.schema().attributes().size(), 3);
		assertTrue(ai.schema().hasAttribute(ATTR_01));
		assertTrue(ai.schema().hasAttribute(ATTR_02));
		assertTrue(ai.schema().hasAttribute(ATTR_03));
		assertFalse(ai.schema().hasAttribute("d"));
		
		assertEquals(ai.schema().dimensions().size(), 3);
		assertTrue(ai.schema().hasDimension(DIM_01));
		assertTrue(ai.schema().hasDimension(DIM_02));
		assertTrue(ai.schema().hasDimension(DIM_03));
		assertFalse(ai.schema().hasDimension("t"));
		
		tx.commit();
	}
	
//	@Test
	public void test_11_create_array()
	{
		Transaction tx = new Transaction();
		String query = String.format(
				"CREATE ARRAY %s" +
				"<" +
				"   %s : int," +
				"   %s : int," +
				"   %s : int" +
				">" +
				"[%s = 0:1,2, %s = 0:3,4, %s = 0:3,2]",
				ARRAY_NAME + "_1", ATTR_01, ATTR_02, ATTR_03,
				DIM_01, DIM_02, DIM_03);
		
		SuaDB.planner().executeUpdate(query, tx);
		
		ArrayInfo ai = SuaDB.mdMgr().getArrayInfo(ARRAY_NAME, tx);
		
		assertEquals(ai.arrayName(), ARRAY_NAME);
		
		assertEquals(ai.schema().attributes().size(), 3);
		assertTrue(ai.schema().hasAttribute(ATTR_01));
		assertTrue(ai.schema().hasAttribute(ATTR_02));
		assertTrue(ai.schema().hasAttribute(ATTR_03));
		assertFalse(ai.schema().hasAttribute("d"));
		
		assertEquals(ai.schema().dimensions().size(), 3);
		assertTrue(ai.schema().hasDimension(DIM_01));
		assertTrue(ai.schema().hasDimension(DIM_02));
		assertTrue(ai.schema().hasDimension(DIM_03));
		assertFalse(ai.schema().hasDimension("t"));
		
		tx.commit();
	}
	
	@Test
	public void test_19_list_create_check()
	{
		Transaction tx = new Transaction();
		Boolean result = false;
		Scan s = list(tx);
		
		System.out.println("List() result : ");
		assertTrue(s.next());
		do{
			String arrayName = s.getString(ArrayMgr.STR_ARRAY_NAME);
			System.out.println(arrayName);
			if(arrayName.equals(ARRAY_NAME))
			{
				result = true;
				break;
			}
		}while (s.next());
		
		tx.commit();

		assertTrue(result);
	}
	
	@Test
	public void test_20_array_input_mannual() throws IOException
	{
		try
		{
			Field upField = Planner.class.getDeclaredField("uplanner");
			upField.setAccessible(true);
			
			FileWriter fw = new FileWriter(FILE_PATH);
			fw.write(DummyData.getInputDummy_3A_3D(dummy));
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
	public void test_20_array_input() throws IOException
	{
		Transaction tx = new Transaction();
		
		try
		{
			FileWriter fw = new FileWriter(FILE_PATH);
			fw.write(DummyData.getInputDummy_3A_3D(dummy));
			fw.close();
			
			String query = "INPUT(" + ARRAY_NAME + ", \'" + FILE_PATH + "\')";
			
			SuaDB.planner().executeUpdate(query, tx);
			
			eraseFile(FILE_PATH);
		}catch (Exception e)
		{
			System.out.println(e.getMessage());
			throw e;
		}finally
		{
			tx.commit();
		}
	}

	@Test
	public void test_30_scan_array()
	{
		Transaction tx = new Transaction();
		String query = "SCAN(" + ARRAY_NAME + ")";
		
		Plan scanPlan = SuaDB.planner().createQueryPlan(query, tx);
		Scan s = scanPlan.open();
		
		int num = 0;
		while (s.next())
		{
			testValue(s);
			
			num++;
		}
		
		assertEquals(num, dummyCellNum);
		
		tx.commit();
	}
	
	public void testValue(Scan s)
	{
		int attr_01 = s.getInt(ATTR_01);
		int attr_02 = s.getInt(ATTR_02);
		int attr_03 = s.getInt(ATTR_03);
		
		int dim_01 = s.getDimension(DIM_01);
		int dim_02 = s.getDimension(DIM_02);
		int dim_03 = s.getDimension(DIM_03);
		
		try
		{
			System.out.print("(" + dim_01 + ", " + dim_02 + ", " + dim_03 + ") ");
			System.out.print("a : " + attr_01 + ",\tb : " + attr_02 + ",\tc : " + attr_03);
			System.out.print(String.format("\tExpect %s", dummy[dim_01][dim_02][dim_03].toString()));

			if(s.isNull(ATTR_01))
				assertEquals("Check ATTR " + ATTR_01, dummy[dim_01][dim_02][dim_03].a, null);
			else
				assertEquals("Check ATTR " + ATTR_01, (int)dummy[dim_01][dim_02][dim_03].a, s.getInt(ATTR_01));

			if(s.isNull(ATTR_02))
				assertEquals("Check ATTR " + ATTR_02, dummy[dim_01][dim_02][dim_03].b, null);
			else
				assertEquals("Check ATTR " + ATTR_02, (int)dummy[dim_01][dim_02][dim_03].b, s.getInt(ATTR_02));

			if(s.isNull(ATTR_03))
				assertEquals("Check ATTR " + ATTR_03, dummy[dim_01][dim_02][dim_03].c, null);
			else
				assertEquals("Check ATTR " + ATTR_03, (int)dummy[dim_01][dim_02][dim_03].c, s.getInt(ATTR_03));

			System.out.println("\tTrue");
		} catch (Exception e)
		{
			System.out.println(String.format("\tFalse : %s", e.getMessage()));
			e.printStackTrace();
		}
	}
	
	@Test
	public void test_50_filter_3D_array()
	{
		List<T3A<Integer, Integer, Integer>> targets = new ArrayList<>();
		targets.add(new T3A<>(0, 1, 2));
		targets.add(new T3A<>(0, 2, 0));
		targets.add(new T3A<>(0, 2, 1));
		
		Transaction tx = new Transaction();
		
		int attr_01_condition = 9;
		String condition = ATTR_01 + " = " + Integer.toString(attr_01_condition);
		String query = String.format(
				"FILTER(%s, %s)", ARRAY_NAME, condition);
		
		Plan p = SuaDB.planner().createQueryPlan(query, tx);
		Scan s = p.open();
		
		while (s.next())
		{
			int attr_01 = s.getInt(ATTR_01);
			int attr_02 = s.getInt(ATTR_02);
			int attr_03 = s.getInt(ATTR_03);
			
			int dim_01 = s.getDimension(DIM_01);
			int dim_02 = s.getDimension(DIM_02);
			int dim_03 = s.getDimension(DIM_03);
			
			System.out.println("(" + dim_01 + ", " + dim_02 + ", " + dim_03 + ") ");
			
			assertEquals(attr_01, attr_01_condition);
			assertTrue(targets.contains(new T3A<>(dim_01, dim_02, dim_03)));
			targets.remove(new T3A<>(dim_01, dim_02, dim_03));
		}
		
		assertTrue(targets.size() == 0);
		
		tx.commit();
	}
	
	@Test
	public void test_50_project()
	{
		Transaction tx = new Transaction();
		List<String> attributes = Arrays.asList("b", "c");
		String projectCondition = String.join(", ", attributes);
		
		String query = String.format(
				"PROJECT(%s, %s)", ARRAY_NAME, projectCondition);
		Plan p = SuaDB.planner().createQueryPlan(query, tx);
		Scan s = p.open();
		
		int num = 0;
		while (s.next())
		{
			assertFalse(s.hasField(ATTR_01));
			assertTrue(s.hasField(ATTR_02));
			assertTrue(s.hasField(ATTR_03));
			
			int attr_02 = s.getInt(ATTR_02);
			int attr_03 = s.getInt(ATTR_03);
			
			int dim_01 = s.getDimension(DIM_01);
			int dim_02 = s.getDimension(DIM_02);
			int dim_03 = s.getDimension(DIM_03);
			
			try
			{
				System.out.print("(" + dim_01 + ", " + dim_02 + ", " + dim_03 + ") ");
				System.out.print("b : " + attr_02 + ", c : " + attr_03);
				
				if(s.isNull(ATTR_02))
					assertEquals("Check ATTR " + ATTR_02, dummy[dim_01][dim_02][dim_03].b, null);
				else
					assertEquals("Check ATTR " + ATTR_02, (int)dummy[dim_01][dim_02][dim_03].b, s.getInt(ATTR_02));
				
				if(s.isNull(ATTR_03))
					assertEquals("Check ATTR " + ATTR_03, dummy[dim_01][dim_02][dim_03].c, null);
				else
					assertEquals("Check ATTR " + ATTR_03, (int)dummy[dim_01][dim_02][dim_03].c, s.getInt(ATTR_03));
				
				
				System.out.println("\tTrue");
			} catch (Exception e)
			{
				System.out.println("\tFalse");
				e.printStackTrace();
			}
			
			num++;
		}
		
		assertEquals(num, dummyCellNum);
		tx.commit();
	}

	@Test
	public void test_55_between()
	{
		Transaction tx = new Transaction();

		try
		{
			Region region = new Region(Arrays.asList(0, 1, 1), Arrays.asList(0, 2, 2));
			String query = String.format("BETWEEN(%s, %s)", ARRAY_NAME, region.toString());

			Plan scanPlan = SuaDB.planner().createQueryPlan(query, tx);
			Scan s = scanPlan.open();

			int num = 0;
			while (s.next())
			{
				testValue(s);

				assertEquals(region.compareTo(s.getCurrentDimension()), 0);

				num++;
			}

			assertEquals(num, 4);
		}catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			tx.commit();
		}
	}

	@Test
	public void test_56_between_naive()
	{
		Transaction tx = new Transaction();

		try
		{
			Region region = new Region(Arrays.asList(0, 1, 1), Arrays.asList(0, 2, 2));
			String query = String.format("BETWEENNAIVE(%s, %s)", ARRAY_NAME, region.toString());

			Plan scanPlan = SuaDB.planner().createQueryPlan(query, tx);
			Scan s = scanPlan.open();

			int num = 0;
			while (s.next())
			{
				testValue(s);

				assertEquals(region.compareTo(s.getCurrentDimension()), 0);

				num++;
			}

			assertEquals(num, 4);
		}catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			tx.commit();
		}
	}
	
	@Test
	public void test_70_multilevel_query()
	{
		List<T3A<Integer, Integer, Integer>> targets = new ArrayList<>();
		targets.add(new T3A<>(0, 0, 0));
		targets.add(new T3A<>(0, 0, 1));
		targets.add(new T3A<>(0, 2, 0));
		targets.add(new T3A<>(0, 2, 1));
		
		Transaction tx = new Transaction();
		List<String> attributes = Arrays.asList("b");
		String projectCondition = String.join(", ", attributes);
		int attr_01_condition = 10;
		int attr_02_condition = 50;
		String filterCondition = String.join(
				" and ", Arrays.asList(
						String.format("a < %d", attr_01_condition),
						String.format(" b > %d", attr_02_condition)));
		
		String query = String.format(
				"PROJECT(FILTER(%s, %s), %s)", ARRAY_NAME, filterCondition, projectCondition);
		Plan p = SuaDB.planner().createQueryPlan(query, tx);
		Scan s = p.open();
		
		int num = 0;
		while (s.next())
		{
			assertFalse(s.hasField(ATTR_01));
			assertTrue(s.hasField(ATTR_02));
			assertFalse(s.hasField(ATTR_03));
			
			int attr_02 = s.getInt(ATTR_02);
			
			int dim_01 = s.getDimension(DIM_01);
			int dim_02 = s.getDimension(DIM_02);
			int dim_03 = s.getDimension(DIM_03);
			
			try
			{
				System.out.print("(" + dim_01 + ", " + dim_02 + ", " + dim_03 + ") ");
				System.out.print("b : " + attr_02);
				
				if(s.isNull(ATTR_02))
					assertTrue("Check ATTR " + ATTR_02, false);
				else
				{
					assertEquals("Check ATTR " + ATTR_02, (int)dummy[dim_01][dim_02][dim_03].b, s.getInt(ATTR_02));
					assertTrue("Check Filter Condition ", s.getInt(ATTR_02) > attr_02_condition);
				}
				
				System.out.println("\tTrue");
			} catch (Exception e)
			{
				System.out.println("\tFalse");
				e.printStackTrace();
			}
			
			assertTrue(targets.contains(new T3A<>(dim_01, dim_02, dim_03)));
			targets.remove(new T3A<>(dim_01, dim_02, dim_03));
		}
		
		assertTrue(targets.size() == 0);
		
		tx.commit();
	}
	
	@Test
	public void test_80_remove_3D_array()
	{
		Transaction tx = new Transaction();
		String query = "REMOVE(" + ARRAY_NAME +")";
		SuaDB.planner().executeUpdate(query, tx);
		tx.commit();
	}
	
	@Test
	public void test_81_list_array()
	{
		
		Transaction tx = new Transaction();
		String query =
				"LIST(arrays)";
		
		Plan plan = SuaDB.planner().createQueryPlan(query, tx);
		Scan s = plan.open();
		
		assertFalse("empty database", s.next());
		tx.commit();
	}
}
