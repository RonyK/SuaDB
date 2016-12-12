package suadb.server;

import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.FileWriter;
import java.io.IOException;

import suadb.query.Plan;
import suadb.query.Scan;
import suadb.record.ArrayInfo;
import suadb.test.DummyData;
import suadb.test.SuaDBExeTestBase;
import suadb.test.T3A;
import suadb.tx.Transaction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by Rony on 2016-12-12.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SuaDB_3D_ABC_JOIN extends SuaDBExeTestBase
{
	static final String ARRAY_NAME_LEFT = "TARRAY_3D_3A_LEFT";
	static final String ARRAY_NAME_RIGHT = "TARRAY_3D_3A_RIGHT";
	
	static String FILE_PATH_LEFT;
	static String FILE_PATH_RIGHT;
	
	static final String ATTR_LEFT_01 = "a";
	static final String ATTR_LEFT_02 = "b";
	static final String ATTR_LEFT_03 = "c";
	
	static final String DIM_LEFT_01 = "x";
	static final String DIM_LEFT_02 = "y";
	static final String DIM_LEFT_03 = "z";
	
	static final String ATTR_RIGHT_01 = "aa";
	static final String ATTR_RIGHT_02 = "bb";
	static final String ATTR_RIGHT_03 = "cc";
	
	static final String DIM_RIGHT_01 = "xx";
	static final String DIM_RIGHT_02 = "yy";
	static final String DIM_RIGHT_03 = "zz";
	
	static final T3A<Integer, Integer, Integer>[][][] dummyLeft = DummyData.getArrayDummy_3A_3D();
	static final T3A<Integer, Integer, Integer>[][][] dummyRight = DummyData.getArrayDummy_3A_3D_ver2();
	
	@BeforeClass
	public static void init()
	{
		FILE_PATH_LEFT = homeDir + "\\" + ARRAY_NAME_LEFT + ".txt";
		FILE_PATH_RIGHT = homeDir + "\\" + ARRAY_NAME_RIGHT + ".txt";
		
		System.out.println(DummyData.getInputDummy_3A_3D(dummyLeft));
		System.out.println(DummyData.getInputDummy_3A_3D(dummyRight));
	}
	
	@Test
	public void test_10_create_array_left()
	{
		Transaction tx = new Transaction();
		
		try
		{
			String query = String.format(
					"CREATE ARRAY %s" +
							"<" +
							"   %s : int," +
							"   %s : int," +
							"   %s : int" +
							">" +
							"[%s = 0:1,2, %s = 0:3,2, %s = 0:3,2]",
					ARRAY_NAME_LEFT, ATTR_LEFT_01, ATTR_LEFT_02, ATTR_LEFT_03,
					DIM_LEFT_01, DIM_LEFT_02, DIM_LEFT_03);
			
			SuaDB.planner().executeUpdate(query, tx);
			
			ArrayInfo ai = SuaDB.mdMgr().getArrayInfo(ARRAY_NAME_LEFT, tx);
			
			assertEquals(ai.arrayName(), ARRAY_NAME_LEFT);
			
			assertEquals(ai.schema().attributes().size(), 3);
			assertTrue(ai.schema().hasAttribute(ATTR_LEFT_01));
			assertTrue(ai.schema().hasAttribute(ATTR_LEFT_02));
			assertTrue(ai.schema().hasAttribute(ATTR_LEFT_03));
			
			assertEquals(ai.schema().dimensions().size(), 3);
			assertTrue(ai.schema().hasDimension(DIM_LEFT_01));
			assertTrue(ai.schema().hasDimension(DIM_LEFT_02));
			assertTrue(ai.schema().hasDimension(DIM_LEFT_03));
		} catch (Exception e)
		{
			e.printStackTrace();
			assertTrue(false);
		} finally
		{
			tx.commit();
		}
	}
	
	@Test
	public void test_10_create_array_right()
	{
		Transaction tx = new Transaction();
		
		try
		{
			String query = String.format(
					"CREATE ARRAY %s" +
							"<" +
							"   %s : int," +
							"   %s : int," +
							"   %s : int" +
							">" +
							"[%s = 0:1,2, %s = 0:3,2, %s = 0:3,2]",
					ARRAY_NAME_RIGHT, ATTR_RIGHT_01, ATTR_RIGHT_02, ATTR_RIGHT_03,
					DIM_RIGHT_01, DIM_RIGHT_02, DIM_RIGHT_03);
			
			SuaDB.planner().executeUpdate(query, tx);
			
			ArrayInfo ai = SuaDB.mdMgr().getArrayInfo(ARRAY_NAME_RIGHT, tx);
			
			assertEquals(ai.arrayName(), ARRAY_NAME_RIGHT);
			
			assertEquals(ai.schema().attributes().size(), 3);
			assertTrue(ai.schema().hasAttribute(ATTR_RIGHT_01));
			assertTrue(ai.schema().hasAttribute(ATTR_RIGHT_02));
			assertTrue(ai.schema().hasAttribute(ATTR_RIGHT_03));
			
			assertEquals(ai.schema().dimensions().size(), 3);
			assertTrue(ai.schema().hasDimension(DIM_RIGHT_01));
			assertTrue(ai.schema().hasDimension(DIM_RIGHT_02));
			assertTrue(ai.schema().hasDimension(DIM_RIGHT_03));
		} catch (Exception e)
		{
			e.printStackTrace();
			assertTrue(false);
		} finally
		{
			tx.commit();
		}
	}
	
	@Test
	public void test_20_array_input_left() throws IOException
	{
		Transaction tx = new Transaction();
		
		try
		{
			FileWriter fw = new FileWriter(FILE_PATH_LEFT);
			fw.write(DummyData.getInputDummy_3A_3D(dummyLeft));
			fw.close();
			
			String query = "INPUT(" + ARRAY_NAME_LEFT + ", \'" + FILE_PATH_LEFT + "\')";
			
			SuaDB.planner().executeUpdate(query, tx);
			
			eraseFile(FILE_PATH_LEFT);
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
	public void test_20_array_input_right() throws IOException
	{
		Transaction tx = new Transaction();
		
		try
		{
			FileWriter fw = new FileWriter(FILE_PATH_RIGHT);
			fw.write(DummyData.getInputDummy_3A_3D(dummyRight));
			fw.close();
			
			String query = "INPUT(" + ARRAY_NAME_RIGHT + ", \'" + FILE_PATH_RIGHT + "\')";
			
			SuaDB.planner().executeUpdate(query, tx);
			
			eraseFile(FILE_PATH_RIGHT);
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
	public void test_30_scan_left_array()
	{
		Transaction tx = new Transaction();
		String query = "SCAN(" + ARRAY_NAME_LEFT + ")";
		
		Plan scanPlan = SuaDB.planner().createQueryPlan(query, tx);
		Scan s = scanPlan.open();
		
		int num = 0;
		while (s.next())
		{
			testLeftValue(s);
			num++;
		}
		
		assertTrue(num > 0);
		
		tx.commit();
	}
	
	@Test
	public void test_30_scan_right_array()
	{
		Transaction tx = new Transaction();
		String query = "SCAN(" + ARRAY_NAME_RIGHT + ")";
		
		Plan scanPlan = SuaDB.planner().createQueryPlan(query, tx);
		Scan s = scanPlan.open();
		
		int num = 0;
		while (s.next())
		{
			testRightValue(s);
			num++;
		}
		
		assertTrue(num > 0);
		
		tx.commit();
	}
	
	@Test
	public void test_50_join()
	{
		Transaction tx = new Transaction();
		
		try
		{
			String query = String.format("JOIN(%s, %s)", ARRAY_NAME_LEFT, ARRAY_NAME_RIGHT);
			
			Plan scanPlan = SuaDB.planner().createQueryPlan(query, tx);
			Scan s = scanPlan.open();
			
			assertTrue(s.hasField(ATTR_LEFT_01));
			assertTrue(s.hasField(ATTR_LEFT_02));
			assertTrue(s.hasField(ATTR_LEFT_03));
			assertTrue(s.hasField(ATTR_RIGHT_01));
			assertTrue(s.hasField(ATTR_RIGHT_02));
			assertTrue(s.hasField(ATTR_RIGHT_03));
			
			assertTrue(s.hasDimension(DIM_LEFT_01));
			assertTrue(s.hasDimension(DIM_LEFT_02));
			assertTrue(s.hasDimension(DIM_LEFT_03));
			assertFalse(s.hasDimension(DIM_RIGHT_01));
			assertFalse(s.hasDimension(DIM_RIGHT_02));
			assertFalse(s.hasDimension(DIM_RIGHT_03));
			
			int num = 0;
			while (s.next())
			{
				testJoinValue(s);
				num++;
			}
			
			assertTrue(num > 0);
			
			System.out.println(String.format("OUTPUT : %d", num));
		} catch (Exception e)
		{
			e.printStackTrace();
			assertTrue(false);
		} finally
		{
			tx.commit();
		}
	}
	
	public void testLeftValue(Scan s)
	{
		int attr_left_01 = s.getInt(ATTR_LEFT_01);
		int attr_left_02 = s.getInt(ATTR_LEFT_02);
		int attr_left_03 = s.getInt(ATTR_LEFT_03);
		
		int dim_left_01 = s.getDimension(DIM_LEFT_01);
		int dim_left_02 = s.getDimension(DIM_LEFT_02);
		int dim_left_03 = s.getDimension(DIM_LEFT_03);
		
		try
		{
			// ASSERT LEFT ATTRIBUTES
			if(s.isNull(ATTR_LEFT_01))
				assertEquals(
						"Check ATTR " + ATTR_LEFT_01,
						dummyLeft[dim_left_01][dim_left_02][dim_left_03].a, null);
			else
				assertEquals(
						"Check ATTR " + ATTR_LEFT_01,
						(int)dummyLeft[dim_left_01][dim_left_02][dim_left_03].a, attr_left_01);
			
			if(s.isNull(ATTR_LEFT_02))
				assertEquals(
						"Check ATTR " + ATTR_LEFT_02,
						dummyLeft[dim_left_01][dim_left_02][dim_left_03].b, null);
			else
				assertEquals(
						"Check ATTR " + ATTR_LEFT_02,
						(int)dummyLeft[dim_left_01][dim_left_02][dim_left_03].b, attr_left_02);
			
			if(s.isNull(ATTR_LEFT_03))
				assertEquals(
						"Check ATTR " + ATTR_LEFT_03,
						dummyLeft[dim_left_01][dim_left_02][dim_left_03].c, null);
			else
				assertEquals(
						"Check ATTR " + ATTR_LEFT_03,
						(int)dummyLeft[dim_left_01][dim_left_02][dim_left_03].c, attr_left_03);
		} catch (Exception e)
		{
			System.out.print("(" + dim_left_01 + ", " + dim_left_02 + ", " + dim_left_03 + ") ");
			System.out.println(String.format(
					"(%s : %d, %s : %d, %s : %d)",
					ATTR_LEFT_01, attr_left_01, ATTR_LEFT_02, attr_left_02, ATTR_LEFT_03, attr_left_03));
			e.printStackTrace();
			System.out.println(String.format("\tException : %s", e.getMessage()));
			assertTrue(false);
		}
	}
	
	public void testRightValue(Scan s)
	{
		int attr_right_01 = s.getInt(ATTR_RIGHT_01);
		int attr_right_02 = s.getInt(ATTR_RIGHT_02);
		int attr_right_03 = s.getInt(ATTR_RIGHT_03);
		
		int dim_right_01 = s.getDimension(DIM_RIGHT_01);
		int dim_right_02 = s.getDimension(DIM_RIGHT_02);
		int dim_right_03 = s.getDimension(DIM_RIGHT_03);
		
		try
		{
			System.out.print("(" + dim_right_01 + ", " + dim_right_02 + ", " + dim_right_03 + ") ");
			System.out.println(String.format(
					"(%s : %d, %s : %d, %s : %d)",
					ATTR_RIGHT_01, dim_right_01, ATTR_RIGHT_02, dim_right_02, ATTR_RIGHT_03, dim_right_03));
			
			// ASSERT RIGHT ATTRIBUTES
			if(s.isNull(ATTR_RIGHT_01))
				assertEquals(
						"Check ATTR " + ATTR_RIGHT_01,
						dummyRight[dim_right_01][dim_right_02][dim_right_03].a, null);
			else
				assertEquals(
						"Check ATTR " + ATTR_RIGHT_01,
						(int)dummyRight[dim_right_01][dim_right_02][dim_right_03].a, attr_right_01);
			
			if(s.isNull(ATTR_RIGHT_02))
				assertEquals(
						"Check ATTR " + ATTR_RIGHT_02,
						dummyRight[dim_right_01][dim_right_02][dim_right_03].b, null);
			else
				assertEquals(
						"Check ATTR " + ATTR_RIGHT_02,
						(int)dummyRight[dim_right_01][dim_right_02][dim_right_03].b, attr_right_02);
			
			if(s.isNull(ATTR_RIGHT_03))
				assertEquals(
						"Check ATTR " + ATTR_RIGHT_03,
						dummyRight[dim_right_01][dim_right_02][dim_right_03].c, null);
			else
				assertEquals(
						"Check ATTR " + ATTR_RIGHT_03,
						(int)dummyRight[dim_right_01][dim_right_02][dim_right_03].c, attr_right_03);
		} catch (Exception e)
		{
			System.out.print("(" + dim_right_01 + ", " + dim_right_02 + ", " + dim_right_03 + ") ");
			System.out.println(String.format(
					"(%s : %d, %s : %d, %s : %d)",
					ATTR_RIGHT_01, dim_right_01, ATTR_RIGHT_02, dim_right_02, ATTR_RIGHT_03, dim_right_03));
			e.printStackTrace();
			System.out.println(String.format("\tException : %s", e.getMessage()));
			assertTrue(false);
		}
	}
	
	public void testJoinValue(Scan s)
	{
		int attr_left_01 = s.getInt(ATTR_LEFT_01);
		int attr_left_02 = s.getInt(ATTR_LEFT_02);
		int attr_left_03 = s.getInt(ATTR_LEFT_03);
		int attr_right_01 = s.getInt(ATTR_RIGHT_01);
		int attr_right_02 = s.getInt(ATTR_RIGHT_02);
		int attr_right_03 = s.getInt(ATTR_RIGHT_03);
		
		int dim_left_01 = s.getDimension(DIM_LEFT_01);
		int dim_left_02 = s.getDimension(DIM_LEFT_02);
		int dim_left_03 = s.getDimension(DIM_LEFT_03);
		
		System.out.print("(" + dim_left_01 + ", " + dim_left_02 + ", " + dim_left_03 + ") ");
		System.out.println(String.format(
				"(%s : %d, %s : %d, %s : %d, %s : %d, %s : %d, %s : %d)",
				ATTR_LEFT_01, attr_left_01, ATTR_LEFT_02, attr_left_02, ATTR_LEFT_03, attr_left_03,
				ATTR_RIGHT_01, attr_right_01, ATTR_RIGHT_02, attr_right_02, ATTR_RIGHT_03, attr_right_03));
		
		try
		{
			// ASSERT LEFT ATTRIBUTES
			if(s.isNull(ATTR_LEFT_01))
			{
				System.out.print("a : NULL \t");
				assertEquals("Check ATTR " + ATTR_LEFT_01, dummyLeft[dim_left_01][dim_left_02][dim_left_03].a, null);
			}else
				assertEquals(
						"Check ATTR " + ATTR_LEFT_01,
						(int)dummyLeft[dim_left_01][dim_left_02][dim_left_03].a, attr_left_01);

			if(s.isNull(ATTR_LEFT_02))
			{
				System.out.print("b : NULL \t");
				assertEquals("Check ATTR " + ATTR_LEFT_02, dummyLeft[dim_left_01][dim_left_02][dim_left_03].b, null);
			}else
				assertEquals(
						"Check ATTR " + ATTR_LEFT_02,
						(int)dummyLeft[dim_left_01][dim_left_02][dim_left_03].b, attr_left_02);

			if(s.isNull(ATTR_LEFT_03))
			{
				System.out.print("c : NULL \t");
				assertEquals("Check ATTR " + ATTR_LEFT_03, dummyLeft[dim_left_01][dim_left_02][dim_left_03].c, null);
			}else
				assertEquals(
						"Check ATTR " + ATTR_LEFT_03,
						(int)dummyLeft[dim_left_01][dim_left_02][dim_left_03].c, attr_left_03);
			System.out.println();

			// ASSERT RIGHT ATTRIBUTES
			if(s.isNull(ATTR_RIGHT_01))
			{
				System.out.print("aa : NULL \t");
				assertEquals("Check ATTR " + ATTR_RIGHT_01, dummyRight[dim_left_01][dim_left_02][dim_left_03].a, null);
			}else
				assertEquals(
						"Check ATTR " + ATTR_RIGHT_01,
						(int)dummyRight[dim_left_01][dim_left_02][dim_left_03].a, attr_right_01);

			if(s.isNull(ATTR_RIGHT_02))
			{
				System.out.print("bb : NULL \t");
				assertEquals("Check ATTR " + ATTR_RIGHT_02, dummyRight[dim_left_01][dim_left_02][dim_left_03].b, null);
			}else
				assertEquals(
						"Check ATTR " + ATTR_RIGHT_02,
						(int)dummyRight[dim_left_01][dim_left_02][dim_left_03].b, attr_right_02);

			if(s.isNull(ATTR_RIGHT_03))
			{
				System.out.print("cc : NULL \t");
				assertEquals("Check ATTR " + ATTR_RIGHT_03, dummyRight[dim_left_01][dim_left_02][dim_left_03].c, null);
			}else
				assertEquals(
						"Check ATTR " + ATTR_RIGHT_03,
						(int)dummyRight[dim_left_01][dim_left_02][dim_left_03].c, attr_right_03);
			System.out.println();
		} catch (Exception e)
		{
			System.out.print("(" + dim_left_01 + ", " + dim_left_02 + ", " + dim_left_03 + ") ");
			System.out.println(String.format(
					"(%s : %d, %s : %d, %s : %d, %s : %d, %s : %d, %s : %d)",
					ATTR_LEFT_01, attr_left_01, ATTR_LEFT_02, attr_left_02, ATTR_LEFT_03, attr_left_03,
					ATTR_RIGHT_01, attr_right_01, ATTR_RIGHT_02, attr_right_02, ATTR_RIGHT_03, attr_right_03));
			e.printStackTrace();
			System.out.println(String.format("\tException : %s", e.getMessage()));
			assertTrue(false);
		}
	}
}
