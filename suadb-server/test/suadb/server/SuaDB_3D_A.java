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

import suadb.parse.InputArrayData;
import suadb.planner.BasicUpdatePlanner;
import suadb.planner.Planner;
import suadb.query.Plan;
import suadb.query.Region;
import suadb.query.Scan;
import suadb.record.ArrayInfo;
import suadb.record.Schema;
import suadb.record.Schema.DimensionInfo;
import suadb.test.DummyData;
import suadb.test.SuaDBExeTestBase;
import suadb.tx.Transaction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by rony on 2016-12-12.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SuaDB_3D_A extends SuaDBExeTestBase
{
	static final String ARRAY_NAME = "TARRAY3DA";
	static String FILE_PATH;
	static int dummy[][][];

	static final String ATTR = "a";

	static final String DIM_01 = "x";
	static final String DIM_02 = "y";
	static final String DIM_03 = "z";

//	static final int DIM_01_START = 3;
//	static final int DIM_01_END = 22;
//	static final int DIM_01_SIZE = 5;
//
//	static final int DIM_02_START = 7;
//	static final int DIM_02_END = 32;
//	static final int DIM_02_SIZE = 4;
//
//	static final int DIM_03_START = 1;
//	static final int DIM_03_END = 20;
//	static final int DIM_03_SIZE = 20;

	static final int DIM_01_START = 0;
	static final int DIM_01_END = 20;
	static final int DIM_01_SIZE = 5;

	static final int DIM_02_START = 0;
	static final int DIM_02_END = 30;
	static final int DIM_02_SIZE = 4;

	static final int DIM_03_START = 0;
	static final int DIM_03_END = 20;
	static final int DIM_03_SIZE = 20;

	static String inputData;

	static int betweenCount = 0;
	static int betweenNaiveCount = 0;

	@BeforeClass
	public static void init()
	{
		DummyData dd = new DummyData();
		Schema schema = new Schema();
		schema.addDimension("x", DIM_01_START, DIM_01_END, DIM_01_SIZE);
		schema.addDimension("y", DIM_02_START, DIM_02_END, DIM_02_SIZE);
		schema.addDimension("z", DIM_03_START, DIM_03_END, DIM_03_SIZE);

		List<DimensionInfo> dimensionInfos = new ArrayList<>();
		for(DimensionInfo dInfo : schema.dimensionInfo().values())
		{
			dimensionInfos.add(dInfo);
		}

		dummy = dd.generate3DDummy(dimensionInfos);

		inputData = "[";

		for(int x = DIM_01_START; x <= DIM_01_END; x++)
		{
			inputData += "[";
			for(int y = DIM_02_START; y <= DIM_02_END; y++)
			{
				inputData += "[";
				for(int z = DIM_03_START; z <= DIM_03_END; z++)
				{
					if(z != 1)
					{
						inputData += ",";
					}
					inputData += "(" + Integer.toString(dummy[x][y][z]) + ")";
				}
				inputData += "]";
			}
			inputData += "]";
		}
		inputData += "]";

		FILE_PATH = homeDir + "\\" + ARRAY_NAME + ".txt";
	}

	@Test
	public void test_10_create_array()
	{
		Transaction tx = new Transaction();

		try
		{
			String query = String.format(
					"CREATE ARRAY %s" +
							"<" +
							"   %s : int" +
							">" +
							"[%s = %d:%d,%d, %s = %d:%d,%d, %s = %d:%d,%d]",
					ARRAY_NAME, ATTR,
					DIM_01, DIM_01_START, DIM_01_END, DIM_01_SIZE,
					DIM_02, DIM_02_START, DIM_02_END, DIM_02_SIZE,
					DIM_03, DIM_03_START, DIM_03_END, DIM_03_SIZE);

			SuaDB.planner().executeUpdate(query, tx);

			ArrayInfo ai = SuaDB.mdMgr().getArrayInfo(ARRAY_NAME, tx);

			assertEquals(ai.arrayName(), ARRAY_NAME);

			assertEquals(ai.schema().attributes().size(), 1);
			assertTrue(ai.schema().hasAttribute(ATTR));
			assertFalse(ai.schema().hasAttribute("d"));

			assertEquals(ai.schema().dimensions().size(), 3);
			assertTrue(ai.schema().hasDimension(DIM_01));
			assertTrue(ai.schema().hasDimension(DIM_02));
			assertTrue(ai.schema().hasDimension(DIM_03));
			assertFalse(ai.schema().hasDimension("t"));
		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			throw e;
		} finally
		{
			tx.commit();
		}
	}

	@Test
	public void test_20_array_input() throws IOException
	{
		Transaction tx = new Transaction();
		try
		{
			FileWriter fw = new FileWriter(FILE_PATH);
			fw.write(inputData);
			fw.close();

			String query = "INPUT(" + ARRAY_NAME + ", \'" + FILE_PATH + "\')";

			SuaDB.planner().executeUpdate(query, tx);

			eraseFile(FILE_PATH);
		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			throw e;
		} finally
		{
			tx.commit();
		}
	}

	@Test
	public void test_30_scan_array()
	{
		Transaction tx = new Transaction();
		try
		{
			String query = "SCAN(" + ARRAY_NAME + ")";

			Plan scanPlan = SuaDB.planner().createQueryPlan(query, tx);
			Scan s = scanPlan.open();

			int num = 0;
			while (s.next())
			{
				testValue(s);

				num++;
			}

			assertTrue(num > 0);
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			tx.commit();
		}
	}

	@Test
	public void test_50_between()
	{
		Transaction tx = new Transaction();
		try
		{
			Region region = new Region(Arrays.asList(5, 10, 1), Arrays.asList(15, 12, 5));
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

			assertTrue(num > 0);
			betweenCount = num;

			System.out.println(String.format("OUTPUT : %d", num));
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			tx.commit();
		}
	}

	@Test
	public void test_51_between_naive()
	{
		Transaction tx = new Transaction();
		try
		{
			Region region = new Region(Arrays.asList(5, 10, 1), Arrays.asList(15, 12, 5));
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

			assertTrue(num > 0);
			betweenNaiveCount = num;
			assertEquals(betweenCount, betweenNaiveCount);

			System.out.println(String.format("OUTPUT : %d", num));
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			tx.commit();
		}
	}

	public void testValue(Scan s)
	{
		int attr_01 = s.getInt(ATTR);

		int dim_01 = s.getDimension(DIM_01);
		int dim_02 = s.getDimension(DIM_02);
		int dim_03 = s.getDimension(DIM_03);

		try
		{
			System.out.print("(" + dim_01 + ", " + dim_02 + ", " + dim_03 + ") ");
			System.out.println("a : " + attr_01);
			
			if(s.isNull(ATTR))
			{
				System.out.println("NULL");
				assertEquals("Check ATTR " + ATTR, dummy[dim_01][dim_02][dim_03], null);
			}
			else
			{
				assertEquals("Check ATTR " + ATTR, dummy[dim_01][dim_02][dim_03], s.getInt(ATTR));
			}
		} catch (Exception e)
		{
			System.out.print("(" + dim_01 + ", " + dim_02 + ", " + dim_03 + ") ");
			System.out.println("a : " + attr_01);
			System.out.println(String.format("\tException : %s", e.getMessage()));
		}
	}
}
