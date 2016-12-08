package suadb.server;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import suadb.query.Plan;
import suadb.query.Scan;
import suadb.record.ArrayInfo;
import suadb.test.SuaDBExeTestBase;
import suadb.tx.Transaction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by Rony on 2016-12-07.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SuaDB_Multi_Array extends SuaDBExeTestBase
{
	static final String ARRAY_NAMES[] = {"TD1","TD2","TD3","TD4","TD5","TD6","TD7","TD8","TD9","TD10"};

	static final String ATTR_01 = "a";
	static final String ATTR_02 = "b";
	static final String ATTR_03 = "c";

	static final String DIM_01 = "x";
	static final String DIM_02 = "y";
	static final String DIM_03 = "z";


	@Test
	public void test_10_create_array()
	{
		for (String arrayName : ARRAY_NAMES) {
			Transaction tx = new Transaction();
			String query = String.format(
					"CREATE ARRAY %s" +
							"<" +
							"   %s : int," +
							"   %s : int," +
							"   %s : int" +
							">" +
							"[%s = 0:1,2, %s = 0:3,4, %s = 0:1,2]",
					arrayName, ATTR_01, ATTR_02, ATTR_03,
					DIM_01, DIM_02, DIM_03);

			SuaDB.planner().executeUpdate(query, tx);

			ArrayInfo ai = SuaDB.mdMgr().getArrayInfo(arrayName, tx);

			assertEquals(ai.arrayName(), arrayName);

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
	}

	@Test
	public void test_80_remove_3D_array()
	{
		for (String arrayName : ARRAY_NAMES) {
			Transaction tx = new Transaction();
			String query = "REMOVE(" + arrayName + ")";
			SuaDB.planner().executeUpdate(query, tx);
			tx.commit();
		}
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
