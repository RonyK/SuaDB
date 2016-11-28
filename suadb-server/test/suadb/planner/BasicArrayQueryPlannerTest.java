package suadb.planner;

import org.junit.BeforeClass;
import org.junit.Test;

import suadb.test.SuaDBTestBase;


/**
 * Created by rony on 16. 11. 17.
 */
public class BasicArrayQueryPlannerTest extends SuaDBTestBase
{
	static QueryPlanner qPlanner;
	
	@BeforeClass
	public static void setup()
	{
		qPlanner = new BasicArrayQueryPlanner();
	}
	
	@Test
	public void test_03_query_scan()
	{
		// TODO :: Test Scan query
	}
	
	@Test
	public void test_05_query_composition_01()
	{
		// TODO :: Test composition query
	}
}