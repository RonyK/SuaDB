package suadb.server;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import suadb.query.Plan;
import suadb.query.Scan;
import suadb.record.ArrayInfo;
import suadb.test.DummyData;
import suadb.test.SuaDBExeTestBase;
import suadb.tx.Transaction;

import java.io.FileWriter;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by ILHYUN on 2016-12-08.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SuaDB_LargeInput extends SuaDBExeTestBase {
    static final String ARRAY_NAME = "TARRAY3DABC";
    static String FILE_PATH = "out.txt";
    static final String ATTR_01 = "tiv";
    static final String ATTR_02 = "policyid";

    static final String DIM_01 = "lat";         // from 24.547514  to 30.98982  , mapped to integer 0~399
    static final String DIM_02 = "long";        // from -87.44729 to -80.033257  , mapped to integer 0~399
    static final String DIM_03 = "year";        // 2011 and 2012, mapped to integers 0 and 1, respectively


    @Test
    public void test_09_list_empty_check()
    {
        Transaction tx = new Transaction();
        Scan s = list(tx);

        assertFalse("Check Empty Result", s.next());
        tx.commit();
    }

    @Test
    public void test_10_create_large_input_array()
    {
        Transaction tx = new Transaction();
        String query = String.format(
                "CREATE ARRAY %s" +
                        "<" +
                        "   %s : int," +
                        "   %s : string(12)" +
                        ">" +
                        "[%s = 0:399,50, %s = 0:399,50, %s = 0:1,1]",
                ARRAY_NAME, ATTR_01, ATTR_02,
                DIM_01, DIM_02, DIM_03);

        SuaDB.planner().executeUpdate(query, tx);

        ArrayInfo ai = SuaDB.mdMgr().getArrayInfo(ARRAY_NAME, tx);

        assertEquals(ai.arrayName(), ARRAY_NAME);

        assertEquals(ai.schema().attributes().size(), 2);
        assertTrue(ai.schema().hasAttribute(ATTR_01));
        assertTrue(ai.schema().hasAttribute(ATTR_02));
        assertFalse(ai.schema().hasAttribute("d"));

        assertEquals(ai.schema().dimensions().size(), 3);
        assertTrue(ai.schema().hasDimension(DIM_01));
        assertTrue(ai.schema().hasDimension(DIM_02));
        assertTrue(ai.schema().hasDimension(DIM_03));
        assertFalse(ai.schema().hasDimension("t"));

        tx.commit();
    }


    @Test
    public void test_20_array_large_input() throws IOException
    {
        // be patient! this might take long time to finish
        Transaction tx = new Transaction();

        try
        {

            String query = "INPUT(" + ARRAY_NAME + ", \'" + FILE_PATH + "\')";
            SuaDB.planner().executeUpdate(query, tx);
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
    public void test_80_remove_large_input_array()
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

