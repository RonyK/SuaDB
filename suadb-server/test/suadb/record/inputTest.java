package suadb.record;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import suadb.server.SuaDB;
import suadb.test.SuaDBTestBase;
import suadb.tx.Transaction;

import java.util.ArrayList;
import java.util.List;

import static java.sql.Types.INTEGER;
import static java.sql.Types.VARCHAR;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by ILHYUN on 2016-11-23.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class inputTest extends SuaDBTestBase {

	private Schema schema;
	private ArrayInfo arrayinfo;
	private ArrayFile arrayfile;

	@BeforeClass
	public static void beforeClass() {
		SuaDB.init(dbName);

	}



	/**
	 * This test doesn't have assert statements
	 * Just for checking input() & print() array.
	 *
	 * homeDir/test.txt
	 [
	 [[(0,100,1),(1,99),(2,,3),(,97,4)],
	 [(4,,),(,95,),(6),()],
	 [(8,92),(9,91),(,,11),(11,89)],
	 [(12,88,13),(,13,14),(14,86,15),(15,85,16)]]
	 ]
	 */
	@Test
	public void test_00_input() {
		int start[] = {0, 0,0};
		int end[] = {1, 3,1};
		int chunksize[] = {2, 4,2};


		schema = new Schema();

		for (int i = 0; i < start.length; i++) {
			schema.addDimension("dim" + i, start[i], end[i], chunksize[i]);
		}

		schema.addAttribute("att0", INTEGER, 0);
		schema.addAttribute("att1", VARCHAR, 7);
		schema.addAttribute("att2", INTEGER, 0);

		arrayinfo = new ArrayInfo("testArray", schema);

		Transaction tx = new Transaction();
		arrayfile = new ArrayFile(arrayinfo, tx);

		arrayfile.input(homeDir+"/test.txt");

		arrayfile.printArray();

		arrayfile.close();
		tx.commit();
	}


	@AfterClass
	public static void tearDown() {
		try {
			SuaDB.fileMgr().flushAllFiles();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
