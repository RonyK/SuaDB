package suadb.record;

import exception.ArrayInputException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import suadb.server.SuaDB;
import suadb.test.DummyData;
import suadb.test.SuaDBExeTestBase;
import suadb.test.SuaDBTestBase;
import suadb.tx.Transaction;

import java.io.FileWriter;
import java.io.IOException;

import static java.sql.Types.INTEGER;
import static java.sql.Types.VARCHAR;

/**
 * Created by ILHYUN on 2016-11-23.
 *
 * This test doesn't have assert statement
 * Just for checking input() & print() array.
 *
 * homeDir/test.txt
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class InputTest extends SuaDBExeTestBase
{
	private static String FILE_PATH;
	
	private Schema schema;
	private ArrayInfo arrayinfo;
	private ArrayFile arrayfile;

	@BeforeClass
	public static void beforeClass() throws IOException
	{
		FILE_PATH = homeDir+"/test.txt";

		FileWriter fw = new FileWriter(FILE_PATH);
		fw.write(DummyData.getInputDummy_3A_3D(DummyData.getArrayDummy_3A_3D()));
		fw.close();
	}

	@Test
	public void test_00_input()
	{
		int start[] = {0, 0,0};
		int end[] = {1, 3,3};
		int chunksize[] = {2, 4,2};

		
		schema = new Schema();

		for (int i = 0; i < start.length; i++)
		{
			schema.addDimension("dim" + i, start[i], end[i], chunksize[i]);
		}

		schema.addAttribute("att0", INTEGER, 0);
		schema.addAttribute("att1", VARCHAR, 7);
		schema.addAttribute("att2", INTEGER, 0);

		arrayinfo = new ArrayInfo("testArray", schema);

		Transaction tx = new Transaction();
		arrayfile = new ArrayFile(arrayinfo, tx);

		try {
			arrayfile.input(homeDir+"/test.txt");
		} catch (ArrayInputException e) {
			e.printStackTrace();
		}

		arrayfile.printArray();

		arrayfile.close();
		tx.commit();
	}
	
	@AfterClass
	public static void tearDown()
	{
		eraseFile(FILE_PATH);
	}
}
