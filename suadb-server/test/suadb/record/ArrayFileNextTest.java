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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by ILHYUN on 2016-11-23.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ArrayFileNextTest extends SuaDBTestBase {

	private Schema schema;
	private ArrayInfo arrayinfo;
	private ArrayFile arrayfile;

	@BeforeClass
	public static void beforeClass() {
		SuaDB.init(dbName);

	}

	//IHSUh
	@Test
	public void test_00_arrayfile_next() {

		// this is an example of creating an 3-dimensional array with to attributes
		int start[] = {0, 0, 0};
		int end[] = {11, 11, 7};
		int chunksize[] = {4, 3, 2};


		schema = new Schema();

		for (int i = 0; i < 3; i++) {
			schema.addDimension("dim" + i, start[i], end[i], chunksize[i]);
		}

		schema.addAttribute("attA", INTEGER, 999999);   // length doesn't matter for INTEGER type
		schema.addAttribute("attB", VARCHAR, 7);
		schema.addAttribute("attC", INTEGER, 7);

		arrayinfo = new ArrayInfo("testArray", schema);

		// the below code shows how to write values to array cells

		Transaction tx = new Transaction();
		arrayfile = new ArrayFile(arrayinfo, tx);

		// to specify a cell, you should make a list of dimension values,
		// the order of dimension you put in the list is critical. you should follow the order of array definition
		List<Integer> dimensionvalue = new ArrayList<Integer>();
		dimensionvalue.add(0);
		dimensionvalue.add(0);
		dimensionvalue.add(0);
		CID cid = new CID(dimensionvalue, arrayinfo);

		arrayfile.beforeFirst();

		int index = 0;
		for (int i = start[0]; i <= end[0]; i++) {
			for (int j = start[1]; j <= end[1]; j++) {
				for (int k = start[2]; k <= end[2]; k++) {
					dimensionvalue.set(0, i);
					dimensionvalue.set(1, j);
					dimensionvalue.set(2, k);
					arrayfile.moveToCid(cid);
					arrayfile.setInt("attA", index);
					if (k % 2 == 0)//If k is multiples of two,
						arrayfile.setString("attB", Integer.toString(index));//Set attB values.

					arrayfile.setInt("attC", index);
					index++;
				}
			}
		}

		arrayfile.close();
		tx.commit();


		//ArrayFile.getCurrentDimensionValues() Test
		tx = new Transaction();
		arrayfile = new ArrayFile(arrayinfo, tx);

		arrayfile.beforeFirst();
		while(arrayfile.next())
			System.out.println(arrayfile.getCurrentDimensionValues());


		Schema schema = arrayinfo.schema();
		List<String> dimensions = new ArrayList<String>(schema.dimensions());
		int numOfDimensions = dimensions.size();
		int totalChunkNum = 1;
		for (int i = 0; i < dimensions.size(); i++)
			totalChunkNum *= schema.getNumOfChunk(dimensions.get(i));
		int cellsInChunk = 1;
		for(int i=0;i<dimensions.size();i++)
			cellsInChunk *= schema.chunkSize(dimensions.get(i));


		int[] result = new int[numOfDimensions];
		arrayfile.beforeFirst();
		for (int c = 0; c < totalChunkNum; c++) {
			//Calculate the left-bottom coordinate of the chunk.
			int temp;
			int chunkNum = c;
			for (int i = 0; i < numOfDimensions; i++) {
				temp = 1;
				for (int j = i + 1; j < numOfDimensions; j++)
					temp *= schema.getNumOfChunk(dimensions.get(j));

				result[i] = ((chunkNum / temp) * schema.chunkSize(dimensions.get(i)));
				chunkNum %= temp;
			}


			//One chunk size iteration.
			for (int i = result[0]; i < result[0]+schema.chunkSize(dimensions.get(0)); i++) {
				for (int j = result[1]; j < result[1]+schema.chunkSize(dimensions.get(1)); j++) {
					for (int k = result[2]; k < result[2]+schema.chunkSize(dimensions.get(2)); k++) {
						assertTrue(arrayfile.next());
						CID currentCID = arrayfile.getCurrentDimensionValues();
						List<Integer> currentDimension = currentCID.dimensionValues();

						assertTrue(currentDimension.get(0) == i);
						assertTrue(currentDimension.get(1) == j);
						assertTrue(currentDimension.get(2) == k);
					}
				}
			}

		}
		assertFalse(arrayfile.next());

		arrayfile.close();
		tx.commit();

		//ArrayFile.moveToCid -> next() Test
		tx = new Transaction();
		arrayfile = new ArrayFile(arrayinfo, tx);

		arrayfile.beforeFirst();

		dimensionvalue.set(0,0);
		dimensionvalue.set(1,2);
		dimensionvalue.set(2,1);
		arrayfile.moveToCid(cid);

		assertTrue(arrayfile.next());
		List<Integer> dimension = arrayfile.getCurrentDimensionValues().dimensionValues();
		assertTrue(dimension.get(0) == 1);
		assertTrue(dimension.get(1) == 0);
		assertTrue(dimension.get(2) == 0);
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
