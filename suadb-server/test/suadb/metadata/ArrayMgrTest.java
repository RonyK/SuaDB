package suadb.metadata;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import suadb.record.ArrayInfo;
import suadb.record.Schema;
import suadb.server.SuaDB;
import suadb.test.SuaDBTestBase;
import suadb.tx.Transaction;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;
import static java.sql.Types.*;

/**
 * Created by CDS on 2016-12-06.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ArrayMgrTest extends SuaDBTestBase {

	int numOfDimension = 3;
	int numOfAttribute = 3;
	String arrayName = "metadataTestArray";

	int[] start={0,0,0};
	int[] end={9,11,13};
	int[] chunkSize={2,3,7};

	int[] attrType={INTEGER,VARCHAR,INTEGER};
	int[] attrLength={0,10,0};


	@BeforeClass
	public static void beforeClass() {
		SuaDB.init(dbName);
	}

	@Test
	public void testCreateArray() throws Exception {
		Transaction tx = new Transaction();
		Schema schema = new Schema();

		//Add Dimensions
		for(int i=0;i<numOfDimension;i++)
			schema.addDimension("dim"+i,start[i],end[i],chunkSize[i]);

		//Add Attributes
		for(int i=0;i<numOfAttribute;i++)
			schema.addAttribute("attr"+i,attrType[i],attrLength[i]);

		//Create the Array
		SuaDB.mdMgr().createArray(arrayName, schema, tx);
		tx.commit();
	}

	@Test
	public void testGetArrayInfo() throws Exception {
		Transaction tx = new Transaction();
		ArrayInfo arrayInfo = SuaDB.mdMgr().getArrayInfo(arrayName,tx);
		Schema schema = arrayInfo.schema();

		//Check dimension information
		List<String> dimensions = new ArrayList<String>(schema.dimensions());

		assertTrue(numOfDimension == dimensions.size());
		for(int i=0;i<numOfDimension;i++) {
			String dimension = dimensions.get(i);
			assertTrue(schema.hasDimension(dimension));
			assertTrue(dimension.equals("dim" + i));
			assertTrue(schema.start(dimension) == start[i]);
			assertTrue(schema.end(dimension) == end[i]);
			assertTrue(schema.chunkSize(dimension) == chunkSize[i]);
		}

		//Check attribute information
		List<String> attributes = new ArrayList<String>(schema.attributes());

		assertTrue(numOfAttribute == attributes.size());
		for(int i=0;i<numOfAttribute;i++){
			String attribute = attributes.get(i);
			assertTrue(schema.hasAttribute(attribute));
			assertTrue(attribute.equals("attr"+i));
			assertTrue(schema.type(attribute) == attrType[i]);
			assertTrue(schema.length(attribute) == attrLength[i]);
		}


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