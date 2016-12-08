package suadb.record;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import suadb.server.SuaDB;
import suadb.test.SuaDBTestBase;
import suadb.tx.Transaction;

import java.io.File;

import static org.junit.Assert.*;

/**
 * Created by CDS on 2016-12-08.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class RecordFileTest extends SuaDBTestBase{

	@BeforeClass
	public final static void beforeTest(){
		SuaDB.init(dbName);
	}

	private static final String STRING_FIELD = "testStringField";
	private static final String INT_FIELD = "testIntField";
	@Test
	public void testInsert() throws Exception {
		Transaction transaction = new Transaction();

		TableInfo tableInfo;

		Schema schema = new Schema();
		schema.addStringField(STRING_FIELD,100);
		schema.addIntField(INT_FIELD);
		tableInfo = new TableInfo("testTable",schema);

		RecordFile recordFile = new RecordFile(tableInfo, transaction);
		for(int i=0;i<10;i++) {
			recordFile.insert();
			recordFile.setString(STRING_FIELD, "test"+i);
			recordFile.setInt(INT_FIELD, i);
		}

		recordFile.beforeFirst();
		int i=0;
		for(;i<10;i++){
			recordFile.next();
			assertTrue(recordFile.getString(STRING_FIELD).equals("test"+i));
			assertTrue(recordFile.getInt(INT_FIELD)==i);
		}

		recordFile.close();


		transaction.commit();
	}

	@AfterClass
	public static void tearDown(){
		try{
			SuaDB.fileMgr().flushAllFiles();
		} catch (Exception e){
			e.printStackTrace();
		}
	}
}