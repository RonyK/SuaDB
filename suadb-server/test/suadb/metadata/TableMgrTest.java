package suadb.metadata;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import suadb.record.RecordFile;
import suadb.record.Schema;
import suadb.record.TableInfo;
import suadb.server.SuaDB;
import suadb.test.SuaDBTestBase;
import suadb.tx.Transaction;

import java.io.File;

import static org.junit.Assert.*;

/**
 * Created by dh_st_000 on 2016-12-07.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TableMgrTest {
	protected static String dbName = "testDB";
	protected static String arrayFileName = "testArray.arr";

	protected static String homeDir;
	protected static File dbDirectory;

	@BeforeClass
	public final static void SuaDBTestBase_Init() {
		homeDir = System.getProperty("user.home");
		dbDirectory = new File(homeDir, dbName);
		SuaDB.init(dbName);
	}

	@Test
	public void testCreateTable_GetTableInfo() throws Exception {
		Transaction tx = new Transaction();

		TableMgr tableMgr = new TableMgr(false, tx);//tblcat & fldcat created

		TableInfo tcatmd = tableMgr.getTableInfo("tblcat", tx);
		RecordFile tcatfile = new RecordFile(tcatmd, tx);
		while (tcatfile.next()) {
			String tblname = tcatfile.getString("tblname");//testArray1~3 expected
			System.out.println(tblname);
			TableInfo md = tableMgr.getTableInfo(tblname, tx);
		}

		tx.commit();


//
//		Schema schema = new Schema();
//		schema.addStringField("testStringField", 20);
//		schema.addIntField("testIntField");
//
//		tableMgr.createTable("testArray1", schema, tx);
//		tableMgr.createTable("testArray2", schema, tx);
//		tableMgr.createTable("testArray3", schema, tx);

//		TableInfo tcatmd = tableMgr.getTableInfo("tblcat", tx);
//		RecordFile tcatfile = new RecordFile(tcatmd, tx);
//		while(tcatfile.next()) {
//			String tblname = tcatfile.getString("tblname");//testArray1~3 expected
//			System.out.println(tblname);
//			TableInfo md = tableMgr.getTableInfo(tblname, tx);
//		}
	}

//	@AfterClass
//	public static void tearDown() {
//		try {
//			SuaDB.fileMgr().flushAllFiles();
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//	}
}