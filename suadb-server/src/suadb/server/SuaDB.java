package suadb.server;

import suadb.buffer.BufferMgr;
import suadb.file.FileMgr;
import suadb.log.LogMgr;
import suadb.metadata.MetadataMgr;
import suadb.planner.BasicQueryPlanner;
import suadb.planner.BasicUpdatePlanner;
import suadb.planner.Planner;
import suadb.planner.QueryPlanner;
import suadb.planner.UpdatePlanner;
import suadb.tx.Transaction;

/**
 * The class that provides system-wide static global values.
 * These values must be initialized by the method
 * {@link #init(String) init} before use.
 * The methods {@link #initFileMgr(String) initFileMgr},
 * {@link #initFileAndLogMgr(String) initFileAndLogMgr},
 * {@link #initFileLogAndBufferMgr(String) initFileLogAndBufferMgr},
 * and {@link #initMetadataMgr(boolean, Transaction) initMetadataMgr}
 * provide limited initialization, and are useful for 
 * debugging purposes.
 * 
 * @author Edward Sciore
 */
public class SuaDB
{
	public static int BUFFER_SIZE = 15;
	public static String LOG_FILE = "suadb.log";

	private static FileMgr	  fm;
	private static BufferMgr	bm;
	private static LogMgr		logm;
	private static MetadataMgr mdm;

	/**
	 * Initializes the system.
	 * This method is called during system startup.
	 * @param dirname the name of the database directory
	 */
	public static void init(String dirname) {
		initFileLogAndBufferMgr(dirname);
		Transaction tx = new Transaction();
		boolean isnew = fm.isNew();
		if (isnew)
			System.out.println("creating new database");
		else {
			System.out.println("recovering existing database");
			tx.recover();
		}
		initMetadataMgr(isnew, tx);
		tx.commit();
	}

	// The following initialization methods are useful for
	// testing the lower-level components of the system
	// without having to initialize everything.

	/**
	 * Initializes only the suadb.file manager.
	 * @param dirname the name of the database directory
	 */
	public static void initFileMgr(String dirname) {
		fm = new FileMgr(dirname);
	}

	/**
	 * Initializes the suadb.file and log managers.
	 * @param dirname the name of the database directory
	 */
	public static void initFileAndLogMgr(String dirname) {
		initFileMgr(dirname);
		logm = new LogMgr(LOG_FILE);
	}

	/**
	 * Initializes the suadb.file, log, and suadb.buffer managers.
	 * @param dirname the name of the database directory
	 */
	public static void initFileLogAndBufferMgr(String dirname) {
		initFileAndLogMgr(dirname);
		bm = new BufferMgr(BUFFER_SIZE);
	}

	/**
	 * Initializes suadb.metadata manager.
	 * @param isnew an indication of whether a new
	 * database needs to be created.
	 * @param tx the transaction performing the initialization
	 */
	public static void initMetadataMgr(boolean isnew, Transaction tx) {
		mdm = new MetadataMgr(isnew, tx);
	}

	public static FileMgr	  fileMgr()	{ return fm; }
	public static BufferMgr	bufferMgr() { return bm; }
	public static LogMgr		logMgr()	 { return logm; }
	public static MetadataMgr mdMgr()	  { return mdm; }

	/**
	 * Creates a suadb.planner for SQL commands.
	 * To change how the suadb.planner works, modify this method.
	 * @return the system's suadb.planner for SQL commands
	 */public static Planner planner() {
		QueryPlanner  qplanner = new BasicQueryPlanner();
		UpdatePlanner uplanner = new BasicUpdatePlanner();
		return new Planner(qplanner, uplanner);
	}
}
