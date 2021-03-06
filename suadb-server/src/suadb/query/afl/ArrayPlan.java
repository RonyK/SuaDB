package suadb.query.afl;

import suadb.metadata.StatInfo;
import suadb.query.Plan;
import suadb.query.Scan;
import suadb.record.ArrayInfo;
import suadb.record.Schema;
import suadb.server.SuaDB;
import suadb.tx.Transaction;

/**
 * Created by rony on 16. 11. 18.
 */
public class ArrayPlan implements Plan
{
	private Transaction tx;
	private ArrayInfo ti;
	private StatInfo si;
	
	public ArrayPlan(String arrayName, Transaction tx)
	{
		this.tx = tx;
		ti = SuaDB.mdMgr().getArrayInfo(arrayName, tx);
		// TODO :: Get StatInfo
//		si = SuaDB.mdMgr().getStatInfo(arrayName, ti, tx);
	}
	
	/**
	 * Creates a table scan for this suadb.query.
	 * @see suadb.query.Plan#open()
	 */
	public Scan open() {
		return new ArrayScan(ti, tx);
	}
	
	/**
	 * Estimates the number of block accesses for the table,
	 * which is obtainable from the statistics manager.
	 * @see suadb.query.Plan#blocksAccessed()
	 */
	public int blocksAccessed() {
		return si.blocksAccessed();
	}
	
	/**
	 * Estimates the number of records in the table,
	 * which is obtainable from the statistics manager.
	 * @see suadb.query.Plan#recordsOutput()
	 */
	public int recordsOutput() {
		return si.recordsOutput();
	}
	
	/**
	 * Estimates the number of distinct field values in the table,
	 * which is obtainable from the statistics manager.
	 * @see suadb.query.Plan#distinctValues(java.lang.String)
	 */
	public int distinctValues(String fldname) {
		return si.distinctValues(fldname);
	}
	
	/**
	 * Determines the schema of the table,
	 * which is obtainable from the catalog manager.
	 * @see suadb.query.Plan#schema()
	 */
	public Schema schema() {
		return ti.schema();
	}
}
