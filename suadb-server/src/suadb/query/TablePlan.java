package suadb.query;

import suadb.server.SuaDB;
import suadb.tx.Transaction;
import suadb.metadata.*;
import suadb.record.*;

/** The Plan class corresponding to a table.
  * @author Edward Sciore
  */
public class TablePlan implements Plan {
	private Transaction tx;
	private TableInfo ti;
	private StatInfo si;

	/**
	 * Creates a leaf node in the suadb.query tree corresponding
	 * to the specified table.
	 * @param tblname the name of the table
	 * @param tx the calling transaction
	 */
	public TablePlan(String tblname, Transaction tx) {
		this.tx = tx;
		ti = SuaDB.mdMgr().getTableInfo(tblname, tx);
		si = SuaDB.mdMgr().getStatInfo(tblname, ti, tx);
	}

	/**
	 * Creates a table scan for this suadb.query.
	 * @see suadb.query.Plan#open()
	 */
	public Scan open() {
		return new TableScan(ti, tx);
	}

	/**
	 * Estimates the number of chunk accesses for the table,
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
