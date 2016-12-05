package suadb.index.query;

import suadb.parse.Constant;
import suadb.tx.Transaction;
import suadb.record.Schema;
import suadb.metadata.IndexInfo;
import suadb.query.*;
import suadb.index.Index;

/** The Plan class corresponding to the <i>indexselect</i>
  * relational algebra operator.
  * @author Edward Sciore
  */
public class IndexSelectPlan implements Plan {
	private Plan p;
	private IndexInfo ii;
	private Constant val;

	/**
	 * Creates a new indexselect node in the suadb.query tree
	 * for the specified suadb.index and selection constant.
	 * @param p the input table
	 * @param ii information about the suadb.index
	 * @param val the selection constant
	 * @param tx the calling transaction
	 */
	public IndexSelectPlan(Plan p, IndexInfo ii, Constant val, Transaction tx) {
		this.p = p;
		this.ii = ii;
		this.val = val;
	}

	/**
	 * Creates a new indexselect scan for this suadb.query
	 * @see suadb.query.Plan#open()
	 */
	public Scan open() {
		// throws an exception if p is not a tableplan.
		TableScan ts = (TableScan) p.open();
		Index idx = ii.open();
		return new IndexSelectScan(idx, val, ts);
	}

	/**
	 * Estimates the number of chunk accesses to compute the
	 * suadb.index selection, which is the same as the
	 * suadb.index traversal cost plus the number of matching data records.
	 * @see suadb.query.Plan#blocksAccessed()
	 */
	public int blocksAccessed() {
		return ii.blocksAccessed() + recordsOutput();
	}

	/**
	 * Estimates the number of output records in the suadb.index selection,
	 * which is the same as the number of search key values
	 * for the suadb.index.
	 * @see suadb.query.Plan#recordsOutput()
	 */
	public int recordsOutput() {
		return ii.recordsOutput();
	}

	/**
	 * Returns the distinct values as defined by the suadb.index.
	 * @see suadb.query.Plan#distinctValues(java.lang.String)
	 */
	public int distinctValues(String fldname) {
		return ii.distinctValues(fldname);
	}

	/**
	 * Returns the schema of the data table.
	 * @see suadb.query.Plan#schema()
	 */
	public Schema schema() {
		return p.schema();
	}
}
