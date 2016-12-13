package suadb.materialize;

import static suadb.file.Page.BLOCK_SIZE;
import suadb.tx.Transaction;
import suadb.record.*;
import suadb.query.*;

/**
 * The Plan class for the <i>suadb.materialize</i> operator.
 * @author Edward Sciore
 */
public class MaterializePlan implements Plan {
	private Plan srcplan;
	private Transaction tx;

	/**
	 * Creates a suadb.materialize plan for the specified suadb.query.
	 * @param srcplan the plan of the underlying suadb.query
	 * @param tx the calling transaction
	 */
	public MaterializePlan(Plan srcplan, Transaction tx) {
		this.srcplan = srcplan;
		this.tx = tx;
	}

	/**
	 * This method loops through the underlying suadb.query,
	 * copying its output records into a temporary table.
	 * It then returns a table scan for that table.
	 * @see suadb.query.Plan#open()
	 */
	public Scan open() {
		Schema sch = srcplan.schema();
		TempTable temp = new TempTable(sch, tx);
		Scan src = srcplan.open();
		UpdateScan dest = temp.open();
		while (src.next()) {
			dest.insert();
			for (String fldname : sch.fields())
				dest.setVal(fldname, src.getVal(fldname));
		}
		src.close();
		dest.beforeFirst();
		return dest;
	}

	/**
	 * Returns the estimated number of blocks in the
	 * materialized table.
	 * It does <i>not</i> include the one-time cost
	 * of materializing the records.
	 * @see suadb.query.Plan#blocksAccessed()
	 */
	public int blocksAccessed() {
		// create a dummy TableInfo object to calculate suadb.record length
		TableInfo ti = new TableInfo("", srcplan.schema());
		double rpb = (double) (BLOCK_SIZE / ti.recordLength());
		return (int) Math.ceil(srcplan.recordsOutput() / rpb);
	}

	/**
	 * Returns the number of records in the materialized table,
	 * which is the same as in the underlying plan.
	 * @see suadb.query.Plan#recordsOutput()
	 */
	public int recordsOutput() {
		return srcplan.recordsOutput();
	}

	/**
	 * Returns the number of distinct field values,
	 * which is the same as in the underlying plan.
	 * @see suadb.query.Plan#distinctValues(java.lang.String)
	 */
	public int distinctValues(String fldname) {
		return srcplan.distinctValues(fldname);
	}

	/**
	 * Returns the schema of the materialized table,
	 * which is the same as in the underlying plan.
	 * @see suadb.query.Plan#schema()
	 */
	public Schema schema() {
		return srcplan.schema();
	}
}
