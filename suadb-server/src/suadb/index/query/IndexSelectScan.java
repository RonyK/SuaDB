package suadb.index.query;

import suadb.parse.Constant;
import suadb.query.sql.TableScan;
import suadb.record.CID;
import suadb.record.RID;
import suadb.query.*;
import suadb.index.Index;

import java.util.List;

/**
 * The scan class corresponding to the select relational
 * algebra operator.
 * @author Edward Sciore
 */
public class IndexSelectScan implements Scan {
	private Index idx;
	private Constant val;
	private TableScan ts;

	/**
	 * Creates an suadb.index select scan for the specified
	 * suadb.index and selection constant.
	 * @param idx the suadb.index
	 * @param val the selection constant
	 */
	public IndexSelectScan(Index idx, Constant val, TableScan ts) {
		this.idx = idx;
		this.val = val;
		this.ts  = ts;
		beforeFirst();
	}

	/**
	 * Positions the scan before the first suadb.record,
	 * which in this case means positioning the suadb.index
	 * before the first instance of the selection constant.
	 * @see suadb.query.Scan#beforeFirst()
	 */
	public void beforeFirst() {
		idx.beforeFirst(val);
	}

	/**
	 * Moves to the next suadb.record, which in this case means
	 * moving the suadb.index to the next suadb.record satisfying the
	 * selection constant, and returning false if there are
	 * no more such suadb.index records.
	 * If there is a next suadb.record, the method moves the
	 * tablescan to the corresponding data suadb.record.
	 * @see suadb.query.Scan#next()
	 */
	public boolean next() {
		boolean ok = idx.next();
		if (ok) {
			RID rid = idx.getDataRid();
			ts.moveToRid(rid);
		}
		return ok;
	}

	/**
	 * Closes the scan by closing the suadb.index and the tablescan.
	 * @see suadb.query.Scan#close()
	 */
	public void close() {
		idx.close();
		ts.close();
	}

	/**
	 * Returns the value of the field of the current data suadb.record.
	 * @see suadb.query.Scan#getVal(java.lang.String)
	 */
	public Constant getVal(String fldname) {
		return ts.getVal(fldname);
	}

	/**
	 * Returns the value of the field of the current data suadb.record.
	 * @see suadb.query.Scan#getInt(java.lang.String)
	 */
	public int getInt(String fldname) {
		return ts.getInt(fldname);
	}

	/**
	 * Returns the value of the field of the current data suadb.record.
	 * @see suadb.query.Scan#getString(java.lang.String)
	 */
	public String getString(String fldname) {
		return ts.getString(fldname);
	}

	public List<Integer> getCurrentDimension() { return ts.getCurrentDimension(); }

	/**
	 * Returns whether the data suadb.record has the specified field.
	 * @see suadb.query.Scan#hasField(java.lang.String)
	 */
	public boolean hasField(String fldname) {
		return ts.hasField(fldname);
	}
	
	@Override
	public Constant getDimensionVal(String dimName)
	{
		return ts.getDimensionVal(dimName);
	}
	
	@Override
	public int getDimension(String dimName)
	{
		return ts.getDimension(dimName);
	}
	
	public boolean hasDimension(String dimname) { return ts.hasDimension(dimname); }

	public void moveToCid(CID cid) { ts.moveToCid(cid); }
}
