package suadb.query.afl;

import suadb.parse.Constant;
import suadb.parse.Predicate;
import suadb.query.PredicateExecutor;
import suadb.query.Scan;
import suadb.query.UpdateScan;
import suadb.record.*;

import java.util.List;

/**
 * The scan class corresponding to the <i>select</i> relational
 * algebra operator.
 * All methods except next delegate their work to the
 * underlying scan.
 * @author Edward Sciore
 */
public class SelectScan implements UpdateScan
{
	private Scan s;
	private PredicateExecutor pred;

	/**
	 * Creates a select scan having the specified underlying
	 * scan and predicate.
	 * @param s the scan of the underlying suadb.query
	 * @param pred the selection predicate
	 */
	public SelectScan(Scan s, PredicateExecutor pred) {
		this.s = s;
		this.pred = pred;
	}

	// Scan methods

	public void beforeFirst() {
		s.beforeFirst();
	}

	/**
	 * Move to the next suadb.record satisfying the predicate.
	 * The method repeatedly calls next on the underlying scan
	 * until a suitable suadb.record is found, or the underlying scan
	 * contains no more records.
	 * @see suadb.query.Scan#next()
	 */
	public boolean next() {
		while (s.next())
			if (pred.isSatisfied(s))
			return true;
		return false;
	}

	public void close() {
		s.close();
	}

	public Constant getVal(String fldname) {
		return s.getVal(fldname);
	}

	public int getInt(String fldname) {
		return s.getInt(fldname);
	}

	public String getString(String fldname) {
		return s.getString(fldname);
	}

	public boolean hasField(String fldname) {
		return s.hasField(fldname);
	}
	
	@Override
	public Constant getDimensionVal(String dimName)
	{
		return s.getDimensionVal(dimName);
	}
	
	@Override
	public int getDimension(String dimName)
	{
		return s.getDimension(dimName);
	}

	public boolean hasDimension(String dimname) { return s.hasDimension(dimname); }

	public List<Integer> getCurrentDimension() { return s.getCurrentDimension(); }

	public void moveToCid(CID cid) { s.moveToCid(cid); }

	// UpdateScan methods

	public void setVal(String fldname, Constant val) {
		UpdateScan us = (UpdateScan) s;
		us.setVal(fldname, val);
	}

	public void setInt(String fldname, int val) {
		UpdateScan us = (UpdateScan) s;
		us.setInt(fldname, val);
	}

	public void setString(String fldname, String val) {
		UpdateScan us = (UpdateScan) s;
		us.setString(fldname, val);
	}

	public void delete() {
		UpdateScan us = (UpdateScan) s;
		us.delete();
	}

	public void insert() {
		UpdateScan us = (UpdateScan) s;
		us.insert();
	}

//	public RID getRid() {
//		UpdateScan us = (UpdateScan) s;
//		return us.getRid();
//	}
//
//	public void moveToRid(RID rid) {
//		UpdateScan us = (UpdateScan) s;
//		us.moveToRid(rid);
//	}
}
