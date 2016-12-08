package suadb.index.query;

import suadb.parse.Constant;
import suadb.query.*;
import suadb.index.Index;
import suadb.query.sql.TableScan;
import suadb.record.CID;

import java.util.List;

/**
 * The scan class corresponding to the indexjoin relational
 * algebra operator.
 * The code is very similar to that of ProductScan, 
 * which makes sense because an suadb.index join is essentially
 * the product of each LHS suadb.record with the matching RHS suadb.index records.
 * @author Edward Sciore
 */
public class IndexJoinScan implements Scan {
	private Scan s;
	private TableScan ts;  // the data table
	private Index idx;
	private String joinfield;

	/**
	 * Creates an suadb.index join scan for the specified LHS scan and
	 * RHS suadb.index.
	 * @param s the LHS scan
	 * @param idx the RHS suadb.index
	 * @param joinfield the LHS field used for joining
	 */
	public IndexJoinScan(Scan s, Index idx, String joinfield, TableScan ts) {
		this.s = s;
		this.idx  = idx;
		this.joinfield = joinfield;
		this.ts = ts;
		beforeFirst();
	}

	/**
	 * Positions the scan before the first suadb.record.
	 * That is, the LHS scan will be positioned at its
	 * first suadb.record, and the suadb.index will be positioned
	 * before the first suadb.record for the join value.
	 * @see suadb.query.Scan#beforeFirst()
	 */
	public void beforeFirst() {
		s.beforeFirst();
		s.next();
		resetIndex();
	}

	/**
	 * Moves the scan to the next suadb.record.
	 * The method moves to the next suadb.index suadb.record, if possible.
	 * Otherwise, it moves to the next LHS suadb.record and the
	 * first suadb.index suadb.record.
	 * If there are no more LHS records, the method returns false.
	 * @see suadb.query.Scan#next()
	 */
	public boolean next() {
		while (true) {
			if (idx.next()) {
				ts.moveToRid(idx.getDataRid());
				return true;
			}
			if (!s.next())
				return false;
			resetIndex();
		}
	}

	/**
	 * Closes the scan by closing its LHS scan and its RHS suadb.index.
	 * @see suadb.query.Scan#close()
	 */
	public void close() {
		s.close();
		idx.close();
		ts.close();
	}

	/**
	 * Returns the Constant value of the specified field.
	 * @see suadb.query.Scan#getVal(java.lang.String)
	 */
	public Constant getVal(String fldname) {
		if (ts.hasField(fldname))
			return ts.getVal(fldname);
		else
			return s.getVal(fldname);
	}

	/**
	 * Returns the integer value of the specified field.
	 * @see suadb.query.Scan#getVal(java.lang.String)
	 */
	public int getInt(String fldname) {
		if (ts.hasField(fldname))
			return ts.getInt(fldname);
		else
			return s.getInt(fldname);
	}

	/**
	 * Returns the string value of the specified field.
	 * @see suadb.query.Scan#getVal(java.lang.String)
	 */
	public String getString(String fldname) {
		if (ts.hasField(fldname))
			return ts.getString(fldname);
		else
			return s.getString(fldname);
	}
	
	@Override
	public boolean isNull(String attrName)
	{
		return s.isNull(attrName);
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
	
	public List<Integer> getCurrentDimension() { return  ts.getCurrentDimension(); }

	/** Returns true if the field is in the schema.
	  * @see suadb.query.Scan#hasField(java.lang.String)
	  */
	public boolean hasField(String fldname) {
		return ts.hasField(fldname) || s.hasField(fldname);
	}

	public boolean hasDimension(String dimname) { return s.hasDimension(dimname); }

	private void resetIndex() {
		Constant searchkey = s.getVal(joinfield);
		idx.beforeFirst(searchkey);
	}

	public void moveToCid(CID cid) { s.moveToCid(cid); }
}
