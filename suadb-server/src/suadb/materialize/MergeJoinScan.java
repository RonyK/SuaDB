package suadb.materialize;

import suadb.parse.Constant;
import suadb.query.*;
import suadb.record.CID;

import java.util.List;

/**
 * The Scan class for the <i>mergejoin</i> operator.
 * @author Edward Sciore
 */
public class MergeJoinScan implements Scan {
	private Scan s1;
	private SortScan s2;
	private String fldname1, fldname2;
	private Constant joinval = null;

	/**
	 * Creates a mergejoin scan for the two underlying sorted scans.
	 * @param s1 the LHS sorted scan
	 * @param s2 the RHS sorted scan
	 * @param fldname1 the LHS join field
	 * @param fldname2 the RHS join field
	 */
	public MergeJoinScan(Scan s1, SortScan s2, String fldname1, String fldname2) {
		this.s1 = s1;
		this.s2 = s2;
		this.fldname1 = fldname1;
		this.fldname2 = fldname2;
		beforeFirst();
	}

	/**
	 * Positions the scan before the first suadb.record,
	 * by positioning each underlying scan before
	 * their first records.
	 * @see suadb.query.Scan#beforeFirst()
	 */
	public void beforeFirst() {
		s1.beforeFirst();
		s2.beforeFirst();
	}

	/**
	 * Closes the scan by closing the two underlying scans.
	 * @see suadb.query.Scan#close()
	 */
	public void close() {
		s1.close();
		s2.close();
	}

	/**
	 * Moves to the next suadb.record.  This is where the action is.
	 * <P>
	 * If the next RHS suadb.record has the same join value,
	 * then move to it.
	 * Otherwise, if the next LHS suadb.record has the same join value,
	 * then reposition the RHS scan back to the first suadb.record
	 * having that join value.
	 * Otherwise, repeatedly move the scan having the smallest
	 * value until a common join value is found.
	 * When one of the scans runs out of records, return false.
	 * @see suadb.query.Scan#next()
	 */
	public boolean next() {
		boolean hasmore2 = s2.next();
		if (hasmore2 && s2.getVal(fldname2).equals(joinval))
			return true;

		boolean hasmore1 = s1.next();
		if (hasmore1 && s1.getVal(fldname1).equals(joinval)) {
			s2.restorePosition();
			return true;
		}

		while (hasmore1 && hasmore2) {
			Constant v1 = s1.getVal(fldname1);
			Constant v2 = s2.getVal(fldname2);
			if (v1.compareTo(v2) < 0)
				hasmore1 = s1.next();
			else if (v1.compareTo(v2) > 0)
				hasmore2 = s2.next();
			else {
				s2.savePosition();
				joinval  = s2.getVal(fldname2);
				return true;
			}
		}
		return false;
	}

	/**
	 * Returns the value of the specified field.
	 * The value is obtained from whichever scan
	 * contains the field.
	 * @see suadb.query.Scan#getVal(java.lang.String)
	 */
	public Constant getVal(String fldname) {
		if (s1.hasField(fldname))
			return s1.getVal(fldname);
		else
			return s2.getVal(fldname);
	}

	/**
	 * Returns the integer value of the specified field.
	 * The value is obtained from whichever scan
	 * contains the field.
	 * @see suadb.query.Scan#getInt(java.lang.String)
	 */
	public int getInt(String fldname) {
		if (s1.hasField(fldname))
			return s1.getInt(fldname);
		else
			return s2.getInt(fldname);
	}

	/**
	 * Returns the string value of the specified field.
	 * The value is obtained from whichever scan
	 * contains the field.
	 * @see suadb.query.Scan#getString(java.lang.String)
	 */
	public String getString(String fldname) {
		if (s1.hasField(fldname))
			return s1.getString(fldname);
		else
			return s2.getString(fldname);
	}
	
	@Override
	public boolean isNull(String attrName)
	{
		if(s1.hasField(attrName))
		{
			return s1.isNull(attrName);
		}else
		{
			return s2.isNull(attrName);
		}
	}
	
	/**
	 * Returns true if the specified field is in
	 * either of the underlying scans.
	 * @see suadb.query.Scan#hasField(java.lang.String)
	 */
	public boolean hasField(String fldname) {
		return s1.hasField(fldname) || s2.hasField(fldname);
	}
	
	@Override
	public Constant getDimensionVal(String dimName)
	{
		if(s1.hasDimension(dimName))
		{
			return s1.getDimensionVal(dimName);
		}else
		{
			return s2.getDimensionVal(dimName);
		}
	}
	
	@Override
	public int getDimension(String dimName)
	{
		if(s1.hasDimension(dimName))
		{
			return s1.getDimension(dimName);
		}else
		{
			return s2.getDimension(dimName);
		}
	}
	
	public CID getCurrentDimension() { return  s1.getCurrentDimension(); }
	
	public void moveToCid(CID cid) { s1.moveToCid(cid); }
	
	public boolean hasDimension(String dimName) {return s1.hasDimension(dimName); }


}