package suadb.query;

import suadb.parse.Constant;
import suadb.record.CID;

import java.util.List;

/**
 * The scan class corresponding to the <i>product</i> relational
 * algebra operator.
 * @author Edward Sciore
 */
public class ProductScan implements Scan {
	private Scan s1, s2;

	/**
	 * Creates a product scan having the two underlying scans.
	 * @param s1 the LHS scan
	 * @param s2 the RHS scan
	 */
	public ProductScan(Scan s1, Scan s2) {
		this.s1 = s1;
		this.s2 = s2;
		s1.next();
	}

	/**
	 * Positions the scan before its first suadb.record.
	 * In other words, the LHS scan is positioned at
	 * its first suadb.record, and the RHS scan
	 * is positioned before its first suadb.record.
	 * @see suadb.query.Scan#beforeFirst()
	 */
	public void beforeFirst() {
		s1.beforeFirst();
		s1.next();
		s2.beforeFirst();
	}

	/**
	 * Moves the scan to the next suadb.record.
	 * The method moves to the next RHS suadb.record, if possible.
	 * Otherwise, it moves to the next LHS suadb.record and the
	 * first RHS suadb.record.
	 * If there are no more LHS records, the method returns false.
	 * @see suadb.query.Scan#next()
	 */
	public boolean next() {
		if (s2.next())
			return true;
		else {
			s2.beforeFirst();
			return s2.next() && s1.next();
		}
	}

	/**
	 * Closes both underlying scans.
	 * @see suadb.query.Scan#close()
	 */
	public void close() {
		s1.close();
		s2.close();
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

	//TODO : Binary operator scan has two current coordinates
	public List<Integer> getCurrentDimension() { return  s1.getCurrentDimension(); }

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

	public boolean hasDimension(String dimName) {
		return s1.hasDimension(dimName) || s2.hasDimension(dimName);
	}

	public void moveToCid(CID cid) { s1.moveToCid(cid); }


}
