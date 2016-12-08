package suadb.materialize;

import suadb.parse.Constant;
import suadb.record.CID;
import suadb.record.RID;
import suadb.query.*;
import java.util.*;

/**
 * The Scan class for the <i>sort</i> operator.
 * @author Edward Sciore
 */
/**
 * @author sciore
 *
 */
public class SortScan implements Scan {
	private UpdateScan s1, s2=null, currentscan=null;
	private RecordComparator comp;
	private boolean hasmore1, hasmore2=false;
	private List<RID> savedposition;

	/**
	 * Creates a sort scan, given a list of 1 or 2 runs.
	 * If there is only 1 run, then s2 will be null and
	 * hasmore2 will be false.
	 * @param runs the list of runs
	 * @param comp the suadb.record comparator
	 */
	public SortScan(List<TempTable> runs, RecordComparator comp) {
		this.comp = comp;
		s1 = (UpdateScan) runs.get(0).open();
		hasmore1 = s1.next();
		if (runs.size() > 1) {
			s2 = (UpdateScan) runs.get(1).open();
			hasmore2 = s2.next();
		}
	}

	/**
	 * Positions the scan before the first suadb.record in sorted order.
	 * Internally, it moves to the first suadb.record of each underlying scan.
	 * The variable currentscan is set to null, indicating that there is
	 * no current scan.
	 * @see suadb.query.Scan#beforeFirst()
	 */
	public void beforeFirst() {
		currentscan = null;
		s1.beforeFirst();
		hasmore1 = s1.next();
		if (s2 != null) {
			s2.beforeFirst();
			hasmore2 = s2.next();
		}
	}

	/**
	 * Moves to the next suadb.record in sorted order.
	 * First, the current scan is moved to the next suadb.record.
	 * Then the lowest suadb.record of the two scans is found, and that
	 * scan is chosen to be the new current scan.
	 * @see suadb.query.Scan#next()
	 */
	public boolean next() {
		if (currentscan != null) {
			if (currentscan == s1)
				hasmore1 = s1.next();
			else if (currentscan == s2)
				hasmore2 = s2.next();
		}

		if (!hasmore1 && !hasmore2)
			return false;
		else if (hasmore1 && hasmore2) {
			if (comp.compare(s1, s2) < 0)
				currentscan = s1;
			else
				currentscan = s2;
		}
		else if (hasmore1)
			currentscan = s1;
		else if (hasmore2)
			currentscan = s2;
		return true;
	}

	/**
	 * Closes the two underlying scans.
	 * @see suadb.query.Scan#close()
	 */
	public void close() {
		s1.close();
		if (s2 != null)
			s2.close();
	}

	/**
	 * Gets the Constant value of the specified field
	 * of the current scan.
	 * @see suadb.query.Scan#getVal(java.lang.String)
	 */
	public Constant getVal(String fldname) {
		return currentscan.getVal(fldname);
	}

	/**
	 * Gets the integer value of the specified field
	 * of the current scan.
	 * @see suadb.query.Scan#getInt(java.lang.String)
	 */
	public int getInt(String fldname) {
		return currentscan.getInt(fldname);
	}

	/**
	 * Gets the string value of the specified field
	 * of the current scan.
	 * @see suadb.query.Scan#getString(java.lang.String)
	 */
	public String getString(String fldname) {
		return currentscan.getString(fldname);
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
	
	public List<Integer> getCurrentDimension() { return  currentscan.getCurrentDimension(); }

	/**
	 * Returns true if the specified field is in the current scan.
	 * @see suadb.query.Scan#hasField(java.lang.String)
	 */
	public boolean hasField(String fldname) {
		return currentscan.hasField(fldname);
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

	public boolean hasDimension(String dimname) {
		return currentscan.hasDimension(dimname);
	}

	public void moveToCid(CID cid) { currentscan.moveToCid(cid); }
	/**
	 * Saves the position of the current suadb.record,
	 * so that it can be restored at a later time.
	 */
	public void savePosition() {
		// TODO - RonyK
//		RID rid1 = s1.getRid();
//		RID rid2 = (s2 == null) ? null : s2.getRid();
//		savedposition = Arrays.asList(rid1,rid2);
	}

	/**
	 * Moves the scan to its previously-saved position.
	 */
	public void restorePosition() {
		RID rid1 = savedposition.get(0);
		RID rid2 = savedposition.get(1);
		// TODO - RonyK
//		s1.moveToRid(rid1);
//		if (rid2 != null)
//			s2.moveToRid(rid2);
	}
}
