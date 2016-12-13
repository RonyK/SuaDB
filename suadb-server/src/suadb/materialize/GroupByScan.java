package suadb.materialize;

import suadb.parse.Constant;
import suadb.query.*;
import suadb.record.CID;

import java.util.*;

/**
 * The Scan class for the <i>groupby</i> operator.
 * @author Edward Sciore
 */
public class GroupByScan implements Scan {
	private Scan s;
	private Collection<String> groupfields;
	private Collection<AggregationFn> aggfns;
	private GroupValue groupval;
	private boolean moregroups;

	/**
	 * Creates a groupby scan, given a grouped table scan.
	 * @param s the grouped scan
	 * @param groupfields the group fields
	 * @param aggfns the aggregation functions
	 */
	public GroupByScan(Scan s, Collection<String> groupfields, Collection<AggregationFn> aggfns) {
		this.s = s;
		this.groupfields = groupfields;
		this.aggfns = aggfns;
		beforeFirst();
	}

	/**
	 * Positions the scan before the first group.
	 * Internally, the underlying scan is always
	 * positioned at the first suadb.record of a group, which
	 * means that this method moves to the
	 * first underlying suadb.record.
	 * @see suadb.query.Scan#beforeFirst()
	 */
	public void beforeFirst() {
		s.beforeFirst();
		moregroups = s.next();
	}

	/**
	 * Moves to the next group.
	 * The key of the group is determined by the
	 * group values at the current suadb.record.
	 * The method repeatedly reads underlying records until
	 * it encounters a suadb.record having a different key.
	 * The aggregation functions are called for each suadb.record
	 * in the group.
	 * The values of the grouping fields for the group are saved.
	 * @see suadb.query.Scan#next()
	 */
	public boolean next() {
		if (!moregroups)
			return false;
		for (AggregationFn fn : aggfns)
			fn.processFirst(s);
		groupval = new GroupValue(s, groupfields);
		while(moregroups = s.next()) {
			GroupValue gv = new GroupValue(s, groupfields);
			if (!groupval.equals(gv))
				break;
			for (AggregationFn fn : aggfns)
				fn.processNext(s);
		}
		return true;
	}

	/**
	 * Closes the scan by closing the underlying scan.
	 * @see suadb.query.Scan#close()
	 */
	public void close() {
		s.close();
	}

	/**
	 * Gets the Constant value of the specified field.
	 * If the field is a group field, then its value can
	 * be obtained from the saved group value.
	 * Otherwise, the value is obtained from the
	 * appropriate aggregation function.
	 * @see suadb.query.Scan#getVal(java.lang.String)
	 */
	public Constant getVal(String fldname) {
		if (groupfields.contains(fldname))
			return groupval.getVal(fldname);
		for (AggregationFn fn : aggfns)
			if (fn.fieldName().equals(fldname))
			return fn.value();
		throw new RuntimeException("field " + fldname + " not found.");
	}

	/**
	 * Gets the integer value of the specified field.
	 * If the field is a group field, then its value can
	 * be obtained from the saved group value.
	 * Otherwise, the value is obtained from the
	 * appropriate aggregation function.
	 * @see suadb.query.Scan#getVal(java.lang.String)
	 */
	public int getInt(String fldname) {
		return (Integer)getVal(fldname).asJavaVal();
	}

	/**
	 * Gets the string value of the specified field.
	 * If the field is a group field, then its value can
	 * be obtained from the saved group value.
	 * Otherwise, the value is obtained from the
	 * appropriate aggregation function.
	 * @see suadb.query.Scan#getVal(java.lang.String)
	 */
	public String getString(String fldname) {
		return (String)getVal(fldname).asJavaVal();
	}
	
	@Override
	public boolean isNull(String attrName)
	{
		return s.isNull(attrName);
	}
	
	public CID getCurrentDimension() { return s.getCurrentDimension(); }
	/* Returns true if the specified field is either a
	 * grouping field or created by an aggregation function.
	 * @see suadb.suadb.query.Scan#hasField(java.lang.String)
	 */
	public boolean hasField(String fldname) {
		if (groupfields.contains(fldname))
			return true;
		for (AggregationFn fn : aggfns)
			if (fn.fieldName().equals(fldname))
			return true;
		return false;
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
	
	public boolean hasDimension(String dimName) { return s.hasDimension(dimName); }

	public void moveToCid(CID cid) { s.moveToCid(cid); }
}

