package suadb.materialize;

import suadb.parse.Constant;
import suadb.query.*;

/**
 * The <i>max</i> aggregation function.
 * @author Edward Sciore
 */
public class MaxFn implements AggregationFn {
	private String fldname;
	private Constant val;

	/**
	 * Creates a max aggregation function for the specified field.
	 * @param fldname the name of the aggregated field
	 */
	public MaxFn(String fldname) {
		this.fldname = fldname;
	}

	/**
	 * Starts a new maximum to be the
	 * field value in the current suadb.record.
	 * @see suadb.materialize.AggregationFn#processFirst(suadb.query.Scan)
	 */
	public void processFirst(Scan s) {
		val = s.getVal(fldname);
	}

	/**
	 * Replaces the current maximum by the field value
	 * in the current suadb.record, if it is higher.
	 * @see suadb.materialize.AggregationFn#processNext(suadb.query.Scan)
	 */
	public void processNext(Scan s) {
		Constant newval = s.getVal(fldname);
		if (newval.compareTo(val) > 0)
			val = newval;
	}

	/**
	 * Returns the field's name, prepended by "maxof".
	 * @see suadb.materialize.AggregationFn#fieldName()
	 */
	public String fieldName() {
		return "maxof" + fldname;
	}

	/**
	 * Returns the current maximum.
	 * @see suadb.materialize.AggregationFn#value()
	 */
	public Constant value() {
		return val;
	}
}
