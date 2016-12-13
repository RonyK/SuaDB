package suadb.query.afl;

import suadb.parse.Predicate;
import suadb.query.Plan;
import suadb.query.PredicateExecutor;
import suadb.query.Scan;
import suadb.record.Schema;

/** The Plan class corresponding to the <i>select</i>
  * relational algebra operator.
  * @author Edward Sciore
  */
public class SelectPlan implements Plan
{
	private Plan p;
	private PredicateExecutor pred;

	/**
	 * Creates a new select node in the suadb.query tree,
	 * having the specified subquery and predicate.
	 * @param p the subquery
	 * @param pred the predicate
	 */
	public SelectPlan(Plan p, Predicate pred) {
		this.p = p;
		this.pred = new PredicateExecutor(pred, schema());
	}

	/**
	 * Creates a select scan for this suadb.query.
	 * @see suadb.query.Plan#open()
	 */
	public Scan open() {
		Scan s = p.open();
		return new SelectScan(s, pred);
	}

	/**
	 * Estimates the number of chunk accesses in the selection,
	 * which is the same as in the underlying suadb.query.
	 * @see suadb.query.Plan#blocksAccessed()
	 */
	public int blocksAccessed() {
		return p.blocksAccessed();
	}

	/**
	 * Estimates the number of output records in the selection,
	 * which is determined by the
	 * reduction factor of the predicate.
	 * @see suadb.query.Plan#recordsOutput()
	 */
	public int recordsOutput() {
		return p.recordsOutput() / pred.reductionFactor(p);
	}

	/**
	 * Estimates the number of distinct field values
	 * in the projection.
	 * If the predicate contains a term equating the specified
	 * field to a constant, then this value will be 1.
	 * Otherwise, it will be the number of the distinct values
	 * in the underlying suadb.query
	 * (but not more than the size of the output table).
	 * @see suadb.query.Plan#distinctValues(java.lang.String)
	 */
	public int distinctValues(String fldname) {
		if (pred.equatesWithConstant(fldname) != null)
			return 1;
		else {
			String fldname2 = pred.equatesWithField(fldname);
			if (fldname2 != null)
				return Math.min(p.distinctValues(fldname),
									 p.distinctValues(fldname2));
			else
				return Math.min(p.distinctValues(fldname),
									 recordsOutput());
		}
	}

	/**
	 * Returns the schema of the selection,
	 * which is the same as in the underlying suadb.query.
	 * @see suadb.query.Plan#schema()
	 */
	public Schema schema() {
		return p.schema();
	}
}
