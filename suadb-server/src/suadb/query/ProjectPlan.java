package suadb.query;

import suadb.record.Schema;
import java.util.Collection;

/** The Plan class corresponding to the <i>project</i>
  * relational algebra operator.
  * @author Edward Sciore
  */
public class ProjectPlan implements Plan {
	private Plan p;
	private Schema schema = new Schema();

	/**
	 * Creates a new project node in the suadb.query tree,
	 * having the specified subquery and field list.
	 * @param p the subquery
	 * @param fieldlist the list of fields
	 */
	public ProjectPlan(Plan p, Collection<String> fieldlist) {
		this.p = p;
		for (String fldname : fieldlist)
			schema.add(fldname, p.schema());
	}

	/**
	 * Creates a project scan for this suadb.query.
	 * @see suadb.query.Plan#open()
	 */
	public Scan open() {
		Scan s = p.open();
		return new ProjectScan(s, schema.fields());
	}

	/**
	 * Estimates the number of chunk accesses in the projection,
	 * which is the same as in the underlying suadb.query.
	 * @see suadb.query.Plan#blocksAccessed()
	 */
	public int blocksAccessed() {
		return p.blocksAccessed();
	}

	/**
	 * Estimates the number of output records in the projection,
	 * which is the same as in the underlying suadb.query.
	 * @see suadb.query.Plan#recordsOutput()
	 */
	public int recordsOutput() {
		return p.recordsOutput();
	}

	/**
	 * Estimates the number of distinct field values
	 * in the projection,
	 * which is the same as in the underlying suadb.query.
	 * @see suadb.query.Plan#distinctValues(java.lang.String)
	 */
	public int distinctValues(String fldname) {
		return p.distinctValues(fldname);
	}

	/**
	 * Returns the schema of the projection,
	 * which is taken from the field list.
	 * @see suadb.query.Plan#schema()
	 */
	public Schema schema() {
		return schema;
	}
}
