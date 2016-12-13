package suadb.planner;

import suadb.parse.QueryData;
import suadb.tx.Transaction;
import suadb.query.Plan;

/**
 * The interface implemented by planners for 
 * the SQL select statement.
 * @author Edward Sciore
 *
 */
public interface QueryPlanner {

	/**
	 * Creates a plan for the parsed suadb.query.
	 * @param data the parsed representation of the suadb.query
	 * @param tx the calling transaction
	 * @return a plan for that suadb.query
	 */
	public Plan createPlan(QueryData data, Transaction tx);


}
