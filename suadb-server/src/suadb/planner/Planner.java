package suadb.planner;

import suadb.tx.Transaction;
import suadb.parse.*;
import suadb.query.*;

/**
 * The object that executes SQL statements.
 * @author sciore
 */
public class Planner {
	private QueryPlanner qplanner;
	private UpdatePlanner uplanner;

	public Planner(QueryPlanner qplanner, UpdatePlanner uplanner) {
		this.qplanner = qplanner;
		this.uplanner = uplanner;
	}

	/**
	 * Creates a plan for an AQL filter statement, using the supplied suadb.planner.
	 * @param qry the AQL suadb.query string
	 * @param tx the transaction
	 * @return the scan corresponding to the suadb.query plan
	 */


	public Plan createQueryPlan(String qry, Transaction tx) {
		Parser parser = new Parser(qry);
		QueryData data = parser.query();
		return qplanner.createPlan(data,tx);
	}

	/**
	 * Executes an SQL insert, delete, modify, or
	 * create statement.
	 * The method dispatches to the appropriate method of the
	 * supplied update suadb.planner,
	 * depending on what the parser returns.
	 * @param cmd the SQL update string
	 * @param tx the transaction
	 * @return an integer denoting the number of affected records
	 */
	public int executeUpdate(String cmd, Transaction tx) {
		Parser parser = new Parser(cmd);
		Object obj = parser.updateCmd();
		if (obj instanceof InsertData)
			return uplanner.executeInsert((InsertData)obj, tx);
		else if (obj instanceof DeleteData)
			return uplanner.executeDelete((DeleteData)obj, tx);
		else if (obj instanceof ModifyData)
			return uplanner.executeModify((ModifyData)obj, tx);
		else if (obj instanceof InputArrayData)
			return uplanner.executeInputArray((InputArrayData)obj, tx);
		else if (obj instanceof CreateArrayData)
			return uplanner.executeCreateArray((CreateArrayData)obj, tx);
		else if (obj instanceof CreateViewData)
			return uplanner.executeCreateView((CreateViewData)obj, tx);
		else if (obj instanceof CreateIndexData)
			return uplanner.executeCreateIndex((CreateIndexData)obj, tx);
		else if (obj instanceof RemoveArrayData)
			return uplanner.executeRemoveArray((RemoveArrayData)obj, tx);
		else
			return 0;
	}
}
