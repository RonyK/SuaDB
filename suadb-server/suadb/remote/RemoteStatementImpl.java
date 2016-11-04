package suadb.remote;

import suadb.tx.Transaction;
import suadb.query.Plan;
import suadb.server.SuaDB;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

/**
 * The RMI server-side implementation of RemoteStatement.
 * @author Edward Sciore
 */
@SuppressWarnings("serial")
class RemoteStatementImpl extends UnicastRemoteObject implements RemoteStatement {
	private RemoteConnectionImpl rconn;

	public RemoteStatementImpl(RemoteConnectionImpl rconn) throws RemoteException {
		this.rconn = rconn;
	}

	/**
	 * Executes the specified SQL suadb.query string.
	 * The method calls the suadb.query suadb.planner to create a plan
	 * for the suadb.query. It then sends the plan to the
	 * RemoteResultSetImpl constructor for processing.
	 * @see suadb.remote.RemoteStatement#executeQuery(java.lang.String)
	 */
	public RemoteResultSet executeQuery(String qry) throws RemoteException {
		try {
			Transaction tx = rconn.getTransaction();
			Plan pln = SuaDB.planner().createQueryPlan(qry, tx);
			return new RemoteResultSetImpl(pln, rconn);
		}
		catch(RuntimeException e) {
			rconn.rollback();
			throw e;
		}
	}

	/**
	 * Executes the specified SQL update command.
	 * The method sends the command to the update suadb.planner,
	 * which executes it.
	 * @see suadb.remote.RemoteStatement#executeUpdate(java.lang.String)
	 */
	public int executeUpdate(String cmd) throws RemoteException {
		try {
			Transaction tx = rconn.getTransaction();
			int result = SuaDB.planner().executeUpdate(cmd, tx);
			rconn.commit();
			return result;
		}
		catch(RuntimeException e) {
			rconn.rollback();
			throw e;
		}
	}
}
