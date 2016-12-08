package suadb.remote;

import suadb.record.Schema;
import suadb.query.*;
import suadb.server.SuaDB;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.List;

import static java.sql.Types.INTEGER;
import static java.sql.Types.VARCHAR;

/**
 * The RMI server-side implementation of RemoteResultSet.
 * @author Edward Sciore
 */
@SuppressWarnings("serial")
class RemoteResultSetImpl extends UnicastRemoteObject implements RemoteResultSet {
	private Scan s;
	private Schema sch;
	private RemoteConnectionImpl rconn;

	/**
	 * Creates a RemoteResultSet object.
	 * The specified plan is opened, and the scan is saved.
	 * @param plan the suadb.query plan
	 * @param rconn TODO
	 * @throws RemoteException
	 */
	public RemoteResultSetImpl(Plan plan, RemoteConnectionImpl rconn) throws RemoteException {
		s = plan.open();
		sch = plan.schema();
		this.rconn = rconn;
	}

	public List<Integer> getCurrentDimension() throws RemoteException{
		try{
			return s.getCurrentDimension();
		}catch(RuntimeException e){
			rconn.rollback();
			throw e;
		}
	}
	/**
	 * Moves to the next suadb.record in the result set,
	 * by moving to the next suadb.record in the saved scan.
	 * @see suadb.remote.RemoteResultSet#next()
	 */
	public boolean next() throws RemoteException {
		try {
			return s.next();
		}
		catch(RuntimeException e) {
			rconn.rollback();
			throw e;
		}
	}

	/**
	 * Returns the integer value of the specified field,
	 * by returning the corresponding value on the saved scan.
	 * @see suadb.remote.RemoteResultSet#getInt(java.lang.String)
	 */
	public int getInt(String fldname) throws RemoteException {
		try {
			return s.getInt(fldname);
		}
		catch(RuntimeException e) {
			rconn.rollback();
			throw e;
		}
	}

	/**
	 * Returns the integer value of the specified field,
	 * by returning the corresponding value on the saved scan.
	 * @see suadb.remote.RemoteResultSet#getInt(java.lang.String)
	 */
	public String getString(String fldname) throws RemoteException {
		try {
			return s.getString(fldname);
		}
		catch(RuntimeException e) {
			rconn.rollback();
			throw e;
		}
	}

	/**
	 * Returns the result set's suadb.metadata,
	 * by passing its schema into the RemoteMetaData constructor.
	 * @see suadb.remote.RemoteResultSet#getMetaData()
	 */
	public RemoteMetaData getMetaData() throws RemoteException {
		return new RemoteMetaDataImpl(sch);
	}

	/**
	 * Closes the result set by closing its scan.
	 * @see suadb.remote.RemoteResultSet#close()
	 */
	public void close() throws RemoteException {
		s.close();
		rconn.commit();
	}
}

