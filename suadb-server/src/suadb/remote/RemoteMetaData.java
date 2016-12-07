package suadb.remote;

import java.rmi.*;

/**
 * The RMI suadb.remote interface corresponding to ResultSetMetaData.
 * The methods are identical to those of ResultSetMetaData, 
 * except that they throw RemoteExceptions instead of SQLExceptions.
 * @author Edward Sciore
 */
public interface RemoteMetaData extends Remote {
	public int getDimensionCount() throws RemoteException;
	public int getAttributeCount() throws RemoteException;
	public String getDimensionName(int index) throws RemoteException;
	public String getAttributeName(int index) throws RemoteException;
	public int getAttributeType(int index) throws RemoteException;
}

