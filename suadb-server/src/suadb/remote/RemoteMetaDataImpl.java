package suadb.remote;

import suadb.record.Schema;
import static java.sql.Types.INTEGER;
import static java.sql.Types.VARCHAR;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
import java.sql.Types.*;


/**
 * The RMI server-side implementation of RemoteMetaData.
 * @author Edward Sciore
 */
@SuppressWarnings("serial")
public class RemoteMetaDataImpl extends UnicastRemoteObject implements RemoteMetaData {
	private Schema sch;
	private List<String> dimensions = new ArrayList<String>();
	private List<String> attributes = new ArrayList<String>();

	/**
	 * Creates a suadb.metadata object that wraps the specified schema.
	 * The method also creates a list to hold the schema's
	 * collection of field names,
	 * so that the fields can be accessed by position.
	 * @param sch the schema
	 * @throws RemoteException
	 */
	public RemoteMetaDataImpl(Schema sch) throws RemoteException {
		this.sch = sch;
		dimensions.addAll(sch.dimensions());
		attributes.addAll(sch.attributes());
	}

	/**
	 * Returns the size of the dimension list.
	 */
	public int getDimensionCount() throws RemoteException{
		return dimensions.size();
	}
	/**
	 * Returns the size of the attribute list.
	 */
	public int getAttributeCount() throws RemoteException{
		return attributes.size();
	}

	/**
	 * Returns the dimension name for the specified index number.
	 * In JDBC, index numbers start with 1, so the field
	 * is taken from position (index-1) in the list.
	 */
	public String getDimensionName(int index) throws RemoteException{
		return dimensions.get(index-1);
	}
	/**
	 * Returns the attribute name for the specified index number.
	 * In JDBC, index numbers start with 1, so the field
	 * is taken from position (index-1) in the list.
	 */
	public String getAttributeName(int index) throws RemoteException{
		return attributes.get(index-1);
	}

	/**
	 * Returns the type of the specified index.
	 * The method first finds the name of the attribute in that index,
	 * and then looks up its type in the schema.
	 */
	public int getAttributeType(int index) throws RemoteException {
		String attribute = getAttributeName(index);
		return sch.type(attribute);
	}

}
