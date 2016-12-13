package suadb.remote;

import java.rmi.RemoteException;
import java.sql.*;
import java.util.List;

import suadb.record.CID;

/**
 * front-end
 * An adapter class that wraps RemoteResultSet.
 * Its methods do nothing except transform RemoteExceptions
 * into SQLExceptions.
 * @author Edward Sciore
 */
public class SimpleResultSet extends ResultSetAdapter {
	private RemoteResultSet rrs;

	public SimpleResultSet(RemoteResultSet s) {
		rrs = s;
	}

	public CID getCurrentDimension() throws SQLException{
		try {
			return rrs.getCurrentDimension();
		}catch(Exception e){
			throw new SQLException(e);
		}

	}

	public boolean next() throws SQLException {
		try {
			return rrs.next();
		}
		catch (Exception e) {
			throw new SQLException(e);
		}
	}

	public NullInfo whichIsNull() throws SQLException{
		try{
			return rrs.whichIsNull();
		}catch(Exception e){
			throw new SQLException(e);
		}

	}

	public int getInt(String fldname) throws SQLException {
		try {
			return rrs.getInt(fldname);
		}
		catch (Exception e) {
			throw new SQLException(e);
		}
	}

	public String getString(String fldname) throws SQLException {
		try {
			return rrs.getString(fldname);
		}
		catch (Exception e) {
			throw new SQLException(e);
		}
	}

	public ResultSetMetaData getMetaData() throws SQLException {
		try {
			RemoteMetaData rmd = rrs.getMetaData();
			return new SimpleMetaData(rmd);
		}
		catch (Exception e) {
			throw new SQLException(e);
		}
	}

	public void close() throws SQLException {
		try {
			rrs.close();
		}
		catch (Exception e) {
			throw new SQLException(e);
		}
	}
}

