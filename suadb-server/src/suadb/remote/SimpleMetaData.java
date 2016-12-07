package suadb.remote;

import java.sql.*;

/**
 * client-side
 * An adapter class that wraps RemoteMetaData.
 * Its methods do nothing except transform RemoteExceptions
 * into SQLExceptions.
 * @author Edward Sciore
 */
public class SimpleMetaData extends ResultSetMetaDataAdapter {
	private RemoteMetaData rmd;

	public SimpleMetaData(RemoteMetaData md) {
		rmd = md;
	}

	public int getAttributeCount() throws SQLException {
		try {
			return rmd.getAttributeCount();
		}
		catch(Exception e) {
			throw new SQLException(e);
		}
	}
	public int getDimensionCount() throws SQLException {
		try {
			return rmd.getDimensionCount();
		}
		catch(Exception e) {
			throw new SQLException(e);
		}
	}

	public String getAttributeName(int column) throws SQLException {
		try {
			return rmd.getAttributeName(column);
		}
		catch (Exception e) {
			throw new SQLException(e);
		}
	}
	public String getDimensionName(int column) throws SQLException {
		try {
			return rmd.getDimensionName(column);
		}
		catch (Exception e) {
			throw new SQLException(e);
		}
	}

	public int getAttributeType(int column) throws SQLException {
		try {
			return rmd.getAttributeType(column);
		}
		catch (Exception e) {
			throw new SQLException(e);
		}
	}
}

