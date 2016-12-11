package suadb.query;

import suadb.parse.Constant;
import suadb.record.CID;

import java.util.List;

/**
 * The interface will be implemented by each suadb.query scan.
 * There is a Scan class for each relational
 * algebra operator.
 * @author Edward Sciore
 */
public interface Scan {

	/**
	 * Positions the scan before its first suadb.record.
	 */
	void	  beforeFirst();

	/**
	 * Moves the scan to the next suadb.record.
	 * @return false if there is no next suadb.record
	 */
	boolean  next();

	/**
	 * Closes the scan and its subscans, if any.
	 */
	void	  close();

	/**
	 * Returns the value of the specified field in the current suadb.record.
	 * The value is expressed as a Constant.
	 * @param fldname the name of the field
	 * @return the value of that field, expressed as a Constant.
	 */
	Constant getVal(String fldname);

	/**
	 * Returns the value of the specified integer field
	 * in the current suadb.record.
	 * @param fldname the name of the field
	 * @return the field's integer value in the current suadb.record
	 */
	int		getInt(String fldname);


	/**
	 * Returns the value of the specified string field
	 * in the current suadb.record.
	 * @param fldname the name of the field
	 * @return the field's string value in the current suadb.record
	 */
	String	getString(String fldname);
	
	boolean isNull(String attrName);

	/**
	 * Returns true if the scan has the specified field.
	 * @param fldname the name of the field
	 * @return true if the scan has that field
	 */
	boolean  hasField(String fldname);

	/**
	 * Returns true if the scan has the specified dimension.
	 * @param dimName the name of the field
	 * @return true if the scan has that field
	 */
	boolean hasDimension(String dimName);
	
	/**
	 * Returns the value of the dimensions
	 * in the current suadb.record.
	 * @return the dimensions' value in the current suadb.record
	 */
	Constant getDimensionVal(String dimName);

	int      getDimension(String dimName);
	
	CID getCurrentDimension();

	/**
	 * Positions the scan so that the current suadb.record has
	 * the specified CID.
	 * @param cid the CID of the desired suadb.record
	 */
	void moveToCid(CID cid);
}
