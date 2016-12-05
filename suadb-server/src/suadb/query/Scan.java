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
	public void	  beforeFirst();

	/**
	 * Moves the scan to the next suadb.record.
	 * @return false if there is no next suadb.record
	 */
	public boolean  next();

	/**
	 * Closes the scan and its subscans, if any.
	 */
	public void	  close();

	/**
	 * Returns the value of the specified field in the current suadb.record.
	 * The value is expressed as a Constant.
	 * @param fldname the name of the field
	 * @return the value of that field, expressed as a Constant.
	 */
	public Constant getVal(String fldname);

	/**
	 * Returns the value of the specified integer field
	 * in the current suadb.record.
	 * @param fldname the name of the field
	 * @return the field's integer value in the current suadb.record
	 */
	public int		getInt(String fldname);


	/**
	 * Returns the value of the specified string field
	 * in the current suadb.record.
	 * @param fldname the name of the field
	 * @return the field's string value in the current suadb.record
	 */
	public String	getString(String fldname);

	/**
	 * Returns true if the scan has the specified field.
	 * @param fldname the name of the field
	 * @return true if the scan has that field
	 */
	public boolean  hasField(String fldname);

	/**
	 * Returns true if the scan has the specified dimension.
	 * @param dimname the name of the field
	 * @return true if the scan has that field
	 */

	public boolean hasDimension(String dimname);
	
	/**
	 * Returns the value of the dimensions
	 * in the current suadb.record.
	 * @return the dimensions' value in the current suadb.record
	 */
	public Constant getDimensionVal(String dimName);

	public int      getDimension(String dimName);
	
	public List<Integer> getCurrentDimension();

	/**
	 * Positions the scan so that the current suadb.record has
	 * the specified CID.
	 * @param cid the CID of the desired suadb.record
	 */
	public void moveToCid(CID cid);



}
