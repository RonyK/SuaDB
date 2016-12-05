package suadb.query;

import suadb.parse.Constant;

/**
 * The interface implemented by all updateable scans.
 * @author Edward Sciore
 */
public interface UpdateScan extends Scan {
	/**
	 * Modifies the field value of the current suadb.record.
	 * @param fldname the name of the field
	 * @param val the new value, expressed as a Constant
	 */
	public void setVal(String fldname, Constant val);

	/**
	 * Modifies the field value of the current suadb.record.
	 * @param fldname the name of the field
	 * @param val the new integer value
	 */
	public void setInt(String fldname, int val);

	/**
	 * Modifies the field value of the current suadb.record.
	 * @param fldname the name of the field
	 * @param val the new string value
	 */
	public void setString(String fldname, String val);

	/**
	 * Inserts a new suadb.record somewhere in the scan.
	 */
	public void insert();

	/**
	 * Deletes the current suadb.record from the scan.
	 */
	public void delete();

	/**
	 * Returns the RID of the current suadb.record.
	 * @return the RID of the current suadb.record
	 */
//	public RID  getRid();

	/**
	 * Positions the scan so that the current suadb.record has
	 * the specified RID.
	 * @param rid the RID of the desired suadb.record
	 */
//	public void moveToRid(RID rid);
}
