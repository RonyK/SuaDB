package suadb.index;

import suadb.record.RID;
import suadb.query.Constant;

/**
 * This interface contains methods to traverse an suadb.index.
 * @author Edward Sciore
 *
 */
public interface Index {

	/**
	 * Positions the suadb.index before the first suadb.record
	 * having the specified search key.
	 * @param searchkey the search key value.
	 */
	public void	 beforeFirst(Constant searchkey);

	/**
	 * Moves the suadb.index to the next suadb.record having the
	 * search key specified in the beforeFirst method.
	 * Returns false if there are no more such suadb.index records.
	 * @return false if no other suadb.index records have the search key.
	 */
	public boolean next();

	/**
	 * Returns the dataRID value stored in the current suadb.index suadb.record.
	 * @return the dataRID stored in the current suadb.index suadb.record.
	 */
	public RID	  getDataRid();

	/**
	 * Inserts an suadb.index suadb.record having the specified
	 * dataval and dataRID values.
	 * @param dataval the dataval in the new suadb.index suadb.record.
	 * @param datarid the dataRID in the new suadb.index suadb.record.
	 */
	public void	 insert(Constant dataval, RID datarid);

	/**
	 * Deletes the suadb.index suadb.record having the specified
	 * dataval and dataRID values.
	 * @param dataval the dataval of the deleted suadb.index suadb.record
	 * @param datarid the dataRID of the deleted suadb.index suadb.record
	 */
	public void	 delete(Constant dataval, RID datarid);

	/**
	 * Closes the suadb.index.
	 */
	public void	 close();
}
