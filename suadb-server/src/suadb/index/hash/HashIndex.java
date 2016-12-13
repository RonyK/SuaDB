package suadb.index.hash;

import suadb.parse.Constant;
import suadb.query.sql.TableScan;
import suadb.tx.Transaction;
import suadb.record.*;
import suadb.index.Index;

/**
 * A static hash implementation of the Index interface.
 * A fixed number of buckets is allocated (currently, 100),
 * and each bucket is implemented as a suadb.file of suadb.index records.
 * @author Edward Sciore
 */
public class HashIndex implements Index {
	public static int NUM_BUCKETS = 100;
	private String idxname;
	private Schema sch;
	private Transaction tx;
	private Constant searchkey = null;
	private TableScan ts = null;

	/**
	 * Opens a hash suadb.index for the specified suadb.index.
	 * @param idxname the name of the suadb.index
	 * @param sch the schema of the suadb.index records
	 * @param tx the calling transaction
	 */
	public HashIndex(String idxname, Schema sch, Transaction tx) {
		this.idxname = idxname;
		this.sch = sch;
		this.tx = tx;
	}

	/**
	 * Positions the suadb.index before the first suadb.index suadb.record
	 * having the specified search key.
	 * The method hashes the search key to determine the bucket,
	 * and then opens a table scan on the suadb.file
	 * corresponding to the bucket.
	 * The table scan for the previous bucket (if any) is closed.
	 * @see suadb.index.Index#beforeFirst(Constant)
	 */
	public void beforeFirst(Constant searchkey) {
		close();
		this.searchkey = searchkey;
		int bucket = searchkey.hashCode() % NUM_BUCKETS;
		String tblname = idxname + bucket;
		TableInfo ti = new TableInfo(tblname, sch);
		ts = new TableScan(ti, tx);
	}

	/**
	 * Moves to the next suadb.record having the search key.
	 * The method loops through the table scan for the bucket,
	 * looking for a matching suadb.record, and returning false
	 * if there are no more such records.
	 * @see suadb.index.Index#next()
	 */
	public boolean next() {
		while (ts.next())
			if (ts.getVal("dataval").equals(searchkey))
				return true;
		return false;
	}

	/**
	 * Retrieves the dataRID from the current suadb.record
	 * in the table scan for the bucket.
	 * @see suadb.index.Index#getDataRid()
	 */
	public RID getDataRid() {
		int blknum = ts.getInt("chunk");
		int id = ts.getInt("id");
		return new RID(blknum, id);
	}

	/**
	 * Inserts a new suadb.record into the table scan for the bucket.
	 * @see suadb.index.Index#insert(Constant, suadb.record.RID)
	 */
	public void insert(Constant val, RID rid) {
		beforeFirst(val);
		ts.insert();
		ts.setInt("chunk", rid.blockNumber());
		ts.setInt("id", rid.id());
		ts.setVal("dataval", val);
	}

	/**
	 * Deletes the specified suadb.record from the table scan for
	 * the bucket.  The method starts at the beginning of the
	 * scan, and loops through the records until the
	 * specified suadb.record is found.
	 * @see suadb.index.Index#delete(Constant, suadb.record.RID)
	 */
	public void delete(Constant val, RID rid) {
		beforeFirst(val);
		while(next())
			if (getDataRid().equals(rid)) {
				ts.delete();
				return;
			}
	}

	/**
	 * Closes the suadb.index by closing the current table scan.
	 * @see suadb.index.Index#close()
	 */
	public void close() {
		if (ts != null)
			ts.close();
	}

	/**
	 * Returns the cost of searching an suadb.index suadb.file having the
	 * specified number of blocks.
	 * The method assumes that all buckets are about the
	 * same size, and so the cost is simply the size of
	 * the bucket.
	 * @param numblocks the number of blocks of suadb.index records
	 * @param rpb the number of records per chunk (not used here)
	 * @return the cost of traversing the suadb.index
	 */
	public static int searchCost(int numblocks, int rpb){
		return numblocks / HashIndex.NUM_BUCKETS;
	}
}
