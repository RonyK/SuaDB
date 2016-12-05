package suadb.multibuffer;

import suadb.record.CID;
import suadb.tx.Transaction;
import suadb.record.TableInfo;
import suadb.query.*;

import java.util.List;

/** 
 * The Scan class for the muti-suadb.buffer version of the
 * <i>product</i> operator.
 * @author Edward Sciore
 */
public class MultiBufferProductScan implements Scan {
	private Scan lhsscan, rhsscan=null, prodscan;
	private TableInfo ti;
	private Transaction tx;
	private int chunksize, nextblknum, filesize;


	/**
	 * Creates the scan class for the product of the LHS scan and a table.
	 * @param lhsscan the LHS scan
	 * @param ti the suadb.metadata for the RHS table
	 * @param tx the current transaction
	 */
	public MultiBufferProductScan(Scan lhsscan, TableInfo ti, Transaction tx) {
		this.lhsscan = lhsscan;
		this.ti = ti;
		this.tx = tx;
		filesize = tx.size(ti.fileName());
		chunksize = BufferNeeds.bestFactor(filesize);
		beforeFirst();
	}

	/**
	 * Positions the scan before the first suadb.record.
	 * That is, the LHS scan is positioned at its first suadb.record,
	 * and the RHS scan is positioned before the first suadb.record of the first chunk.
	 * @see suadb.query.Scan#beforeFirst()
	 */
	public void beforeFirst() {
		nextblknum = 0;
		useNextChunk();
	}

	/**
	 * Moves to the next suadb.record in the current scan.
	 * If there are no more records in the current chunk,
	 * then move to the next LHS suadb.record and the beginning of that chunk.
	 * If there are no more LHS records, then move to the next chunk
	 * and begin again.
	 * @see suadb.query.Scan#next()
	 */
	public boolean next() {
		while (!prodscan.next())
			if (!useNextChunk())
			return false;
		return true;
	}

	/**
	 * Closes the current scans.
	 * @see suadb.query.Scan#close()
	 */
	public void close() {
		prodscan.close();
	}

	/**
	 * Returns the value of the specified field.
	 * The value is obtained from whichever scan
	 * contains the field.
	 * @see suadb.query.Scan#getVal(java.lang.String)
	 */
	public Constant getVal(String fldname) {
		return prodscan.getVal(fldname);
	}

	/**
	 * Returns the integer value of the specified field.
	 * The value is obtained from whichever scan
	 * contains the field.
	 * @see suadb.query.Scan#getInt(java.lang.String)
	 */
	public int getInt(String fldname) {
		return prodscan.getInt(fldname);
	}

	/**
	 * Returns the string value of the specified field.
	 * The value is obtained from whichever scan
	 * contains the field.
	 * @see suadb.query.Scan#getString(java.lang.String)
	 */
	public String getString(String fldname) {
		return prodscan.getString(fldname);
	}

	//TODO : Binary operator scan has two current coordinates
	public List<Integer> getCurrentDimension() { return  prodscan.getCurrentDimension(); }
	/**
	 * Returns true if the specified field is in
	 * either of the underlying scans.
	 * @see suadb.query.Scan#hasField(java.lang.String)
	 */
	public boolean hasField(String fldname) {
		return prodscan.hasField(fldname);
	}

	public boolean hasDimension(String dimname) {
		return prodscan.hasDimension(dimname);
	}

	private boolean useNextChunk() {
		if (rhsscan != null)
			rhsscan.close();
		if (nextblknum >= filesize)
			return false;
		int end = nextblknum + chunksize - 1;
		if (end >= filesize)
			end = filesize - 1;
		rhsscan = new ChunkScan(ti, nextblknum, end, tx);
		lhsscan.beforeFirst();
		prodscan = new ProductScan(lhsscan, rhsscan);
		nextblknum = end + 1;
		return true;
	}

	public void moveToCid(CID cid) { prodscan.moveToCid(cid); }
}

