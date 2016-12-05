package suadb.multibuffer;

import static java.sql.Types.INTEGER;

import suadb.file.Block;
import suadb.file.Chunk;
import suadb.tx.Transaction;
import suadb.record.*;
import suadb.query.*;

import java.util.ArrayList;
import java.util.List;

/**
 * The class for the <i>chunk</i> operator.
 * @author Edward Sciore
 */
public class ChunkScan implements Scan {
	private List<RecordPage> pages;
	private int startbnum, endbnum, current;
	private Schema sch;
	private RecordPage rp;

	/**
	 * Creates a chunk consisting of the specified pages.
	 * @param ti the suadb.metadata for the chunked table
	 * @param startbnum the  starting chunk number
	 * @param endbnum  the ending chunk number
	 * @param tx the current transaction
	 */
	public ChunkScan(TableInfo ti, int startbnum, int endbnum, Transaction tx) {
		pages = new ArrayList<RecordPage>();
		this.startbnum = startbnum;
		this.endbnum	= endbnum;
		this.sch = ti.schema();
		String filename = ti.fileName();
		for (int i=startbnum; i<=endbnum; i++) {
			// TODO :: Pass over size of chunk using ArrayInfo or Schema when you make a new Chunk.
			Block blk = new Block(filename, i);
			pages.add(new RecordPage(blk, ti, tx));
		}
		beforeFirst();
	}


	/**
	 * @see suadb.query.Scan#beforeFirst()
	 */
	public void beforeFirst() {
		moveToBlock(startbnum);
	}

	/**
	 * Moves to the next suadb.record in the current chunk of the chunk.
	 * If there are no more records, then make
	 * the next chunk be current.
	 * If there are no more blocks in the chunk, return false.
	 * @see suadb.query.Scan#next()
	 */
	public boolean next() {
		while (true) {
			if (rp.next())
				return true;
			if (current == endbnum)
				return false;
			moveToBlock(current+1);
		}
	}

	/**
	 * @see suadb.query.Scan#close()
	 */
	public void close() {
		for (RecordPage r : pages)
			r.close();
	}

	/**
	 * @see suadb.query.Scan#getVal(java.lang.String)
	 */
	public Constant getVal(String fldname) {
		if (sch.type(fldname) == INTEGER)
			return new IntConstant(rp.getInt(fldname));
		else
			return new StringConstant(rp.getString(fldname));
	}

	/**
	 * @see suadb.query.Scan#getInt(java.lang.String)
	 */
	public int getInt(String fldname) {
		return rp.getInt(fldname);
	}

	/**
	 * @see suadb.query.Scan#getString(java.lang.String)
	 */
	public String getString(String fldname) {
		return rp.getString(fldname);
	}

	//TODO : Chunk Scan has only Record Page
	public List<Integer> getCurrentDimension() { List<Integer> array = new ArrayList<Integer>(rp.currentId()); return array; }

	/**
	 * @see suadb.query.Scan#hasField(java.lang.String)
	 */
	public boolean hasField(String fldname) {
		return sch.hasField(fldname);
	}

/**
 * @see suadb.query.Scan#hasDimension(java.lang.String)
 */

	public boolean hasDimension(String dimname) { return sch.hasDimension(dimname); }

	//TODO
	public void moveToCid(CID cid) {}

	private void moveToBlock(int blknum) {
		current = blknum;
		rp = pages.get(current - startbnum);
		rp.moveToId(-1);
	}
}