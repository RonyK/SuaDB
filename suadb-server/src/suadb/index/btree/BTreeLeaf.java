package suadb.index.btree;

import suadb.file.Chunk;
import suadb.tx.Transaction;
import suadb.record.*;
import suadb.query.Constant;

/**
 * An object that holds the contents of a B-tree leaf chunk.
 * @author Edward Sciore
 */
public class BTreeLeaf {
	private TableInfo ti;
	private Transaction tx;
	private Constant searchkey;
	private BTreePage contents;
	private int currentslot;

	/**
	 * Opens a page to hold the specified leaf chunk.
	 * The page is positioned immediately before the first suadb.record
	 * having the specified search key (if any).
	 * @param blk a reference to the disk chunk
	 * @param ti the suadb.metadata of the B-tree leaf suadb.file
	 * @param searchkey the search key value
	 * @param tx the calling transaction
	 */
	public BTreeLeaf(Chunk blk, TableInfo ti, Constant searchkey, Transaction tx) {
		this.ti = ti;
		this.tx = tx;
		this.searchkey = searchkey;
		contents = new BTreePage(blk, ti, tx);
		currentslot = contents.findSlotBefore(searchkey);
	}

	/**
	 * Closes the leaf page.
	 */
	public void close() {
		contents.close();
	}

	/**
	 * Moves to the next leaf suadb.record having the
	 * previously-specified search key.
	 * Returns false if there is no more such records.
	 * @return false if there are no more leaf records for the search key
	 */
	public boolean next() {
		currentslot++;
		if (currentslot >= contents.getNumRecs())
			return tryOverflow();
		else if (contents.getDataVal(currentslot).equals(searchkey))
			return true;
		else
			return tryOverflow();
	}

	/**
	 * Returns the dataRID value of the current leaf suadb.record.
	 * @return the dataRID of the current suadb.record
	 */
	public RID getDataRid() {
		return contents.getDataRid(currentslot);
	}

	/**
	 * Deletes the leaf suadb.record having the specified dataRID
	 * @param datarid the dataRId whose suadb.record is to be deleted
	 */
	public void delete(RID datarid) {
		while(next())
			if(getDataRid().equals(datarid)) {
			contents.delete(currentslot);
			return;
		}
	}

	/**
	 * Inserts a new leaf suadb.record having the specified dataRID
	 * and the previously-specified search key.
	 * If the suadb.record does not fit in the page, then
	 * the page splits and the method returns the
	 * directory entry for the new page;
	 * otherwise, the method returns null.
	 * If all of the records in the page have the same dataval,
	 * then the chunk does not split; instead, all but one of the
	 * records are placed into an overflow chunk.
	 * @param datarid the dataRID value of the new suadb.record
	 * @return the directory entry of the newly-split page, if one exists.
	 */
	public DirEntry insert(RID datarid) {
		// bug fix:  If the page has an overflow page
		// and the searchkey of the new suadb.record would be lowest in its page,
		// we need to first move the entire contents of that page to a new chunk
		// and then insert the new suadb.record in the now-empty current page.
		if (contents.getFlag() >= 0 && contents.getDataVal(0).compareTo(searchkey) > 0) {
			Constant firstval = contents.getDataVal(0);
			Chunk newblk = contents.split(0, contents.getFlag());
			currentslot = 0;
			contents.setFlag(-1);
			contents.insertLeaf(currentslot, searchkey, datarid);
			return new DirEntry(firstval, newblk.number());
		}
	  
		currentslot++;
		contents.insertLeaf(currentslot, searchkey, datarid);
		if (!contents.isFull())
			return null;
		// else page is full, so split it
		Constant firstkey = contents.getDataVal(0);
		Constant lastkey  = contents.getDataVal(contents.getNumRecs()-1);
		if (lastkey.equals(firstkey)) {
			// create an overflow chunk to hold all but the first suadb.record
			Chunk newblk = contents.split(1, contents.getFlag());
			contents.setFlag(newblk.number());
			return null;
		}
		else {
			int splitpos = contents.getNumRecs() / 2;
			Constant splitkey = contents.getDataVal(splitpos);
			if (splitkey.equals(firstkey)) {
				// move right, looking for the next key
				while (contents.getDataVal(splitpos).equals(splitkey))
					splitpos++;
				splitkey = contents.getDataVal(splitpos);
			}
			else {
				// move left, looking for first entry having that key
				while (contents.getDataVal(splitpos-1).equals(splitkey))
					splitpos--;
			}
			Chunk newblk = contents.split(splitpos, -1);
			return new DirEntry(splitkey, newblk.number());
		}
	}

	private boolean tryOverflow() {
		Constant firstkey = contents.getDataVal(0);
		int flag = contents.getFlag();
		if (!searchkey.equals(firstkey) || flag < 0)
			return false;
		contents.close();
		Chunk nextblk = new Chunk(ti.fileName(), flag);
		contents = new BTreePage(nextblk, ti, tx);
		currentslot = 0;
		return true;
	}
}
