package suadb.index.btree;

import suadb.file.Chunk;
import suadb.tx.Transaction;
import suadb.record.TableInfo;
import suadb.query.Constant;

/**
 * A B-tree directory chunk.
 * @author Edward Sciore
 */
public class BTreeDir {
	private TableInfo ti;
	private Transaction tx;
	private String filename;
	private BTreePage contents;

	/**
	 * Creates an object to hold the contents of the specified
	 * B-tree chunk.
	 * @param blk a reference to the specified B-tree chunk
	 * @param ti the suadb.metadata of the B-tree directory suadb.file
	 * @param tx the calling transaction
	 */
	BTreeDir(Chunk blk, TableInfo ti, Transaction tx) {
		this.ti = ti;
		this.tx = tx;
		filename = blk.fileName();
		contents = new BTreePage(blk, ti, tx);
	}

	/**
	 * Closes the directory page.
	 */
	public void close() {
		contents.close();
	}

	/**
	 * Returns the chunk number of the B-tree leaf chunk
	 * that contains the specified search key.
	 * @param searchkey the search key value
	 * @return the chunk number of the leaf chunk containing that search key
	 */
	public int search(Constant searchkey) {
		Chunk childblk = findChildBlock(searchkey);
		while (contents.getFlag() > 0) {
			contents.close();
			contents = new BTreePage(childblk, ti, tx);
			childblk = findChildBlock(searchkey);
		}
		return childblk.number();
	}

	/**
	 * Creates a new root chunk for the B-tree.
	 * The new root will have two children:
	 * the old root, and the specified chunk.
	 * Since the root must always be in chunk 0 of the suadb.file,
	 * the contents of the old root will get transferred to a new chunk.
	 * @param e the directory entry to be added as a child of the new root
	 */
	public void makeNewRoot(DirEntry e) {
		Constant firstval = contents.getDataVal(0);
		int level = contents.getFlag();
		Chunk newblk = contents.split(0, level); //ie, transfer all the records
		DirEntry oldroot = new DirEntry(firstval, newblk.number());
		insertEntry(oldroot);
		insertEntry(e);
		contents.setFlag(level+1);
	}

	/**
	 * Inserts a new directory entry into the B-tree chunk.
	 * If the chunk is at level 0, then the entry is inserted there.
	 * Otherwise, the entry is inserted into the appropriate
	 * child node, and the return value is examined.
	 * A non-null return value indicates that the child node
	 * split, and so the returned entry is inserted into
	 * this chunk.
	 * If this chunk splits, then the method similarly returns
	 * the entry information of the new chunk to its caller;
	 * otherwise, the method returns null.
	 * @param e the directory entry to be inserted
	 * @return the directory entry of the newly-split chunk, if one exists; otherwise, null
	 */
	public DirEntry insert(DirEntry e) {
		if (contents.getFlag() == 0)
			return insertEntry(e);
		Chunk childblk = findChildBlock(e.dataVal());
		BTreeDir child = new BTreeDir(childblk, ti, tx);
		DirEntry myentry = child.insert(e);
		child.close();
		return (myentry != null) ? insertEntry(myentry) : null;
	}

	private DirEntry insertEntry(DirEntry e) {
		int newslot = 1 + contents.findSlotBefore(e.dataVal());
		contents.insertDir(newslot, e.dataVal(), e.blockNumber());
		if (!contents.isFull())
			return null;
		// else page is full, so split it
		int level = contents.getFlag();
		int splitpos = contents.getNumRecs() / 2;
		Constant splitval = contents.getDataVal(splitpos);
		Chunk newblk = contents.split(splitpos, level);
		return new DirEntry(splitval, newblk.number());
	}

	private Chunk findChildBlock(Constant searchkey) {
		int slot = contents.findSlotBefore(searchkey);
		if (contents.getDataVal(slot+1).equals(searchkey))
			slot++;
		int blknum = contents.getChildNum(slot);
		return new Chunk(filename, blknum);
	}
}
