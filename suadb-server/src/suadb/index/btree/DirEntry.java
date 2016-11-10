package suadb.index.btree;

import suadb.query.Constant;

/**
 * A directory entry has two components: the number of the child chunk,
 * and the dataval of the first suadb.record in that chunk.
 * @author Edward Sciore
 */
public class DirEntry {
	private Constant dataval;
	private int blocknum;

	/**
	 * Creates a new entry for the specified dataval and chunk number.
	 * @param dataval the dataval
	 * @param blocknum the chunk number
	 */
	public DirEntry(Constant dataval, int blocknum) {
		this.dataval  = dataval;
		this.blocknum = blocknum;
	}

	/**
	 * Returns the dataval component of the entry
	 * @return the dataval component of the entry
	 */
	public Constant dataVal() {
		return dataval;
	}

	/**
	 * Returns the chunk number component of the entry
	 * @return the chunk number component of the entry
	 */
	public int blockNumber() {
		return blocknum;
	}
}

