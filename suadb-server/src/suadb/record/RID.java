package suadb.record;

/**
 * An identifier for a suadb.record within a suadb.file.
 * A RID consists of the chunk number in the suadb.file,
 * and the ID of the suadb.record in that chunk.
 * @author Edward Sciore
 */
public class RID {
	private int blknum;
	private int id;

	/**
	 * Creates a RID for the suadb.record having the
	 * specified ID in the specified chunk.
	 * @param blknum the chunk number where the suadb.record lives
	 * @param id the suadb.record's ID
	 */
	public RID(int blknum, int id) {
		this.blknum = blknum;
		this.id	  = id;
	}

	/**
	 * Returns the chunk number associated with this RID.
	 * @return the chunk number
	 */
	public int blockNumber() {
		return blknum;
	}

	/**
	 * Returns the ID associated with this RID.
	 * @return the ID
	 */
	public int id() {
		return id;
	}

	public boolean equals(Object obj) {
		RID r = (RID) obj;
		return blknum == r.blknum && id==r.id;
	}

	public String toString() {
		return "[" + blknum + ", " + id + "]";
	}
}
