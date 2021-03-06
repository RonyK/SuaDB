package suadb.buffer;

import suadb.file.Page;

/**
 * An interface used to initialize a new chunk on disk.
 * There will be an implementing class for each "type" of
 * disk chunk.
 * @author Edward Sciore
 */
public interface PageFormatter {
	/**
	 * Initializes a page, whose contents will be
	 * written to a new disk chunk.
	 * This method is called only during the method
	 * {@link ChunkBuffer#assignToNew}.
	 * @param p a suadb.buffer page
	 */
	public void format(Page p);
}
