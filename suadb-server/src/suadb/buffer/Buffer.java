package suadb.buffer;

import suadb.file.Block;
import suadb.file.Page;
import suadb.server.SuaDB;

/**
 * An individual suadb.buffer.
 * A suadb.buffer wraps a page and stores information about its status,
 * linke the disk chunk associated with the page.
 *
 * @author Edward Sciore
 */

public class Buffer
{
	private Page contents = new Page();
	private Block block = null;
	private int modifiedBy = -1;  // negative means not modified
	private int logSequenceNumber = -1; // negative means no corresponding log suadb.record

	public int getInt(int offset)
	{
		return contents.getInt(offset);
	}

	public String getString(int offset)
	{
		return contents.getString(offset);
	}

	public void setInt(int offset, int val, int txnum, int lsn) {
		modifiedBy = txnum;
		if (lsn >= 0)
			logSequenceNumber = lsn;
		contents.setInt(offset, val);
	}

	public void setString(int offset, String val, int txnum, int lsn) {
		modifiedBy = txnum;
		if (lsn >= 0)
			logSequenceNumber = lsn;
		contents.setString(offset, val);
	}

	void flush() {
		if (modifiedBy >= 0) {
			SuaDB.logMgr().flush(logSequenceNumber);
			contents.write(block);
			modifiedBy = -1;
		}
	}
	
	boolean isModifiedBy(int txnum) {
		return txnum == modifiedBy;
	}

	void assignToBlock(Block b)
	{
		flush();
		block = b;
		contents.read(b);
	}

	void assignToNew(String fileName, PageFormatter fmtr)
	{
		flush();
		fmtr.format(contents);
		block = contents.append(fileName);
	}
}
