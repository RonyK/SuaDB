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

	public int getInt(int offset)
	{
		return contents.getInt(offset);
	}

	public String getString(int offset)
	{
		return contents.getString(offset);
	}

	public void setInt(int offset, int val) {
		contents.setInt(offset, val);
	}

	public void setString(int offset, String val) {
		contents.setString(offset, val);
	}

	void flush() {
		contents.write(block);
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
