package suadb.file;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import suadb.server.SuaDB;

/**
 * The contents of a disk chunk in memory.
 * A page is treated as an array of BLOCK_SIZE bytes.
 * There are methods to get/set values into this array,
 * and to read/write the contents of this array to a disk chunk.
 * 
 * For an example of how to use Page and 
 * {@link Chunk} objects,
 * consider the following code fragment.  
 * The first portion increments the integer at offset 792 of chunk 6 of suadb.file junk.
 * The second portion stores the string "hello" at offset 20 of a page, 
 * and then appends it to a new chunk of the suadb.file.
 * It then reads that chunk into another page
 * and extracts the value "hello" into variable s.
 * <pre>
 * Page p1 = new Page();
 * Chunk blk = new Chunk("junk", 6);
 * p1.read(blk);
 * int n = p1.getInt(792);
 * p1.setInt(792, n+1);
 * p1.write(blk);
 *
 * Page p2 = new Page();
 * p2.setString(20, "hello");
 * blk = p2.append("junk");
 * Page p3 = new Page();
 * p3.read(blk);
 * String s = p3.getString(20);
 * </pre>
 * @author Edward Sciore
 */
public class Page {
	/**
	 * The number of bytes in a chunk.
	 * This value is set unreasonably low, so that it is easier
	 * to create and test databases having a lot of blocks.
	 * A more realistic value would be 4K.
	 */
	public static final int BLOCK_SIZE = 400;

	/**
	 * The size of an integer in bytes.
	 * This value is almost certainly 4, but it is
	 * a good idea to encode this value as a constant.
	 */
	public static final int INT_SIZE = Integer.SIZE / Byte.SIZE;

	/**
	 * The maximum size, in bytes, of a string of length n.
	 * A string is represented as the encoding of its characters,
	 * preceded by an integer denoting the number of bytes in this encoding.
	 * If the JVM uses the US-ASCII encoding, then each char
	 * is stored in one byte, so a string of n characters
	 * has a size of 4+n bytes.
	 * @param n the size of the string
	 * @return the maximum number of bytes required to store a string of size n
	 */
	public static final int STR_SIZE(int n) {
		float bytesPerChar = Charset.defaultCharset().newEncoder().maxBytesPerChar();
		return INT_SIZE + (n * (int)bytesPerChar);
	}

	protected ByteBuffer contents = ByteBuffer.allocateDirect(BLOCK_SIZE);
	protected FileMgr fileMgr = SuaDB.fileMgr();

	/**
	 * Creates a new page.  Although the constructor takes no arguments,
	 * it depends on a {@link FileMgr} object that it gets from the
	 * method {@link SuaDB#fileMgr()}.
	 * That object is created during system initialization.
	 * Thus this constructor cannot be called until either
	 * {@link SuaDB#init(String)} or
	 * {@link SuaDB#initFileMgr(String)} or
	 * {@link SuaDB#initFileAndLogMgr(String)} or
	 * {@link SuaDB#initFileLogAndBufferMgr(String)}
	 * is called first.
	 */
	public Page() {}

	/**
	 * Populates the page with the contents of the specified disk block.
	 * @param blk a reference to a disk chunk
	 */
	public synchronized void read(Block blk) {
		fileMgr.read(blk, contents);
	}

	/**
	 * Writes the contents of the page to the specified disk block.
	 * @param blk a reference to a disk chunk
	 */
	public synchronized void write(Block blk) {
		fileMgr.write(blk, contents);
	}

	/**
	 * Appends the contents of the page to the specified suadb.file.
	 * @param filename the name of the suadb.file
	 * @return the reference to the newly-created disk chunk
	 */
	public synchronized Block append(String filename) {
		return fileMgr.append(filename, contents);
	}

	/**
	 * Returns the integer value at a specified offset of the page.
	 * If an integer was not stored at that location,
	 * the behavior of the method is unpredictable.
	 * @param offset the byte offset within the page
	 * @return the integer value at that offset
	 */
	public synchronized int getInt(int offset) {
		contents.position(offset);
		return contents.getInt();
	}

	/**
	 * Writes an integer to the specified offset on the page.
	 * @param offset the byte offset within the page
	 * @param val the integer to be written to the page
	 */
	public synchronized void setInt(int offset, int val) {
		contents.position(offset);
		contents.putInt(val);
	}

	/**
	 * Returns the string value at the specified offset of the page.
	 * If a string was not stored at that location,
	 * the behavior of the method is unpredictable.
	 * @param offset the byte offset within the page
	 * @return the string value at that offset
	 */
	public synchronized String getString(int offset) {
		contents.position(offset);
		int len = contents.getInt();
		byte[] byteval = new byte[len];
		contents.get(byteval);
		return new String(byteval);
	}

	/**
	 * Writes a string to the specified offset on the page.
	 * @param offset the byte offset within the page
	 * @param val the string to be written to the page
	 */
	public synchronized void setString(int offset, String val) {
		contents.position(offset);
		byte[] byteval = val.getBytes();
		contents.putInt(byteval.length);
		contents.put(byteval);
	}
}
