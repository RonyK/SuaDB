package suadb.record;

import static java.sql.Types.INTEGER;
import static suadb.file.Page.*;
import static suadb.record.RecordPage.EMPTY;
import suadb.file.Page;
import suadb.buffer.PageFormatter;

/**
 * An object that can format a page to look like a chunk of
 * empty records.
 * @author Edward Sciore
 */
class RecordFormatter implements PageFormatter {
	private TableInfo ti;

	/**
	 * Creates a formatter for a new page of a table.
	 * @param ti the table's suadb.metadata
	 */
	public RecordFormatter(TableInfo ti) {
		this.ti = ti;
	}

	/**
	 * Formats the page by allocating as many suadb.record slots
	 * as possible, given the suadb.record length.
	 * Each suadb.record slot is assigned a flag of EMPTY.
	 * Each integer field is given a value of 0, and
	 * each string field is given a value of "".
	 * @see suadb.buffer.PageFormatter#format(suadb.file.Page)
	 */
	public void format(Page page) {

		int recsize = ti.recordLength() + INT_SIZE;
		for (int pos=0; pos+recsize<=BLOCK_SIZE; pos += recsize) {
			page.setInt(pos, EMPTY);
			makeDefaultRecord(page, pos);
		}
	}

	private void makeDefaultRecord(Page page, int pos) {
		for (String fldname : ti.schema().fields()) {
			int offset = ti.offset(fldname);
			if (ti.schema().type(fldname) == INTEGER)
				page.setInt(pos + INT_SIZE + offset, 0);
			else
				page.setString(pos + INT_SIZE + offset, "");
		}
	}
}
