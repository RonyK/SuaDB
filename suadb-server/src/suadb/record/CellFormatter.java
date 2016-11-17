package suadb.record;

import static java.sql.Types.INTEGER;
import static suadb.file.Page.*;
import static suadb.record.RecordPage.EMPTY;
import suadb.file.Page;
import suadb.buffer.PageFormatter;

/**
 * An object that can format a page to look like a chunk of
 * empty records.
 * @author ILHYUN
 */
class CellFormatter implements PageFormatter {
    private ArrayInfo ai;
    private String attributename;

    /**
     * Creates a formatter for a new page of a table.
     * @param ai the table's suadb.metadata
     */
    public CellFormatter(ArrayInfo ai, String attributename) {
        this.ai = ai; this.attributename=attributename;
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
        int recsize = ai.recordLength(attributename) + INT_SIZE;
        for (int pos=0; pos+recsize<=BLOCK_SIZE; pos += recsize) {
            page.setInt(pos, EMPTY);
            if (ai.schema().type(attributename) == INTEGER)
                page.setInt(pos + INT_SIZE , 0);
            else
                page.setString(pos + INT_SIZE , "");
        }
    }
/*
    private void makeDefaultRecord(Page page, int pos) {
        for (String fldname : ti.schema().fields()) {
            int offset = ti.offset(fldname);
            if (ti.schema().type(fldname) == INTEGER)
                page.setInt(pos + INT_SIZE + offset, 0);
            else
                page.setString(pos + INT_SIZE + offset, "");
        }
    }
    */
}
