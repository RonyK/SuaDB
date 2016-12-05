package suadb.parse;

/**
 * Data for the SQL <i>insert</i> statement.
 * @author Edward Sciore
 */
public class InsertData {
	private String tblname;
	private String filename;
	/**
	 * Saves the table name and the field and value lists.
	 */
	public InsertData(String tblname, String filename) {
		this.tblname = tblname;
		this.filename = filename;
	}

	/**
	 * Returns the name of the affected table.
	 * @return the name of the affected table
	 */
	public String tableName() {
		return tblname;
	}


}

