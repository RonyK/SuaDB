package suadb.metadata;

import suadb.tx.Transaction;
import suadb.record.*;
import java.util.*;

/**
 * The table manager.
 * There are methods to create a table, save the suadb.metadata
 * in the catalog, and obtain the suadb.metadata of a
 * previously-created table.
 * @author Edward Sciore
 *
 */
public class TableMgr {
	/**
	 * The maximum number of characters in any
	 * tablename or fieldname.
	 * Currently, this value is 16.
	 */
	public static final int MAX_NAME = 16;
	
	public static final String TABLE_CATALOG_TABLE = "tblcat";
	public static final String FILED_CATALOG_TABLE = "fldcat";
	
	private static final String STR_TABLE_NAME = "tblname";
	private static final String STR_RECORD_LENGTH = "reclength";
	
	private static final String STR_FILED_NAME = "fldname";
	private static final String STR_TYPE = "type";
	private static final String STR_LENGTH = "length";
	private static final String STR_OFFSET = "offset";

	private TableInfo tcatInfo, fcatInfo;

	/**
	 * Creates a new catalog manager for the database system.
	 * If the database is new, then the two catalog tables
	 * are created.
	 * @param isNew has the value true if the database is new
	 * @param tx the startup transaction
	 */
	public TableMgr(boolean isNew, Transaction tx)
	{
		// TODO :: Add dimension information in table_catalog_table
		Schema tcatSchema = new Schema();
		tcatSchema.addStringField(STR_TABLE_NAME, MAX_NAME);
		tcatSchema.addIntField(STR_RECORD_LENGTH);
		tcatInfo = new TableInfo(TABLE_CATALOG_TABLE, tcatSchema);

		Schema fcatSchema = new Schema();
		fcatSchema.addStringField(STR_TABLE_NAME, MAX_NAME);
		fcatSchema.addStringField(STR_FILED_NAME, MAX_NAME);
		fcatSchema.addIntField(STR_TYPE);
		fcatSchema.addIntField(STR_LENGTH);
		fcatSchema.addIntField(STR_OFFSET);
		fcatInfo = new TableInfo(FILED_CATALOG_TABLE, fcatSchema);

		if (isNew)
		{
			createTable(TABLE_CATALOG_TABLE, tcatSchema, tx);
			createTable(FILED_CATALOG_TABLE, fcatSchema, tx);
		}
	}

	/**
	 * Creates a new table having the specified name and schema.
	 * @param tblname the name of the new table
	 * @param sch the table's schema
	 * @param tx the transaction creating the table
	 */
	public void createTable(String tblname, Schema sch, Transaction tx)
	{
		TableInfo ti = new TableInfo(tblname, sch);
		// insert one suadb.record into tblcat
		RecordFile tcatfile = new RecordFile(tcatInfo, tx);
		tcatfile.insert();
		tcatfile.setString(STR_TABLE_NAME, tblname);
		tcatfile.setInt(STR_RECORD_LENGTH, ti.recordLength());
		tcatfile.close();

		// insert a suadb.record into fldcat for each field
		RecordFile fcatfile = new RecordFile(fcatInfo, tx);
		for (String fldname : sch.fields())
		{
			fcatfile.insert();
			fcatfile.setString(STR_TABLE_NAME, tblname);
			fcatfile.setString(STR_FILED_NAME, fldname);
			fcatfile.setInt	(STR_TYPE,	sch.type(fldname));
			fcatfile.setInt	(STR_LENGTH, sch.length(fldname));
			fcatfile.setInt	(STR_OFFSET, ti.offset(fldname));
		}
		
		fcatfile.close();
	}

	/**
	 * Retrieves the suadb.metadata for the specified table
	 * out of the catalog.
	 * @param tblname the name of the table
	 * @param tx the transaction
	 * @return the table's stored suadb.metadata
	 */
	public TableInfo getTableInfo(String tblname, Transaction tx)
	{
		RecordFile tcatfile = new RecordFile(tcatInfo, tx);
		int reclen = -1;
		while (tcatfile.next())
		{
			if(tcatfile.getString(STR_TABLE_NAME).equals(tblname))
			{
				reclen = tcatfile.getInt(STR_RECORD_LENGTH);
				break;
			}
		}
			
		tcatfile.close();

		RecordFile fcatfile = new RecordFile(fcatInfo, tx);
		Schema sch = new Schema();
		Map<String,Integer> offsets = new HashMap<String,Integer>();
		while (fcatfile.next())
		{
			if (fcatfile.getString(STR_TABLE_NAME).equals(tblname))
			{
				String fldname = fcatfile.getString(STR_FILED_NAME);
				int fldtype	 = fcatfile.getInt(STR_TYPE);
				int fldlen	  = fcatfile.getInt(STR_LENGTH);
				int offset	  = fcatfile.getInt(STR_OFFSET);
				offsets.put(fldname, offset);
				sch.addField(fldname, fldtype, fldlen);
			}
		}
			
		fcatfile.close();
		return new TableInfo(tblname, sch, offsets, reclen);
	}
}