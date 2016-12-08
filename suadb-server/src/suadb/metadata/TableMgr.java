package suadb.metadata;

import java.util.HashMap;
import java.util.Map;

import suadb.record.RecordFile;
import suadb.record.Schema;
import suadb.record.TableInfo;
import suadb.tx.Transaction;

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

	
	public static final String TABLE_TABLE_CATALOG = "tblcat";
	public static final String TABLE_FIELD_CATALOG = "fldcat";
	public static final String STR_TABLE_NAME      = "tblname";
	public static final String STR_RECORD_LENGTH   = "reclength";
	public static final String STR_FIELD_NAME = "fldname";
	public static final String STR_TYPE            = "type";
	public static final String STR_LENGTH          = "length";
	public static final String STR_OFFSET          = "offset";

	private TableInfo tblCatInfo, fldCatInfo, arrCatInfo, dimCatInfo;

	/**
	 * Creates a new catalog manager for the database system.
	 * If the database is new, then the two catalog tables
	 * are created.
	 * @param isNew has the value true if the database is new
	 * @param tx the startup transaction
	 */
	public TableMgr(boolean isNew, Transaction tx)
	{
		Schema tcatSchema = new Schema();
		tcatSchema.addStringField(STR_TABLE_NAME, MAX_NAME);
		tcatSchema.addIntField(STR_RECORD_LENGTH);
		tblCatInfo = new TableInfo(TABLE_TABLE_CATALOG, tcatSchema);

		Schema fcatSchema = new Schema();
		fcatSchema.addStringField(STR_TABLE_NAME, MAX_NAME);
		fcatSchema.addStringField(STR_FIELD_NAME, MAX_NAME);
		fcatSchema.addIntField(STR_TYPE);
		fcatSchema.addIntField(STR_LENGTH);
		fcatSchema.addIntField(STR_OFFSET);
		fldCatInfo = new TableInfo(TABLE_FIELD_CATALOG, fcatSchema);

		if (isNew)
		{
			createTable(TABLE_TABLE_CATALOG, tcatSchema, tx);
			createTable(TABLE_FIELD_CATALOG, fcatSchema, tx);
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
		RecordFile tcatfile = new RecordFile(tblCatInfo, tx);
		tcatfile.insert();
		tcatfile.setString(STR_TABLE_NAME, tblname);
		tcatfile.setInt(STR_RECORD_LENGTH, ti.recordLength());
		tcatfile.close();

		// insert a suadb.record into fldcat for each field
		RecordFile fcatfile = new RecordFile(fldCatInfo, tx);
		for (String fldname : sch.fields())
		{
			fcatfile.insert();
			fcatfile.setString(STR_TABLE_NAME, tblname);
			fcatfile.setString(STR_FIELD_NAME, fldname);
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
		RecordFile tcatfile = new RecordFile(tblCatInfo, tx);
		int reclen = -1;
		while (tcatfile.next())
		{
			String temp = tcatfile.getString(STR_TABLE_NAME);
			if(temp.equals(tblname))
			{
				reclen = tcatfile.getInt(STR_RECORD_LENGTH);
				break;
			}
		}
		tcatfile.close();

		RecordFile fcatfile = new RecordFile(fldCatInfo, tx);
		Schema sch = new Schema();
		Map<String,Integer> offsets = new HashMap<String,Integer>();
		while (fcatfile.next()) {
			String temp = fcatfile.getString(STR_TABLE_NAME);
			if (temp.equals(tblname)) {
				String fldname = fcatfile.getString(STR_FIELD_NAME);
				int fldtype = fcatfile.getInt(STR_TYPE);
				int fldlen = fcatfile.getInt(STR_LENGTH);
				int offset = fcatfile.getInt(STR_OFFSET);
				offsets.put(fldname, offset);
				sch.addField(fldname, fldtype, fldlen);
			}
		}
		fcatfile.close();
//		Schema sch = new Schema();
//		getFieldSchema(tblname, tx, sch);
//
//		Map<String, Integer> offsets = getOffsets(tblname, tx);
//
		return new TableInfo(tblname, sch, offsets, reclen);
	}


	public void getFieldSchema(String name, Transaction tx, Schema schema)
	{
		RecordFile fieldCatFile = new RecordFile(fldCatInfo, tx);
		while (fieldCatFile.next())
		{
			String temp = fieldCatFile.getString(STR_TABLE_NAME);
			if (temp.equals(name))
			{
				String fieldName = fieldCatFile.getString(STR_FIELD_NAME);
				int fldtype	 = fieldCatFile.getInt(STR_TYPE);
				int fldlen	  = fieldCatFile.getInt(STR_LENGTH);
				int offset	  = fieldCatFile.getInt(STR_OFFSET);
				schema.addField(fieldName, fldtype, fldlen);
			}
		}
		fieldCatFile.close();
	}

	public Map<String, Integer> getOffsets(String name, Transaction tx)
	{
		Map<String, Integer> offsets = new HashMap<String, Integer>();
		
		RecordFile filedCatFile = new RecordFile(fldCatInfo, tx);
		while (filedCatFile.next())
		{
			if (filedCatFile.getString(STR_TABLE_NAME).equals(name))
			{
				String filedName = filedCatFile.getString(STR_FIELD_NAME);
				int offset	  = filedCatFile.getInt(STR_OFFSET);
				offsets.put(filedName, offset);
			}
		}
		filedCatFile.close();
		
		return offsets;
	}
}