package suadb.metadata;

import java.util.HashMap;
import java.util.Map;

import suadb.record.ArrayInfo;
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
	public static final int MAX_DIM_STR = 256;
	
	public static final String TABLE_TABLE_CATALOG = "tblcat";
	public static final String TABLE_FILED_CATALOG = "fldcat";
	public static final String TABLE_ARRAY_CATALOG = "arrcat";
	public static final String TABLE_DIMENSION_CATALOG = "dimcat";
	
	private static final String STR_TABLE_NAME      = "tblname";
	private static final String STR_ARRAY_NAME      = "arrayname";
	private static final String STR_DIMENSION       = "dimension";
	private static final String STR_DIMENSION_NAME  = "dimname";
	private static final String STR_DIM_START       = "start";
	private static final String STR_DIM_END         = "end";
	private static final String STR_CHUNK_SIZE      = "chunkSize";
	private static final String STR_ARRAY_ID        = "aid";
	private static final String STR_RECORD_LENGTH   = "reclength";
	private static final String STR_FILED_NAME      = "fldname";
	private static final String STR_TYPE            = "type";
	private static final String STR_LENGTH          = "length";
	private static final String STR_OFFSET          = "offset";

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
		fcatSchema.addStringField(STR_FILED_NAME, MAX_NAME);
		fcatSchema.addIntField(STR_TYPE);
		fcatSchema.addIntField(STR_LENGTH);
		fcatSchema.addIntField(STR_OFFSET);
		fldCatInfo = new TableInfo(TABLE_FILED_CATALOG, fcatSchema);
		
		Schema arrCatSchema = new Schema();
		fcatSchema.addStringField(STR_TABLE_NAME, MAX_NAME);
		fcatSchema.addStringField(STR_DIMENSION, MAX_DIM_STR);
		fcatSchema.addIntField(STR_ARRAY_ID);
		arrCatInfo = new TableInfo(TABLE_ARRAY_CATALOG, arrCatSchema);
		
		Schema dimCatSchema = new Schema();
		// TODO :: Change arrayName to arrayID - RonyK
		dimCatSchema.addStringField(STR_TABLE_NAME, MAX_NAME);
		dimCatSchema.addStringField(STR_DIMENSION_NAME, MAX_NAME);
		dimCatSchema.addIntField(STR_DIM_START);
		dimCatSchema.addIntField(STR_DIM_END);
		dimCatSchema.addIntField(STR_CHUNK_SIZE);
		dimCatInfo = new TableInfo(TABLE_DIMENSION_CATALOG, dimCatSchema);

		if (isNew)
		{
			createTable(TABLE_TABLE_CATALOG, tcatSchema, tx);
			createTable(TABLE_FILED_CATALOG, fcatSchema, tx);
			createTable(TABLE_ARRAY_CATALOG, arrCatSchema, tx);
			createTable(TABLE_DIMENSION_CATALOG, dimCatSchema, tx);
		}
		
		// TODO :: Get Array ID - RonyK
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
		RecordFile tcatfile = new RecordFile(tblCatInfo, tx);
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

		Schema sch = new Schema();
		getFiledSchema(tblname, tx, sch);
		
		Map<String, Integer> offsets = getOffsets(tblname, tx);
		
		return new TableInfo(tblname, sch, offsets, reclen);
	}
	
	public void createArray(String arrayName, Schema schema, Transaction tx)
	{
		ArrayInfo ai = new ArrayInfo(arrayName, schema);
		
		RecordFile arrayCatFile = new RecordFile(arrCatInfo, tx);
		arrayCatFile.insert();
		arrayCatFile.setString(STR_ARRAY_NAME, arrayName);
		arrayCatFile.setString(STR_DIMENSION, schema.dimensions().toString());
		// TODO :: Set Array ID - RonyK
		arrayCatFile.setInt(STR_ARRAY_ID, 0);
		arrayCatFile.close();
		
		RecordFile filedCatFile = new RecordFile(fldCatInfo, tx);
		for (String fldname : schema.fields())
		{
			filedCatFile.insert();
			filedCatFile.setString(STR_TABLE_NAME, arrayName);
			filedCatFile.setString(STR_FILED_NAME, fldname);
			filedCatFile.setInt(STR_TYPE,	schema.type(fldname));
			filedCatFile.setInt(STR_LENGTH, schema.length(fldname));
			// TODO :: ai.offset(filedName); - RonyK
			filedCatFile.setInt(STR_OFFSET, 0);
		}
		
		filedCatFile.close();
	}
	
	public ArrayInfo getArrayInfo(String arrayName, Transaction tx)
	{
		Schema schema = new Schema();
		getFiledSchema(arrayName, tx, schema);
		getDimensionSchema(arrayName, tx, schema);
		
		return new ArrayInfo(arrayName, schema);
	}
	
	public void getFiledSchema(String name, Transaction tx, Schema schema)
	{
		RecordFile filedCatFile = new RecordFile(fldCatInfo, tx);
		while (filedCatFile.next())
		{
			if (filedCatFile.getString(STR_TABLE_NAME).equals(name))
			{
				String filedName = filedCatFile.getString(STR_FILED_NAME);
				int fldtype	 = filedCatFile.getInt(STR_TYPE);
				int fldlen	  = filedCatFile.getInt(STR_LENGTH);
				int offset	  = filedCatFile.getInt(STR_OFFSET);
				schema.addField(filedName, fldtype, fldlen);
			}
		}
		filedCatFile.close();
	}
	
	public void getDimensionSchema(String arrayName, Transaction tx, Schema schema)
	{
		RecordFile dimCatFile = new RecordFile(dimCatInfo, tx);
		while (dimCatFile.next())
		{
			if (dimCatFile.getString(STR_TABLE_NAME).equals(arrayName))
			{
				String dimName = dimCatFile.getString(STR_DIMENSION_NAME);
				int start = dimCatFile.getInt(STR_DIM_START);
				int end = dimCatFile.getInt(STR_DIM_END);
				int chunkSize = dimCatFile.getInt(STR_CHUNK_SIZE);
				schema.addDimension(dimName, start, end, chunkSize);
			}
		}
		dimCatFile.close();
	}
	
	public Map<String, Integer> getOffsets(String name, Transaction tx)
	{
		Map<String, Integer> offsets = new HashMap<String, Integer>();
		
		RecordFile filedCatFile = new RecordFile(fldCatInfo, tx);
		while (filedCatFile.next())
		{
			if (filedCatFile.getString(STR_TABLE_NAME).equals(name))
			{
				String filedName = filedCatFile.getString(STR_FILED_NAME);
				int offset	  = filedCatFile.getInt(STR_OFFSET);
				offsets.put(filedName, offset);
			}
		}
		filedCatFile.close();
		
		return offsets;
	}
}