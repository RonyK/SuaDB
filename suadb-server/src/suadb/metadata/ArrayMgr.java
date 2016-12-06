package suadb.metadata;

import suadb.query.Plan;
import suadb.query.UpdateScan;
import suadb.query.afl.SelectPlan;
import suadb.query.sql.TablePlan;
import suadb.record.*;
import suadb.server.SuaDB;
import suadb.tx.Transaction;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by CDS on 2016-11-15.
 */
public class ArrayMgr {

	//catalog for array, attribute, dimension - CDS
	private TableInfo arrayCatInfo, attributeCatInfo, dimensionCatInfo;

	public static final int MAX_DIM_STR = 20;

	public static final String STR_ARRAY_NAME      = "arrayName";
	public static final String TABLE_ARRAY_CATALOG = "arrayCat";
	public static final String TABLE_ATTRIBUTE_CATALOG = "attributeCat";
	public static final String TABLE_DIMENSION_CATALOG = "dimensionCat";

	private static final String STR_ATTRIBUTE_NAME = "attributeName";
	private static final String STR_DIMENSION_NAME = "dimensionName";
	private static final String STR_DIM_START       = "start";
	private static final String STR_DIM_END         = "end";
	private static final String STR_CHUNK_SIZE      = "chunkSize";
	private static final String STR_TYPE            = "type";
	private static final String STR_LENGTH          = "length";

	/**
	 * Creates a new catalog manager for the database system.
	 * If the database is new, then the two catalog tables
	 * (arrayCat, attributeCat, dimensionCat) - CDS
	 * are created.
	 * @param isNew has the value true if the database is new
	 * @param tx the startup transaction
	 * @param tblmgr TableMgr - CDS
	 */
	public ArrayMgr(boolean isNew, Transaction tx, TableMgr tblmgr) {
		Schema arrayCatSchema = new Schema();
		arrayCatSchema.addStringField(STR_ARRAY_NAME, tblmgr.MAX_NAME);
		arrayCatInfo = new TableInfo(TABLE_ARRAY_CATALOG, arrayCatSchema);

		Schema attributeCatSchema = new Schema();
		attributeCatSchema.addStringField(STR_ARRAY_NAME, tblmgr.MAX_NAME);
		attributeCatSchema.addStringField(STR_ATTRIBUTE_NAME, tblmgr.MAX_NAME);
		attributeCatSchema.addIntField(STR_TYPE);
		attributeCatSchema.addIntField(STR_LENGTH); // The length is meaningful if type == String. - CDS
		attributeCatInfo = new TableInfo(TABLE_ATTRIBUTE_CATALOG, attributeCatSchema);

		Schema dimensionCatSchema = new Schema();
		dimensionCatSchema.addStringField(STR_ARRAY_NAME, tblmgr.MAX_NAME);
		dimensionCatSchema.addStringField(STR_DIMENSION_NAME, MAX_DIM_STR);
		dimensionCatSchema.addIntField(STR_DIM_START);
		dimensionCatSchema.addIntField(STR_DIM_END);
		dimensionCatSchema.addIntField(STR_CHUNK_SIZE);
		dimensionCatInfo = new TableInfo(TABLE_DIMENSION_CATALOG, dimensionCatSchema);

		if (isNew) {
			tblmgr.createTable(TABLE_ARRAY_CATALOG, arrayCatSchema, tx);
			tblmgr.createTable(TABLE_ATTRIBUTE_CATALOG, attributeCatSchema, tx);
			tblmgr.createTable(TABLE_DIMENSION_CATALOG, dimensionCatSchema, tx);
		}
	}

	/**
	 * Creates a new array having the specified name and schema.
	 * @param arrayName the name of the new array
	 * @param sch the array's schema
	 * @param tx the transaction creating the table
	 */
	public void createArray(String arrayName, Schema sch, Transaction tx) {
		ArrayInfo ti = new ArrayInfo(arrayName, sch);

		//insert arrayName into arrayCat - CDS
		RecordFile arrayCatFile = new RecordFile(arrayCatInfo, tx);
		arrayCatFile.insert();
		arrayCatFile.setString(STR_ARRAY_NAME, arrayName);
		arrayCatFile.close();

		//insert attribute information into attributeCat - CDS
		RecordFile attributeCatFile = new RecordFile(attributeCatInfo, tx);
		for (String attributeName : sch.attributes()) {//attribute iteration - CDS
			attributeCatFile.insert();
			attributeCatFile.setString(STR_ARRAY_NAME, arrayName);
			attributeCatFile.setString(STR_ATTRIBUTE_NAME, attributeName);
			attributeCatFile.setInt	(STR_TYPE,	sch.type(attributeName));
			attributeCatFile.setInt	(STR_LENGTH, sch.length(attributeName));
		}
		attributeCatFile.close();

		//insert dimension information into dimensionCat - CDS
		RecordFile dimensionCatFile = new RecordFile(dimensionCatInfo, tx);
		for (String dimensionName : sch.dimensions()) {//attribute iteration - CDS
			dimensionCatFile.insert();
			dimensionCatFile.setString(STR_ARRAY_NAME, arrayName);
			dimensionCatFile.setString(STR_DIMENSION_NAME, dimensionName);
			dimensionCatFile.setInt	(STR_DIM_START,	sch.start(dimensionName));
			dimensionCatFile.setInt	(STR_DIM_END, sch.end(dimensionName));
			dimensionCatFile.setInt	(STR_CHUNK_SIZE, sch.chunkSize(dimensionName));
		}
		dimensionCatFile.close();
	}

	/**
	 * Retrieves the suadb.metadata for the specified array
	 * out of the catalog.
	 * @param arrayName the name of the array
	 * @param tx the transaction
	 * @return the table's stored suadb.metadata
	 */
	public ArrayInfo getArrayInfo(String arrayName, Transaction tx) {

		//For attribute - CDS
		RecordFile attributeCatFile = new RecordFile(attributeCatInfo, tx);
		Schema sch = new Schema();
		while (attributeCatFile.next())
			if (attributeCatFile.getString(STR_ARRAY_NAME).equals(arrayName)) {
				String attributeName = attributeCatFile.getString(STR_ATTRIBUTE_NAME);
				int attributeType = attributeCatFile.getInt(STR_TYPE);
				int attributeLength = attributeCatFile.getInt(STR_LENGTH);

				sch.addAttribute(attributeName, attributeType, attributeLength);
			}
		attributeCatFile.close();

		//For dimension - CDS
		RecordFile dimensionCatFile = new RecordFile(dimensionCatInfo, tx);
		while (dimensionCatFile.next())
			if (dimensionCatFile.getString(STR_ARRAY_NAME).equals(arrayName)) {

				String dimensionName = dimensionCatFile.getString(STR_DIMENSION_NAME);
				int start = dimensionCatFile.getInt(STR_DIM_START);
				int end = dimensionCatFile.getInt(STR_DIM_END);
				int chunkSize = dimensionCatFile.getInt(STR_CHUNK_SIZE);
				sch.addDimension(dimensionName, start, end, chunkSize);
			}
		dimensionCatFile.close();

		return new ArrayInfo(arrayName, sch);
	}

	// Issue #05
	public boolean removeArray(String arrayName, Transaction tx){
		int numberofchunksperdimension[];
		int numberofdimensions = 0;
		int numberofchunks = 1;
		//  1. delete array files
		// 1.1 retrive information to get file names that corresponds to the array that is going to be deleted
		ArrayInfo arrayinfo = getArrayInfo(arrayName, tx);
		Schema schema = arrayinfo.schema();
		Collection<String> attributes = arrayinfo.schema().attributes();
		Collection<String> dimensions = arrayinfo.schema().dimensions();
		numberofdimensions = dimensions.size();
		numberofchunksperdimension = new int[numberofdimensions];
		int index = 0;
		for(String dimname: dimensions){
			// total number of chunks is multiplication of chunks of each dimension
			numberofchunksperdimension[index] = (int)Math.ceil(((float)(schema.end(dimname) - schema.start(dimname)+1))/schema.chunkSize(dimname));
			index++;
		}
		for(int i = 0 ; i < numberofdimensions ; i++){
			numberofchunks *= numberofchunksperdimension[i];
		}
		// 1.2 delete the files
		for(String attribute: attributes){
			for(int i = 0 ; i< numberofchunks ; i++){
				String filename = arrayName + "_" + attribute + "_" + i;
				SuaDB.fileMgr().deleteFile(filename);
			}
		}


		// 2. delete array metadata
		RecordFile dimensionCatFile = new RecordFile(dimensionCatInfo, tx);
		while (dimensionCatFile.next()) {
			if (dimensionCatFile.getString(STR_ARRAY_NAME).equals(arrayName)) {
				dimensionCatFile.delete();
				dimensionCatFile.close();
				return true;
			}
		}
		dimensionCatFile.close();
		return false;
	}
}
