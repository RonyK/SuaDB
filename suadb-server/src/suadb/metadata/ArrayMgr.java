package suadb.metadata;

import suadb.record.ArrayInfo;
import suadb.record.RecordFile;
import suadb.record.Schema;
import suadb.record.TableInfo;
import suadb.tx.Transaction;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by CDS on 2016-11-15.
 */
public class ArrayMgr {

	//catalog for array, attribute, dimension - CDS
	private TableInfo arrayCatInfo, attributeCatInfo, dimensionCatInfo;

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
		arrayCatSchema.addStringField("arrayName", tblmgr.MAX_NAME);
		arrayCatInfo = new TableInfo("arrayCat", arrayCatSchema);

		Schema attributeCatSchema = new Schema();
		attributeCatSchema.addStringField("arrayName", tblmgr.MAX_NAME);
		attributeCatSchema.addStringField("attributeName", tblmgr.MAX_NAME);
		attributeCatSchema.addIntField("type");
		attributeCatSchema.addIntField("length"); // The length is meaningful if type == String. - CDS
		attributeCatInfo = new TableInfo("attributeCat", attributeCatSchema);

		Schema dimensionCatSchema = new Schema();
		dimensionCatSchema.addStringField("arrayName", tblmgr.MAX_NAME);
		dimensionCatSchema.addStringField("dimensionName", tblmgr.MAX_NAME);
		dimensionCatSchema.addIntField("start");
		dimensionCatSchema.addIntField("end");
		dimensionCatSchema.addIntField("chunkSize");
		dimensionCatInfo = new TableInfo("dimensionCat", dimensionCatSchema);

		if (isNew) {
			tblmgr.createTable("arrayCat", arrayCatSchema, tx);
			tblmgr.createTable("attributeCat", attributeCatSchema, tx);
			tblmgr.createTable("dimensionCat", dimensionCatSchema, tx);
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
		arrayCatFile.setString("arrayName", arrayName);
		arrayCatFile.close();

		//insert attribute information into attributeCat - CDS
		RecordFile attributeCatFile = new RecordFile(attributeCatInfo, tx);
		for (String attributeName : sch.attributes()) {//attribute iteration - CDS
			attributeCatFile.insert();
			attributeCatFile.setString("arrayName", arrayName);
			attributeCatFile.setString("attributeName", attributeName);
			attributeCatFile.setInt	("type",	sch.type(attributeName));
			attributeCatFile.setInt	("length", sch.length(attributeName));
		}
		attributeCatFile.close();

		//insert dimension information into dimensionCat - CDS
		RecordFile dimensionCatFile = new RecordFile(dimensionCatInfo, tx);
		for (String dimensionName : sch.dimensions()) {//attribute iteration - CDS
			dimensionCatFile.insert();
			dimensionCatFile.setString("arrayName", arrayName);
			dimensionCatFile.setString("dimensionName", dimensionName);
			dimensionCatFile.setInt	("start",	sch.start(dimensionName));
			dimensionCatFile.setInt	("end", sch.end(dimensionName));
			dimensionCatFile.setInt	("chunkSize", sch.chunkSize(dimensionName));
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
			if (attributeCatFile.getString("arrayName").equals(arrayName)) {
				String attributeName = attributeCatFile.getString("attributeName");
				int attributeType = attributeCatFile.getInt("type");
				int attributeLength = attributeCatFile.getInt("length");

				sch.addAttribute(attributeName, attributeType, attributeLength);
			}
		attributeCatFile.close();

		//For dimension - CDS
		RecordFile dimensionCatFile = new RecordFile(dimensionCatInfo, tx);
		while (dimensionCatFile.next())
			if (dimensionCatFile.getString("arrayName").equals(arrayName)) {

				String dimensionName = dimensionCatFile.getString("dimensionName");
				int start = dimensionCatFile.getInt("start");
				int end = dimensionCatFile.getInt("end");
				int chunkSize = dimensionCatFile.getInt("chunkSize");
				sch.addDimension(dimensionName, start, end, chunkSize);
			}
		dimensionCatFile.close();

		return new ArrayInfo(arrayName, sch);
	}
}
