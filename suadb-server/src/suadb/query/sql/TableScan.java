package suadb.query.sql;

import static java.sql.Types.INTEGER;

import suadb.parse.Constant;
import suadb.parse.IntConstant;
import suadb.parse.StringConstant;
import suadb.query.UpdateScan;
import suadb.tx.Transaction;
import suadb.record.*;

import java.util.ArrayList;
import java.util.List;

/**
 * The Scan class corresponding to a table.
 * A table scan is just a wrapper for a RecordFile object;
 * most methods just delegate to the corresponding
 * RecordFile methods.
 * @author Edward Sciore
 *
 */
public class TableScan implements UpdateScan
{
	private RecordFile rf;
	private Schema sch;

	/**
	 * Creates a new table scan,
	 * and opens its corresponding suadb.record suadb.file.
	 * @param ti the table's suadb.metadata
	 * @param tx the calling transaction
	 */
	public TableScan(TableInfo ti, Transaction tx) {
		rf  = new RecordFile(ti, tx);
		sch = ti.schema();
	}

	// Scan methods

	public void beforeFirst() {
		rf.beforeFirst();
	}

	public boolean next() {
		return rf.next();
	}

	public void close() {
		rf.close();
	}

	/**
	 * Returns the value of the specified field, as a Constant.
	 * The schema is examined to determine the field's type.
	 * If INTEGER, then the suadb.record suadb.file's getInt method is called;
	 * otherwise, the getString method is called.
	 * @see suadb.query.Scan#getVal(java.lang.String)
	 */
	public Constant getVal(String fldname) {
		if (sch.type(fldname) == INTEGER)
			return new IntConstant(rf.getInt(fldname));
		else
			return new StringConstant(rf.getString(fldname));
	}

	public int getInt(String fldname) {
		return rf.getInt(fldname);
	}

	public String getString(String fldname) {
		return rf.getString(fldname);
	}
	
	@Override
	public boolean isNull(String attrName)
	{
		throw new UnsupportedOperationException();
	}
	
	public boolean hasField(String fldname) {
		return sch.hasField(fldname);
	}

	public boolean hasDimension(String dimName) {return sch.hasDimension(dimName); }

	public CID getCurrentDimension()
	{
		List<Integer> list = new ArrayList<Integer>(rf.currentRid().id());
		return new CID(list);
	}

	public void moveToCid(CID cid) { }

	// UpdateScan methods

	/**
	 * Sets the value of the specified field, as a Constant.
	 * The schema is examined to determine the field's type.
	 * If INTEGER, then the suadb.record suadb.file's setInt method is called;
	 * otherwise, the setString method is called.
	 * @see suadb.query.UpdateScan#setVal(java.lang.String, Constant)
	 */
	public void setVal(String fldname, Constant val) {
		if (sch.type(fldname) == INTEGER)
			rf.setInt(fldname, (Integer)val.asJavaVal());
		else
			rf.setString(fldname, (String)val.asJavaVal());
	}

	public void setInt(String fldname, int val) {
		rf.setInt(fldname, val);
	}

	public void setString(String fldname, String val) {
		rf.setString(fldname, val);
	}

	public void delete() {
		rf.delete();
	}

	public void insert() {
		rf.insert();
	}

	public RID getRid() {
		return rf.currentRid();
	}

	public void moveToRid(RID rid) {
		rf.moveToRid(rid);
	}
	
	@Override
	public Constant getDimensionVal(String dimName)
	{
		throw new UnsupportedOperationException();
	}
	
	@Override
	public int getDimension(String dimName)
	{
		throw new UnsupportedOperationException();
	}
}
