package suadb.query;

import suadb.record.*;
import suadb.tx.Transaction;

import java.util.List;

import static java.sql.Types.INTEGER;

/**
 * Created by rony on 16. 11. 18.
 */
public class ArrayScan implements UpdateScan
{
	private ArrayFile rf;
	private Schema sch;
	
	public ArrayScan(ArrayInfo ai, Transaction tx)
	{
		rf  = new ArrayFile(ai, tx);
		sch = ai.schema();
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
	
	public boolean hasField(String fldname) {
		return sch.hasField(fldname);
	}

	public boolean hasDimension(String dimname) { return sch.hasDimension(dimname); }
	
	// UpdateScan methods
	
	/**
	 * Sets the value of the specified field, as a Constant.
	 * The schema is examined to determine the field's type.
	 * If INTEGER, then the suadb.record suadb.file's setInt method is called;
	 * otherwise, the setString method is called.
	 * @see suadb.query.UpdateScan#setVal(java.lang.String, suadb.query.Constant)
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

	// TODO :: Insert() - RonyK
	public void insert() {
//		rf.insert();
	}
	//TODO : Merge with ArrayFile.currentCID implementation
	public List<Integer> getCurrentDimension() { return rf.currentCID.dimensionValues(); }

	public void moveToCid(CID cid) { rf.moveToCid(cid);}
	
//	public RID getRid() {
//		return rf.currentRid();
//	}
//
//	public void moveToRid(RID rid) {
//		rf.moveToRid(rid);
//	}
}
