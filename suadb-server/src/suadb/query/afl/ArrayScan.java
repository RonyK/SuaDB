package suadb.query.afl;

import exception.ArrayInputException;
import suadb.parse.Constant;
import suadb.parse.IntConstant;
import suadb.parse.IntNullConstant;
import suadb.parse.StringConstant;
import suadb.parse.StringNullConstant;
import suadb.query.UpdateScan;
import suadb.record.*;
import suadb.tx.Transaction;

import java.util.UnknownFormatConversionException;

import static java.sql.Types.INTEGER;
import static java.sql.Types.VARCHAR;

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
	public Constant getVal(String fldname)
	{
		if(isNull(fldname))
		{
			if (sch.type(fldname) == INTEGER)
				return new IntNullConstant();
			else if (sch.type(fldname) == VARCHAR)
				return new StringNullConstant();
			else
				throw new UnknownFormatConversionException(String.format("Type : %d", sch.type(fldname)));
		}else
		{
			if (sch.type(fldname) == INTEGER)
				return new IntConstant(rf.getInt(fldname));
			else if (sch.type(fldname) == VARCHAR)
				return new StringConstant(rf.getString(fldname));
			else
				throw new UnknownFormatConversionException(String.format("Type : %d", sch.type(fldname)));
		}
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
		return rf.isNull(attrName);
	}
	
	public boolean hasField(String fldname) {
		return sch.hasField(fldname);
	}

	public boolean hasDimension(String dimName) { return sch.hasDimension(dimName); }
	
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
		// TODO :: EMPTY insert - RonyK
	}
	
	public void insert(String fileName) throws ArrayInputException
	{
		rf.input(fileName);
	}
	
	@Override
	public Constant getDimensionVal(String dimName)
	{
		return rf.getDimensionVal(dimName);
	}
	
	@Override
	public int getDimension(String dimName)
	{
		return rf.getDimension(dimName);
	}
	
	@Override
	public CID getCurrentDimension()
	{
		return rf.getCID();
	}
	
	public void moveToCid(CID cid) { rf.moveToCid(cid);}
}
