package suadb.query.afl;

import suadb.parse.Constant;
import suadb.parse.Predicate;
import suadb.query.PredicateExecutor;
import suadb.query.Scan;
import suadb.query.UpdateScan;
import suadb.record.CID;
import suadb.record.Schema;

import java.util.List;

/**
 * Created by Rony on 2016-11-16.
 */
public class FilterScan implements Scan
{
	private Scan s;
	private PredicateExecutor predicate;
	
	public FilterScan(Scan s, Schema schema, PredicateExecutor predicate)
	{
		this.s = s;
		this.predicate = predicate;
	}
	
	public void beforeFirst()
	{
		s.beforeFirst();
	}

	public boolean next()
	{
		while(s.next())
		{
			if (predicate.isSatisfied(s))
			{
				return true;
			}
		}
		
		return false;
	}
	
	public void close()
	{
		s.close();
	}
	
	public Constant getVal(String fldname) {
		return s.getVal(fldname);
	}
	
	public int getInt(String fldname) {
		return s.getInt(fldname);
	}
	
	public String getString(String fldname) {
		return s.getString(fldname);
	}
	
	@Override
	public Constant getDimensionVal(String dimName)
	{
		return s.getDimensionVal(dimName);
	}
	
	@Override
	public int getDimension(String dimName)
	{
		return s.getDimension(dimName);
	}

	public List<Integer> getCurrentDimension() { return  s.getCurrentDimension(); }
	
	public boolean hasField(String fldname) {
		return s.hasField(fldname);
	}

	public boolean hasDimension(String dimname) {
		return s.hasDimension(dimname);
	}
	
	// UpdateScan methods
	public void setVal(String fldname, Constant val) {
		UpdateScan us = (UpdateScan) s;
		us.setVal(fldname, val);
	}
	
	public void setInt(String fldname, int val) {
		UpdateScan us = (UpdateScan) s;
		us.setInt(fldname, val);
	}
	
	public void setString(String fldname, String val) {
		UpdateScan us = (UpdateScan) s;
		us.setString(fldname, val);
	}
	
	public void delete() {
		UpdateScan us = (UpdateScan) s;
		us.delete();
	}
	
	public void insert() {
		UpdateScan us = (UpdateScan) s;
		us.insert();
	}

	public void moveToCid(CID cid)
	{
		s.moveToCid(cid);
	}
	
//	public RID getRid() {
//		UpdateScan us = (UpdateScan) s;
//		return us.getRid();
//	}
//
//	public void moveToRid(RID rid) {
//		UpdateScan us = (UpdateScan) s;
//		us.moveToRid(rid);
//	}
}
