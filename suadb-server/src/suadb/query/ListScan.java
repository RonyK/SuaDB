package suadb.query;

import suadb.record.Schema;

/**
 * Created by Rony on 2016-11-28.
 */
public class ListScan implements Scan
{
	private Scan s;
	private Schema schema;
	
	public ListScan(Scan s)
	{
		this.s = s;
	}
	
	@Override
	public void beforeFirst()
	{
		s.beforeFirst();
	}
	
	@Override
	public boolean next()
	{
		while(s.next())
		{
			return true;
		}
		
		return false;
	}
	
	@Override
	public void close()
	{
		s.close();
	}
	
	@Override
	public Constant getVal(String fldname)
	{
		return s.getVal(fldname);
	}
	
	@Override
	public int getInt(String fldname)
	{
		return s.getInt(fldname);
	}
	
	@Override
	public String getString(String fldname)
	{
		return s.getString(fldname);
	}
	
	@Override
	public boolean hasField(String fldname)
	{
		return s.hasField(fldname);
	}
}
