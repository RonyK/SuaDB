package suadb.query.afl;

import java.util.List;

import suadb.query.Plan;
import suadb.query.Region;
import suadb.query.Scan;
import suadb.record.Schema;

/**
 * Created by Rony on 2016-12-09.
 */
public class BetweenPlan implements Plan
{
	private Plan p;
	private Region region;
	
	public BetweenPlan(Plan p, Region region)
	{
		this.p = p;
		this.region = region;
	}
	
	@Override
	public Scan open()
	{
		Scan s = p.open();
		return new BetweenScan(s, region, schema());
	}
	
	// TODO :: Below values would be changed.
	@Override
	public int blocksAccessed()
	{
		return p.blocksAccessed();
	}
	
	@Override
	public int recordsOutput()
	{
		return p.recordsOutput();
	}
	
	@Override
	public int distinctValues(String fldname)
	{
		return p.distinctValues(fldname);
	}
	
	@Override
	public Schema schema()
	{
		return p.schema();
	}
}
