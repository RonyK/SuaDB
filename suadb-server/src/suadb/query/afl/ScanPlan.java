package suadb.query.afl;

import suadb.query.Plan;
import suadb.query.Scan;
import suadb.record.Schema;

/**
 * Created by Aram on 2016-11-18.
 */
public class ScanPlan implements Plan
{
	private Plan p;

	public ScanPlan(Plan p)
	{
		this.p = p;
	}

	@Override
	public Scan open()
	{
		Scan s = p.open();
		return new ScanScan(s);
	}

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
