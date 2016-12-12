package suadb.query.afl;

import suadb.query.Plan;
import suadb.query.Scan;
import suadb.record.Schema;

/**
 * Created by Rony on 2016-12-12.
 */
public class JoinPlan implements Plan
{
	private Plan leftPlan, rightPlan;
	private Schema schema = new Schema();
	
	public JoinPlan(Plan leftPlan, Plan rightPlan)
	{
		this.leftPlan = leftPlan;
		this.rightPlan = rightPlan;
		
		schema.addAll(leftPlan.schema());
		schema.addFields(rightPlan.schema());
	}
	
	@Override
	public Scan open()
	{
		Scan leftScan = leftPlan.open();
		Scan rightScan = rightPlan.open();
		
		return new JoinScan(leftScan, rightScan, schema());
	}
	
	@Override
	public int blocksAccessed()
	{
		return leftPlan.blocksAccessed() + rightPlan.blocksAccessed();
	}
	
	@Override
	public int recordsOutput()
	{
		return leftPlan.recordsOutput();
	}
	
	@Override
	public int distinctValues(String fldname)
	{
		if(leftPlan.schema().hasField(fldname))
		{
			return leftPlan.distinctValues(fldname);
		}else
		{
			return rightPlan.distinctValues(fldname);
		}
	}
	
	@Override
	public Schema schema()
	{
		return schema;
	}
}

