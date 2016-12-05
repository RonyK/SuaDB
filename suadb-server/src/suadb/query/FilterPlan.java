package suadb.query;

import suadb.parse.Predicate;
import suadb.record.Schema;

/**
 * Created by Rony on 2016-11-16.
 */
public class FilterPlan implements Plan
{
	private Plan p;
	private PredicateExecutor predicate;
	
	public FilterPlan(Plan p, Predicate predicate)
	{
		this.p = p;
		this.predicate = new PredicateExecutor(predicate, schema());
	}
	
	@Override
	public Scan open()
	{
		Scan s = p.open();
		return new FilterScan(s, p.schema(), predicate);
	}
	
	@Override
	public int blocksAccessed()
	{
		return p.blocksAccessed();
	}
	
	@Override
	public int recordsOutput()
	{
		return p.recordsOutput() / predicate.reductionFactor(p);
	}
	
	@Override
	public int distinctValues(String fieldName)
	{
		if(predicate.equatesWithConstant(fieldName) != null)
		{
			return 1;
		} else if(predicate.biggerThanConstant(fieldName) != null)
		{
			return 1;
		} else if(predicate.smallerThanConstant(fieldName) != null)
		{
			return 1;
		} else
		{
			String fieldName2 = predicate.equatesWithField(fieldName);
			if(fieldName2 != null)
			{
				return Math.max(
						p.distinctValues(fieldName), p.distinctValues(fieldName2));
			}else
			{
				return Math.min(
						p.distinctValues(fieldName), recordsOutput());
			}
		}
	}
	
	@Override
	public Schema schema()
	{
		return p.schema();
	}
}
