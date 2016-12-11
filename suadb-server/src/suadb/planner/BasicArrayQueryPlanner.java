package suadb.planner;

import suadb.parse.ArrayData;
import suadb.parse.BetweenData;
import suadb.parse.BetweenNaiveData;
import suadb.parse.FilterData;
import suadb.parse.ListData;
import suadb.parse.ProjectData;
import suadb.parse.QueryData;
import suadb.parse.ScanData;
import suadb.query.afl.ArrayPlan;
import suadb.query.afl.BetweenNaivePlan;
import suadb.query.afl.BetweenPlan;
import suadb.query.afl.FilterPlan;
import suadb.query.afl.ListPlan;
import suadb.query.Plan;
import suadb.query.afl.ProjectPlan;
import suadb.query.afl.ScanPlan;
import suadb.tx.Transaction;

/**
 * Created by Aram on 2016-11-07.
 */

public class BasicArrayQueryPlanner implements QueryPlanner
{
    public Plan createPlan(QueryData data, Transaction tx)
    {
	    if (data instanceof FilterData)
		    return createFilterPlan((FilterData)data, tx);
	    else if (data instanceof ScanData)
		    return createScanPlan((ScanData)data,tx);
	    else if (data instanceof ProjectData)
		    return createProjectPlan((ProjectData)data,tx);
	    else if (data instanceof ArrayData)
	    	return createArrayPlan((ArrayData)data, tx);
	    else if (data instanceof ListData)
	    	return createListPlan((ListData)data, tx);
	    else if (data instanceof BetweenNaiveData)
		    return createBetweenOldPlan((BetweenNaiveData)data, tx);
	    else if (data instanceof BetweenData)
	    	return createBetweenPlan((BetweenData)data, tx);

	    throw new UnsupportedOperationException();
    }
	
	private Plan createArrayPlan(ArrayData data, Transaction tx)
	{
		Plan p = new ArrayPlan(data.arrayName(), tx);
		return p;
	}
	
	public Plan createScanPlan(ScanData data, Transaction tx)
	{
		Plan p = new ScanPlan(createPlan(data.array(), tx));
		return p;
	}
	
    public Plan createFilterPlan(FilterData data, Transaction tx)
	{
		Plan p = new FilterPlan(createPlan(data.array(), tx), data.predicate());
		return p;
	}
	
	public Plan createProjectPlan(ProjectData data, Transaction tx)
	{
		Plan p = new ProjectPlan(createPlan(data.array(), tx), data.getAttributes());
		return p;
	}
	
	public Plan createListPlan(ListData data, Transaction tx)
	{
		Plan p = new ListPlan(data.target(), tx);
		return p;
	}

	public Plan createBetweenOldPlan(BetweenNaiveData data, Transaction tx)
	{
		Plan p = new BetweenNaivePlan(createPlan(data.array(), tx), data.region());
		return p;
	}
	
	public Plan createBetweenPlan(BetweenData data, Transaction tx)
	{
		Plan p = new BetweenPlan(createPlan(data.array(), tx), data.region());
		return p;
	}
}
