package suadb.planner;
import java.util.List;

import suadb.parse.ArrayData;
import suadb.parse.FilterData;
import suadb.parse.ListData;
import suadb.parse.ProjectData;
import suadb.parse.QueryData;
import suadb.parse.ScanData;
import suadb.query.ArrayPlan;
import suadb.query.FilterPlan;
import suadb.query.ListPlan;
import suadb.query.Plan;
import suadb.query.ProjectPlan;
import suadb.query.ScanPlan;
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
		Plan p = new FilterPlan(createPlan(data, tx), data.predicate());
		return p;
	}
	
	public Plan createProjectPlan(ProjectData data, Transaction tx)
	{
		Plan p = new ProjectPlan(createPlan(data, tx), data.getAttributes());
		return p;
	}
	
	public Plan createListPlan(ListData data, Transaction tx)
	{
		Plan p = new ListPlan(data.target(), tx);
		return p;
	}
}
