package suadb.planner;
import suadb.tx.Transaction;
import suadb.query.*;
import suadb.parse.*;
import suadb.server.SuaDB;
import java.util.*;

/**
 * Created by Aram on 2016-11-07.
 */

public class BasicArrayQueryPlanner implements QueryPlanner {

    /**
     * Creates a suadb.query plan as follows.  It first takes
     * the product of all tables and views; it then selects on the predicate;
     * and finally it projects on the field list.
     */
    public Plan createPlan(QueryData data, Transaction tx) {
        //Step 1: Create a plan for each mentioned table or view
        List<Plan> plans = new ArrayList<Plan>();
	    if (data instanceof FilterData)
		    return createFilterPlan((FilterData)data, tx);
	    else if (data instanceof ScanData)
		    return createScanPlan((ScanData)data,tx);
	    else
		    return createProjectPlan((ProjectData)data,tx);

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
}
