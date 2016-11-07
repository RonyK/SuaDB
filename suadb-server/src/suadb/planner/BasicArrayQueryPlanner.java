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
        for (String tblname : data.tables()) {
            String viewdef = SuaDB.mdMgr().getViewDef(tblname, tx);
            if (viewdef != null)
                plans.add(SuaDB.planner().createQueryPlan(viewdef, tx));
            else
                plans.add(new TablePlan(tblname, tx));
        }

        //Step 2: Create the product of all table plans
        Plan p = plans.remove(0);
        for (Plan nextplan : plans)
            p = new ProductPlan(p, nextplan);

        //Step 3: Add a selection plan for the predicate
        p = new SelectPlan(p, data.pred());

        //Step 4: Project on the field names
        p = new ProjectPlan(p, data.fields());
        return p;
    }
}
