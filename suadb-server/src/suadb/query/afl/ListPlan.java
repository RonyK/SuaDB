package suadb.query.afl;

import suadb.metadata.ArrayMgr;
import suadb.metadata.TableMgr;
import suadb.query.Plan;
import suadb.query.Scan;
import suadb.query.sql.TablePlan;
import suadb.record.Schema;
import suadb.tx.Transaction;

/**
 * Created by Rony on 2016-11-28.
 */
public class ListPlan implements Plan
{
	private Plan p;
	
	public ListPlan(String targetName, Transaction tx)
	{
		/**
		 * List operator has just two option, array and filed.
		 * SciDB support 12 options.
		 *    - arrays, datastores, functions, instances, libraries,
		 *      macros, namespaces, operators, queries, roles, types, users
		 */
		//
		if(targetName == null || targetName.length() == 0 || targetName.equals("arrays"))
		{
			// Default : list(arrays)
			p = new TablePlan(ArrayMgr.TABLE_ARRAY_CATALOG, tx);
		}else if(targetName.equals("fileds"))
		{
			p = new TablePlan(ArrayMgr.TABLE_ATTRIBUTE_CATALOG, tx);
		}else
		{
			throw new UnsupportedOperationException(targetName);
		}
	}
	
	@Override
	public Scan open()
	{
		Scan s = p.open();
		return new ListScan(s);
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
	public int distinctValues(String fldName)
	{
		return p.distinctValues(fldName);
	}
	
	@Override
	public Schema schema()
	{
		return p.schema();
	}
}
