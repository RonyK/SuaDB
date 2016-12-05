package suadb.planner;

import suadb.record.ArrayInfo;
import suadb.record.Schema;
import suadb.server.SuaDB;
import suadb.tx.Transaction;
import suadb.parse.*;
import suadb.query.*;

import static suadb.query.ExpressionExecutorFactory.createExpressionExecutor;

/**
 * The basic suadb.planner for SQL update statements.
 * @author sciore
 */
public class BasicUpdatePlanner implements UpdatePlanner {

	public int executeDelete(DeleteData data, Transaction tx) {
		Plan p = new TablePlan(data.tableName(), tx);
		p = new SelectPlan(p, data.pred());
		UpdateScan us = (UpdateScan) p.open();
		int count = 0;
		while(us.next()) {
			us.delete();
			count++;
		}
		us.close();
		return count;
	}

	public int executeModify(ModifyData data, Transaction tx) {
		Plan p = new TablePlan(data.tableName(), tx);
		p = new SelectPlan(p, data.pred());
		UpdateScan us = (UpdateScan) p.open();
		ArrayInfo ai = SuaDB.mdMgr().getArrayInfo(data.tableName(), tx);
		Schema schema = ai.schema();
		ExpressionExecutor pe = createExpressionExecutor(data.newValue(), schema);
		int count = 0;
		while(us.next()) {
			Constant val = pe.evaluate(us);
			us.setVal(data.targetField(), val);
			count++;
		}
		us.close();
		return count;
	}

	// TODO :: UpdatePlanner For Array
	public int executeInsert(InsertData data, Transaction tx) {
		Plan p = new TablePlan(data.tableName(), tx);
		UpdateScan us = (UpdateScan) p.open();
		us.insert();

		us.close();
		return 1;
	}

	public int executeCreateTable(CreateArrayData data, Transaction tx) {
		SuaDB.mdMgr().createTable(data.arrayName(), data.newSchema(), tx);
		return 0;
	}

	public int executeInputArray(InputArrayData data, Transaction tx)
	{
		Plan p = new ArrayPlan(data.arrayName(), tx);
		UpdateScan us = (UpdateScan) p.open();
		return 0;
	}

	public int executeCreateArray(CreateArrayData data, Transaction tx)
	{
		SuaDB.mdMgr().createArray(data.arrayName(), data.newSchema(), tx);
		return 0;
	}

	public int executeCreateView(CreateViewData data, Transaction tx) {
		SuaDB.mdMgr().createView(data.viewName(), data.viewDef(), tx);
		return 0;
	}
	public int executeCreateIndex(CreateIndexData data, Transaction tx) {
		SuaDB.mdMgr().createIndex(data.indexName(), data.tableName(), data.fieldName(), tx);
		return 0;
	}
}
