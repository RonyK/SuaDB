package suadb.index.planner;

import java.util.Map;

import suadb.record.ArrayInfo;
import suadb.record.Schema;
import suadb.server.SuaDB;
import suadb.tx.Transaction;
import suadb.index.Index;
import suadb.metadata.IndexInfo;
import suadb.parse.*;
import suadb.planner.*;
import suadb.query.*;

import static suadb.query.ExpressionExecutorFactory.createExpressionExecutor;

/**
 * A modification of the basic update suadb.planner.
 * It dispatches each update statement to the corresponding
 * suadb.index suadb.planner.
 * @author Edward Sciore
 */
public class IndexUpdatePlanner implements UpdatePlanner {

	public int executeInsert(InsertData data, Transaction tx) {
		String tblname = data.tableName();
		Plan p = new TablePlan(tblname, tx);

		// first, insert the suadb.record
		UpdateScan s = (UpdateScan) p.open();
		s.insert();
//		RID rid = s.getRid();

		// then modify each field, inserting an suadb.index suadb.record if appropriate
		Map<String,IndexInfo> indexes = SuaDB.mdMgr().getIndexInfo(tblname, tx);

		s.close();
		return 1;
	}

	public int executeDelete(DeleteData data, Transaction tx) {
		String tblname = data.tableName();
		Plan p = new TablePlan(tblname, tx);
		p = new SelectPlan(p, data.pred());
		Map<String,IndexInfo> indexes = SuaDB.mdMgr().getIndexInfo(tblname, tx);

		UpdateScan s = (UpdateScan) p.open();
		int count = 0;
		while(s.next()) {
			// first, delete the suadb.record's RID from every suadb.index
			// TODO :: =====
//			RID rid = s.getRid();
//			for (String fldname : indexes.keySet()) {
//				Constant val = s.getVal(fldname);
//				Index idx = indexes.get(fldname).open();
//				idx.delete(val, rid);
//				idx.close();
//			}
			// TODO :: =====
			// then delete the suadb.record
			s.delete();
			count++;
		}
		s.close();
		return count;
	}

	public int executeModify(ModifyData data, Transaction tx) {
		String tblname = data.tableName();
		String fldname = data.targetField();
		Plan p = new TablePlan(tblname, tx);
		p = new SelectPlan(p, data.pred());

		IndexInfo ii = SuaDB.mdMgr().getIndexInfo(tblname, tx).get(fldname);
		Index idx = (ii == null) ? null : ii.open();

		UpdateScan s = (UpdateScan) p.open();
		ArrayInfo ai = SuaDB.mdMgr().getArrayInfo(data.tableName(), tx);
		Schema schema = ai.schema();
		ExpressionExecutor pe = createExpressionExecutor(data.newValue(), schema);
		int count = 0;
		while(s.next()) {
			// first, update the suadb.record
			Constant newval = pe.evaluate(s);
			Constant oldval = s.getVal(fldname);
			s.setVal(data.targetField(), newval);

			// then update the appropriate suadb.index, if it exists
			if (idx != null) {
//              TODO :: =====
//				RID rid = s.getRid();
//				idx.delete(oldval, rid);
//				idx.insert(newval, rid);
//              TODO :: =====
			}
			count++;
		}
		if (idx != null) idx.close();
		s.close();
		return count;
	}

	public int executeCreateTable(CreateArrayData data, Transaction tx) {
		SuaDB.mdMgr().createTable(data.arrayName(), data.newSchema(), tx);
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
	
	@Override
	public int executeCreateArray(CreateArrayData data, Transaction tx)
	{
		SuaDB.mdMgr().createArray(data.arrayName(), data.newSchema(), tx);
		return 0;
	}
}
