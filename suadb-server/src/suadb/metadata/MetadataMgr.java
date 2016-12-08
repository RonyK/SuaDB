package suadb.metadata;

import suadb.tx.Transaction;
import suadb.record.*;
import java.util.Map;

public class MetadataMgr {
	private static TableMgr  tblmgr;
	private static ViewMgr	viewmgr;
	private static StatMgr	statmgr;
	private static IndexMgr  idxmgr;
	private static ArrayMgr arrayMgr;

	public MetadataMgr(boolean isnew, Transaction tx) {
		tblmgr  = new TableMgr(isnew, tx);
		viewmgr = new ViewMgr(isnew, tblmgr, tx);
		statmgr = new StatMgr(tblmgr, tx);
		idxmgr  = new IndexMgr(isnew, tblmgr, tx);
		arrayMgr = new ArrayMgr(isnew,tx,tblmgr);
	}

	public void createTable(String tblname, Schema sch, Transaction tx) {
		tblmgr.createTable(tblname, sch, tx);
	}

	public TableInfo getTableInfo(String tblname, Transaction tx) {
		return tblmgr.getTableInfo(tblname, tx);
	}

	public void createArray(String arrayName, Schema sch, Transaction tx){
		arrayMgr.createArray(arrayName,sch,tx);
	}

	public ArrayInfo getArrayInfo(String arrayName, Transaction tx){
		return arrayMgr.getArrayInfo(arrayName,tx);
	}
    // Issue #05
	public boolean removeArray(String arrayName, Transaction tx){
		return arrayMgr.removeArray(arrayName,tx);
	}

	public void createView(String viewname, String viewdef, Transaction tx) {
		viewmgr.createView(viewname, viewdef, tx);
	}

	public String getViewDef(String viewname, Transaction tx) {
		return viewmgr.getViewDef(viewname, tx);
	}

	public void createIndex(String idxname, String tblname, String fldname, Transaction tx) {
		idxmgr.createIndex(idxname, tblname, fldname, tx);
	}

	public Map<String,IndexInfo> getIndexInfo(String tblname, Transaction tx) {
		return idxmgr.getIndexInfo(tblname, tx);
	}

	public StatInfo getStatInfo(String tblname, TableInfo ti, Transaction tx) {
		return statmgr.getStatInfo(tblname, ti, tx);
	}

	// TODO :: Get Array State Info
}
