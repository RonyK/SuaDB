package suadb.query;

import org.junit.BeforeClass;
import org.junit.Test;

import suadb.server.SuaDB;
import suadb.test.SuaDBTestBase;
import suadb.tx.Transaction;

import static org.junit.Assert.*;

/**
 * Created by Rony on 2016-11-16.
 */
public class PlanTest extends SuaDBTestBase
{
	@BeforeClass
	public static void setup()
	{
		SuaDB.init(dbName);
	}
}