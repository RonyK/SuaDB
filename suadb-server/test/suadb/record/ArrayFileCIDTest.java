package suadb.record;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.lang.reflect.Method;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;

import suadb.query.Region;
import suadb.test.SuaDBTestBase;
import suadb.tx.Transaction;

import static com.sun.xml.internal.ws.dump.LoggingDumpTube.Position.Before;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

/**
 * Created by Rony on 2016-12-13.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ArrayFileCIDTest extends SuaDBTestBase
{
	static String ARRAY_NAME = "TAARAY";
//	static Method mCalcTargetChunk;
	
	@Mock static Transaction tx;
	
	@BeforeClass
	public static void init()
	{
//		try
//		{
//			mCalcTargetChunk = ArrayFile.class.getMethod("calcTargetChunk", Integer);
//			mCalcTargetChunk.setAccessible(true);
//		}catch (Exception e)
//		{
//			e.printStackTrace();
//		}
	}
	
	@org.junit.Before
	public void before()
	{
		MockitoAnnotations.initMocks(this);
		when(tx.size(anyString(), anyInt())).thenReturn(1);
	}
	
	@Test
	public void test_00_chunkSizeTest() throws Exception
	{
		Schema schema = new Schema();
		schema.addDimension("x", 0, 7, 2);
		schema.addDimension("y", 0, 5, 2);
		schema.addDimension("z", 0, 3, 2);
		
		schema.addField("a", Types.INTEGER, 4);
		ArrayInfo ai = new ArrayInfo(ARRAY_NAME, schema);
		ArrayFile af = new ArrayFile(ai, tx);
		
		Region region0 = af.calcTargetChunk(0);
		assertEquals(new Region(new ArrayList<>(Arrays.asList(0, 0, 0, 1, 1, 1))), region0);
		
		Region region1 = af.calcTargetChunk(1);
		assertEquals(new Region(new ArrayList<>(Arrays.asList(0, 0, 2, 1, 1, 3))), region1);
		
		Region region2 = af.calcTargetChunk(2);
		assertEquals(new Region(new ArrayList<>(Arrays.asList(0, 2, 0, 1, 3, 1))), region2);
		
		Region region3 = af.calcTargetChunk(3);
		assertEquals(new Region(new ArrayList<>(Arrays.asList(0, 2, 2, 1, 3, 3))), region3);
	}
	
	@Test
	public void test_01_chunkSizeTest() throws Exception
	{
		Schema schema = new Schema();
		schema.addDimension("x", 0, 1, 2);
		schema.addDimension("y", 0, 3, 2);
		schema.addDimension("z", 0, 3, 2);
		
		schema.addField("a", Types.INTEGER, 4);
		ArrayInfo ai = new ArrayInfo(ARRAY_NAME, schema);
		ArrayFile af = new ArrayFile(ai, tx);
		
		Region region0 = af.calcTargetChunk(0);
		assertEquals(new Region(new ArrayList<>(Arrays.asList(0, 0, 0, 1, 1, 1))), region0);
		
		Region region1 = af.calcTargetChunk(1);
		assertEquals(new Region(new ArrayList<>(Arrays.asList(0, 0, 2, 1, 1, 3))), region1);
		
		Region region2 = af.calcTargetChunk(2);
		assertEquals(new Region(new ArrayList<>(Arrays.asList(0, 2, 0, 1, 3, 1))), region2);
		
		Region region3 = af.calcTargetChunk(3);
		assertEquals(new Region(new ArrayList<>(Arrays.asList(0, 2, 2, 1, 3, 3))), region3);
	}
}
