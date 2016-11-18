package suadb.parse;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import suadb.test.SuaDBTestBase;

import static org.junit.Assert.*;

/**
 * Test for Parser
 *
 * Created by rony on 16. 11. 17.
 */
public class ParserTest extends SuaDBTestBase
{
	static String arrayName = "testArray";
	@Test
	public void test_03_query_filter()
	{
		String predicate = "0 > x and x = 5 AND 3 < y AnD y < 5";
		String convPredicate = "0>x and x=5 and 3<y and y<5";
		String query = "filter(" + arrayName + ", " + predicate + ");";
		
		Parser p = new Parser(query);
		Object obj = p.query();
		
		assertTrue(obj instanceof FilterData);
		
		FilterData fData = (FilterData)obj;
		assertEquals(fData.array(), new ArrayData(arrayName));
		assertEquals(fData.predicate().toString(), convPredicate);
	}
	
	@Test
	public void test_03_query_project()
	{
		String attributes = "x, y, z";
		List<String> attributesList
				= new ArrayList<>(
						Arrays.asList("x", "y", "z"));
		
		String query = "project(" + arrayName + ", " + attributes + ");";
		
		Parser p = new Parser(query);
		Object obj = p.query();
		
		assertTrue(obj instanceof ProjectData);
		
		ProjectData fData = (ProjectData)obj;
		assertEquals(fData.array(), new ArrayData(arrayName));
		assertEquals(fData.getAttributes(), attributesList);
	}
	
	@Test
	public void test_03_query_scan()
	{
		String query = "scan(" + arrayName + ");";
		
		Parser p = new Parser(query);
		Object obj = p.query();
		
		assertTrue(obj instanceof ScanData);
		
		ScanData fData = (ScanData)obj;
		assertEquals(fData.array(), new ArrayData(arrayName));
	}
	
	@Test
	public void test_05_query_nested_query_01()
	{
		String predicate = "x = 5 and 7 = y";
		String convPredicate = "x=5 and 7=y";
		
		String attributes = "x, y, z";
		String convAttributes = "[x, y, z]";
		String query = "PROJECT(FILTER(SCAN(" + arrayName + "), " + predicate + "), " + attributes + ")";
		System.out.println(query);
		
		Parser p = new Parser(query);
		QueryData qData = p.query();
		
		assertTrue(qData instanceof ProjectData);
		assertEquals(((ProjectData)qData).getAttributes().toString(), convAttributes);
		
		QueryData qNestedData = ((ProjectData)qData).array();
		assertTrue(qNestedData instanceof FilterData);
		assertEquals(((FilterData)qNestedData).predicate().toString(), convPredicate);
		
		QueryData q2NestedData = ((FilterData)qNestedData).array();
		assertTrue(q2NestedData instanceof ScanData);
		assertEquals(((ScanData)q2NestedData).array(), new ArrayData(arrayName));
	}
}