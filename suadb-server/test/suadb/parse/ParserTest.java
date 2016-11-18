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
	@Test
	public void test_03_query_filter()
	{
		String arrayName = "testArray";
		String predicate = "0 < x and x < 5 and 3 < y and y < 5";
		String query = "filter(" + arrayName + ", " + predicate + ");";
		
		Parser p = new Parser(query);
		Object obj = p.query();
		
		assertTrue(obj instanceof FilterData);
		
		FilterData fData = (FilterData)obj;
		assertEquals(fData.array(), new ArrayData(arrayName));
		assertEquals(fData.predicate().toString(), predicate);
	}
	
	@Test
	public void test_03_query_project()
	{
		String arrayName = "testArray";
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
		String arrayName = "testArray";
		String query = "scan(" + arrayName + ");";
		
		Parser p = new Parser(query);
		Object obj = p.query();
		
		assertTrue(obj instanceof ProjectData);
		
		ProjectData fData = (ProjectData)obj;
		assertEquals(fData.array(), new ArrayData(arrayName));
	}
}