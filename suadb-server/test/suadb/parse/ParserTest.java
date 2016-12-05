package suadb.parse;

import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import suadb.test.SuaDBTestBase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test for Parser
 *
 * Created by rony on 16. 11. 17.
 */
public class ParserTest extends SuaDBTestBase
{
	static String arrayName = "testArray";
	static Field fTerms;
	static Field fLHS;
	static Field fRHS;
	
	@BeforeClass
	public static void setup() throws Exception
	{
		fTerms = Predicate.class.getDeclaredField("terms");
		fTerms.setAccessible(true);
		
		fLHS = Term.class.getDeclaredField("lhs");
		fLHS.setAccessible(true);
		
		fRHS = Term.class.getDeclaredField("rhs");
		fRHS.setAccessible(true);
	}
	
	@Test
	public void test_03_query_filter() throws Exception
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
		
		Predicate pred = fData.predicate();
		List<Term> terms = (List<Term>)fTerms.get(pred);
		
		assertEquals(terms.size(), 4);
		for(int i = 0; i < terms.size(); i++)
		{
			Expression lex = (Expression)fLHS.get(terms.get(i));
			Expression rex = (Expression)fRHS.get(terms.get(i));
			
			switch (i)
			{
				case 0:
				{
					assertEquals(terms.get(i).getMathCode(), Term.MATHCODE_GREATER);
					assertEquals(lex.toString(), "0");
					assertEquals(rex.toString(), "x");
					break;
				}
				case 1:
				{
					assertEquals(terms.get(i).getMathCode(), Term.MATHCODE_EQUAL);
					assertEquals(lex.toString(), "x");
					assertEquals(rex.toString(), "5");
					break;
				}
				case 2:
				{
					assertEquals(terms.get(i).getMathCode(), Term.MATHCODE_LESS);
					assertEquals(lex.toString(), "3");
					assertEquals(rex.toString(), "y");
					break;
				}
				case 3:
				{
					assertEquals(terms.get(i).getMathCode(), Term.MATHCODE_LESS);
					assertEquals(lex.toString(), "y");
					assertEquals(rex.toString(), "5");
					break;
				}
			}
		}
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