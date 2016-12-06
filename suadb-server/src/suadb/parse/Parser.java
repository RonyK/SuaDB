package suadb.parse;

import java.util.*;

import suadb.record.Schema;

import static java.sql.Types.DOUBLE;
import static java.sql.Types.INTEGER;
import static java.sql.Types.VARCHAR;

/**
 * The SuaDB parser.
 * @author Edward Sciore
 */
public class Parser
{
	private Lexer lex;

	public Parser(String s)
	{
		lex = new Lexer(s);
	}

// Methods for parsing predicates, terms, expressions, constants, and fields

	public String field()
	{
		return lex.eatId();
	}

	public Constant constant()
	{
		if (lex.matchStringConstant())
			return new StringConstant(lex.eatStringConstant());
		else
			return new IntConstant(lex.eatIntConstant());
	}

	public Expression expression()
	{
		if (lex.matchId())
			return new FieldNameExpression(field());
		else
			return new ConstantExpression(constant());
	}

	// TODO :: How to support operators like; '>=', '<='.
	// Lexer only support char(1) type delim.
	public Term term()
	{
		int mathcode = -1;
		Expression lhs = expression();
		
		if (lex.matchDelim('='))
		{
			mathcode=0;
			lex.eatDelim('=');
		}else if (lex.matchDelim('>'))
		{
			mathcode=1;
			lex.eatDelim('>');
		}else if (lex.matchDelim('<'))
		{
			mathcode=2;
			lex.eatDelim('<');
		}else
		{
			throw new BadSyntaxException();
		}
			
		Expression rhs = expression();
		return new Term(lhs, rhs, mathcode);
	}

	public Predicate predicate()
	{
		Predicate pred = new Predicate(term());
		if (lex.matchKeyword("and")) {
			lex.eatKeyword("and");
			pred.conjoinWith(predicate());
		}
		return pred;
	}

// Methods for parsing queries

	public QueryData query()
	{
		if (lex.matchKeyword("scan"))
		{
			return scan();
		}else if (lex.matchKeyword("project"))
		{
			return project();
		}else if(lex.matchKeyword("filter"))
		{
			return filter();
		}else if(lex.matchKeyword("list"))
		{
			return list();
		}else
		{
			throw new UnsupportedOperationException();
		}
	}
	
	public QueryData array()
	{
		if(lex.matchId())
			return new ArrayData(lex.eatId());
		else
			return query();
	}

	public ProjectData project()
	{
		lex.eatKeyword("project");
		lex.eatDelim('(');
		QueryData array = array();
		lex.eatDelim(',');
		List<String> attributes = fieldList();
		lex.eatDelim(')');

		return new ProjectData(array,attributes);
	}

	public FilterData filter()
	{
		lex.eatKeyword("filter");
		lex.eatDelim('(');
		QueryData array = array();
		lex.eatDelim(',');
		Predicate pred;
		pred = predicate();
		lex.eatDelim(')');
		
		return new FilterData(array,pred);
	}

	public ScanData scan()
	{
		lex.eatKeyword("scan");
		lex.eatDelim('(');
		QueryData array = array();
		lex.eatDelim(')');
		
		return new ScanData(array);
	}

	public ListData list()
	{
		String target = null;
		
		lex.eatKeyword("list");
		lex.eatDelim('(');
		if(!lex.matchDelim(')'))
		{
			target = lex.eatId();
		}
		
		lex.eatDelim(')');
		
		return new ListData(target);
	}

// Methods for parsing the various update commands

	public Object updateCmd()
	{
		if (lex.matchKeyword("input"))
			return input();
		else if(lex.matchKeyword("remove"))
			return remove();
		else
			return create();
	}

	private Object create()
	{
		lex.eatKeyword("create");
		
		return createArray();
	}
	
	// Methods for parsing input commands
	public InputArrayData input() {
		lex.eatKeyword("input");
		lex.eatDelim('(');
		String arrayname = lex.eatId();
		lex.eatDelim(',');
		String inputfile = lex.eatId();  // 'input_file'
		lex.eatDelim(')');
		
		return new InputArrayData(arrayname, inputfile);
	}

	// issue #5
	RemoveArrayData remove() {
		lex.eatKeyword("remove");
		lex.eatDelim('(');
		String arrayname = lex.eatId();
		lex.eatDelim(')');

		return new RemoveArrayData(arrayname);
	}

	private List<String> fieldList() {
		List<String> L = new ArrayList<String>();
		L.add(field());
		if (lex.matchDelim(',')) {
			lex.eatDelim(',');
			L.addAll(fieldList());
		}
		
		return L;
	}

// Method for parsing create table commands
	public CreateArrayData createArray() {
		Schema schema = new Schema();
		lex.eatKeyword("array");
		String arrayname = lex.eatId();
		lex.eatDelim('<');
		schema = schemaDefs();
		lex.eatDelim(']');
		
		return new CreateArrayData(arrayname, schema);
	}

	private Schema schemaDefs() {
		Schema schema = new Schema();
		String fldname = field();
		lex.eatDelim(':');
		if (lex.matchKeyword("int")) {
			lex.eatKeyword("int");
			schema.addAttribute(fldname,INTEGER,0);
		}
		else if(lex.matchKeyword("double")) {
			lex.eatKeyword("double");
			schema.addAttribute(fldname, DOUBLE, 0);
		}
		else{
			lex.eatKeyword("string");
			lex.eatDelim('(');
			int stringMaxLength = lex.eatIntConstant();
			lex.eatDelim(')');
			schema.addAttribute(fldname,VARCHAR, stringMaxLength);
		}
		while (lex.matchDelim(',')) {
			lex.eatDelim(',');
			fldname = field();
			lex.eatDelim(':');
			if (lex.matchKeyword("int")) {
				lex.eatKeyword("int");
				schema.addAttribute(fldname,INTEGER,0);
			}
			else if(lex.matchKeyword("double")) {
				lex.eatKeyword("double");
				schema.addAttribute(fldname, DOUBLE, 0);
			}
			else{
				lex.eatKeyword("string");
				lex.eatDelim('(');
				int stringMaxLength = lex.eatIntConstant();
				lex.eatDelim(')');
				schema.addAttribute(fldname,VARCHAR, stringMaxLength);
			}
		}

		lex.eatDelim('>');
		lex.eatDelim('[');

		String dimname = field();
		lex.eatDelim('=');
		int start = lex.eatIntConstant();
		lex.eatDelim(':');
		int end = lex.eatIntConstant();
		lex.eatDelim(',');
		int chunksize = lex.eatIntConstant();
		schema.addDimension(dimname, start, end, chunksize);

//		lex.eatDelim(',');
//		int overlap = lex.eatIntConstant();

		while (lex.matchDelim(',')) {
			lex.eatDelim(',');
			dimname = field();
			lex.eatDelim('=');
			start = lex.eatIntConstant();
			lex.eatDelim(':');
			end = lex.eatIntConstant();
			lex.eatDelim(',');
			chunksize = lex.eatIntConstant();
			schema.addDimension(dimname, start, end, chunksize);
		}

		return schema;
	}
}

