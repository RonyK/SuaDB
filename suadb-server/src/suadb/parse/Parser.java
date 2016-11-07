package suadb.parse;

import java.util.*;
import suadb.query.*;
import suadb.record.Schema;

/**
 * The SuaDB parser.
 * @author Edward Sciore
 */
public class Parser {
	private Lexer lex;

	public Parser(String s) {
		lex = new Lexer(s);
	}

// Methods for parsing predicates, terms, expressions, constants, and fields

	public String field() {
		return lex.eatId();
	}
	public String dimension() {
		return lex.eatId();
	}

	public Constant constant() {
		if (lex.matchStringConstant())
			return new StringConstant(lex.eatStringConstant());
		else
			return new IntConstant(lex.eatIntConstant());
	}

	public Expression expression() {
		if (lex.matchId())
			return new FieldNameExpression(field());
		else
			return new ConstantExpression(constant());
	}

	public Term term() {
		Expression lhs = expression();
		lex.eatDelim('=');
		Expression rhs = expression();
		return new Term(lhs, rhs);
	}

	public Predicate predicate() {
		Predicate pred = new Predicate(term());
		if (lex.matchKeyword("and")) {
			lex.eatKeyword("and");
			pred.conjoinWith(predicate());
		}
		return pred;
	}

// Methods for parsing queries

	public QueryData query() {
		lex.eatKeyword("select");
		Collection<String> fields = selectList();
		lex.eatKeyword("from");
		Collection<String> tables = tableList();
		Predicate pred = new Predicate();
		if (lex.matchKeyword("where")) {
			lex.eatKeyword("where");
			pred = predicate();
		}
		return new QueryData(fields, tables, pred);
	}

	private Collection<String> selectList() {
		Collection<String> L = new ArrayList<String>();
		L.add(field());
		if (lex.matchDelim(',')) {
			lex.eatDelim(',');
			L.addAll(selectList());
		}
		return L;
	}

	private Collection<String> tableList() {
		Collection<String> L = new ArrayList<String>();
		L.add(lex.eatId());
		if (lex.matchDelim(',')) {
			lex.eatDelim(',');
			L.addAll(tableList());
		}
		return L;
	}

// Methods for parsing the various update commands

	public Object updateCmd() {
		if (lex.matchKeyword("insert"))
			return insert();
		else if (lex.matchKeyword("delete"))
			return delete();
		else if (lex.matchKeyword("update"))
			return modify();
		else
			return create();
	}

	private Object create() {
		lex.eatKeyword("create");
		if (lex.matchKeyword("array"))
			return createArray();
		else if (lex.matchKeyword("view"))
			return createView();
		else
			return createIndex();
	}


// Methods for parsing insert commands

	public InsertData insert() {
		lex.eatKeyword("insert");
		lex.eatKeyword("into");
		String arrayname = lex.eatId();
		lex.eatDelim('(');
		List<String> flds = fieldList();
		lex.eatDelim(')');
		lex.eatKeyword("values");
		lex.eatDelim('(');
		List<Constant> vals = constList();
		lex.eatDelim(')');
		return new InsertData(arrayname, flds, vals);
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

	private List<Constant> constList() {
		List<Constant> L = new ArrayList<Constant>();
		L.add(constant());
		if (lex.matchDelim(',')) {
			lex.eatDelim(',');
			L.addAll(constList());
		}
		return L;
	}



// Method for parsing create table commands

	public CreateTableData createArray() {
		Schema sch = new Schema();
		lex.eatKeyword("array");
		String arrayname = lex.eatId();
		lex.eatDelim('<');
		fieldDefs();
		lex.eatDelim('>');
		lex.eatDelim('[');
		sch = dimensionDefs();
		lex.eatDelim(']');
		return new CreateTableData(arrayname, sch);
	}

	private Schema fieldDefs() {
		Schema schema = fieldDef();
		if (lex.matchDelim(',')) {
			lex.eatDelim(',');
			Schema schema2 = fieldDefs();
			schema.addAll(schema2);
		}
		return schema;
	}

	private Schema fieldDef() {
			String fldname = field();
			return fieldType(fldname);
	}

	private Schema fieldType(String fldname) {
		lex.eatDelim(':');
		if (lex.matchKeyword("int")) {
			lex.eatKeyword("int");
			schema.addIntField(fldname);
		}
		else if(lex.matchKeyword("double")) {
			lex.eatKeyword("double");
			schema.addField(fldname,DOUBLE,0); //add double
		}
		return schema;
	}


	private Schema DimensionDefs() {
		Schema schema = dimensionDef();
		if (lex.matchDelim(',')) {
			lex.eatDelim(',');
			Schema schema2 = fieldDefs();
			schema.addAll(schema2);
		}
		return schema;
	}

	private Schema dimensionDef() {
		String fldname = dimension();
		return dimensionType(fldname);
	}

	private Schema dimensionType(String fldname) {
		if (lex.matchKeyword("int")) {
			lex.eatKeyword("int");
			schema.addIntField(fldname);
		}
		else {
			lex.eatKeyword("varchar");
			lex.eatDelim('(');
			int strLen = lex.eatIntConstant();
			lex.eatDelim(')');
			schema.addStringField(fldname, strLen);
		}
		return schema;
	}

}

