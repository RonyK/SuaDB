package suadb.parse;

import suadb.query.Scan;
import suadb.record.Schema;

/**
 * An expression consisting entirely of a single field.
 * @author Edward Sciore
 *
 */
public class FieldNameExpression implements Expression {
	private String fldname;

	/**
	 * Creates a new expression by wrapping a field.
	 * @param fldname the name of the wrapped field
	 */
	public FieldNameExpression(String fldname) {
		this.fldname = fldname;
	}
	
	public String fldname()
	{
		return fldname;
	}
	
	public String toString() {
		return fldname;
	}
}
