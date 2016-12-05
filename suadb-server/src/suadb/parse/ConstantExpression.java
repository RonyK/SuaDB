package suadb.parse;

import suadb.query.Scan;
import suadb.record.Schema;

/**
 * An expression consisting entirely of a single constant.
 * @author Edward Sciore
 *
 */
public class ConstantExpression implements Expression {
	private Constant val;

	/**
	 * Creates a new expression by wrapping a constant.
	 * @param c the constant
	 */
	public ConstantExpression(Constant c) {
		val = c;
	}
	
	public Constant val()
	{
		return val;
	}
	
	public String toString() {
		return val.toString();
	}
}
