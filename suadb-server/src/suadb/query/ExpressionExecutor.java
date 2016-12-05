package suadb.query;

import suadb.parse.Constant;
import suadb.record.Schema;

/**
 * Created by Rony on 2016-12-05.
 */
public interface ExpressionExecutor
{
	/**
	 * Returns true if the expression is a constant.
	 * @return true if the expression is a constant
	 */
	public boolean  isConstant();
	
	/**
	 * Returns true if the expression is a field reference.
	 * @return true if the expression denotes a field
	 */
	public boolean  isFieldName();
	
	public boolean isDimensionName();
	
	/**
	 * Returns the constant corresponding to a constant expression.
	 * Throws an exception if the expression does not
	 * denote a constant.
	 * @return the expression as a constant
	 */
	public Constant asConstant();
	
	/**
	 * Returns the field name corresponding to a constant expression.
	 * Throws an exception if the expression does not
	 * denote a field.
	 * @return the expression as a field name
	 */
	public String	asFieldName();
	
	public String   asDimensionName();
	
	/**
	 * Evaluates the expression with respect to the
	 * current suadb.record of the specified scan.
	 * @param s the scan
	 * @return the value of the expression, as a Constant
	 */
	public Constant evaluate(Scan s);
	
	/**
	 * Determines if all of the fields mentioned in this expression
	 * are contained in the specified schema.
	 * @param schema the schema
	 * @return true if all fields in the expression are in the schema
	 */
	public boolean  appliesTo(Schema schema);
}
