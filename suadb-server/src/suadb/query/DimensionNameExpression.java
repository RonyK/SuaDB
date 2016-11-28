package suadb.query;

import suadb.record.Schema;

/**
 * Created by Aram on 2016-11-28.
 */
public class DimensionNameExpression implements Expression
{
	private String dimname;

	/**
	 * Creates a new expression by wrapping a dimension.
	 * @param dimname the name of the wrapped dimension
	 */
	public DimensionNameExpression(String dimname) {
		this.dimname = dimname;
	}

	/**
	 * Returns false.
	 * @see suadb.query.Expression#isConstant()
	 */
	public boolean isConstant() {
		return false;
	}

	/**
	 * Returns false.
	 * @see suadb.query.Expression#isFieldName()
	 */
	public boolean isFieldName() {
		return false;
	}

	/**
	 * Returns true.
	 * @see suadb.query.Expression#isDimensionName()
	 */
	public boolean isDimensionName() { return true;	}

	/**
	 * This method should never be called.
	 * Throws a ClassCastException.
	 * @see suadb.query.Expression#asConstant()
	 */
	public Constant asConstant() {
		throw new ClassCastException();
	}

	/**
	 * This method should never be called.
	 * Throws a ClassCastException.
	 * @see suadb.query.Expression#asFieldName()
	 */
	public String asFieldName() {
		throw new ClassCastException();
	}


	/**
	 * Unwraps the dimension name and returns it.
	 * @see suadb.query.Expression#asDimensionName()
	 */
	public String asDimensionName() {
		return dimname;
	}

	/**
	 * Evaluates the dimension by getting its value in the scan.
	 * @see suadb.query.Expression#evaluate(suadb.query.Scan)
	 */
	public Constant evaluate(Scan s) {
		return s.getVal(dimname);
	}

	/**
	 * Returns true if the dimension is in the specified schema.
	 * @see suadb.query.Expression#appliesTo(suadb.record.Schema)
	 */
	public boolean appliesTo(Schema sch) {
		return sch.hasDimension(dimname);
	}

	public String toString() {
		return dimname;
	}
}
