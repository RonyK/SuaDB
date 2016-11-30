package suadb.query;

import suadb.record.Schema;

/**
 * A term is a comparison between two expressions.
 * @author Edward Sciore
 *
 */
public class Term {
	private Expression lhs, rhs;
	private int mathcode;
	
	/**
	 * Creates a new term that compares two expressions
	 * @param lhs  the LHS expression
	 * @param rhs  the RHS expression
	 * @param mathcode comparison operator type
	 *                 0 : =
	 *                 1 : >
	 *                 2 : <
	 */
	public static final int MATHCODE_EQUAL      = 0;
	public static final int MATHCODE_GREATER    = 1;
	public static final int MATHCODE_LESS       = 2;
	
	public Term(Expression lhs, Expression rhs, int mathcode) {
		this.lhs = lhs;
		this.rhs = rhs;
		this.mathcode = mathcode;
	}

	/**
	 * Calculates the extent to which selecting on the term reduces
	 * the number of records output by a suadb.query.
	 * For example if the reduction factor is 2, then the
	 * term cuts the size of the output in half.
	 * @param p the suadb.query's plan
	 * @return the integer reduction factor.
	 */
	public int reductionFactor(Plan p) {
		String lhsName, rhsName;
		if (lhs.isFieldName() && rhs.isFieldName()) {
			lhsName = lhs.asFieldName();
			rhsName = rhs.asFieldName();
			return Math.max(p.distinctValues(lhsName),
								 p.distinctValues(rhsName));
		}
		if (lhs.isFieldName()) {
			lhsName = lhs.asFieldName();
			return p.distinctValues(lhsName);
		}
		if (rhs.isFieldName()) {
			rhsName = rhs.asFieldName();
			return p.distinctValues(rhsName);
		}
		// otherwise, the term equates constants
		if (lhs.asConstant().equals(rhs.asConstant()))
			return 1;
		else
			return Integer.MAX_VALUE;
	}

	/**
	 * Determines if this term is of the form "F=c"
	 * where F is the specified field and c is some constant.
	 * If so, the method returns that constant.
	 * If not, the method returns null.
	 * @param fldname the name of the field
	 * @return either the constant or null
	 */
	public Constant equatesWithConstant(String fldname) {
		if (mathcode!=0)
			return null;
		else
		{
			if (lhs.isFieldName() &&
					lhs.asFieldName().equals(fldname) &&
					rhs.isConstant())
				return rhs.asConstant();
			else if (rhs.isFieldName() &&
					rhs.asFieldName().equals(fldname) &&
					lhs.isConstant())
				return lhs.asConstant();
			else
				return null;
		}
	}

	/**
	 * Determines if this term is of the form "F>c"
	 * where F is the specified field and c is some constant.
	 * If so, the method returns that constant.
	 * If not, the method returns null.
	 * @param fldname the name of the field
	 * @return either the constant or null
	 */
	public Constant biggerThanConstant(String fldname) {
		if (mathcode!=1)
			return null;
		else
		{
			if (lhs.isFieldName() &&
					lhs.asFieldName().equals(fldname) &&
					rhs.isConstant())
				return rhs.asConstant();
			else if (rhs.isFieldName() &&
					rhs.asFieldName().equals(fldname) &&
					lhs.isConstant())
				return lhs.asConstant();
			else
				return null;
		}
	}

	/**
	 * Determines if this term is of the form "F<c"
	 * where F is the specified field and c is some constant.
	 * If so, the method returns that constant.
	 * If not, the method returns null.
	 * @param fldname the name of the field
	 * @return either the constant or null
	 */
	public Constant smallerThanConstant(String fldname) {
		if (mathcode!=2)
			return null;
		else
		{
			if (lhs.isFieldName() &&
					lhs.asFieldName().equals(fldname) &&
					rhs.isConstant())
				return rhs.asConstant();
			else if (rhs.isFieldName() &&
					rhs.asFieldName().equals(fldname) &&
					lhs.isConstant())
				return lhs.asConstant();
			else
				return null;
		}
	}

	/**
	 * Determines if this term is of the form "F1=F2"
	 * where F1 is the specified field and F2 is another field.
	 * If so, the method returns the name of that field.
	 * If not, the method returns null.
	 * @param fldname the name of the field
	 * @return either the name of the other field, or null
	 */
	public String equatesWithField(String fldname) {
		if (mathcode!=0)
			return null;
		else
		{
			if (lhs.isFieldName() &&
					lhs.asFieldName().equals(fldname) &&
					rhs.isFieldName())
				return rhs.asFieldName();
			else if (rhs.isFieldName() &&
					rhs.asFieldName().equals(fldname) &&
					lhs.isFieldName())
				return lhs.asFieldName();
			else
				return null;
		}
	}

	/**
	 * Determines if this term is of the form "F1>F2"
	 * where F1 is the specified field and F2 is another field.
	 * If so, the method returns the name of that field.
	 * If not, the method returns null.
	 * @param fldname the name of the field
	 * @return either the name of the other field, or null
	 */
	public String biggerThanField(String fldname) {
		if (mathcode!=1)
			return null;
		else
		{
			if (lhs.isFieldName() &&
					lhs.asFieldName().equals(fldname) &&
					rhs.isFieldName())
				return rhs.asFieldName();
			else if (rhs.isFieldName() &&
					rhs.asFieldName().equals(fldname) &&
					lhs.isFieldName())
				return lhs.asFieldName();
			else
				return null;
		}
	}

	/**
	 * Determines if this term is of the form "F1<F2"
	 * where F1 is the specified field and F2 is another field.
	 * If so, the method returns the name of that field.
	 * If not, the method returns null.
	 * @param fldname the name of the field
	 * @return either the name of the other field, or null
	 */
	public String smallerThanField(String fldname) {
		if (mathcode!=2)
			return null;
		else
		{
			if (lhs.isFieldName() &&
					lhs.asFieldName().equals(fldname) &&
					rhs.isFieldName())
				return rhs.asFieldName();
			else if (rhs.isFieldName() &&
					rhs.asFieldName().equals(fldname) &&
					lhs.isFieldName())
				return lhs.asFieldName();
			else
				return null;
		}
	}

	/**
	 * Returns true if both of the term's expressions
	 * apply to the specified schema.
	 * @param sch the schema
	 * @return true if both expressions apply to the schema
	 */
	public boolean appliesTo(Schema sch) {
		return lhs.appliesTo(sch) && rhs.appliesTo(sch);
	}

	/**
	 * Returns true if both of the term's expressions
	 * evaluate to the same constant,
	 * with respect to the specified scan.
	 * @param s the scan
	 * @return true if both expressions have the same value in the scan
	 */
	public boolean isSatisfied(Scan s) {
		Constant lhsval = lhs.evaluate(s);
		Constant rhsval = rhs.evaluate(s);
		
		switch (mathcode)
		{
			case MATHCODE_GREATER:
			{
				if(lhsval.compareTo(rhsval)>0)
					return true;
				else
					return false;
			}
			case MATHCODE_LESS:
			{
				if(lhsval.compareTo(rhsval)<0)
					return true;
				else
					return false;
			}
			case MATHCODE_EQUAL:
			default:
			{
				return rhsval.equals(lhsval);
			}
		}
	}

	public int getMathcode() { return mathcode; }

	public String toString() {
		switch (mathcode)
		{
			case MATHCODE_GREATER:
				return lhs.toString() + ">" + rhs.toString();
			case MATHCODE_LESS:
				return lhs.toString() + "<" + rhs.toString();
			case MATHCODE_EQUAL:
			default:
				return lhs.toString() + "=" + rhs.toString();
		}
	}
}
