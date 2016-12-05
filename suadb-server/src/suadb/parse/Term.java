package suadb.parse;

/**
 * A term is a comparison between two expressions.
 * @author Edward Sciore
 *
 */
public class Term {
	private Expression lhs, rhs;
	private int matchCode;
	
	/**
	 * Creates a new term that compares two expressions
	 * @param lhs  the LHS expression
	 * @param rhs  the RHS expression
	 * @param matchCode comparison operator type
	 *                 0 : =
	 *                 1 : >
	 *                 2 : <
	 */
	public static final int MATHCODE_EQUAL      = 0;
	public static final int MATHCODE_GREATER    = 1;
	public static final int MATHCODE_LESS       = 2;
	
	public Term(Expression lhs, Expression rhs, int matchCode) {
		this.lhs = lhs;
		this.rhs = rhs;
		this.matchCode = matchCode;
	}
	
	public int getMatchCode() { return matchCode; }
	
	public Expression lhs()
	{
		return lhs;
	}
	
	public Expression rhs()
	{
		return rhs;
	}
	
	@Override
	public String toString() {
		switch (matchCode)
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
