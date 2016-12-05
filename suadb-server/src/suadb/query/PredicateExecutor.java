package suadb.query;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import suadb.parse.BadSyntaxException;
import suadb.parse.Constant;
import suadb.parse.ConstantExpression;
import suadb.parse.Expression;
import suadb.parse.FieldNameExpression;
import suadb.parse.Predicate;
import suadb.parse.Term;
import suadb.record.Schema;

import static suadb.query.ExpressionExecutorFactory.createExpressionExecutor;

/**
 * Created by Rony on 2016-12-05.
 */
public class PredicateExecutor
{
	private List<TermExecutor> terms = new ArrayList<>();
	
	public PredicateExecutor()
	{
		
	}
	
	public PredicateExecutor(Predicate predicate, Schema schema)
	{
		for (Term t : predicate.getTerms())
		{
			Expression eL = t.lhs();
			Expression eR = t.rhs();
			int matchCode = t.getMatchCode();
			
			ExpressionExecutor eeL = createExpressionExecutor(eL, schema);
			ExpressionExecutor eeR = createExpressionExecutor(eR, schema);
			
			terms.add(new TermExecutor(eeL, eeR, matchCode));
		}
	}
	
	/**
	 * Returns true if the predicate evaluates to true
	 * with respect to the specified scan.
	 * @param s the scan
	 * @return true if the predicate is true in the scan
	 */
	public boolean isSatisfied(Scan s) {
		for (TermExecutor t : terms)
			if (!t.isSatisfied(s))
				return false;
		return true;
	}
	
	/**
	 * Calculates the extent to which selecting on the predicate
	 * reduces the number of records output by a suadb.query.
	 * For example if the reduction factor is 2, then the
	 * predicate cuts the size of the output in half.
	 * @param p the suadb.query's plan
	 * @return the integer reduction factor.
	 */
	public int reductionFactor(Plan p) {
		int factor = 1;
		for (TermExecutor t : terms)
			factor *= t.reductionFactor(p);
		return factor;
	}
	
	/**
	 * Returns the subpredicate that applies to the specified schema.
	 * @param sch the schema
	 * @return the subpredicate applying to the schema
	 */
	public PredicateExecutor selectPred(Schema sch) {
		PredicateExecutor result = new PredicateExecutor();
		for (TermExecutor t : terms)
			if (t.appliesTo(sch))
				result.terms.add(t);
		if (result.terms.size() == 0)
			return null;
		else
			return result;
	}
	
	/**
	 * Returns the subpredicate consisting of terms that apply
	 * to the union of the two specified schemas,
	 * but not to either schema separately.
	 * @param sch1 the first schema
	 * @param sch2 the second schema
	 * @return the subpredicate whose terms apply to the union of the two schemas but not either schema separately.
	 */
	public PredicateExecutor joinPred(Schema sch1, Schema sch2) {
		PredicateExecutor result = new PredicateExecutor();
		Schema newsch = new Schema();
		newsch.addAll(sch1);
		newsch.addAll(sch2);
		for (TermExecutor t : terms)
			if (!t.appliesTo(sch1)  &&
					!t.appliesTo(sch2) &&
					t.appliesTo(newsch))
				result.terms.add(t);
		if (result.terms.size() == 0)
			return null;
		else
			return result;
	}
	
	/**
	 * Determines if there is a term of the form "F=c"
	 * where F is the specified field and c is some constant.
	 * If so, the method returns that constant.
	 * If not, the method returns null.
	 * @param fldname the name of the field
	 * @return either the constant or null
	 */
	public Constant equatesWithConstant(String fldname) {
		for (TermExecutor t : terms) {
			Constant c = t.equatesWithConstant(fldname);
			if (c != null)
				return c;
		}
		return null;
	}
	
	/**
	 * Determines if there is a term of the form "F>c"
	 * where F is the specified field and c is some constant.
	 * If so, the method returns that constant.
	 * If not, the method returns null.
	 * @param fldname the name of the field
	 * @return either the constant or null
	 */
	public Constant biggerThanConstant(String fldname) {
		for (TermExecutor t : terms) {
			Constant c = t.biggerThanConstant(fldname);
			if (c != null)
				return c;
		}
		return null;
	}
	
	/**
	 * Determines if there is a term of the form "F>c"
	 * where F is the specified field and c is some constant.
	 * If so, the method returns that constant.
	 * If not, the method returns null.
	 * @param fldname the name of the field
	 * @return either the constant or null
	 */
	public Constant smallerThanConstant(String fldname) {
		for (TermExecutor t : terms) {
			Constant c = t.smallerThanConstant(fldname);
			if (c != null)
				return c;
		}
		return null;
	}
	
	/**
	 * Determines if there is a term of the form "F1=F2"
	 * where F1 is the specified field and F2 is another field.
	 * If so, the method returns the name of that field.
	 * If not, the method returns null.
	 * @param fldname the name of the field
	 * @return the name of the other field, or null
	 */
	public String equatesWithField(String fldname) {
		for (TermExecutor t : terms) {
			String s = t.equatesWithField(fldname);
			if (s != null)
				return s;
		}
		return null;
	}
	
	/**
	 * Determines if there is a term of the form "F1>F2"
	 * where F1 is the specified field and F2 is another field.
	 * If so, the method returns the name of that field.
	 * If not, the method returns null.
	 * @param fldname the name of the field
	 * @return the name of the other field, or null
	 */
	public String biggerThanField(String fldname) {
		for (TermExecutor t : terms) {
			String s = t.biggerThanField(fldname);
			if (s != null)
				return s;
		}
		return null;
	}
	
	/**
	 * Determines if there is a term of the form "F1<F2"
	 * where F1 is the specified field and F2 is another field.
	 * If so, the method returns the name of that field.
	 * If not, the method returns null.
	 * @param fldname the name of the field
	 * @return the name of the other field, or null
	 */
	public String smallerThanField(String fldname) {
		for (TermExecutor t : terms) {
			String s = t.smallerThanField(fldname);
			if (s != null)
				return s;
		}
		return null;
	}
	
	public String toString() {
		Iterator<TermExecutor> iter = terms.iterator();
		if (!iter.hasNext())
			return "";
		String result = iter.next().toString();
		while (iter.hasNext())
			result += " and " + iter.next().toString();
		return result;
	}
}
