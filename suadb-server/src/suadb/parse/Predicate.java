package suadb.parse;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import suadb.query.Plan;
import suadb.query.Scan;
import suadb.record.Schema;
/**
 * A predicate is a Boolean combination of terms.
 * @author Edward Sciore
 *
 */
public class Predicate {
	private List<Term> terms = new ArrayList<Term>();

	/**
	 * Creates an empty predicate, corresponding to "true".
	 */
	public Predicate() {}

	/**
	 * Creates a predicate containing a single term.
	 * @param t the term
	 */
	public Predicate(Term t) {
		terms.add(t);
	}

	/**
	 * Modifies the predicate to be the conjunction of
	 * itself and the specified predicate.
	 * @param pred the other predicate
	 */
	public void conjoinWith(Predicate pred) {
		terms.addAll(pred.terms);
	}
	
	public List<Term> getTerms()
	{
		return terms;
	}

	public String toString() {
		Iterator<Term> iter = terms.iterator();
		if (!iter.hasNext())
			return "";
		String result = iter.next().toString();
		while (iter.hasNext())
			result += " and " + iter.next().toString();
		return result;
	}
}
