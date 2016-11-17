package suadb.parse;

import suadb.query.*;
import java.util.*;

/**
 * Data for the SQL <i>select</i> statement.
 * @author Edward Sciore
 */
public class QueryData {
	private Collection<String> fields;
	private String array;
	private Predicate pred;

	/**
	 * Saves the field and table list and predicate.
	 */
	public QueryData( String array , Predicate pred) {

		this.array = array;
		this.pred = pred;
	}

	public Collection<String> fields() {
		return null;
	}
	public String tables() { return array; }
	/**
	 * Returns the predicate that describes which
	 * records should be in the output table.
	 * @return the suadb.query predicate
	 */
	public Predicate pred() {
		return pred;
	}

	public String toString() {
		String result = "filter(";
		result += array;
		result += ",";
		String predstring = pred.toString();
		if (!predstring.equals(""))
			result += predstring+")";
		return result;
	}
}
