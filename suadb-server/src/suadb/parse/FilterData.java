package suadb.parse;

import suadb.query.Predicate;

/**
 * The operator : filter()
 *
 * Synopsis :
 *      filter( srcArray, predicate )
 *
 * Created by Rony on 2016-11-16.
 */
public class FilterData implements QueryData
{
	private QueryData array;
	private Predicate predicate;
	
	public FilterData(QueryData array, Predicate predicate)
	{
		this.array = array;
		this.predicate = predicate;
	}
	
	public String array()
	{
		return array.srcArrayName();
	}
	
	/**
	 * Returns the predicate that describes which
	 * records should be in the output table.
	 *
	 * @return the suadb.query predicate
	 */
	public Predicate predicate()
	{
		return predicate;
	}
	
	public String toString()
	{
		return "filter(" + array.srcArrayName() + ", " + predicate.toString() + ")";
	}
}
