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
public class FilterData
{
	private String srcArrayName;
	private Predicate predicate;
	
	public FilterData(String srcArrayName, Predicate predicate)
	{
		this.srcArrayName = srcArrayName;
		this.predicate = predicate;
	}
	
	public String srcArrayName()
	{
		return srcArrayName;
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
		return "filter(" + srcArrayName + ", " + predicate.toString() + ")";
	}
}
