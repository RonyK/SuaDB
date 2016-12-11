package suadb.parse;

import suadb.query.Region;

/**
 * Created by rony on 2016-12-12.
 */
public class BetweenNaiveData implements QueryData
{
	private QueryData array;
	private Region region;

	public BetweenNaiveData(QueryData array, Region region)
	{
		this.array = array;
		this.region = region;
	}

	public QueryData array()
	{
		return array;
	}

	public Region region()
	{
		return region;
	}

	@Override
	public String toString()
	{
		return "between(" + array.toString() + ", " + region.toString() + ")";
	}
}
