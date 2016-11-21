package suadb.parse;

/**
 * The operator : scan()
 *
 * Synopsis :
 *      scan( srcArray )
 *
 * Created by Aram on 2016-11-17.
 */
public class ScanData implements QueryData
{
	private QueryData array;

	public ScanData(QueryData array) {
		this.array = array;
	}

	public QueryData array() { return array; }

	public String toString()
	{
		return "scan(" + array.toString() + ")";
	}
}
