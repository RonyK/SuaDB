package suadb.parse;

/**
 * The operator : join();
 *
 * Synopsis :
 *      join( left_array, right_array )
 *
 *      -----------------------------------------------------------------------
 *      |                         |   Cell of Array B                         |
 *      |                         |-------------------------------------------|
 *      |                         | Empty             | Non-Empty             |
 *      --------------------------|-------------------------------------------|
 *      | Cell of | Empty         | Result contains   | Result contains       |
 *      | Array A |               | EMPTY cell        | EMPTY cell            |
 *      |         |---------------|-------------------------------------------|
 *      |         | Non-Empty     | Result contains   | Result cell contains  |
 *      |         |               | EMPTY cell        | attributes of both    |
 *      |         |               |                   | A and B               |
 *      -----------------------------------------------------------------------
 *
 *      During the operation, compatbility of dimensions means the following :
 *          - Same number of dimensions
 *          - Same dimension boundaries for each corresponding pair
 *          - Same starting index value for each corresponding pair
 *          - Chunk size may differ
 *
 * Created by Rony on 2016-12-12.
 */
public class JoinData implements QueryData
{
	private QueryData leftArray;
	private QueryData rightArray;
	
	public JoinData(QueryData leftArray, QueryData rightArray)
	{
		this.leftArray = leftArray;
		this.rightArray = rightArray;
	}
	
	public QueryData leftArray()
	{
		return leftArray;
	}
	
	public QueryData rightArray()
	{
		return rightArray;
	}
	
	public String toString()
	{
		return String.format("join(%s,%s)", leftArray.toString(), rightArray.toString());
	}
}
