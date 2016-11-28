package suadb.parse;
import java.util.*;
/**
 * The operator : project()
 *
 * Synopsis :
 *      project( srcArray, attribute1, attribute2, ... )
 *
 * Created by Aram on 2016-11-15.
 */
public class ProjectData implements QueryData
{
	private QueryData array;
	private List<String> attributes ;
	/**
	 * Saves the table name and the field and value lists.
	 */
	public ProjectData(QueryData array, List<String> attributes) {
		this.array = array;
		this.attributes = attributes;
	}

	/**
	 * Returns the name of the affected table.
	 * @return the name of the affected table
	 */
	public QueryData array() {
		return array;
	}

	public List<String> getAttributes() {
		return attributes;
	}

	public String toString()
	{
		String result = "project("+ array.toString()+",";
		for (String attname : attributes)
		{
			result += attname + ",";
		}
		result = result.substring(0, result.length() - 2);
		result += ")";
		return result;
	}
}