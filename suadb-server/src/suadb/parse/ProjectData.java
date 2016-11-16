package suadb.parse;
import java.util.*;

/**
 * Created by Aram on 2016-11-15.
 */
public class ProjectData
{
	private String arrayname;
	private List<String> attributes ;
	/**
	 * Saves the table name and the field and value lists.
	 */
	public ProjectData(String arrayname, List<String> attributes) {
		this.arrayname = arrayname;
		this.attributes = attributes;
	}

	/**
	 * Returns the name of the affected table.
	 * @return the name of the affected table
	 */
	public String arrayName() {
		return arrayname;
	}

	public List<String> getAttributes() {
		return attributes;
	}

}
