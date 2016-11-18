package suadb.parse;

/**
 * Created by Aram on 2016-11-18.
 */
public class ArrayData implements QueryData
{
	private String arrayname;
	public ArrayData(String arrayname) {
		this.arrayname = arrayname;
	}
	public String arrayName() {
		return arrayname;
	}
	public String toString() { return arrayname; }
	
	@Override
	public boolean equals(Object obj)
	{
		if (obj instanceof ArrayData)
			return arrayname == ((ArrayData)obj).arrayName();
		
		return false;
	}
}
