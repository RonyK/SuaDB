package suadb.parse;

/**
 * Created by Rony on 2016-11-28.
 */
public class ListData implements QueryData
{
	private String target;
	public ListData(String target)
	{
		if (target == null || target.length() == 0)
		{
			this.target = "arrays";
		}
		
		this.target = target;
	}
	
	public String target()
	{
		return target;
	}
	
	public String toString()
	{
		return target;
	}
	
	@Override
	public boolean equals(Object obj)
	{
		if (obj instanceof ListData)
		{
			return this.target.equals(((ListData) obj).target);
		}

		return false;
	}
}
