package suadb.parse;

/**
 * Created by Rony on 2016-12-08.
 */
public class StringNullConstant extends StringConstant implements NullableConstant
{
	public StringNullConstant()
	{
		super("NULL");
	}
	
	@Override
	public boolean equals(Object obj)
	{
		if (obj == null)    return true;
		if (obj instanceof StringNullConstant)  return true;
		
		return false;
	}
	
	@Override
	public int compareTo(Constant c)
	{
		if(equals(c))   return 0;
		
		return 1;
	}
}
