package suadb.parse;
/**
 * Created by Rony on 2016-12-08.
 */
public class IntNullConstant extends IntConstant implements NullableConstant
{
	public IntNullConstant()
	{
		super(0);
	}
	
	@Override
	public boolean equals(Object obj)
	{
		if(obj == null)     return true;
		if(obj instanceof IntNullConstant)  return true;
		
		return false;
	}
	
	@Override
	public int compareTo(Constant c)
	{
		if(equals(c))   return 0;
		
		// All other values are larger than NULL
		return 1;
	}
	
	@Override
	public String toString()
	{
		return "NULL";
	}
}
