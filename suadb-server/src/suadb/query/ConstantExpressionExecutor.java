package suadb.query;

import suadb.parse.Constant;
import suadb.record.Schema;

/**
 * Created by Rony on 2016-12-05.
 */
public class ConstantExpressionExecutor implements ExpressionExecutor
{
	private Constant val;
	
	public ConstantExpressionExecutor(Constant val)
	{
		this.val = val;
	}
	
	@Override
	public boolean isConstant()
	{
		return true;
	}
	
	@Override
	public boolean isFieldName()
	{
		return false;
	}
	
	@Override
	public boolean isDimensionName()
	{
		return false;
	}
	
	@Override
	public Constant asConstant()
	{
		return val;
	}
	
	@Override
	public String asFieldName()
	{
		throw new ClassCastException();
	}
	
	@Override
	public String asDimensionName()
	{
		throw new ClassCastException();
	}
	
	@Override
	public Constant evaluate(Scan s)
	{
		return val;
	}
	
	@Override
	public boolean appliesTo(Schema schema)
	{
		return true;
	}
	
	public String toString()
	{
		return val.toString();
	}
}
