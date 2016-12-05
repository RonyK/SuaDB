package suadb.query;

import suadb.parse.Constant;
import suadb.record.Schema;

/**
 * Created by Rony on 2016-12-05.
 */
public class DimensionExpressionExecutor implements ExpressionExecutor
{
	private String dimensionName;
	
	public DimensionExpressionExecutor(String dimensionName)
	{
		this.dimensionName = dimensionName;
	}
	
	@Override
	public boolean isConstant()
	{
		return false;
	}
	
	@Override
	public boolean isFieldName()
	{
		return false;
	}
	
	@Override
	public boolean isDimensionName()
	{
		return true;
	}
	
	@Override
	public Constant asConstant()
	{
		return null;
	}
	
	@Override
	public String asFieldName()
	{
		return null;
	}
	
	@Override
	public String asDimensionName()
	{
		return dimensionName;
	}
	
	@Override
	public Constant evaluate(Scan s)
	{
		return s.getDimension(dimensionName);
	}
	
	@Override
	public boolean appliesTo(Schema schema)
	{
		return false;
	}
}
