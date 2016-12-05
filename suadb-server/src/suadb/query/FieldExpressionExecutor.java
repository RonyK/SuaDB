package suadb.query;

import suadb.parse.Constant;
import suadb.record.Schema;

/**
 * Created by Rony on 2016-12-05.
 */
public class FieldExpressionExecutor implements ExpressionExecutor
{
	private String fieldName;
	
	public FieldExpressionExecutor(String fieldName)
	{
		this.fieldName = fieldName;
	}
	
	@Override
	public boolean isConstant()
	{
		return false;
	}
	
	@Override
	public boolean isFieldName()
	{
		return true;
	}
	
	@Override
	public boolean isDimensionName()
	{
		return false;
	}
	
	@Override
	public Constant asConstant()
	{
		throw new ClassCastException();
	}
	
	@Override
	public String asFieldName()
	{
		return fieldName;
	}
	
	@Override
	public String asDimensionName()
	{
		throw new ClassCastException();
	}
	
	@Override
	public Constant evaluate(Scan s)
	{
		return s.getVal(fieldName);
	}
	
	@Override
	public boolean appliesTo(Schema schema)
	{
		return schema.hasField(fieldName);
	}
	
	public String toString()
	{
		return fieldName;
	}
}
