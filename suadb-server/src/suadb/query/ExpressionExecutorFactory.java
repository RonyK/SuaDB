package suadb.query;

import suadb.parse.BadSyntaxException;
import suadb.parse.ConstantExpression;
import suadb.parse.Expression;
import suadb.parse.FieldNameExpression;
import suadb.record.Schema;

/**
 * Created by Rony on 2016-12-05.
 */
public class ExpressionExecutorFactory
{
	static public ExpressionExecutor createExpressionExecutor(Expression e, Schema schema)
	{
		if(e instanceof ConstantExpression)
		{
			return new ConstantExpressionExecutor(((ConstantExpression) e).val());
		}else if (e instanceof FieldNameExpression)
		{
			if(schema.hasDimension(((FieldNameExpression) e).fldname()))
			{
				return new DimensionExpressionExecutor(((FieldNameExpression) e).fldname());
			} else if(schema.hasField(((FieldNameExpression) e).fldname()))
			{
				return new FieldExpressionExecutor(((FieldNameExpression) e).fldname());
			}else
			{
				throw new BadSyntaxException();
			}
		}else {
			throw new BadSyntaxException();
		}
	}
}
