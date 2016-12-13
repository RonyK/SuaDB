package suadb.exceptions;

/**
 * Created by Rony on 2016-12-09.
 */
public class CannotComparableException extends SuaDBException
{
	public CannotComparableException()
	{
	}
	
	public CannotComparableException(String message)
	{
		super(message);
	}
	
	public CannotComparableException(String message, Throwable cause)
	{
		super(message, cause);
	}
	
	public CannotComparableException(Throwable cause)
	{
		super(cause);
	}
	
	public CannotComparableException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace)
	{
		super(message, cause, enableSuppression, writableStackTrace);
	}
}
