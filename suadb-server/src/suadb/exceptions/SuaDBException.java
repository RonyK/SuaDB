package suadb.exceptions;

/**
 * Created by Rony on 2016-12-09.
 */
public class SuaDBException extends Exception
{
	public SuaDBException()
	{
	}
	
	public SuaDBException(String message)
	{
		super(message);
	}
	
	public SuaDBException(String message, Throwable cause)
	{
		super(message, cause);
	}
	
	public SuaDBException(Throwable cause)
	{
		super(cause);
	}
	
	public SuaDBException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace)
	{
		super(message, cause, enableSuppression, writableStackTrace);
	}
}
