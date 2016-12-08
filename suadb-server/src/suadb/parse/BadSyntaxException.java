package suadb.parse;

/**
 * A runtime exception indicating that the submitted suadb.query
 * has incorrect syntax.
 * @author Edward Sciore
 */
@SuppressWarnings("serial")
public class BadSyntaxException extends RuntimeException
{
	public static final String STR_DUPLICATE_ATTRIBUTE = "Duplicate attribute";
	public static final String STR_DUPLICATE_DIMENSION = "Duplicate dimension";
	
	public BadSyntaxException()
	{
		
	}
	
	public BadSyntaxException(String message)
	{
		super(message);
	}
	
	public BadSyntaxException(String message, Throwable cause)
	{
		super(message, cause);
	}
}
