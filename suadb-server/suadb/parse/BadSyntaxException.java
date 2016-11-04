package suadb.parse;

/**
 * A runtime exception indicating that the submitted suadb.query
 * has incorrect syntax.
 * @author Edward Sciore
 */
@SuppressWarnings("serial")
public class BadSyntaxException extends RuntimeException {
   public BadSyntaxException() {
   }
}
