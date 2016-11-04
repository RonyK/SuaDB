package suadb.query;

import suadb.record.Schema;

/**
 * An expression consisting entirely of a single constant.
 * @author Edward Sciore
 *
 */
public class ConstantExpression implements Expression {
   private Constant val;
   
   /**
    * Creates a new expression by wrapping a constant.
    * @param c the constant
    */
   public ConstantExpression(Constant c) {
      val = c;
   }
   
   /**
    * Returns true.
    * @see suadb.query.Expression#isConstant()
    */
   public boolean isConstant() {
      return true;
   }
   
   /**
    * Returns false.
    * @see suadb.query.Expression#isFieldName()
    */
   public boolean isFieldName() {
      return false;
   }
   
   /**
    * Unwraps the constant and returns it.
    * @see suadb.query.Expression#asConstant()
    */
   public Constant asConstant() {
      return val;
   }
   
   /**
    * This method should never be called.
    * Throws a ClassCastException.
    * @see suadb.query.Expression#asFieldName()
    */
   public String asFieldName() {
      throw new ClassCastException();
   }
   
   /**
    * Returns the constant, regardless of the scan.
    * @see suadb.query.Expression#evaluate(suadb.query.Scan)
    */
   public Constant evaluate(Scan s) {
      return val;
   }
   
   /**
    * Returns true, because a constant applies to any schema.
    * @see suadb.query.Expression#appliesTo(suadb.record.Schema)
    */
   public boolean appliesTo(Schema sch) {
      return true;
   }
   
   public String toString() {
      return val.toString();
   }
}
