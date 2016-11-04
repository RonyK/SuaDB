package suadb.materialize;

import suadb.query.*;

/**
 * The interface implemented by aggregation functions.
 * Aggregation functions are used by the <i>groupby</i> operator.
 * @author Edward Sciore
 */
public interface AggregationFn {
   
   /**
    * Uses the current suadb.record of the specified scan
    * to be the first suadb.record in the group.
    * @param s the scan to aggregate over.
    */
   void processFirst(Scan s);
   
   /**
    * Uses the current suadb.record of the specified scan
    * to be the next suadb.record in the group.
    * @param s the scan to aggregate over.
    */
   void processNext(Scan s);
   
   /**
    * Returns the name of the new aggregation field.
    * @return the name of the new aggregation field
    */
   String fieldName();
   
   /**
    * Returns the computed aggregation value.
    * @return the computed aggregation value
    */
   Constant value();
}
