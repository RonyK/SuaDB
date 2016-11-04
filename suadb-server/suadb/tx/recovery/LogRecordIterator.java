package suadb.tx.recovery;

import static suadb.tx.recovery.LogRecord.*;
import java.util.Iterator;
import suadb.log.BasicLogRecord;
import suadb.server.SuaDB;

/**
 * A class that provides the ability to read records
 * from the log in reverse order.
 * Unlike the similar class 
 * {@link suadb.log.LogIterator LogIterator},
 * this class understands the meaning of the log records.
 * @author Edward Sciore
 */
class LogRecordIterator implements Iterator<LogRecord> {
   private Iterator<BasicLogRecord> iter = SuaDB.logMgr().iterator();
   
   public boolean hasNext() {
      return iter.hasNext();
   }
   
   /**
    * Constructs a log suadb.record from the values in the
    * current basic log suadb.record.
    * The method first reads an integer, which denotes
    * the type of the log suadb.record.  Based on that type,
    * the method calls the appropriate LogRecord constructor
    * to read the remaining values.
    * @return the next log suadb.record, or null if no more records
    */
   public LogRecord next() {
      BasicLogRecord rec = iter.next();
      int op = rec.nextInt();
      switch (op) {
         case CHECKPOINT:
            return new CheckpointRecord(rec);
         case START:
            return new StartRecord(rec);
         case COMMIT:
            return new CommitRecord(rec);
         case ROLLBACK:
            return new RollbackRecord(rec);
         case SETINT:
            return new SetIntRecord(rec);
         case SETSTRING:
            return new SetStringRecord(rec);
         default:
            return null;
      }
   } 
   
   public void remove() {
      throw new UnsupportedOperationException();
   }
}
