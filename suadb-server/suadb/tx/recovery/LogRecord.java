package suadb.tx.recovery;

import suadb.log.LogMgr;
import suadb.server.SuaDB;

/**
 * The interface implemented by each type of log suadb.record.
 * @author Edward Sciore
 */
public interface LogRecord {
   /**
    * The six different types of log suadb.record
    */
   static final int CHECKPOINT = 0, START = 1,
      COMMIT = 2, ROLLBACK  = 3,
      SETINT = 4, SETSTRING = 5;
   
   static final LogMgr logMgr = SuaDB.logMgr();
   
   /**
    * Writes the suadb.record to the log and returns its LSN.
    * @return the LSN of the suadb.record in the log
    */
   int writeToLog();
   
   /**
    * Returns the log suadb.record's type.
    * @return the log suadb.record's type
    */
   int op();
   
   /**
    * Returns the transaction id stored with
    * the log suadb.record.
    * @return the log suadb.record's transaction id
    */
   int txNumber();
   
   /**
    * Undoes the operation encoded by this log suadb.record.
    * The only log suadb.record types for which this method
    * does anything interesting are SETINT and SETSTRING.
    * @param txnum the id of the transaction that is performing the undo.
    */
   void undo(int txnum);
}