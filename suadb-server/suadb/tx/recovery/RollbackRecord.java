package suadb.tx.recovery;

import suadb.log.BasicLogRecord;

/**
 * The ROLLBACK log suadb.record.
 * @author Edward Sciore
 */
class RollbackRecord implements LogRecord {
   private int txnum;
   
   /**
    * Creates a new rollback log suadb.record for the specified transaction.
    * @param txnum the ID of the specified transaction
    */
   public RollbackRecord(int txnum) {
      this.txnum = txnum;
   }
   
   /**
    * Creates a log suadb.record by reading one other value from the log.
    * @param rec the basic log suadb.record
    */
   public RollbackRecord(BasicLogRecord rec) {
      txnum = rec.nextInt();
   }
   
   /** 
    * Writes a rollback suadb.record to the log.
    * This log suadb.record contains the ROLLBACK operator,
    * followed by the transaction id.
    * @return the LSN of the last log value
    */
   public int writeToLog() {
      Object[] rec = new Object[] {ROLLBACK, txnum};
      return logMgr.append(rec);
   }
   
   public int op() {
      return ROLLBACK;
   }
   
   public int txNumber() {
      return txnum;
   }
   
   /**
    * Does nothing, because a rollback suadb.record
    * contains no undo information.
    */
   public void undo(int txnum) {}
   
   public String toString() {
      return "<ROLLBACK " + txnum + ">";
   }
}
