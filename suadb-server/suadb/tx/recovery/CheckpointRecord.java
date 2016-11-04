package suadb.tx.recovery;

import suadb.log.BasicLogRecord;

/**
 * The CHECKPOINT log suadb.record.
 * @author Edward Sciore
 */
class CheckpointRecord implements LogRecord {
   
   /**
    * Creates a quiescent checkpoint suadb.record.
    */
   public CheckpointRecord() {}
   
   /**
    * Creates a log suadb.record by reading no other values
    * from the basic log suadb.record.
    * @param rec the basic log suadb.record
    */
   public CheckpointRecord(BasicLogRecord rec) {}
   
   /** 
    * Writes a checkpoint suadb.record to the log.
    * This log suadb.record contains the CHECKPOINT operator,
    * and nothing else.
    * @return the LSN of the last log value
    */
   public int writeToLog() {
      Object[] rec = new Object[] {CHECKPOINT};
      return logMgr.append(rec);
   }
   
   public int op() {
      return CHECKPOINT;
   }
   
   /**
    * Checkpoint records have no associated transaction,
    * and so the method returns a "dummy", negative txid.
    */
   public int txNumber() {
      return -1; // dummy value
   }
   
   /**
    * Does nothing, because a checkpoint suadb.record
    * contains no undo information.
    */
   public void undo(int txnum) {}
   
   public String toString() {
      return "<CHECKPOINT>";
   }
}
