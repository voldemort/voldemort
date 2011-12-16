package voldemort.store.leveldb.leveldbjni;

public class ReadOptions{
  private long self = -1;

  private static native long _new();
  private static native boolean _setFillCache(long h, boolean b);
  private static native boolean _setVerifyChecksums(long h, boolean b);
  // private static native boolean _setSnapshot(long h, Snapshot b);
  
  public ReadOptions(){
    self = _new();
  }

  /**
   * Should the data read for this iteration be cached in memory?
   * Callers may wish to set this field to false for bulk scans.
   * Default: true
   */ 
  public void setFillCache(boolean b){
    _setFillCache(self, b);
  }

  /**
   * If true, all data read from underlying storage will be
   * verified against corresponding checksums.
   * Default: false
   */ 
  public void setVerifyChecksums(boolean b){
    _setVerifyChecksums(self, b);
  }

  /**
   * If "snapshot" is non-NULL, read as of the supplied snapshot
   * (which must belong to the DB that is being read and which must
   * not have been released).  If "snapshot" is NULL, use an impliicit
   * snapshot of the state at the beginning of this read operation.
   * Default: NULL
   */
  // public void setSnapshot(Snapshot b){
  //   _setSnapshot(self, b);
  // }
}