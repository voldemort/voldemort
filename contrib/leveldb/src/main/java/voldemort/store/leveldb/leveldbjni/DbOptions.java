package voldemort.store.leveldb.leveldbjni;
public class DbOptions{
  public static int kNoCompression = 0x0;
  public static int kSnappyCompression = 0x1;

  private static native boolean _setCreateIfMissing(long h, boolean b);
  private static native boolean _setErrorIfExists(long h,boolean b);
  private static native boolean _setParanoidChecks(long h,boolean b);
  private static native boolean _setWriteBufferSize(long h,long b);
  private static native boolean _setMaxOpenFile(long h,int b);
  private static native boolean _setLRUBlockCache(long h,long lrusize);
  private static native boolean _setBlockSize(long h,int b);
  private static native boolean _setBlockRestartInterval(long h,int b);
  private static native boolean _setCompressionType(long h,int b);

  static {
    System.loadLibrary("ldb");
  }


  private long self = -1;
  private static native long _new();

  public DbOptions(){
    self = _new();
  }

  public void setCreateIfMissing(boolean b){
    _setCreateIfMissing(self, b);
  }

  public void setErrorIfExists(boolean b){
    _setErrorIfExists(self, b);
  }

  public void setParanoidChecks(boolean b){
    _setParanoidChecks(self, b);
  }

  /**
   * Amount of data to build up in memory (backed by an unsorted log
   * on disk) before converting to a sorted on-disk file.
   * 
   * Larger values increase performance, especially during bulk loads.
   * Up to two write buffers may be held in memory at the same time,
   * so you may wish to adjust this parameter to control memory usage.
   * Also, a larger write buffer will result in a longer recovery time
   * the next time the database is opened.
   */
  public void setWriteBufferSize(long s){
    _setWriteBufferSize(self,s);
  }

  /**
   * Number of open files that can be used by the DB.  You may need to
   * increase this if your database has a large working set (budget
   * one open file per 2MB of working set).
   */
  public void setMaxOpenFile(int b){
    _setMaxOpenFile(self, b);
  }

  public void setLRUBlockCache(long size){
    _setLRUBlockCache(self, size);
  }

  public void setBlockSize(int b){
    _setBlockSize(self, b);
  }

  public void setBlockRestartInterval(int b){
    _setBlockRestartInterval(self, b);
  }

  public void setCompressionType(int b){
    _setCompressionType(self, b);
  }

}