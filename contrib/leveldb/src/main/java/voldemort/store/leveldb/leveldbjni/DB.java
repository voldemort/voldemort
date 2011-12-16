package voldemort.store.leveldb.leveldbjni;

import java.util.concurrent.*;
import java.util.Arrays;


public class DB {
  private long self = -1;
  private final ExecutorService pool;
  private final int poolsize;

  private final boolean iterable;
  private final DbIterator[] iterators;
  private static native long _open(DbOptions o, String path);
  private static native int _put(long h, WriteOptions o, byte[] key, byte[] value);
  private static native int _delete(long h, WriteOptions wo, byte[] key);
  private static native byte[] _get(long h, ReadOptions ro, byte[] key);
  private static native int _write(long h, WriteOptions wo, WriteBatch b);
  private static native void _close(long h);
  private static native long _newIterator(long h, ReadOptions o);
  private static native String _getProperty(long h, String propertyName);
  private static native int _destroyDB(String name, DbOptions o);
  private static native void _compactRange(long h, byte[] start, byte[] end);

  static {
    System.loadLibrary("ldb");
  }
  /**
   * @param   poolSize  Number of worker in the thread pool (ie potential thread 
   * running concurrently on leveldb). LevelDB is not optimized for concurrent
   * access and is full of locks. We noticed better performances with 2 or 3 
   * workers only on a single 4 core cpu. Use 0 for no thread pool.
   * @param   iterable  Should we use the iterator API instead of the regular DB.get api.
   * We noticed that using the iterator api helps to prevent periodic 
   * slow down for gets
   * 
   * @See Gettable.call()
   */
  public DB(DbOptions o,String path, int poolSize, boolean iterable) 
    throws Exception 
  {
    self = _open(o, path);
    if(self == 0) throw new Exception("dbInit");
    this.poolsize = poolSize;
    if(poolsize > 0)
      pool = Executors.newFixedThreadPool(poolsize);
    else pool = null;
    this.iterable = iterable;
    iterators = new DbIterator[poolsize];
    tcount = 0;


    DbIterator it = newIterator(new ReadOptions());
    int count = 0;
    long ksize = 0l;
    long vsize = 0l;
    for (it.seekToFirst(); it.hasNext(); it.next()) {
      count++;
      ksize += it.key().length;
      vsize += it.value().length;
    }
    System.out.println(String.format("Num keys : %s",count));
    System.out.println(String.format("Key size : %s Mb",
      ((double)ksize) / 1024 / 1024));
    System.out.println(String.format("value size : %s Mb",
      ((double)vsize) / 1024 / 1024));
    System.out.println(getProperty("leveldb.stats"));
  }

  public void close() {
    _close(self);
    self = 0;
    if(pool != null)
      pool.shutdown();
  }

  public String getProperty(String property) {
    return _getProperty(self, property);
  }

  public void compactDB() {_compactRange(self,null,null);}

  public static int destroyDB(DbOptions dbo, String path) {
    return _destroyDB(path,dbo);
  }

  public DbIterator newIterator(ReadOptions ro){
    final long itp = _newIterator(self, ro);
    if(itp!=0)
      return new DbIterator(){
        @Override long pointer() {return itp;}
      };
    else return null;
  }

  final Object lock = new Object();
  static int tcount = 0;
  class Gettable implements Callable<byte[]> {
    byte[] key;
    long h;
    ReadOptions ro;
    
    Gettable(ReadOptions ro, byte[] key, long h) { 
      this.ro = ro; this.key = key;this.h = h; 
    }
    public byte[] call() {
      if(iterable){
        DbIterator it = null;
        try{
          it = newIterator(ro);
          it.seek(key);
          if(it.hasNext() && Arrays.equals(it.key(),key)){
            return it.value();
          }else return null;
        }catch(Exception e){
          e.printStackTrace();
          return null;
        }finally{
          if(it!=null)
            it.close();
        }
      }else
        return _get(h,ro,key);
    }
  }

  /**
   * Pooled get, if we don't use that we'll fall on LevelDB's single mutex and
   * waste a lot of time on that for larger amount of client thread (20-40+)
   */
  public byte[] pget(ReadOptions ro, byte[] key) {
    try{
      return pool.submit(new Gettable(ro, key,self)).get();
    }catch(Exception e){
      // System.exit(1);
      e.printStackTrace();
      return null;
    }
  }

  class Puttable implements Callable<Integer> {
    byte[] key;
    byte[] value;
    long h;
    WriteOptions o;
    Puttable(WriteOptions o,byte[] key,byte[] value, long h) { 
      this.key = key; 
      this.value = value;
      this.h = h;
      this.o = o; 
    }
    public Integer call() {
      return _put(h,o,key,value);
    }
  }
  public int pput(WriteOptions o, byte[] key, byte[] value) {
    try{
      return pool.submit(new Puttable(o,key,value,self)).get();
    }catch(Exception e){
      e.printStackTrace();
      return -1;
    }
  }

  public int put(byte[] key, byte[] value) {
    return put(null,key,value);
  }

  public int put(String key, String value) {
    return put(null,key,value);
  }

  public int delete(byte[] key) {
    return _delete(self,null,key);
  }

  public int delete(String key) {
    return _delete(self,null,key.getBytes());
  }

  public byte[] get(byte[] key) {
    return get(null,key);
  }

  public String get(String key) {
    return get(null,key);
  }

  public int write(WriteBatch b) {
    return _write(self,null,b);
  }


  public int put(WriteOptions o, byte[] key, byte[] value) {
    if(pool == null)
      return _put(self,o,key,value);
    else
      return pput(o,key,value);
  }

  public int put(WriteOptions o, String key, String value) {
    if(pool == null)
      return _put(self,o,key.getBytes(),value.getBytes());
    else
      return pput(o,key.getBytes(),value.getBytes());
  }

  public int delete(WriteOptions o, byte[] key) {
    return _delete(self,o,key);
  }

  public int delete(WriteOptions o, String key) {
    return _delete(self,o,key.getBytes());
  }

  public byte[] get(ReadOptions o, byte[] key) {
    if(pool == null)
      return _get(self,o,key);
    else
      return pget(o,key);
  }

  public String get(ReadOptions o, String key) {
    if(pool == null)
      return new String(_get(self,o,key.getBytes()));
    else
      return new String(pget(o,key.getBytes()));
  }

  public int write(WriteOptions o, WriteBatch b) {
    return _write(self,o,b);
  }


}
