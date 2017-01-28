package voldemort.store.leveldb.leveldbjni;

public class WriteBatch {
  private long self = -1;

  private static native long _new();
  private static native int _put(long h, byte[] key, byte[] value);
  private static native int _delete(long h, byte[] key);
  
  public WriteBatch(){
    self = _new();
  }

  public int put(byte[] key, byte[] value) {
    return _put(self,key,value);
  }

  public int put(String key, String value) {
    return _put(self,key.getBytes(),value.getBytes());
  }

  public int delete(byte[] key) {
    return _delete(self,key);
  }

  public int delete(String key) {
    return _delete(self,key.getBytes());
  }
}