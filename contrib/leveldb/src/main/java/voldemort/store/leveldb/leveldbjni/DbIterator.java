package voldemort.store.leveldb.leveldbjni;

public abstract class DbIterator {

  abstract long pointer();
  private static native void _seekToFirst(long h);
  private static native void _seek(long h,byte[] ba);
  private static native void _next(long h);
  private static native boolean _valid(long h);
  private static native byte[] _key(long h);
  private static native byte[] _value(long h);
  private static native String _status(long h);
  private static native void _close(long h);

  public boolean hasNext() {return _valid(pointer());}
  public void next() {_next(pointer());}
  public void seekToFirst() {_seekToFirst(pointer());}
  public void seek(byte[] ba) {_seek(pointer(), ba);}
  public String status() {return _status(pointer());}
  public byte[] key() {return _key(pointer());}
  public byte[] value() {return _value(pointer());}
  public void close() {_close(pointer());}
}