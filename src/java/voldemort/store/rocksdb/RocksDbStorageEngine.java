package voldemort.store.rocksdb;

import org.rocksdb.RocksDB;
import voldemort.store.AbstractStorageEngine;
import voldemort.utils.ByteArray;

/**
 * A StorageEngine that uses RocksDB for persistence
 *
 *
 */
public class RocksDbStorageEngine extends AbstractStorageEngine<ByteArray, byte[], byte[]> {
  private RocksDB rdb;

  public RocksDbStorageEngine(String name, RocksDB rdbInstance) {
    super(name);
    this.rdb = rdbInstance;
  }
}