package voldemort.store.rocksdb;

import voldemort.store.AbstractStorageEngine;
import voldemort.utils.ByteArray;

/**
 * A StorageEngine that uses RocksDB for persistence
 *
 *
 */
public class RocksDbStorageEngine extends AbstractStorageEngine<ByteArray, byte[], byte[]> {
  public RocksDbStorageEngine(String name) {
    super(name);
  }
}