package voldemort.store.rocksdb;

import voldemort.routing.RoutingStrategy;
import voldemort.store.StorageConfiguration;
import voldemort.store.StorageEngine;
import voldemort.store.StoreDefinition;
import voldemort.utils.ByteArray;

public class RocksDbStorageConfiguration implements StorageConfiguration {
  @Override
  public StorageEngine<ByteArray, byte[], byte[]> getStore(StoreDefinition storeDef, RoutingStrategy strategy) {
    return null;
  }

  @Override
  public String getType() {
    return null;
  }

  @Override
  public void update(StoreDefinition storeDef) {
  
  }

  @Override
  public void close() {
  
  }

  @Override
  public void removeStorageEngine(StorageEngine<ByteArray, byte[], byte[]> engine) {
  
  }
}
