package voldemort.store.rocksdb;

import org.apache.log4j.Logger;
import org.rocksdb.*;
import org.rocksdb.util.SizeUnit;
import voldemort.routing.RoutingStrategy;
import voldemort.server.VoldemortConfig;
import voldemort.store.StorageConfiguration;
import voldemort.store.StorageEngine;
import voldemort.store.StorageInitializationException;
import voldemort.store.StoreDefinition;
import voldemort.utils.ByteArray;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class RocksDbStorageConfiguration implements StorageConfiguration {

  static {
    RocksDB.loadLibrary();
  }

  public static final String TYPE_NAME = "rocksdb";

  private static Logger logger = Logger.getLogger(RocksDbStorageConfiguration.class);

  private String rdbDataDirectory;

  private Map<String, RocksDbStorageEngine> stores = new HashMap<String, RocksDbStorageEngine>();

  public RocksDbStorageConfiguration(VoldemortConfig config) {
    this.rdbDataDirectory = config.getRdbDataDirectory();
  }

  @Override
  public StorageEngine<ByteArray, byte[], byte[]> getStore(StoreDefinition storeDef, RoutingStrategy strategy) {
    String storeName = storeDef.getName();

    if (!stores.containsKey(storeName)) {
      String dataDir = rdbDataDirectory + "/" + storeName;

      new File(dataDir).mkdirs();

      // TODO: Validate those default mandatory options and make them configurable
      Options rdbOptions = new Options().setCreateIfMissing(true)
              .createStatistics()
              .setWriteBufferSize(8 * SizeUnit.KB)
              .setMaxWriteBufferNumber(3)
              .setDisableSeekCompaction(true)
              .setBlockSize(64 * SizeUnit.KB)
              .setMaxBackgroundCompactions(10)
              .setFilter(new BloomFilter(10))
              .setCompressionType(CompressionType.SNAPPY_COMPRESSION);

      try {
        RocksDB rdbStore = RocksDB.open(rdbOptions, dataDir);

        RocksDbStorageEngine rdbStorageEngine = new RocksDbStorageEngine(storeName, rdbStore);

        stores.put(storeName, rdbStorageEngine);
      } catch (Exception e) {
        throw new StorageInitializationException(e);
      }
    }

    return stores.get(storeName);
  }

  @Override
  public String getType() {
    return TYPE_NAME;
  }

  @Override
  public void update(StoreDefinition storeDef) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() {
    for (RocksDbStorageEngine rdbStorageEngine : stores.values()) {
      rdbStorageEngine.getRdb().close();
    }

    stores.clear();
  }

  @Override
  public void removeStorageEngine(StorageEngine<ByteArray, byte[], byte[]> engine) {
    RocksDbStorageEngine rdbStorageEngine = (RocksDbStorageEngine) engine;

    rdbStorageEngine.getRdb().close();

    stores.remove(rdbStorageEngine.getName());
  }
}
