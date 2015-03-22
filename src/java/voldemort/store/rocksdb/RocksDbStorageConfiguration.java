package voldemort.store.rocksdb;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.util.SizeUnit;

import voldemort.routing.RoutingStrategy;
import voldemort.server.VoldemortConfig;
import voldemort.store.StorageConfiguration;
import voldemort.store.StorageEngine;
import voldemort.store.StorageInitializationException;
import voldemort.store.StoreBinaryFormat;
import voldemort.store.StoreDefinition;
import voldemort.utils.ByteArray;

public class RocksDbStorageConfiguration implements StorageConfiguration {

    static {
        RocksDB.loadLibrary();
    }

    private final int lockStripes;

    public static final String TYPE_NAME = "rocksdb";

    private static Logger logger = Logger.getLogger(RocksDbStorageConfiguration.class);

    private final VoldemortConfig voldemortconfig;

    private Map<String, RocksDbStorageEngine> stores = new HashMap<String, RocksDbStorageEngine>();

    public RocksDbStorageConfiguration(VoldemortConfig config) {
        /**
         * - TODO 1. number of default locks need to debated. This default is
         * same as that of Krati's. 2. Later add the property to VoldemortConfig
         */
        this.voldemortconfig = config;
        this.lockStripes = config.getAllProps().getInt("rocksdb.lock.stripes", 50);
    }

    @Override
    public StorageEngine<ByteArray, byte[], byte[]> getStore(StoreDefinition storeDef,
                                                             RoutingStrategy strategy) {
        String storeName = storeDef.getName();

        if(!stores.containsKey(storeName)) {
            String dataDir = this.voldemortconfig.getRdbDataDirectory() + "/" + storeName;

            new File(dataDir).mkdirs();

            // TODO: Validate those default mandatory options and make them
            // configurable
            try {
                Options rdbOptions = new Options().setCreateIfMissing(true)
                                                  .createStatistics()
                                                  .setWriteBufferSize(8 * SizeUnit.KB)
                                                  .setMaxWriteBufferNumber(3)
                                                  .setMaxBackgroundCompactions(10)
                                                  .setCompressionType(org.rocksdb.CompressionType.SNAPPY_COMPRESSION);

                RocksDB rdbStore = null;
                RocksDbStorageEngine rdbStorageEngine;
                if(this.voldemortconfig.getRocksdbPrefixKeysWithPartitionId()) {
                    rdbOptions.useFixedLengthPrefixExtractor(StoreBinaryFormat.PARTITIONID_PREFIX_SIZE);
                    rdbStore = RocksDB.open(rdbOptions, dataDir);
                    rdbStorageEngine = new PartitionPrefixedRocksDbStorageEngine(storeName,
                                                                                 rdbStore,
                                                                                 lockStripes,
                                                                                 strategy,
                                                                                 voldemortconfig.isRocksdbEnableReadLocks());
                } else {
                    rdbStore = RocksDB.open(rdbOptions, dataDir);
                    rdbStorageEngine = new RocksDbStorageEngine(storeName,
                                                                rdbStore,
                                                                lockStripes,
                                                                voldemortconfig.isRocksdbEnableReadLocks());
                }
                stores.put(storeName, rdbStorageEngine);
            } catch(Exception e) {
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
        for(RocksDbStorageEngine rdbStorageEngine: stores.values()) {
            rdbStorageEngine.getRocksDB().close();
        }

        stores.clear();
    }

    @Override
    public void removeStorageEngine(StorageEngine<ByteArray, byte[], byte[]> engine) {
        RocksDbStorageEngine rdbStorageEngine = (RocksDbStorageEngine) engine;

        rdbStorageEngine.getRocksDB().close();

        stores.remove(rdbStorageEngine.getName());
    }
}
