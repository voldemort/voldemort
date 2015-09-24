package voldemort.store.rocksdb;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;

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

            Properties dbProperties = parseProperties(VoldemortConfig.ROCKSDB_DB_OPTIONS);
            DBOptions dbOptions = (dbProperties.size() > 0) ?
                    DBOptions.getDBOptionsFromProps(dbProperties) : new DBOptions();
            if (dbOptions == null) {
                throw new StorageInitializationException("Unable to parse Data Base Options.");
            }
            dbOptions.setCreateIfMissing(true);
            dbOptions.createStatistics();

            Properties cfProperties = parseProperties(VoldemortConfig.ROCKSDB_CF_OPTIONS);
            if(this.voldemortconfig.getRocksdbPrefixKeysWithPartitionId()) {
                cfProperties.setProperty("prefix_extractor", "fixed:" + StoreBinaryFormat.PARTITIONID_PREFIX_SIZE);
            }
            ColumnFamilyOptions cfOptions = (cfProperties.size() > 0) ?
                    ColumnFamilyOptions.getColumnFamilyOptionsFromProps(cfProperties) : new ColumnFamilyOptions();
            if (cfOptions == null) {
                throw new StorageInitializationException("Unable to parse Column Family Options.");
            }

            // Create the default Column Family.
            List<ColumnFamilyDescriptor> cfdList = new ArrayList<ColumnFamilyDescriptor>();
            cfdList.add(new ColumnFamilyDescriptor("default".getBytes(), cfOptions));
            List<ColumnFamilyHandle> cfhList = new ArrayList<ColumnFamilyHandle>();

            try {
                RocksDB rdbStore;
                RocksDbStorageEngine rdbStorageEngine;
                if(this.voldemortconfig.getRocksdbPrefixKeysWithPartitionId()) {
                    rdbStore = RocksDB.open(dbOptions, dataDir, cfdList, cfhList);
                    rdbStorageEngine = new PartitionPrefixedRocksDbStorageEngine(storeName,
                                                                                 rdbStore,
                                                                                 lockStripes,
                                                                                 strategy,
                                                                                 voldemortconfig.isRocksdbEnableReadLocks());
                } else {
                    rdbStore = RocksDB.open(dbOptions, dataDir, cfdList, cfhList);
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

    private Properties parseProperties(String prefix) {
        Properties properties = new Properties();
        for (Map.Entry<String, String> entry : voldemortconfig.getAllProps().entrySet()) {
            String key = entry.getKey();
            if (key.startsWith(prefix)) {
                properties.put(key. substring(prefix.length()), entry.getValue());
            }
        }
        return properties;
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
