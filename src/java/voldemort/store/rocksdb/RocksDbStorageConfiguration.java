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
            dbOptions.setCreateMissingColumnFamilies(true);
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

            // Create a non default Column Family tp hold the store data.
            List<ColumnFamilyDescriptor> descriptors = new ArrayList<ColumnFamilyDescriptor>();
            descriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOptions));
            descriptors.add(new ColumnFamilyDescriptor(storeName.getBytes(), cfOptions));
            List<ColumnFamilyHandle> handles = new ArrayList<ColumnFamilyHandle>();

            try {
                RocksDB rdbStore = RocksDB.open(dbOptions, dataDir, descriptors, handles);
                // Dispose of the default Column Family immediately.  We don't use it and if it has not been disposed
                // by the time the DB is closed then the RocksDB code can terminate abnormally (if the RocksDB code is
                // built with assertions enabled). The handle will go out of scope on its own and the Java finalizer
                // will (eventually) do this for us, but, that is not fast enough for the unit tests.
                handles.get(0).dispose();
                ColumnFamilyHandle storeHandle = handles.get(1);

                RocksDbStorageEngine rdbStorageEngine;
                if(this.voldemortconfig.getRocksdbPrefixKeysWithPartitionId()) {
                    rdbStorageEngine = new PartitionPrefixedRocksDbStorageEngine(storeName,
                                                                                 rdbStore,
                                                                                 storeHandle,
                                                                                 cfOptions,
                                                                                 lockStripes,
                                                                                 strategy,
                                                                                 voldemortconfig.isRocksdbEnableReadLocks());
                } else {
                    rdbStorageEngine = new RocksDbStorageEngine(storeName,
                                                                rdbStore,
                                                                storeHandle,
                                                                cfOptions,
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
            rdbStorageEngine.close();
        }

        stores.clear();
    }

    @Override
    public void removeStorageEngine(StorageEngine<ByteArray, byte[], byte[]> engine) {
        RocksDbStorageEngine rdbStorageEngine = (RocksDbStorageEngine) engine;

        rdbStorageEngine.close();

        stores.remove(rdbStorageEngine.getName());
    }
}
