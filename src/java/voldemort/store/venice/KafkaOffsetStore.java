package voldemort.store.venice;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import voldemort.store.bdb.BdbRuntimeConfig;
import voldemort.store.bdb.BdbStorageEngine;

import java.io.File;

/**
 * A dedicated store for committing and persisting all Kafka offset instances.
 */
public class KafkaOffsetStore {

    private static BdbStorageEngine offsetStore;
    private static final String OFFSET_STORE_NAME = "offsets";

    /* Cannot instantiate singleton */
    private KafkaOffsetStore() {

    }

    public static BdbStorageEngine getOffsetStore(String bdbPath) {

        if (null == offsetStore) {
            offsetStore = createOffsetStore(bdbPath);
        }
        return offsetStore;
    }

    /**
     *  Initialize the BDB Database
     * */
    private static BdbStorageEngine createOffsetStore(String bdbPath) {

        BdbRuntimeConfig config = new BdbRuntimeConfig();
        config.setAllowObsoleteWrites(true);

        EnvironmentConfig envCfg = new EnvironmentConfig();
        envCfg.setAllowCreate(true);
        envCfg.setTransactional(true);

        DatabaseConfig dbCfg = new DatabaseConfig();
        dbCfg.setAllowCreate(true);
        dbCfg.setTransactional(true);

        File offsetBdbDir = new File(bdbPath);
        if (!offsetBdbDir.exists()) {
            offsetBdbDir.mkdirs();
        }

        Environment env = new Environment(offsetBdbDir, envCfg);
        Database db = env.openDatabase(null, OFFSET_STORE_NAME, dbCfg);

        return new BdbStorageEngine(OFFSET_STORE_NAME, env, db, config);
    }

}
