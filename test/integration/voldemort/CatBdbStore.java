package voldemort;

import java.io.File;
import java.util.Iterator;

import voldemort.serialization.StringSerializer;
import voldemort.store.Entry;
import voldemort.store.StorageEngine;
import voldemort.store.bdb.BdbStorageEngine;
import voldemort.store.serialized.SerializingStorageEngine;
import voldemort.utils.ByteArray;
import voldemort.utils.Utils;
import voldemort.versioning.Versioned;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

public class CatBdbStore {

    public static void main(String[] args) throws Exception {
        if(args.length != 1)
            Utils.croak("USAGE: java " + CatBdbStore.class.getName() + " bdb_dir");

        EnvironmentConfig environmentConfig = new EnvironmentConfig();
        environmentConfig = new EnvironmentConfig();
        environmentConfig.setTxnNoSync(true);
        environmentConfig.setAllowCreate(true);
        environmentConfig.setTransactional(true);
        Environment environment = new Environment(new File(args[0]), environmentConfig);
        DatabaseConfig databaseConfig = new DatabaseConfig();
        databaseConfig.setAllowCreate(true);
        databaseConfig.setTransactional(true);
        databaseConfig.setSortedDuplicates(false);
        Database database = environment.openDatabase(null, "test", databaseConfig);
        StorageEngine<ByteArray, byte[]> store = new BdbStorageEngine("test", environment, database);
        StorageEngine<String, String> stringStore = new SerializingStorageEngine<String, String>(store,
                                                                                                 new StringSerializer(),
                                                                                                 new StringSerializer());
        Iterator<Entry<String, Versioned<String>>> iter = stringStore.entries();
        while(iter.hasNext()) {
            Entry<String, Versioned<String>> entry = iter.next();
            System.out.println(entry.getKey() + " => " + entry.getValue().getValue());
        }
    }
}
