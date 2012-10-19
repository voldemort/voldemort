package voldemort.store.bdb.dataconversion;

import java.io.File;

import org.apache.log4j.Logger;

import voldemort.cluster.Cluster;
import voldemort.xml.ClusterMapper;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

public abstract class AbstractBdbConversion {

    String storeName;
    Database srcDB;
    Environment srcEnv;

    Database dstDB;
    Environment dstEnv;
    Cluster cluster;

    Cursor cursor;

    Logger logger = Logger.getLogger(BdbConvertData.class);

    AbstractBdbConversion(String storeName,
                          String clusterXmlPath,
                          String sourceEnvPath,
                          String destEnvPath,
                          int logFileSize,
                          int nodeMax) throws Exception {
        this.cluster = new ClusterMapper().readCluster(new File(clusterXmlPath));
        this.storeName = storeName;

        // Configure src environment handle
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setTransactional(true);
        envConfig.setAllowCreate(false);
        envConfig.setReadOnly(true);
        envConfig.setCacheSize(1024 * 1024 * 1024);

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(false);
        dbConfig.setSortedDuplicates(areDuplicatesNeededForSrc());
        dbConfig.setReadOnly(true);

        srcEnv = new Environment(new File(sourceEnvPath), envConfig);
        srcDB = srcEnv.openDatabase(null, storeName, dbConfig);

        // Configure dest environment handle
        File newEnvDir = new File(destEnvPath);
        if(!newEnvDir.exists()) {
            newEnvDir.mkdirs();
        }

        envConfig = new EnvironmentConfig();
        envConfig.setTransactional(false);
        envConfig.setAllowCreate(true);
        envConfig.setReadOnly(false);
        envConfig.setCacheSize(1024 * 1024 * 1024);
        envConfig.setConfigParam(EnvironmentConfig.LOG_FILE_MAX,
                                 Long.toString(logFileSize * 1024L * 1024L));
        envConfig.setDurability(Durability.COMMIT_NO_SYNC);

        dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(false);
        dbConfig.setAllowCreate(true);
        dbConfig.setSortedDuplicates(areDuplicatesNeededForDest());
        dbConfig.setDeferredWrite(true);
        dbConfig.setNodeMaxEntries(nodeMax);

        dstEnv = new Environment(newEnvDir, envConfig);
        dstDB = dstEnv.openDatabase(null, storeName, dbConfig);

    }

    public void close() {
        if(cursor != null)
            cursor.close();

        srcDB.close();
        srcEnv.close();

        dstDB.sync();
        dstDB.close();
        dstEnv.close();
    }

    public abstract void transfer() throws Exception;

    public abstract boolean areDuplicatesNeededForSrc();

    public abstract boolean areDuplicatesNeededForDest();
}
