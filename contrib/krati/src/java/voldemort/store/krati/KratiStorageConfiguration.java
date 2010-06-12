package voldemort.store.krati;

import java.io.File;

import org.apache.log4j.Logger;

import voldemort.server.VoldemortConfig;
import voldemort.store.StorageConfiguration;
import voldemort.store.StorageEngine;
import voldemort.utils.ByteArray;
import voldemort.utils.Props;

public class KratiStorageConfiguration implements StorageConfiguration {

    public static final String TYPE_NAME = "krati";

    private static Logger logger = Logger.getLogger(KratiStorageConfiguration.class);

    private String dataDirectory;
    private int segmentFileSizeMB, initLevel;
    private double hashLoadFactor;
    private final Object lock = new Object();

    public KratiStorageConfiguration(VoldemortConfig config) {
        Props props = config.getAllProps();
        this.dataDirectory = props.getString("krati.segment.datadirectory", "/tmp/kratiSeg");
        this.segmentFileSizeMB = props.getInt("krati.segment.filesize.mb", 256);
        this.hashLoadFactor = props.getDouble("krati.load.factor", 0.75);
        this.initLevel = props.getInt("krati.initlevel", 2);
    }

    public void close() {}

    public StorageEngine<ByteArray, byte[], byte[]> getStore(String storeName) {
        synchronized(lock) {
            File storeDir = new File(dataDirectory, storeName);
            if(!storeDir.exists()) {
                logger.info("Creating Krati data directory '" + storeDir.getAbsolutePath() + ".");
                storeDir.mkdirs();
            }
            return new KratiStorageEngine(storeName,
                                          segmentFileSizeMB,
                                          hashLoadFactor,
                                          initLevel,
                                          storeDir);
        }
    }

    public String getType() {
        return TYPE_NAME;
    }

}
