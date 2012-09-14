package voldemort.store.krati;

import java.io.File;

import krati.core.segment.MappedSegmentFactory;
import krati.core.segment.SegmentFactory;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.routing.RoutingStrategy;
import voldemort.server.VoldemortConfig;
import voldemort.store.StorageConfiguration;
import voldemort.store.StorageEngine;
import voldemort.store.StoreDefinition;
import voldemort.utils.ByteArray;
import voldemort.utils.Props;
import voldemort.utils.ReflectUtils;

public class KratiStorageConfiguration implements StorageConfiguration {

    public static final String TYPE_NAME = "krati";

    private static Logger logger = Logger.getLogger(KratiStorageConfiguration.class);

    private final String dataDirectory;
    private final int lockStripes;
    private final int segmentFileSizeMb;
    private final int initLevel;
    private final double hashLoadFactor;
    private final Object lock = new Object();
    private final Class<?> factoryClass;

    public KratiStorageConfiguration(VoldemortConfig config) {
        Props props = config.getAllProps();
        File kratiDir = new File(config.getDataDirectory(), "krati");
        kratiDir.mkdirs();
        this.dataDirectory = kratiDir.getAbsolutePath();
        this.segmentFileSizeMb = props.getInt("krati.segment.filesize.mb", 256);
        this.hashLoadFactor = props.getDouble("krati.load.factor", 0.75);
        this.initLevel = props.getInt("krati.initlevel", 2);
        this.lockStripes = props.getInt("krati.lock.stripes", 50);
        this.factoryClass = ReflectUtils.loadClass(props.getString("krati.segment.factory.class",
                                                                   MappedSegmentFactory.class.getName()));
    }

    public void close() {}

    public StorageEngine<ByteArray, byte[], byte[]> getStore(StoreDefinition storeDef,
                                                             RoutingStrategy strategy) {
        synchronized(lock) {
            File storeDir = new File(dataDirectory, storeDef.getName());
            if(!storeDir.exists()) {
                logger.info("Creating krati data directory '" + storeDir.getAbsolutePath() + "'.");
                storeDir.mkdirs();
            }

            SegmentFactory segmentFactory = (SegmentFactory) ReflectUtils.callConstructor(factoryClass);
            return new KratiStorageEngine(storeDef.getName(),
                                          segmentFactory,
                                          segmentFileSizeMb,
                                          lockStripes,
                                          hashLoadFactor,
                                          initLevel,
                                          storeDir);
        }
    }

    public String getType() {
        return TYPE_NAME;
    }

    public void update(StoreDefinition storeDef) {
        throw new VoldemortException("Storage config updates not permitted for "
                                     + this.getClass().getCanonicalName());
    }
}
