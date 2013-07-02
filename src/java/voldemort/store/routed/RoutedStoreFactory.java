package voldemort.store.routed;

import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.TimeoutConfig;
import voldemort.cluster.Cluster;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.nonblockingstore.NonblockingStore;
import voldemort.store.nonblockingstore.ThreadPoolBasedNonblockingStoreImpl;
import voldemort.store.slop.Slop;
import voldemort.utils.ByteArray;

import com.google.common.collect.Maps;

public class RoutedStoreFactory {

    private ExecutorService threadPool;

    private final RoutedStoreConfig routedStoreConfig;

    private final Logger logger = Logger.getLogger(getClass());

    public RoutedStoreFactory(RoutedStoreConfig routedStoreConfig) {
        this.routedStoreConfig = routedStoreConfig;
        this.threadPool = null;
    }

    @Deprecated
    public RoutedStoreFactory(ExecutorService threadPool, TimeoutConfig timeoutConfig) {
        this.threadPool = threadPool;
        this.routedStoreConfig = new RoutedStoreConfig();
        routedStoreConfig.setTimeoutConfig(timeoutConfig);
    }

    public NonblockingStore toNonblockingStore(Store<ByteArray, byte[], byte[]> store) {
        if(store instanceof NonblockingStore) {
            return (NonblockingStore) store;
        } else if(threadPool == null) {
            throw new VoldemortException("ThreadPool is not initialized. ThreadPool is required in RoutedStoreFactory constructor if using blocking stores");
        } else {
            if(logger.isEnabledFor(Level.WARN)) {
                logger.warn("Using pseudo NonblockingStore implementation for " + store.getClass());
            }
            return new ThreadPoolBasedNonblockingStoreImpl(threadPool, store);
        }
    }

    @Deprecated
    public void setThreadPool(ExecutorService threadPool) {
        this.threadPool = threadPool;
    }

    public RoutedStore create(Cluster cluster,
                              StoreDefinition storeDefinition,
                              Map<Integer, Store<ByteArray, byte[], byte[]>> nodeStores,
                              FailureDetector failureDetector) {
        Map<Integer, NonblockingStore> nonblockingStores = Maps.newHashMap();

        for(Map.Entry<Integer, Store<ByteArray, byte[], byte[]>> entry: nodeStores.entrySet())
            nonblockingStores.put(entry.getKey(), toNonblockingStore(entry.getValue()));

        return create(cluster,
                      storeDefinition,
                      nodeStores,
                      nonblockingStores,
                      null,
                      null,
                      failureDetector);
    }

    public RoutedStore create(Cluster cluster,
                              StoreDefinition storeDefinition,
                              Map<Integer, Store<ByteArray, byte[], byte[]>> nodeStores,
                              Map<Integer, NonblockingStore> nonblockingStores,
                              Map<Integer, Store<ByteArray, Slop, byte[]>> slopStores,
                              Map<Integer, NonblockingStore> nonblockingSlopStores,
                              FailureDetector failureDetector) {

        return new PipelineRoutedStore(nodeStores,
                                       nonblockingStores,
                                       slopStores,
                                       nonblockingSlopStores,
                                       cluster,
                                       storeDefinition,
                                       failureDetector,
                                       routedStoreConfig);
    }
}
