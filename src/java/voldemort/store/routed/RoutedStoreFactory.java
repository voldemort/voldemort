package voldemort.store.routed;

import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
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

    private final Logger logger = Logger.getLogger(getClass());

    public RoutedStoreFactory() {
        this.threadPool = null;
    }

    // TODO using threadPool to simulate nonblocking store isn't a good way and
    // should be deprecated
    // There are a few tests stilling using this path and should be cleaned up
    // They can be identified by examining the call Hierarchy of this method
    @Deprecated
    public RoutedStoreFactory(ExecutorService threadPool) {
        this.threadPool = threadPool;
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
                              FailureDetector failureDetector,
                              RoutedStoreConfig routedStoreConfig) {
        Map<Integer, NonblockingStore> nonblockingStores = Maps.newHashMap();

        for(Map.Entry<Integer, Store<ByteArray, byte[], byte[]>> entry: nodeStores.entrySet())
            nonblockingStores.put(entry.getKey(), toNonblockingStore(entry.getValue()));

        return create(cluster,
                      storeDefinition,
                      nodeStores,
                      nonblockingStores,
                      null,
                      null,
                      failureDetector,
                      routedStoreConfig);
    }

    public RoutedStore create(Cluster cluster,
                              StoreDefinition storeDefinition,
                              Map<Integer, Store<ByteArray, byte[], byte[]>> nodeStores,
                              Map<Integer, NonblockingStore> nonblockingStores,
                              Map<Integer, Store<ByteArray, Slop, byte[]>> slopStores,
                              Map<Integer, NonblockingStore> nonblockingSlopStores,
                              FailureDetector failureDetector,
                              RoutedStoreConfig routedStoreConfig) {
        return new PipelineRoutedStore(nodeStores,
                                       nonblockingStores,
                                       slopStores,
                                       nonblockingSlopStores,
                                       cluster,
                                       storeDefinition,
                                       failureDetector,
                                       routedStoreConfig.getRepairReads(),
                                       routedStoreConfig.getTimeoutConfig(),
                                       routedStoreConfig.getClientZoneId(),
                                       routedStoreConfig.isJmxEnabled(),
                                       routedStoreConfig.getIdentifierString(),
                                       routedStoreConfig.getZoneAffinity());
    }
}
