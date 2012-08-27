package voldemort.store.routed;

import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.TimeoutConfig;
import voldemort.cluster.Cluster;
import voldemort.cluster.Zone;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.routing.RoutingStrategyType;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.nonblockingstore.NonblockingStore;
import voldemort.store.nonblockingstore.ThreadPoolBasedNonblockingStoreImpl;
import voldemort.store.slop.Slop;
import voldemort.utils.ByteArray;
import voldemort.utils.SystemTime;

import com.google.common.collect.Maps;

public class RoutedStoreFactory {

    private final boolean isPipelineRoutedStoreEnabled;

    private final ExecutorService threadPool;

    private final TimeoutConfig timeoutConfig;

    private final Logger logger = Logger.getLogger(getClass());

    public RoutedStoreFactory(boolean isPipelineRoutedStoreEnabled,
                              ExecutorService threadPool,
                              TimeoutConfig timeoutConfig) {
        this.isPipelineRoutedStoreEnabled = isPipelineRoutedStoreEnabled;
        this.threadPool = threadPool;
        this.timeoutConfig = timeoutConfig;
    }

    public NonblockingStore toNonblockingStore(Store<ByteArray, byte[], byte[]> store) {
        if(store instanceof NonblockingStore)
            return (NonblockingStore) store;

        if(logger.isEnabledFor(Level.WARN))
            logger.warn("Using pseudo NonblockingStore implementation for " + store.getClass());

        return new ThreadPoolBasedNonblockingStoreImpl(threadPool, store);
    }

    public RoutedStore create(Cluster cluster,
                              StoreDefinition storeDefinition,
                              Map<Integer, Store<ByteArray, byte[], byte[]>> nodeStores,
                              Map<Integer, NonblockingStore> nonblockingStores,
                              Map<Integer, Store<ByteArray, Slop, byte[]>> slopStores,
                              Map<Integer, NonblockingStore> nonblockingSlopStores,
                              boolean repairReads,
                              int clientZoneId,
                              FailureDetector failureDetector) {
        return create(cluster,
                      storeDefinition,
                      nodeStores,
                      nonblockingStores,
                      slopStores,
                      nonblockingSlopStores,
                      repairReads,
                      clientZoneId,
                      failureDetector,
                      false,
                      0);
    }

    public RoutedStore create(Cluster cluster,
                              StoreDefinition storeDefinition,
                              Map<Integer, Store<ByteArray, byte[], byte[]>> nodeStores,
                              Map<Integer, NonblockingStore> nonblockingStores,
                              Map<Integer, Store<ByteArray, Slop, byte[]>> slopStores,
                              Map<Integer, NonblockingStore> nonblockingSlopStores,
                              boolean repairReads,
                              int clientZoneId,
                              FailureDetector failureDetector,
                              boolean jmxEnabled,
                              int jmxId) {
        if(isPipelineRoutedStoreEnabled) {
            return new PipelineRoutedStore(storeDefinition.getName(),
                                           nodeStores,
                                           nonblockingStores,
                                           slopStores,
                                           nonblockingSlopStores,
                                           cluster,
                                           storeDefinition,
                                           repairReads,
                                           clientZoneId,
                                           timeoutConfig,
                                           failureDetector,
                                           jmxEnabled,
                                           jmxId);
        } else {
            if(storeDefinition.getRoutingStrategyType()
                              .compareTo(RoutingStrategyType.ZONE_STRATEGY) == 0) {
                throw new VoldemortException("Zone Routing for store '" + storeDefinition.getName()
                                             + "' not supported using thread pool routed store.");
            }

            if(slopStores != null)
                throw new VoldemortException("Hinted Handoff for store '"
                                             + storeDefinition.getName()
                                             + "' not supported using thread pool routed store.");

            return new ThreadPoolRoutedStore(storeDefinition.getName(),
                                             nodeStores,
                                             cluster,
                                             storeDefinition,
                                             repairReads,
                                             threadPool,
                                             timeoutConfig,
                                             failureDetector,
                                             SystemTime.INSTANCE);
        }
    }

    public RoutedStore create(Cluster cluster,
                              StoreDefinition storeDefinition,
                              Map<Integer, Store<ByteArray, byte[], byte[]>> nodeStores,
                              boolean repairReads,
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
                      repairReads,
                      Zone.DEFAULT_ZONE_ID,
                      failureDetector);
    }

}
