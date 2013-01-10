package voldemort.server;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import voldemort.VoldemortException;
import voldemort.annotations.concurrency.Threadsafe;
import voldemort.server.storage.RepairJob;
import voldemort.store.StorageEngine;
import voldemort.store.Store;
import voldemort.store.slop.SlopStorageEngine;
import voldemort.store.stats.StreamingStats;
import voldemort.utils.ByteArray;
import voldemort.utils.JmxUtils;
import voldemort.utils.Pair;

/**
 * A wrapper class that holds all the server's stores--storage engines, routed
 * stores, and remote stores.
 * 
 * The store repository holds various store types:
 * <ol>
 * <li>Local stores &ndash; These are the local stores used to perform reads and
 * writes on the local node.</li>
 * <li>Storage Engines &ndash; These are also local, but represent the lowest
 * level of the storage chain (no wrappers).</li>
 * <li>Routed Stores &ndash; Writes to these stores will be rerouted to the
 * appropriate nodes.</li>
 * <li>Node Stores &ndash; These represent socket stores for writing to
 * individual nodes.</li>
 * </ol>
 * 
 * 
 */
@Threadsafe
public class StoreRepository {

    /*
     * TODO: Ideally we should just have one set of local stores, but this would
     * mean converting all the store wrappers to StorageEngines, then we could
     * make the getLocal return a storage engine.
     */

    /*
     * The store used for storing slop for future handoff. Transforms are
     * meaningless for slop stores. Hence the transforms byte array will be null
     */
    private volatile SlopStorageEngine slopStore;

    /*
     * Unrouted stores, local to this node
     */
    private final ConcurrentMap<String, Store<ByteArray, byte[], byte[]>> localStores;

    /*
     * Local storage engine for this node. This is lowest level persistence
     * abstraction, these StorageEngines provide an iterator over their values.
     */
    private final ConcurrentMap<String, StorageEngine<ByteArray, byte[], byte[]>> storageEngines;

    /**
     * Aggregated statistics about streaming operations
     */
    private StreamingStats aggregatedStreamStats;
    /**
     * Maintains statistics about streaming reads/writes performed against all
     * the local storage engines in this node
     */
    private ConcurrentMap<String, StreamingStats> streamingStatsMap;

    /*
     * Routed stores that write and read from multiple nodes
     */
    private final ConcurrentMap<String, Store<ByteArray, byte[], byte[]>> routedStores;

    /*
     * Stores that connect to a single node only and represent a direct
     * connection to the storage on that node
     */
    private final ConcurrentMap<Pair<String, Integer>, Store<ByteArray, byte[], byte[]>> nodeStores;

    /*
     * Stores that add redirectingSocketStores
     */
    private final ConcurrentMap<Pair<String, Integer>, Store<ByteArray, byte[], byte[]>> redirectingSocketStores;

    /*
     * Repair Job object registered with StoreRepository
     */
    private RepairJob repairJob;

    /**
     * Constructor invoked by tests
     */
    public StoreRepository() {
        this(true);
    }

    public StoreRepository(boolean jmxEnabled) {
        super();
        this.localStores = new ConcurrentHashMap<String, Store<ByteArray, byte[], byte[]>>();
        this.storageEngines = new ConcurrentHashMap<String, StorageEngine<ByteArray, byte[], byte[]>>();
        if(jmxEnabled) {
            this.streamingStatsMap = new ConcurrentHashMap<String, StreamingStats>();
            this.aggregatedStreamStats = new StreamingStats();
        }
        this.routedStores = new ConcurrentHashMap<String, Store<ByteArray, byte[], byte[]>>();
        this.nodeStores = new ConcurrentHashMap<Pair<String, Integer>, Store<ByteArray, byte[], byte[]>>();
        this.redirectingSocketStores = new ConcurrentHashMap<Pair<String, Integer>, Store<ByteArray, byte[], byte[]>>();
    }

    public boolean hasLocalStore(String name) {
        return this.localStores.containsKey(name);
    }

    public Store<ByteArray, byte[], byte[]> getLocalStore(String storeName) {
        return localStores.get(storeName);
    }

    public Store<ByteArray, byte[], byte[]> removeLocalStore(String storeName) {
        return localStores.remove(storeName);
    }

    public void addLocalStore(Store<ByteArray, byte[], byte[]> store) {
        Store<ByteArray, byte[], byte[]> found = this.localStores.putIfAbsent(store.getName(),
                                                                              store);
        if(found != null)
            throw new VoldemortException("Store '" + store.getName()
                                         + "' has already been initialized.");
    }

    public List<Store<ByteArray, byte[], byte[]>> getAllLocalStores() {
        return new ArrayList<Store<ByteArray, byte[], byte[]>>(this.localStores.values());
    }

    public boolean hasStorageEngine(String name) {
        return this.storageEngines.containsKey(name);
    }

    public StorageEngine<ByteArray, byte[], byte[]> getStorageEngine(String storeName) {
        return this.storageEngines.get(storeName);
    }

    public void addStorageEngine(StorageEngine<ByteArray, byte[], byte[]> engine) {
        StorageEngine<ByteArray, byte[], byte[]> found = this.storageEngines.putIfAbsent(engine.getName(),
                                                                                         engine);
        if(found != null)
            throw new VoldemortException("Storage Engine '" + engine.getName()
                                         + "' has already been initialized.");

        // register streaming stats object for the store
        if(streamingStatsMap != null) {
            // lazily register the aggregated mbean
            if(storageEngines.size() == 1) {
                JmxUtils.registerMbean(aggregatedStreamStats,
                                       JmxUtils.createObjectName(this.getClass().getCanonicalName(),
                                                                 "aggregated-streaming-stats"));
            }

            StreamingStats stat = new StreamingStats(aggregatedStreamStats);
            JmxUtils.registerMbean(stat, JmxUtils.createObjectName(this.getClass()
                                                                       .getCanonicalName(),
                                                                   engine.getName()
                                                                           + "-streaming-stats"));
            streamingStatsMap.putIfAbsent(engine.getName(), stat);
        }
    }

    public Store<ByteArray, byte[], byte[]> removeStorageEngine(String storeName) {
        // register streaming stats object for the store
        if(streamingStatsMap != null) {
            JmxUtils.unregisterMbean(JmxUtils.createObjectName(this.getClass().getCanonicalName(),
                                                               storeName));
            streamingStatsMap.remove(storeName);
            // lazily unregister the aggregated mbean
            if(storageEngines.size() == 1) {
                JmxUtils.unregisterMbean(JmxUtils.createObjectName(this.getClass()
                                                                       .getCanonicalName(),
                                                                   "aggregated-streaming-stats"));
            }
        }
        return this.storageEngines.remove(storeName);
    }

    public List<StorageEngine<ByteArray, byte[], byte[]>> getAllStorageEngines() {
        return new ArrayList<StorageEngine<ByteArray, byte[], byte[]>>(this.storageEngines.values());
    }

    public List<StorageEngine<ByteArray, byte[], byte[]>> getStorageEnginesByClass(Class<? extends StorageEngine<?, ?, ?>> c) {
        List<StorageEngine<ByteArray, byte[], byte[]>> l = new ArrayList<StorageEngine<ByteArray, byte[], byte[]>>();
        for(StorageEngine<ByteArray, byte[], byte[]> engine: this.storageEngines.values())
            if(engine.getClass().equals(c))
                l.add(engine);
        return l;
    }

    public boolean hasRoutedStore(String name) {
        return this.routedStores.containsKey(name);
    }

    public Store<ByteArray, byte[], byte[]> getRoutedStore(String storeName) {
        return routedStores.get(storeName);
    }

    public void addRoutedStore(Store<ByteArray, byte[], byte[]> store) {
        Store<ByteArray, byte[], byte[]> found = this.routedStores.putIfAbsent(store.getName(),
                                                                               store);
        if(found != null)
            throw new VoldemortException("Store '" + store.getName()
                                         + "' has already been initialized.");
    }

    public List<Store<ByteArray, byte[], byte[]>> getAllRoutedStores() {
        return new ArrayList<Store<ByteArray, byte[], byte[]>>(this.routedStores.values());
    }

    public Store<ByteArray, byte[], byte[]> removeRoutedStore(String storeName) {
        return this.routedStores.remove(storeName);
    }

    public boolean hasNodeStore(String name, int nodeId) {
        return this.nodeStores.containsKey(Pair.create(name, nodeId));
    }

    public Store<ByteArray, byte[], byte[]> getNodeStore(String storeName, Integer id) {
        return nodeStores.get(Pair.create(storeName, id));
    }

    public Store<ByteArray, byte[], byte[]> removeNodeStore(String storeName, int nodeId) {
        return this.nodeStores.remove(Pair.create(storeName, nodeId));
    }

    public void addNodeStore(int nodeId, Store<ByteArray, byte[], byte[]> store) {
        Pair<String, Integer> key = Pair.create(store.getName(), nodeId);
        Store<ByteArray, byte[], byte[]> found = this.nodeStores.putIfAbsent(key, store);
        if(found != null)
            throw new VoldemortException("Store '" + store.getName() + "' for node " + nodeId
                                         + " has already been initialized.");
    }

    public List<Pair<Integer, Store<ByteArray, byte[], byte[]>>> getAllNodeStores() {
        List<Pair<Integer, Store<ByteArray, byte[], byte[]>>> vals = new ArrayList<Pair<Integer, Store<ByteArray, byte[], byte[]>>>();
        for(Map.Entry<Pair<String, Integer>, Store<ByteArray, byte[], byte[]>> entry: this.nodeStores.entrySet())
            vals.add(Pair.create(entry.getKey().getSecond(), entry.getValue()));
        return vals;
    }

    public boolean hasRedirectingSocketStore(String name, int nodeId) {
        return this.redirectingSocketStores.containsKey(Pair.create(name, nodeId));
    }

    public Store<ByteArray, byte[], byte[]> getRedirectingSocketStore(String storeName, Integer id) {
        return redirectingSocketStores.get(Pair.create(storeName, id));
    }

    public void addRedirectingSocketStore(int nodeId, Store<ByteArray, byte[], byte[]> store) {
        Pair<String, Integer> key = Pair.create(store.getName(), nodeId);
        Store<ByteArray, byte[], byte[]> found = this.redirectingSocketStores.putIfAbsent(key,
                                                                                          store);
        if(found != null)
            throw new VoldemortException("Store '" + store.getName() + "' for node " + nodeId
                                         + " has already been initialized.");
    }

    public SlopStorageEngine getSlopStore() {
        if(this.slopStore == null)
            throw new IllegalStateException("Slop store has not been set!");
        return this.slopStore;
    }

    public void setSlopStore(SlopStorageEngine slopStore) {
        this.slopStore = slopStore;
    }

    public boolean hasSlopStore() {
        return this.slopStore != null;
    }

    public RepairJob getRepairJob() {
        return repairJob;
    }

    public void registerRepairJob(RepairJob job) {
        repairJob = job;
    }

    public StreamingStats getStreamingStats(String store) {
        return streamingStatsMap.get(store);
    }
}
