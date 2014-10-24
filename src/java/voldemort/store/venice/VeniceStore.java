package voldemort.store.venice;

import org.apache.log4j.Logger;
import voldemort.VoldemortException;
import voldemort.store.DelegatingStore;
import voldemort.store.Store;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A store wrapper which allows Kafka messages to be read into Voldemort stores
 */
public class VeniceStore<K, V, T> extends DelegatingStore<K, V, T> {

    private static final Logger logger = Logger.getLogger(VeniceStore.class.getName());

    private ExecutorService executor;
    private VeniceConsumerTask task;

    // offset management
    private Map<Integer, VeniceConsumerTask> partitionTaskMap;
    private Map<Integer, Long> partitionOffsetMap;

    // server level configs
    private List<String> seedBrokers;
    private int port;
    private VeniceConsumerTuning consumerTuning;

    // store level configs
    private String topic;
    private int replicaCount;

    public VeniceStore(Store<K, V, T> store, List<String> seedBrokers, int port, String topic, int replicaCount, VeniceConsumerTuning consumerTuning) {

        super(store);

        this.seedBrokers = seedBrokers;
        this.port = port;
        this.consumerTuning = consumerTuning;
        this.topic = topic;
        this.replicaCount = replicaCount;
        this.partitionOffsetMap = new ConcurrentHashMap<Integer, Long>();
        this.partitionTaskMap = new HashMap<Integer, VeniceConsumerTask>();

        startUpConsumers();
    }

    // TODO: These params must match the value given to kakfa, so look into ways that can be done.
    // should we be reading Kafka config files?
    // or creating a wrapper script that starts Kafka and Venice together?
    public void startUpConsumers() {

        // Create n consumers for p partitions. Total count will be n * p.
        for (int partition = 0; partition < replicaCount; partition++) {

            if (partitionOffsetMap.containsKey(partition)) {
                task = new VeniceConsumerTask(this, seedBrokers, port, topic, partition,
                        partitionOffsetMap.get(partition), consumerTuning);
            } else {
                task = new VeniceConsumerTask(this, seedBrokers, port, topic, partition, consumerTuning);
                partitionOffsetMap.put(partition, new Long(-1));
            }

            // launch each consumer task on a new thread
            executor = Executors.newFixedThreadPool(1);
            executor.submit(task);
            partitionTaskMap.put(partition, task);
        }
    }

    /**
     *  TODO: offset management, and when to trigger such operations
     *  Updates the offset for the corresponding partition.
     *  This can be used by either the consumers or management tools
     * */
    public void updatePartitionOffset(int partition, long newOffset) {
        partitionOffsetMap.put(partition, newOffset);
        logger.debug("Updating read partition: " + partition + ", " + newOffset);
    }

    /**
     *  Pause Kafka consumption; this can be triggered by throttling or hadoop pushes.
     * */
    public void pauseConsumption() {
        // TODO: for conflict resolution, this will have to be implemented on a subset of tasks
        for (int p : partitionTaskMap.keySet()) {
            partitionTaskMap.get(p).setConsumerState(VeniceConsumerState.PAUSED);
        }
    }

    /**
     *  Resume Kafka consumption
     * */
    public void resumeConsumption() {
        // TODO: for conflict resolution, this will have to be implemented on a subset of tasks
        for (int p : partitionTaskMap.keySet()) {
            partitionTaskMap.get(p).setConsumerState(VeniceConsumerState.RUNNING);
        }
    }

    @Override
    public void close() throws VoldemortException {
        executor.shutdown();
        getInnerStore().close();
    }

    @Override
    public boolean delete(K key, Version version) throws VoldemortException {
        return super.delete(key, version);
    }

    @Override
    public Map<K, List<Versioned<V>>> getAll(Iterable<K> keys, Map<K, T> transforms)
            throws VoldemortException {
        // Venice layer does not have properties affecting reads, thus getAll is unaffected
        return super.getAll(keys, transforms);
    }

    @Override
    public List<Versioned<V>> get(K key, T transform) throws VoldemortException {
        // Venice layer does not have properties affecting reads, thus get is unaffected
        return super.get(key, transform);
    }

    @Override
    public void put(K key, Versioned<V> value, T transform) throws VoldemortException {
        super.put(key, value, transform);
    }

}
