package voldemort.store.venice;

import org.apache.log4j.Logger;
import voldemort.VoldemortException;
import voldemort.store.DelegatingStore;
import voldemort.store.Store;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

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
    private KafkaConsumerConfig consumerConfig;
    private VeniceConsumerTask task;

    private Map<Integer, Long> partitionOffsetMap = new ConcurrentHashMap<Integer, Long>();

    public VeniceStore(Store<K, V, T> store, KafkaConsumerConfig consumerConfig) {
        super(store);
        this.consumerConfig = consumerConfig;
        startUpConsumers();
    }

    // TODO: These params must match the value given to kakfa, so look into ways that can be done.
    // should we be reading Kafka config files?
    // or creating a wrapper script that starts Kafka and Venice together?
    public void startUpConsumers() {

        // Create n consumers for p partitions. Total count will be n * p.
        for (int partition = 0; partition < consumerConfig.getKafkaPartitionReplicaCount(); partition++) {

            if (partitionOffsetMap.containsKey(partition)) {
                task = new VeniceConsumerTask(this, consumerConfig, partition, partitionOffsetMap.get(partition));
            } else {
                task = new VeniceConsumerTask(this, consumerConfig, partition);
                partitionOffsetMap.put(partition, new Long(0));
            }

            // launch each consumer task on a new thread
            executor = Executors.newFixedThreadPool(consumerConfig.getKafkaPartitionNumThreads());
            for (int i = 0; i < consumerConfig.getKafkaPartitionNumThreads(); i++) {
                executor.submit(task);
            }
        }
    }

    /**
     *  Updates the offset for the corresponding partition
     * */
    public void updatePartitionOffset(int partition, long newOffset) {
        // using concurrent hashmap here to handle parallelism
        partitionOffsetMap.put(partition, newOffset);
        logger.debug("Updating read partition: " + partition + ", " + newOffset);
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
