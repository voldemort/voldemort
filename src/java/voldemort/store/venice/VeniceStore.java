package voldemort.store.venice;

import org.apache.log4j.Logger;
import voldemort.VoldemortException;
import voldemort.store.DelegatingStore;
import voldemort.store.Store;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A store wrapper
 */
public class VeniceStore<K, V, T> extends DelegatingStore<K, V, T> {

  private static final Logger logger = Logger.getLogger(VeniceStore.class.getName());

  private ExecutorService executor;

  // TODO: These params must match the value given to kakfa, so look into ways that can be done.
  public VeniceStore(Store<K, V, T> store, String topicName, List<String> brokerHosts,
                     int brokerPort, int numKafkaPartitions, int numThreadsPerPartition) {

    super(store);
    VeniceConsumerTask task;

    // Create n consumers for p partitions. Total count will be n * p.
    for (int partition = 0; partition < numKafkaPartitions; partition++) {

      task = new VeniceConsumerTask(this, topicName, partition, brokerHosts, brokerPort);

      // launch each consumer task on a new thread
      executor = Executors.newFixedThreadPool(numThreadsPerPartition);
      for (int i = 0; i < numThreadsPerPartition; i++) {
        executor.submit(task);
      }
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
