package voldemort.store.venice;

import org.apache.log4j.Logger;
import voldemort.store.bdb.BdbStorageEngine;
import voldemort.utils.ByteArray;
import voldemort.utils.KafkaTopicUtils;
import voldemort.versioning.Versioned;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 *  A task which periodically sends its Kafka offsets to a BDB store.
 *  On store startup, the initial offsets are read from this store.
 *
 *  The key for each offset is "storeName_partition"
 *
 * */
public class KafkaOffsetCommitmentTask implements Runnable {

    private static final Logger logger = Logger.getLogger(KafkaOffsetCommitmentTask.class.getName());

    private Map<Integer, Long> partitionOffsetMap;
    private Collection<Integer> partitionIds;
    private String storeName;
    private String bdbPath;
    private int offsetCommitCycle;

    public KafkaOffsetCommitmentTask(String storeName, Collection<Integer> partitionIds, VeniceConsumerConfig config) {
        this.storeName = storeName;
        this.partitionIds = partitionIds;
        this.bdbPath = config.getOffsetBbdPath();
        this.offsetCommitCycle = config.getOffsetCommitCycle();
        this.partitionOffsetMap = getInitialOffsets();
    }

    public void run() {

        while (true) {
            try {
                Thread.sleep(offsetCommitCycle);
            } catch (InterruptedException e) {
            }
            commitOffsets();
        }
    }

    /**
     * Reads the initial values of the offset store
     * */
    private ConcurrentHashMap<Integer, Long> getInitialOffsets() {

        ConcurrentHashMap<Integer, Long> offsetMap = new ConcurrentHashMap<Integer, Long>();
        BdbStorageEngine offsetStore = KafkaOffsetStore.getOffsetStore(bdbPath);

        for (Integer partition : partitionIds) {

            // try to find the offset in BDB store
            String key = new StringBuffer(storeName).append("_").append(partition).toString();
            byte[] partitionKey = key.getBytes();
            List<Versioned<byte[]>> result = offsetStore.get(new ByteArray(partitionKey), null);

            // initialize if not found
            if (result.isEmpty()) {
                offsetMap.put(partition, new Long(-1));
            } else {
                long offset = KafkaTopicUtils.bytestoLong(result.get(result.size() - 1).getValue());
                logger.info("From offset store, partition " + partition + " last offset at: " + offset);
                offsetMap.put(partition, offset);
            }
        }
        return offsetMap;
    }

    /**
     *  Returns the offset map as an entire object
     * */
    public Map<Integer, Long> getPartitionMap() {
        return partitionOffsetMap;
    }

    /**
     *  Update the Kafka offset; this is called by the consumers
     * */
    public void updateOffset(int partition, long offset) {
        // Using concurrent hashmap to guarantee synchronization
        partitionOffsetMap.put(partition, offset);
    }

    /**
     *  Commit the offsets into storage. This method is called periodically.
     * */
    private void commitOffsets() {

        long totalChange = 0;
        BdbStorageEngine offsetStore = KafkaOffsetStore.getOffsetStore(bdbPath);

        for (Integer partition : partitionOffsetMap.keySet()) {

            String key = new StringBuffer(storeName).append("_").append(partition).toString();
            byte[] partitionKey = key.getBytes();
            long originalOffset = 0;

            List<Versioned<byte[]>> result = offsetStore.get(new ByteArray(partitionKey), null);
            if (result.size() > 0) {
                originalOffset = KafkaTopicUtils.bytestoLong(result.get(result.size() - 1).getValue());
            }

            long offset = partitionOffsetMap.get(partition);
            offsetStore.put(new ByteArray(partitionKey),
                    new Versioned<byte[]>(KafkaTopicUtils.longToBytes(offset)), null);

            totalChange += (offset - originalOffset);
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Committed " + totalChange + " offsets in " + offsetCommitCycle + " ms.");
        }

    }

    /**
     *  Get the Kafka offset for a given partition
     * */
    public long getOffset(int partition) {
        return partitionOffsetMap.get(partition);
    }

}