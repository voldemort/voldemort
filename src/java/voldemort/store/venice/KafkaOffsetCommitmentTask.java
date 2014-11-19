package voldemort.store.venice;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import org.apache.log4j.Logger;
import voldemort.store.bdb.BdbRuntimeConfig;
import voldemort.store.bdb.BdbStorageEngine;
import voldemort.utils.ByteArray;
import voldemort.utils.KafkaTopicUtils;
import voldemort.versioning.Versioned;

import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 *  A task which periodically sends its Kafka offsets to a BDB store.
 *  On store startup, the initial offsets are read from this store.
 *
 * */
public class KafkaOffsetCommitmentTask implements Runnable {

    private static final Logger logger = Logger.getLogger(KafkaOffsetCommitmentTask.class.getName());
    private static final String OFFSET_STORE_NAME = "offsets";

    private BdbStorageEngine offsetStore;
    private Map<Integer, Long> partitionOffsetMap;
    private Collection<Integer> partitionIds;
    private int offsetCommitCycle;

    public KafkaOffsetCommitmentTask(String bdbPath, Collection<Integer> partitionIds, VeniceConsumerConfig config) {
        this.partitionIds = partitionIds;
        this.offsetStore = startOffsetStore(bdbPath);
        this.partitionOffsetMap = getInitialOffsets();
        this.offsetCommitCycle = config.getOffsetCommitCycle();
    }

    public void run() {

        while (true) {
            logger.debug("Committing Offset.");
            commitOffsets();
            try {
                Thread.sleep(offsetCommitCycle);
            } catch (InterruptedException e) {
            }
        }
    }

    /**
     *  Initialize the BDB Database
     * */
    private BdbStorageEngine startOffsetStore(String bdbPath) {
        BdbRuntimeConfig config = new BdbRuntimeConfig();
        config.setAllowObsoleteWrites(true);

        EnvironmentConfig envCfg = new EnvironmentConfig();
        envCfg.setAllowCreate(true);
        envCfg.setTransactional(true);

        DatabaseConfig dbCfg = new DatabaseConfig();
        dbCfg.setAllowCreate(true);
        dbCfg.setTransactional(true);

        File offsetBdbDir = new File(bdbPath);
        if (!offsetBdbDir.exists()) {
            offsetBdbDir.mkdirs();
        }

        Environment env = new Environment(offsetBdbDir, envCfg);
        Database db = env.openDatabase(null, OFFSET_STORE_NAME, dbCfg);

        return new BdbStorageEngine(OFFSET_STORE_NAME, env, db, config);
    }

    /**
     * Reads the initial values of the offset store
     * */
    private ConcurrentHashMap<Integer, Long> getInitialOffsets() {

        ConcurrentHashMap<Integer, Long> offsetMap = new ConcurrentHashMap<Integer, Long>();
        for (Integer partition : partitionIds) {
            // try to find the offset in BDB store
            byte[] partitionKey = {partition.byteValue()};
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

        for (Integer partition : partitionOffsetMap.keySet()) {

            byte[] partitionKey = { partition.byteValue() } ;
            long offset = partitionOffsetMap.get(partition);
            offsetStore.put(new ByteArray(partitionKey),
                    new Versioned<byte[]>(KafkaTopicUtils.longToBytes(offset)), null);
        }
    }

    /**
     *  Get the Kafka offset for a given partition
     * */
    public long getOffset(int partition) {
        return partitionOffsetMap.get(partition);
    }

}