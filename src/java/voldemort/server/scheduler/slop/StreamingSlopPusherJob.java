package voldemort.server.scheduler.slop;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.server.StoreRepository;
import voldemort.store.StorageEngine;
import voldemort.store.UnreachableStoreException;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.slop.Slop;
import voldemort.store.slop.SlopStorageEngine;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.EventThrottler;
import voldemort.utils.Pair;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class StreamingSlopPusherJob implements Runnable {

    private final static Logger logger = Logger.getLogger(StreamingSlopPusherJob.class.getName());
    public final static String TYPE_NAME = "streaming";

    private final static Versioned<Slop> END = Versioned.value(null);
    private final static Object lock = new Object();

    private final MetadataStore metadataStore;
    private final StoreRepository storeRepo;
    private final FailureDetector failureDetector;
    private final Map<Integer, SynchronousQueue<Versioned<Slop>>> slopQueues;
    private final ExecutorService consumerExecutor;
    private final EventThrottler writeThrottler;
    private final EventThrottler readThrottler;
    private final AdminClient adminClient;

    public StreamingSlopPusherJob(StoreRepository storeRepo,
                                  MetadataStore metadataStore,
                                  FailureDetector failureDetector,
                                  long maxReadBytesPerSec,
                                  long maxWriteBytesPerSec) {
        this.storeRepo = storeRepo;
        this.metadataStore = metadataStore;
        this.failureDetector = failureDetector;

        Cluster cluster = metadataStore.getCluster();
        this.slopQueues = Maps.newHashMapWithExpectedSize(cluster.getNumberOfNodes());
        this.consumerExecutor = Executors.newFixedThreadPool(cluster.getNumberOfNodes());
        this.writeThrottler = new EventThrottler(maxWriteBytesPerSec);
        this.readThrottler = new EventThrottler(maxReadBytesPerSec);
        this.adminClient = new AdminClient(cluster,
                                           new AdminClientConfig().setMaxThreads(cluster.getNumberOfNodes())
                                                                  .setMaxConnectionsPerNode(1));
    }

    public void run() {

        // don't try to run slop pusher job when rebalancing
        if(!metadataStore.getServerState().equals(MetadataStore.VoldemortState.NORMAL_SERVER)) {
            logger.error("Cannot run slop pusher job since cluster is rebalancing");
            return;
        }

        // Allow only one job to run at one time - Two jobs may get started if
        // someone disables and enables (through JMX) immediately during a run
        synchronized(lock) {

            SlopStorageEngine slopStorageEngine = storeRepo.getSlopStore();
            ClosableIterator<Pair<ByteArray, Versioned<Slop>>> iterator = null;
            try {
                StorageEngine<ByteArray, Slop, byte[]> slopStore = slopStorageEngine.asSlopStore();
                iterator = slopStore.entries();

                while(iterator.hasNext()) {
                    Pair<ByteArray, Versioned<Slop>> keyAndVal;
                    try {
                        keyAndVal = iterator.next();
                        Versioned<Slop> versioned = keyAndVal.getSecond();

                        // Retrieve the node
                        int nodeId = versioned.getValue().getNodeId();

                        SynchronousQueue<Versioned<Slop>> slopQueue = slopQueues.get(nodeId);
                        if(slopQueue == null) {
                            // No previous slop queue, add one
                            slopQueue = new SynchronousQueue<Versioned<Slop>>();
                            slopQueues.put(nodeId, slopQueue);
                            consumerExecutor.submit(new SlopConsumer(nodeId, slopQueue)).get();
                        }

                        slopQueue.put(versioned);
                        readThrottler.maybeThrottle(nBytesRead(keyAndVal));
                    } catch(Exception e) {
                        logger.error("Exception in the entries, escaping the loop ", e);
                        break;
                    }
                }

            } catch(Exception e) {
                logger.error(e, e);
            } finally {

                // Adding to poison pill
                for(SynchronousQueue<Versioned<Slop>> slopQueue: slopQueues.values()) {
                    try {
                        slopQueue.put(END);
                    } catch(InterruptedException e) {
                        logger.info("Error putting poison pill");
                    }
                }

                consumerExecutor.shutdown();
                try {
                    if(iterator != null)
                        iterator.close();
                } catch(Exception e) {
                    logger.error("Failed to close entries.", e);
                }
                // Shut down admin client as not to waste connections
                adminClient.stop();
            }

        }
    }

    private int nBytesRead(Pair<ByteArray, Versioned<Slop>> keyAndVal) {
        return keyAndVal.getFirst().length() + slopSize(keyAndVal.getSecond());
    }

    /**
     * Returns the approximate size of slop to help in throttling
     * 
     * @param slopVersioned The versioned slop whose size we want
     * @return Size in bytes
     */
    private int slopSize(Versioned<Slop> slopVersioned) {
        int nBytes = 0;
        Slop slop = slopVersioned.getValue();
        nBytes += slop.getKey().length();
        nBytes += ((VectorClock) slopVersioned.getVersion()).sizeInBytes();
        switch(slop.getOperation()) {
            case PUT: {
                nBytes += slop.getValue().length;
                break;
            }
            case DELETE: {
                break;
            }
            default:
                logger.error("Unknown slop operation: " + slop.getOperation());
        }
        return nBytes;
    }

    private class SlopIterator extends AbstractIterator<Versioned<Slop>> {

        private final SynchronousQueue<Versioned<Slop>> slopQueue;
        private final List<Pair<ByteArray, Version>> deleteBatch;

        private final static int BATCH_SIZE = 10000;

        private int writtenLast = 0;
        private long count = 0L;

        public SlopIterator(SynchronousQueue<Versioned<Slop>> slopQueue,
                            List<Pair<ByteArray, Version>> deleteBatch) {
            this.slopQueue = slopQueue;
            this.deleteBatch = deleteBatch;
        }

        @SuppressWarnings("finally")
        @Override
        protected Versioned<Slop> computeNext() {
            try {
                Versioned<Slop> head = null;
                boolean shutDown = false;
                while(!shutDown) {
                    head = slopQueue.poll();
                    if(head == null)
                        continue;

                    if(head.equals(END)) {
                        shutDown = true;
                    } else {
                        count++;
                        if(count % BATCH_SIZE == 0) {
                            deleteOldBatch();
                        }

                        writeThrottler.maybeThrottle(writtenLast);
                        writtenLast = slopSize(head);

                        deleteBatch.add(Pair.create(head.getValue().makeKey(), head.getVersion()));
                        return head;
                    }
                }
            } catch(Exception e) {
                throw new VoldemortException("Iterator failed", e);
            } finally {
                return endOfData();
            }
        }

        private void deleteOldBatch() {
            for(Pair<ByteArray, Version> entry: deleteBatch)
                storeRepo.getSlopStore().delete(entry.getFirst(), entry.getSecond());
        }
    }

    private class SlopConsumer implements Runnable {

        private final int nodeId;
        private SynchronousQueue<Versioned<Slop>> slopQueue;
        private final List<Pair<ByteArray, Version>> deleteBatch;
        private long startTime;

        public SlopConsumer(int nodeId, SynchronousQueue<Versioned<Slop>> slopQueue) {
            this.nodeId = nodeId;
            this.slopQueue = slopQueue;
            this.deleteBatch = Lists.newArrayList();
            this.startTime = System.currentTimeMillis();
        }

        public void run() {
            try {
                adminClient.updateSlopEntries(nodeId, new SlopIterator(slopQueue, deleteBatch));
            } catch(UnreachableStoreException e) {
                failureDetector.recordException(metadataStore.getCluster().getNodeById(nodeId),
                                                System.currentTimeMillis() - this.startTime,
                                                e);
                throw e;
            } catch(Exception e) {
                logger.error(e, e);
            } finally {
                // Clean up the remaining delete batch
                for(Pair<ByteArray, Version> entry: deleteBatch)
                    storeRepo.getSlopStore().delete(entry.getFirst(), entry.getSecond());

                // Clean the slop queue and remove the queue from the global
                // queue
                slopQueue.clear();
                slopQueues.remove(nodeId);
            }
        }
    }
}
