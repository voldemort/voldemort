package voldemort.server.scheduler.slop;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.server.StoreRepository;
import voldemort.server.VoldemortConfig;
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
    private final Cluster cluster;
    private final List<Future> consumerResults;
    private final VoldemortConfig voldemortConfig;
    private ConcurrentHashMap<Integer, Long> attemptedByNode, succeededByNode;

    public StreamingSlopPusherJob(StoreRepository storeRepo,
                                  MetadataStore metadataStore,
                                  FailureDetector failureDetector,
                                  VoldemortConfig voldemortConfig) {
        this.storeRepo = storeRepo;
        this.metadataStore = metadataStore;
        this.failureDetector = failureDetector;
        this.voldemortConfig = voldemortConfig;

        this.cluster = metadataStore.getCluster();
        this.slopQueues = Maps.newHashMapWithExpectedSize(cluster.getNumberOfNodes());
        this.consumerExecutor = Executors.newFixedThreadPool(cluster.getNumberOfNodes(),
                                                             new ThreadFactory() {

                                                                 public Thread newThread(Runnable r) {
                                                                     Thread thread = new Thread(r);
                                                                     thread.setName("slop-pusher");
                                                                     return thread;
                                                                 }
                                                             });
        this.writeThrottler = new EventThrottler(voldemortConfig.getSlopMaxWriteBytesPerSec());
        this.readThrottler = new EventThrottler(voldemortConfig.getSlopMaxReadBytesPerSec());
        this.adminClient = new AdminClient(cluster,
                                           new AdminClientConfig().setMaxThreads(cluster.getNumberOfNodes())
                                                                  .setMaxConnectionsPerNode(1));
        this.consumerResults = Lists.newArrayList();
        this.attemptedByNode = new ConcurrentHashMap<Integer, Long>(cluster.getNumberOfNodes());
        this.succeededByNode = new ConcurrentHashMap<Integer, Long>(cluster.getNumberOfNodes());

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
            boolean exceptionOccurred = false;
            Date startTime = new Date();
            logger.info("Started streaming slop pusher job at " + startTime);

            SlopStorageEngine slopStorageEngine = storeRepo.getSlopStore();
            ClosableIterator<Pair<ByteArray, Versioned<Slop>>> iterator = null;

            // Clearing the statistics
            AtomicLong attemptedPushes = new AtomicLong(0);
            for(Node node: cluster.getNodes()) {
                attemptedByNode.put(node.getId(), 0L);
                succeededByNode.put(node.getId(), 0L);
            }

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
                        Node node = cluster.getNodeById(nodeId);

                        attemptedPushes.incrementAndGet();
                        Long attempted = attemptedByNode.get(nodeId);
                        attemptedByNode.put(nodeId, attempted + 1L);
                        if(attemptedPushes.get() % 10000 == 0)
                            logger.info("Attempted pushing " + attemptedPushes + " slops");

                        logger.trace("On slop = " + versioned.getValue().getNodeId() + " => "
                                     + new String(versioned.getValue().getKey().get()));

                        if(failureDetector.isAvailable(node)) {
                            SynchronousQueue<Versioned<Slop>> slopQueue = slopQueues.get(nodeId);
                            if(slopQueue == null) {
                                // No previous slop queue, add one
                                slopQueue = new SynchronousQueue<Versioned<Slop>>();
                                slopQueues.put(nodeId, slopQueue);
                                consumerResults.add(consumerExecutor.submit(new SlopConsumer(nodeId,
                                                                                             slopQueue,
                                                                                             slopStorageEngine)));
                            }
                            slopQueue.offer(versioned,
                                            voldemortConfig.getClientRoutingTimeoutMs(),
                                            TimeUnit.MILLISECONDS);
                            readThrottler.maybeThrottle(nBytesRead(keyAndVal));
                        } else {
                            logger.trace(node + " declared down, won't push slop");
                        }
                    } catch(RejectedExecutionException e) {
                        throw new VoldemortException("Ran out of threads in executor", e);
                    }
                }

            } catch(InterruptedException e) {
                logger.warn("Interrupted exception", e);
                exceptionOccurred = true;
            } catch(Exception e) {
                logger.error(e, e);
                exceptionOccurred = true;
            } finally {
                try {
                    if(iterator != null)
                        iterator.close();
                } catch(Exception e) {
                    logger.warn("Failed to close iterator cleanly as database might be closed", e);
                }

                // Adding to poison pill
                for(SynchronousQueue<Versioned<Slop>> slopQueue: slopQueues.values()) {
                    try {
                        slopQueue.offer(END,
                                        voldemortConfig.getClientRoutingTimeoutMs(),
                                        TimeUnit.MILLISECONDS);
                    } catch(InterruptedException e) {
                        logger.warn("Error putting poison pill", e);
                    }
                }

                for(Future result: consumerResults) {
                    try {
                        result.get();
                    } catch(Exception e) {
                        logger.warn("Exception in consumer", e);
                    }
                }

                // Only if exception didn't take place do we update the counts
                if(!exceptionOccurred) {
                    Map<Integer, Long> outstanding = Maps.newHashMapWithExpectedSize(cluster.getNumberOfNodes());
                    for(int nodeId: succeededByNode.keySet()) {
                        logger.debug("Slops pushed to node " + nodeId + " - "
                                     + succeededByNode.get(nodeId));
                        outstanding.put(nodeId, attemptedByNode.get(nodeId)
                                                - succeededByNode.get(nodeId));
                    }

                    slopStorageEngine.resetStats(outstanding);
                }

                // Shut down admin client as not to waste connections
                consumerResults.clear();
                slopQueues.clear();
            }

            logger.info("Completed streaming slop pusher job which started at " + startTime);
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

    /**
     * Smart slop iterator which keeps two previous batches of data
     * 
     */
    private class SlopIterator extends AbstractIterator<Versioned<Slop>> {

        private final SynchronousQueue<Versioned<Slop>> slopQueue;
        private final List<Pair<ByteArray, Version>> deleteBatch;

        private int writtenLast = 0;
        private long slopsDone = 0L;
        private boolean shutDown = false, isComplete = false;

        public SlopIterator(SynchronousQueue<Versioned<Slop>> slopQueue,
                            List<Pair<ByteArray, Version>> deleteBatch) {
            this.slopQueue = slopQueue;
            this.deleteBatch = deleteBatch;
        }

        public boolean isComplete() {
            return isComplete;
        }

        @Override
        protected Versioned<Slop> computeNext() {
            try {
                Versioned<Slop> head = null;
                while(!shutDown) {
                    head = slopQueue.poll();
                    if(head == null)
                        continue;

                    if(head.equals(END)) {
                        shutDown = true;
                        isComplete = true;
                    } else {
                        slopsDone++;
                        if(slopsDone % voldemortConfig.getSlopBatchSize() == 0) {
                            shutDown = true;
                        }

                        writeThrottler.maybeThrottle(writtenLast);
                        writtenLast = slopSize(head);
                        deleteBatch.add(Pair.create(head.getValue().makeKey(), head.getVersion()));
                        return head;
                    }
                }
                return endOfData();
            } catch(Exception e) {
                logger.error("Got an exception " + e);
                return endOfData();
            }
        }

    }

    private class SlopConsumer implements Runnable {

        private final int nodeId;
        private SynchronousQueue<Versioned<Slop>> slopQueue;
        private long startTime;
        private SlopStorageEngine slopStorageEngine;

        // Keep two lists to track deleted items
        private List<Pair<ByteArray, Version>> previous, current;

        public SlopConsumer(int nodeId,
                            SynchronousQueue<Versioned<Slop>> slopQueue,
                            SlopStorageEngine slopStorageEngine) {
            this.nodeId = nodeId;
            this.slopQueue = slopQueue;
            this.slopStorageEngine = slopStorageEngine;
            this.previous = Lists.newArrayList();
            this.current = Lists.newArrayList();
        }

        public void run() {
            try {
                SlopIterator iterator = null;
                do {
                    if(!current.isEmpty()) {
                        if(!previous.isEmpty()) {
                            for(Pair<ByteArray, Version> entry: previous) {
                                slopStorageEngine.delete(entry.getFirst(), entry.getSecond());
                            }
                            Long succeeded = succeededByNode.get(nodeId);
                            succeeded += previous.size();
                            succeededByNode.put(nodeId, succeeded);
                            previous.clear();
                        }
                        previous = null;
                        previous = current;
                        current = Lists.newArrayList();
                    }
                    this.startTime = System.currentTimeMillis();
                    iterator = new SlopIterator(slopQueue, current);
                    adminClient.updateSlopEntries(nodeId, iterator);
                } while(!iterator.isComplete());

                // Clear up both previous and current
                if(!previous.isEmpty()) {
                    for(Pair<ByteArray, Version> entry: previous)
                        slopStorageEngine.delete(entry.getFirst(), entry.getSecond());
                    Long succeeded = succeededByNode.get(nodeId);
                    succeeded += previous.size();
                    succeededByNode.put(nodeId, succeeded);
                    previous.clear();
                }
                if(!current.isEmpty()) {
                    for(Pair<ByteArray, Version> entry: current)
                        slopStorageEngine.delete(entry.getFirst(), entry.getSecond());
                    Long succeeded = succeededByNode.get(nodeId);
                    succeeded += current.size();
                    succeededByNode.put(nodeId, succeeded);
                    current.clear();
                }

            } catch(UnreachableStoreException e) {
                failureDetector.recordException(metadataStore.getCluster().getNodeById(nodeId),
                                                System.currentTimeMillis() - this.startTime,
                                                e);
                throw e;
            } finally {
                // Clean the slop queue and remove the queue from the global
                // queue
                slopQueue.clear();
                slopQueues.remove(nodeId);
            }
        }
    }
}
