package voldemort.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.log4j.Logger;

import voldemort.VoldemortApplicationException;
import voldemort.VoldemortException;
import voldemort.client.ClientConfig;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.client.protocol.admin.BaseStreamingClient;
import voldemort.client.protocol.admin.QueryKeyResult;
import voldemort.client.protocol.admin.StreamingClientConfig;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.StoreRoutingPlan;
import voldemort.serialization.SerializerDefinition;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreUtils;
import voldemort.versioning.ChainedResolver;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.TimeBasedInconsistencyResolver;
import voldemort.versioning.VectorClock;
import voldemort.versioning.VectorClockInconsistencyResolver;
import voldemort.versioning.VectorClockUtils;
import voldemort.versioning.Versioned;

import com.google.common.collect.Lists;

/**
 * Tool to fork lift data over from a source cluster to a destination cluster.
 * When used in conjunction with a client that "double writes" to both the
 * clusters, this can be a used as a feasible store migration tool to move an
 * existing store to a new cluster.
 * 
 * There are two modes around how the divergent versions of a key are
 * consolidated from the source cluster. :
 * 
 * 1) Primary only Resolution (
 * {@link ClusterForkLiftTool#SinglePartitionForkLiftTask}: The entries on the
 * primary partition are moved over to the destination cluster with empty vector
 * clocks. if any key has multiple versions on the primary, they are resolved.
 * This approach is fast and is best suited if you deem the replicas being very
 * much in sync with each other. This is the DEFAULT mode
 * 
 * 2) Global Resolution (
 * {@link ClusterForkLiftTool#SinglePartitionGloballyResolvingForkLiftTask} :
 * The keys belonging to a partition are fetched out of the primary replica, and
 * for each such key, the corresponding values are obtained from all other
 * replicas, using get(..) operations. These versions are then resolved and
 * written back to the destination cluster as before. This approach is slow
 * since it involves several roundtrips to the server for each key (some
 * potentially cross colo) and hence should be used when thorough version
 * resolution is neccessary or the admin deems the replicas being fairly
 * out-of-sync
 * 
 * 
 * In both mode, the default chained resolver (
 * {@link VectorClockInconsistencyResolver} +
 * {@link TimeBasedInconsistencyResolver} is used to determine a final resolved
 * version.
 * 
 * NOTES:
 * 
 * 1) If the tool fails for some reason in the middle, the admin can restart the
 * tool for the failed partitions alone. The keys that were already written in
 * the failed partitions, will all experience {@link ObsoleteVersionException}
 * and the un-inserted keys will be inserted.
 * 
 * 2) Since the forklift writes are issued with empty vector clocks, they will
 * always yield to online writes happening on the same key, before or during the
 * forklift window. Of course, after the forklift window, the destination
 * cluster resumes normal operation.
 * 
 * 3) For now, we will fallback to fetching the key from the primary replica,
 * fetch the values out manually, resolve and write it back. PitFalls : primary
 * somehow does not have the key.
 * 
 * Two scenarios.
 * 
 * 1) Key active after double writes: the situation is the result of slop not
 * propagating to the primary. But double writes would write the key back to
 * destination cluster anyway. We are good.
 * 
 * 2) Key inactive after double writes: This indicates a problem elsewhere. This
 * is a base guarantee voldemort should offer.
 * 
 * 4) Zoned <-> Non Zoned forklift implications.
 * 
 * When forklifting data from a non-zoned to zoned cluster, both destination
 * zones will be populated with data, by simply running the tool once with the
 * respective bootstrap urls. If you need to forklift data from zoned to
 * non-zoned clusters (i.e your replication between datacenters is not handled
 * by Voldemort), then you need to run the tool twice for each destination
 * non-zoned cluster. Zoned -> Zoned and Non-Zoned -> Non-Zoned forklifts are
 * trivial.
 * 
 */
public class ClusterForkLiftTool implements Runnable {

    /*
     * different modes available with the forklift tool
     */
    enum ForkLiftTaskMode {
        global_resolution, /* Fetch data from all partitions and do resolution */
        primary_resolution, /*
                             * Fetch data from primary partition and do
                             * resolution
                             */
        no_resolution /* fetch data from primary parition and do no resolution */
    }

    private static Logger logger = Logger.getLogger(ClusterForkLiftTool.class);
    private static final int DEFAULT_MAX_PUTS_PER_SEC = 500;
    private static final int DEFAULT_PROGRESS_PERIOD_OPS = 100000;
    private static final int DEFAULT_PARTITION_PARALLELISM = 8;
    private static final int DEFAULT_WORKER_POOL_SHUTDOWN_WAIT_MINS = 5;

    private static final String OVERWRITE_OPTION = "overwrite";
    private static final String IGNORE_SCHEMA_MISMATCH = "ignore-schema-mismatch";
    private static final String OVERWRITE_WARNING_MESSAGE = "**WARNING** If source and destination has overlapping keys, will overwrite the destination values "
                                                            + " using source. The option is ir-reversible. The old value if exists in the destination cluster will "
                                                            + " be permanently lost. For keys that only exists in destination, they will be left un-modified. ";

    private final AdminClient srcAdminClient;
    private final BaseStreamingClient dstStreamingClient;
    private final List<String> storesList;
    private final ExecutorService workerPool;
    private final int progressOps;
    private final HashMap<String, StoreDefinition> srcStoreDefMap;
    private final List<Integer> partitionList;
    private final ForkLiftTaskMode mode;
    private final Boolean overwrite;

    private static List<StoreDefinition> getStoreDefinitions(AdminClient adminClient) {
        return adminClient.metadataMgmtOps.getRemoteStoreDefList().getValue();
    }

    public ClusterForkLiftTool(String srcBootstrapUrl,
                               String dstBootstrapUrl,
                               Boolean overwrite,
                               boolean ignoreSchemaMismatch,
                               int maxPutsPerSecond,
                               int partitionParallelism,
                               int progressOps,
                               List<String> storesList,
                               List<Integer> partitions,
                               ForkLiftTaskMode mode) {

        if(storesList == null || storesList.size() == 0) {
            throw new IllegalArgumentException("One or more stores expected");
        }
        // set up AdminClient on source cluster
        this.srcAdminClient = new AdminClient(srcBootstrapUrl,
                                              new AdminClientConfig(),
                                              new ClientConfig());

        // set up streaming client to the destination cluster
        Props props = new Props();
        props.put("streaming.platform.bootstrapURL", dstBootstrapUrl);
        props.put("streaming.platform.throttle.qps", maxPutsPerSecond);
        StreamingClientConfig config = new StreamingClientConfig(props);
        this.dstStreamingClient = new BaseStreamingClient(config);
        this.mode = mode;
        this.overwrite = overwrite;

        this.storesList = storesList;

        this.srcStoreDefMap = checkStoresOnBothSides(ignoreSchemaMismatch);

        // determine the partitions to be fetched
        if(partitions != null) {
            this.partitionList = partitions;
        } else {
            this.partitionList = new ArrayList<Integer>(srcAdminClient.getAdminClientCluster()
                                                                      .getNumberOfPartitions());
            for(Node node: srcAdminClient.getAdminClientCluster().getNodes())
                this.partitionList.addAll(node.getPartitionIds());
            // shuffle the partition list so the fetching will equally spread
            // across the source cluster
            Collections.shuffle(this.partitionList);
            if(this.partitionList.size() > srcAdminClient.getAdminClientCluster()
                                                         .getNumberOfPartitions()) {
                throw new VoldemortException("Incorrect partition mapping in source cluster");
            }
        }

        // set up thread pool to parallely forklift partitions
        this.workerPool = Executors.newFixedThreadPool(partitionParallelism);
        this.progressOps = progressOps;

    }

    private HashMap<String, StoreDefinition> checkStoresOnBothSides(boolean ignoreSchemaMismatch) {
        List<StoreDefinition> srcStoreDefs = getStoreDefinitions(srcAdminClient);
        HashMap<String, StoreDefinition> srcStoreDefMap = StoreUtils.getStoreDefsAsMap(srcStoreDefs);
        List<StoreDefinition> dstStoreDefs = getStoreDefinitions(dstStreamingClient.getAdminClient());
        HashMap<String, StoreDefinition> dstStoreDefMap = StoreUtils.getStoreDefsAsMap(dstStoreDefs);

        Set<String> storesToSkip = new HashSet<String>();
        for(String store: storesList) {
            if(!srcStoreDefMap.containsKey(store)) {
                String message = "Store " + store + " does not exist in source cluster ";
                logger.warn(message);
                throw new VoldemortApplicationException(message);
            }
            StoreDefinition srcStoreDef = srcStoreDefMap.get(store); 

            if(!dstStoreDefMap.containsKey(store)) {
                String message = "Store " + store + " does not exist in destination cluster ";
                logger.warn(message);
                throw new VoldemortApplicationException(message);
            }
            StoreDefinition dstStoreDef = dstStoreDefMap.get(store);
            
            if(!ignoreSchemaMismatch) {
                SerializerDefinition srcKeySerializer = srcStoreDef.getKeySerializer();
                SerializerDefinition dstKeySerializer = dstStoreDef.getKeySerializer();
                if(srcKeySerializer.equals(dstKeySerializer) == false) {
                    String message = "Store "
                                     + store
                                     + " Key schema does not match between Source and destination \n";
                    message += "Source : " + srcKeySerializer.getFormattedString() + "\n";
                    message += "Destination : " + dstKeySerializer.getFormattedString() + "\n";
                    logger.warn(message);
                    throw new VoldemortApplicationException(message);
                }

                SerializerDefinition srcValueSerializer = srcStoreDef.getValueSerializer();
                SerializerDefinition dstValueSerializer = dstStoreDef.getValueSerializer();
                if(srcValueSerializer.equals(dstValueSerializer) == false) {
                    String message = "Store "
                                     + store
                                     + " Value schema does not match between Source and destination \n";
                    message += "Source : " + srcValueSerializer.getFormattedString() + "\n";
                    message += "Destination : " + dstValueSerializer.getFormattedString() + "\n";
                    logger.warn(message);
                    throw new VoldemortApplicationException(message);
                }

            }
        }
        return srcStoreDefMap;
    }

    /**
     * TODO this base class can potentially provide some framework of execution
     * for the subclasses, to yield a better objected oriented design (progress
     * tracking etc)
     * 
     */
    abstract class SinglePartitionForkLiftTask {

        protected int partitionId;
        protected CountDownLatch latch;
        protected StoreRoutingPlan storeInstance;
        protected String workName;
        private Set<Integer> dstServerIds;
        private long entriesForkLifted = 0;

        SinglePartitionForkLiftTask(StoreRoutingPlan storeInstance,
                                    int partitionId,
                                    CountDownLatch latch) {
            this.partitionId = partitionId;
            this.latch = latch;
            this.storeInstance = storeInstance;
            workName = "[Store: " + storeInstance.getStoreDefinition().getName() + ", Partition: "
                       + this.partitionId + "] ";
            dstServerIds = dstStreamingClient.getAdminClient().getAdminClientCluster().getNodeIds();
        }

        void streamingPut(ByteArray key, Versioned<byte[]> value) {
            if(overwrite) {
                VectorClock denseClock = VectorClockUtils.makeClockWithCurrentTime(dstServerIds);
                Versioned<byte[]> updatedValue = new Versioned<byte[]>(value.getValue(), denseClock);
                dstStreamingClient.streamingPut(key, updatedValue);
            } else {
                dstStreamingClient.streamingPut(key, value);
            }

            entriesForkLifted++;
            if(entriesForkLifted % progressOps == 0) {
                logger.info(workName + " fork lifted " + entriesForkLifted
                            + " entries successfully");
            }
        }

        void printSummary() {
            logger.info(workName + "Completed processing " + entriesForkLifted + " records");
        }
    }

    /**
     * Fetches keys belonging the primary partition, and then fetches values for
     * that key from all replicas in a non-streaming fashion, applies the
     * default resolver and writes it back to the destination cluster
     * 
     * TODO a streaming N way merge is the more efficient & correct solution.
     * Without this, the resolving can be very slow due to cross data center
     * get(..)
     */
    class SinglePartitionGloballyResolvingForkLiftTask extends SinglePartitionForkLiftTask
            implements Runnable {

        SinglePartitionGloballyResolvingForkLiftTask(StoreRoutingPlan storeInstance,
                                                     int partitionId,
                                                     CountDownLatch latch) {
            super(storeInstance, partitionId, latch);
        }

        public void run() {
            String storeName = this.storeInstance.getStoreDefinition().getName();
            try {
                logger.info(workName + "Starting processing");
                ChainedResolver<Versioned<byte[]>> resolver = new ChainedResolver<Versioned<byte[]>>(new VectorClockInconsistencyResolver<byte[]>(),
                                                                                                     new TimeBasedInconsistencyResolver<byte[]>());
                Iterator<ByteArray> keyItr = srcAdminClient.bulkFetchOps.fetchKeys(storeInstance.getNodeIdForPartitionId(this.partitionId),
                                                                                   storeName,
                                                                                   Lists.newArrayList(this.partitionId),
                                                                                   null,
                                                                                   true);
                List<Integer> nodeList = storeInstance.getReplicationNodeList(this.partitionId);
                while(keyItr.hasNext()) {
                    ByteArray keyToResolve = keyItr.next();
                    Map<Integer, QueryKeyResult> valuesMap = doReads(nodeList, keyToResolve.get());
                    List<Versioned<byte[]>> values = new ArrayList<Versioned<byte[]>>(valuesMap.size());
                    for(Map.Entry<Integer, QueryKeyResult> entry: valuesMap.entrySet()) {
                        int nodeId = entry.getKey();
                        QueryKeyResult result = entry.getValue();

                        if(result.hasException()) {
                            logger.error(workName + "key fetch failed for key "
                                                 + ByteUtils.toHexString(keyToResolve.get())
                                                 + " on node " + nodeId,
                                         result.getException());
                            break;
                        }
                        values.addAll(result.getValues());
                    }

                    List<Versioned<byte[]>> resolvedVersions = resolver.resolveConflicts(values);
                    // after timestamp based resolving there should be only one
                    // version. Insert that to the destination cluster with
                    // empty vector clock
                    if(resolvedVersions.size() > 1) {
                        throw new VoldemortException("More than one resolved versions, key: "
                                                     + ByteUtils.toHexString(keyToResolve.get())
                                                     + " vals:" + resolvedVersions);
                    }
                    Versioned<byte[]> value = new Versioned<byte[]>(resolvedVersions.get(0)
                                                                                    .getValue());
                    streamingPut(keyToResolve, value);
                }
                printSummary();
            } catch(Exception e) {
                // all work should stop if we get here
                logger.error(workName + "Error forklifting data ", e);
            } finally {
                latch.countDown();
            }
        }

        /**
         * 
         * @param nodeIdList
         * @param keyInBytes
         * @return
         */
        private Map<Integer, QueryKeyResult> doReads(final List<Integer> nodeIdList,
                                                     final byte[] keyInBytes) {
            Map<Integer, QueryKeyResult> nodeIdToKeyValues = new HashMap<Integer, QueryKeyResult>();

            ByteArray key = new ByteArray(keyInBytes);
            for(int nodeId: nodeIdList) {
                List<Versioned<byte[]>> values = null;
                try {
                    values = srcAdminClient.storeOps.getNodeKey(storeInstance.getStoreDefinition()
                                                                             .getName(),
                                                                nodeId,
                                                                key);
                    nodeIdToKeyValues.put(nodeId, new QueryKeyResult(key, values));
                } catch(VoldemortException ve) {
                    nodeIdToKeyValues.put(nodeId, new QueryKeyResult(key, ve));
                }
            }
            return nodeIdToKeyValues;
        }
    }

    /**
     * Simply fetches the data for the partition from the primary replica and
     * writes it into the destination cluster. Works well when the replicas are
     * fairly consistent.
     * 
     */
    class SinglePartitionPrimaryResolvingForkLiftTask extends SinglePartitionForkLiftTask implements
            Runnable {

        SinglePartitionPrimaryResolvingForkLiftTask(StoreRoutingPlan storeInstance,
                                                    int partitionId,
                                                    CountDownLatch latch) {
            super(storeInstance, partitionId, latch);
        }

        @Override
        public void run() {
            String storeName = this.storeInstance.getStoreDefinition().getName();
            ChainedResolver<Versioned<byte[]>> resolver = new ChainedResolver<Versioned<byte[]>>(new VectorClockInconsistencyResolver<byte[]>(),
                                                                                                 new TimeBasedInconsistencyResolver<byte[]>());
            try {
                logger.info(workName + "Starting processing");
                Iterator<Pair<ByteArray, Versioned<byte[]>>> entryItr = srcAdminClient.bulkFetchOps.fetchEntries(storeInstance.getNodeIdForPartitionId(this.partitionId),
                                                                                                                 storeName,
                                                                                                                 Lists.newArrayList(this.partitionId),
                                                                                                                 null,
                                                                                                                 true);
                ByteArray prevKey = null;
                List<Versioned<byte[]>> vals = new ArrayList<Versioned<byte[]>>();

                while(entryItr.hasNext()) {
                    Pair<ByteArray, Versioned<byte[]>> record = entryItr.next();
                    ByteArray key = record.getFirst();
                    Versioned<byte[]> versioned = record.getSecond();

                    if(prevKey != null && !prevKey.equals(key)) {
                        // resolve and write, if you see a new key
                        List<Versioned<byte[]>> resolvedVersions = resolver.resolveConflicts(vals);
                        if(resolvedVersions.size() > 1) {
                            throw new VoldemortException("More than one resolved versions, key: "
                                                         + ByteUtils.toHexString(prevKey.get())
                                                         + " vals:" + resolvedVersions);
                        }
                        Versioned<byte[]> resolvedVersioned = resolvedVersions.get(0);
                        // an empty vector clock will ensure, online traffic
                        // will always win over the forklift writes
                        Versioned<byte[]> newEntry = new Versioned<byte[]>(resolvedVersioned.getValue(),
                                                                           new VectorClock(((VectorClock) resolvedVersioned.getVersion()).getTimestamp()));

                        streamingPut(prevKey, newEntry);
                        vals = new ArrayList<Versioned<byte[]>>();
                    }
                    vals.add(versioned);
                    prevKey = key;
                }

                // process the last record
                if(vals.size() > 0) {
                    List<Versioned<byte[]>> resolvedVals = resolver.resolveConflicts(vals);
                    assert resolvedVals.size() == 1;
                    Versioned<byte[]> resolvedVersioned = resolvedVals.get(0);
                    Versioned<byte[]> newEntry = new Versioned<byte[]>(resolvedVersioned.getValue(),
                                                                       new VectorClock(((VectorClock) resolvedVersioned.getVersion()).getTimestamp()));
                    streamingPut(prevKey, newEntry);
                }

                printSummary();
            } catch(Exception e) {
                // if for some reason this partition fails, we will have retry
                // again for those partitions alone.
                logger.error(workName + "Error forklifting data ", e);
            } finally {
                latch.countDown();
            }
        }
    }

    /**
     * Simply fetches the data for the partition from the primary replica and
     * writes it into the destination cluster without resolving any of the
     * conflicting values
     * 
     */
    class SinglePartitionNoResolutionForkLiftTask extends SinglePartitionForkLiftTask implements
            Runnable {

        SinglePartitionNoResolutionForkLiftTask(StoreRoutingPlan storeInstance,
                                                int partitionId,
                                                CountDownLatch latch) {
            super(storeInstance, partitionId, latch);
        }

        @Override
        public void run() {
            String storeName = this.storeInstance.getStoreDefinition().getName();
            try {
                logger.info(workName + "Starting processing");
                Iterator<Pair<ByteArray, Versioned<byte[]>>> entryItr = srcAdminClient.bulkFetchOps.fetchEntries(storeInstance.getNodeIdForPartitionId(this.partitionId),
                                                                                                                 storeName,
                                                                                                                 Lists.newArrayList(this.partitionId),
                                                                                                                 null,
                                                                                                                 true);

                while(entryItr.hasNext()) {
                    Pair<ByteArray, Versioned<byte[]>> record = entryItr.next();
                    ByteArray key = record.getFirst();
                    Versioned<byte[]> versioned = record.getSecond();
                    streamingPut(key, versioned);
                }
                printSummary();

            } catch(Exception e) {
                // if for some reason this partition fails, we will have retry
                // again for those partitions alone.
                logger.error(workName + "Error forklifting data ", e);
            } finally {
                latch.countDown();
            }

        }
    }

    @Override
    public void run() {
        final Cluster srcCluster = srcAdminClient.getAdminClientCluster();
        try {
            // process stores one-by-one
            for(String store: storesList) {
                logger.info("Processing store " + store);
                dstStreamingClient.initStreamingSession(store, new Callable<Object>() {

                    @Override
                    public Object call() throws Exception {

                        return null;
                    }
                }, new Callable<Object>() {

                    @Override
                    public Object call() throws Exception {

                        return null;
                    }
                }, true);

                final CountDownLatch latch = new CountDownLatch(srcCluster.getNumberOfPartitions());
                StoreRoutingPlan storeInstance = new StoreRoutingPlan(srcCluster,
                                                                      srcStoreDefMap.get(store));

                // submit work on every partition that is to be forklifted
                for(Integer partitionId: partitionList) {
                    if(this.mode == ForkLiftTaskMode.global_resolution) {
                        // do thorough global resolution across replicas
                        SinglePartitionGloballyResolvingForkLiftTask work = new SinglePartitionGloballyResolvingForkLiftTask(storeInstance,
                                                                                                                             partitionId,
                                                                                                                             latch);
                        workerPool.submit(work);
                    } else if(this.mode == ForkLiftTaskMode.primary_resolution) {
                        // do the less cleaner, but much faster route
                        SinglePartitionPrimaryResolvingForkLiftTask work = new SinglePartitionPrimaryResolvingForkLiftTask(storeInstance,
                                                                                                                           partitionId,
                                                                                                                           latch);
                        workerPool.submit(work);
                    } else if(this.mode == ForkLiftTaskMode.no_resolution) {
                        // do the less cleaner, but much faster route
                        SinglePartitionNoResolutionForkLiftTask work = new SinglePartitionNoResolutionForkLiftTask(storeInstance,
                                                                                                                   partitionId,
                                                                                                                   latch);
                        workerPool.submit(work);
                    }
                }

                // wait till all the partitions are processed
                latch.await();
                dstStreamingClient.closeStreamingSession();
                logger.info("Finished processing store " + store);
            }
        } catch(Exception e) {
            logger.error("Exception running forklift tool", e);
        } finally {
            workerPool.shutdown();
            try {
                workerPool.awaitTermination(DEFAULT_WORKER_POOL_SHUTDOWN_WAIT_MINS,
                                            TimeUnit.MINUTES);
            } catch(InterruptedException ie) {
                logger.error("InterruptedException while waiting for worker pool to shutdown", ie);
            }
            srcAdminClient.close();
            dstStreamingClient.getAdminClient().close();
        }
    }

    /**
     * Return args parser
     * 
     * @return program parser
     * */
    private static OptionParser getParser() {
        OptionParser parser = new OptionParser();
        parser.accepts("help", "print help information");
        parser.accepts("src-url", "[REQUIRED] bootstrap URL of source cluster")
              .withRequiredArg()
              .describedAs("source-bootstrap-url")
              .ofType(String.class);
        parser.accepts("dst-url", "[REQUIRED] bootstrap URL of destination cluster")
              .withRequiredArg()
              .describedAs("destination-bootstrap-url")
              .ofType(String.class);
        parser.accepts("stores",
                       "Store names to forklift. Comma delimited list or singleton. [Default: ALL SOURCE STORES]")
              .withRequiredArg()
              .describedAs("stores")
              .withValuesSeparatedBy(',')
              .ofType(String.class);
        parser.accepts("partitions",
                       "partitions to forklift. Comma delimited list or singleton. [Default: ALL SOURCE PARTITIONS]")
              .withRequiredArg()
              .describedAs("partitions")
              .withValuesSeparatedBy(',')
              .ofType(Integer.class);
        parser.accepts("max-puts-per-second",
                       "Maximum number of put(...) operations issued against destination cluster per second. [Default: "
                               + DEFAULT_MAX_PUTS_PER_SEC + " ]")
              .withRequiredArg()
              .describedAs("maxPutsPerSecond")
              .ofType(Integer.class);
        parser.accepts("progress-period-ops",
                       "Number of operations between progress info is displayed. [Default: "
                               + DEFAULT_PROGRESS_PERIOD_OPS + " ]")
              .withRequiredArg()
              .describedAs("progressPeriodOps")
              .ofType(Integer.class);
        parser.accepts("parallelism",
                       "Number of partitions to fetch in parallel. [Default: "
                               + DEFAULT_PARTITION_PARALLELISM + " ]")
              .withRequiredArg()
              .describedAs("partitionParallelism")
              .ofType(Integer.class);
        parser.accepts("mode",
                       "Determines if a thorough global resolution needs to be done, by comparing all replicas. [Default: "
                               + ForkLiftTaskMode.primary_resolution.toString()
                               + " Fetch from primary alone ]")
              .withOptionalArg()
              .describedAs("mode")
              .ofType(String.class);
        parser.accepts(OVERWRITE_OPTION, OVERWRITE_WARNING_MESSAGE)
              .withOptionalArg()
              .describedAs("overwriteExistingValue")
              .ofType(Boolean.class)
              .defaultsTo(false);

        return parser;
    }


    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
        OptionParser parser = null;
        OptionSet options = null;
        try {
            parser = getParser();
            options = parser.parse(args);
        } catch(Exception oe) {
            logger.error("Exception processing command line options", oe);
            parser.printHelpOn(System.out);
            return;
        }

        /* validate options */
        if(options.has("help")) {
            parser.printHelpOn(System.out);
            return;
        }

        if(!options.has("src-url") || !options.has("dst-url") || !options.has("stores")) {
            logger.error("'src-url' 'dst-url' and 'stores' are mandatory parameters ");
            parser.printHelpOn(System.out);
            return;
        }

        String srcBootstrapUrl = (String) options.valueOf("src-url");
        String dstBootstrapUrl = (String) options.valueOf("dst-url");
        int maxPutsPerSecond = DEFAULT_MAX_PUTS_PER_SEC;
        if(options.has("max-puts-per-second"))
            maxPutsPerSecond = (Integer) options.valueOf("max-puts-per-second");
        List<String> storesList = null;
        if(options.has("stores")) {
            storesList = new ArrayList<String>((List<String>) options.valuesOf("stores"));
        }
        List<Integer> partitions = null;
        if(options.has("partitions")) {
            partitions = (List<Integer>) options.valuesOf("partitions");
        }

        int partitionParallelism = DEFAULT_PARTITION_PARALLELISM;
        if(options.has("parallelism")) {
            partitionParallelism = (Integer) options.valueOf("parallelism");
        }
        int progressOps = DEFAULT_PROGRESS_PERIOD_OPS;
        if(options.has("progress-period-ops")) {
            progressOps = (Integer) options.valueOf("progress-period-ops");
        }

        ForkLiftTaskMode mode = ForkLiftTaskMode.primary_resolution;

        if(options.has("mode")) {
            mode = ForkLiftTaskMode.valueOf((String) options.valueOf("mode"));
            if(mode == null)
                mode = ForkLiftTaskMode.primary_resolution;
        }

        Boolean overwrite = extractBoolOption(options, OVERWRITE_OPTION);
        boolean ignoreSchemaMismatch = extractBoolOption(options, IGNORE_SCHEMA_MISMATCH);

        ClusterForkLiftTool forkLiftTool = new ClusterForkLiftTool(srcBootstrapUrl,
                                                                   dstBootstrapUrl,
                                                                   overwrite,
                                                                   ignoreSchemaMismatch,
                                                                   maxPutsPerSecond,
                                                                   partitionParallelism,
                                                                   progressOps,
                                                                   storesList,
                                                                   partitions,
                                                                   mode);
        forkLiftTool.run();
        // TODO cleanly shut down the hanging threadpool
        System.exit(0);
    }

    private static boolean extractBoolOption(OptionSet options, String optionName) {
        boolean optionValue = false;
        if(options.has(optionName)) {
            if(options.hasArgument(optionName)) {
                optionValue = (Boolean) options.valueOf(optionName);
            } else {
                optionValue = true;
            }
        }
        return optionValue;
    }

}
