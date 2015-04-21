package voldemort.store.readonly.swapper;

import java.io.File;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.ClientConfig;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.store.readonly.ReadOnlyUtils;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import voldemort.utils.CmdUtils;
import voldemort.utils.Time;
import voldemort.xml.ClusterMapper;

public class AdminStoreSwapper {

    private static final Logger logger = Logger.getLogger(AdminStoreSwapper.class);

    private AdminClient adminClient;
    private long timeoutMs;
    private boolean rollbackFailedSwap = false;
    private final List<FailedFetchStrategy> failedFetchStrategyList = Lists.newArrayList();

    protected final Cluster cluster;
    protected final ExecutorService executor;

    private AdminStoreSwapper(Cluster cluster, ExecutorService executor) {
        this.cluster = cluster;
        this.executor = executor;
    }

    /**
     * 
     * @param cluster The cluster metadata
     * @param executor Executor to use for running parallel fetch / swaps
     * @param adminClient The admin client to use for querying
     * @param timeoutMs Time out in ms
     * @param disableFailedOnlyNodeDuringFailedFetch Boolean to indicate we want
     *        to attempt to disable the store on nodes that fail to fetch in order
     *        to move forward with the swap even when some fetches fail.
     * @param deleteFailedFetch Boolean to indicate we want to delete data on
     *        successful nodes after a fetch fails somewhere. If both this and
     *        disableFailedOnly are set, then deleting failed fetch is only tried
     *        if disabling the failed nodes is considered impossible.
     * @param rollbackFailedSwap Boolean to indicate we want to rollback the
     *        data on successful nodes after a swap fails somewhere.
     */
    public AdminStoreSwapper(Cluster cluster,
                             ExecutorService executor,
                             AdminClient adminClient,
                             long timeoutMs,
                             boolean disableFailedOnlyNodeDuringFailedFetch,
                             boolean deleteFailedFetch,
                             boolean rollbackFailedSwap) {
        this(cluster, executor);
        this.adminClient = adminClient;
        this.timeoutMs = timeoutMs;
        this.rollbackFailedSwap = rollbackFailedSwap;
        if (disableFailedOnlyNodeDuringFailedFetch) {
            failedFetchStrategyList.add(new DisableFailedOnlyFailedFetchStrategy(adminClient));
        }
        if (deleteFailedFetch) {
            failedFetchStrategyList.add(new DeleteAllFailedFetchStrategy(adminClient));
        }
    }

    /**
     * 
     * @param cluster The cluster metadata
     * @param executor Executor to use for running parallel fetch / swaps
     * @param adminClient The admin client to use for querying
     * @param timeoutMs Time out in ms
     */
    public AdminStoreSwapper(Cluster cluster,
                             ExecutorService executor,
                             AdminClient adminClient,
                             long timeoutMs) {
        this(cluster, executor);
        this.adminClient = adminClient;
        this.timeoutMs = timeoutMs;
    }

    public void swapStoreData(String storeName, String basePath, long pushVersion) {
        Map<Integer, Response> fetchResponseMap = invokeFetch(storeName, basePath, pushVersion);
        invokeSwap(storeName, fetchResponseMap);
        for (AdminStoreSwapper.Response response: fetchResponseMap.values()) {
            if (!response.isSuccessful()) {
                throw new RecoverableFailedFetchException("Swap succeeded in under-replicated mode. Rethrowing original fetch exception.", response.getException());
            }
        }
    }

    public void invokeRollback(final String storeName, final long pushVersion) {
        Exception exception = null;
        for(Node node: cluster.getNodes()) {
            try {
                logger.info("Attempting rollback for node " + node.getId() + " storeName = "
                            + storeName);
                adminClient.readonlyOps.rollbackStore(node.getId(), storeName, pushVersion);
                logger.info("Rollback succeeded for node " + node.getId());
            } catch(Exception e) {
                exception = e;
                logger.error("Exception thrown during rollback operation on node " + node.getId()
                             + ": ", e);
            }
        }

        if(exception != null)
            throw new VoldemortException(exception);

    }

    public Map<Integer, Response> invokeFetch(final String storeName,
                                    final String basePath,
                                    final long pushVersion) {
        // do fetch
        Map<Integer, Future<String>> fetchDirs = new HashMap<Integer, Future<String>>();
        for(final Node node: cluster.getNodes()) {
            fetchDirs.put(node.getId(), executor.submit(new Callable<String>() {

                public String call() throws Exception {
                    String storeDir = basePath + "/node-" + node.getId();
                    logger.info("Invoking fetch for node " + node.getId() + " for " + storeDir);
                    String response = adminClient.readonlyOps.fetchStore(node.getId(),
                                                                         storeName,
                                                                         storeDir,
                                                                         pushVersion,
                                                                         timeoutMs);
                    if(response == null)
                        throw new VoldemortException("Fetch request on node " + node.getId() + " ("
                                                     + node.getHost() + ") failed");
                    logger.info("Fetch succeeded on node " + node.getId());
                    return response.trim();
                }
            }));
        }

        Map<Integer, Response> fetchResponseMap = Maps.newTreeMap();
        boolean fetchErrors = false;

        for(final Node node: cluster.getNodes()) {
            Integer nodeId = node.getId();
            Future<String> val = fetchDirs.get(nodeId);
            try {
                String response = val.get();
                fetchResponseMap.put(nodeId, new Response(response));
            } catch(Exception e) {
                fetchErrors = true;
                fetchResponseMap.put(nodeId, new Response(e));
            }
        }

        if(fetchErrors) {
            // Log the errors for the user
            for(Map.Entry<Integer, Response> entry: fetchResponseMap.entrySet()) {
                if (!entry.getValue().isSuccessful()) {
                    logger.error("Error on node " + entry.getKey() + " during push : ",
                            entry.getValue().getException());
                }
            }

            Iterator<FailedFetchStrategy> strategyIterator = failedFetchStrategyList.iterator();
            boolean swapIsPossible = false;
            FailedFetchStrategy strategy = null;
            while (strategyIterator.hasNext() && !swapIsPossible) {
                strategy = strategyIterator.next();
                try {
                    swapIsPossible = strategy.dealWithIt(storeName, pushVersion, fetchResponseMap);
                } catch (Exception e) {
                    if (strategyIterator.hasNext()) {
                        logger.error("Got an exception while trying to execute: " + strategy.toString() +
                                ". Continuing with next strategy.", e);
                    } else {
                        logger.error("Got an exception while trying to execute the last remaining strategy: "
                                + strategy.toString() + ". Swap will be aborted.", e);
                    }
                }
            }

            if (swapIsPossible) {
                // We're good... We'll return the fetchResponseMap.
            } else {
                throw new UnrecoverableFailedFetchException("Exception during push. Swap will be aborted.");
            }
        }
        return fetchResponseMap;
    }

    public void invokeSwap(final String storeName, final Map<Integer, Response> fetchResponseMap) {
        // do swap
        Map<Integer, String> previousDirs = new HashMap<Integer, String>();
        HashMap<Integer, Exception> exceptions = Maps.newHashMap();

        for(Map.Entry<Integer, Response> entry: fetchResponseMap.entrySet()) {
            Response response = entry.getValue();
            if (response.isSuccessful()) {
                Integer nodeId = entry.getKey();
                String dir = response.getResponse();
                try {
                    logger.info("Attempting swap for node " + nodeId + " dir = " + dir);
                    previousDirs.put(nodeId, adminClient.readonlyOps.swapStore(nodeId, storeName, dir));
                    logger.info("Swap succeeded for node " + nodeId);
                } catch(Exception e) {
                    exceptions.put(nodeId, e);
                }
            }
        }

        if(!exceptions.isEmpty()) {

            if(rollbackFailedSwap) {
                // Rollback data on successful nodes
                for(int successfulNodeId: previousDirs.keySet()) {
                    try {
                        logger.info("Rolling back data on successful node " + successfulNodeId);
                        adminClient.readonlyOps.rollbackStore(successfulNodeId,
                                storeName,
                                ReadOnlyUtils.getVersionId(new File(previousDirs.get(successfulNodeId))));
                        logger.info("Rollback succeeded for node " + successfulNodeId);
                    } catch(Exception e) {
                        logger.error("Exception thrown during rollback ( after swap ) operation on node "
                                             + successfulNodeId + ": ",
                                     e);
                    }
                }
            }

            // Finally log the errors for the user
            for(int failedNodeId: exceptions.keySet()) {
                logger.error("Error on node " + failedNodeId + " during swap : ",
                             exceptions.get(failedNodeId));
            }

            throw new VoldemortException("Exception during swaps on nodes "
                                         + Joiner.on(",").join(exceptions.keySet()) + " failed");
        }

    }

    public static void main(String[] args) throws Exception {
        OptionParser parser = new OptionParser();
        parser.accepts("help", "print usage information");
        parser.accepts("cluster", "[REQUIRED] the voldemort cluster.xml file ")
                .withRequiredArg()
                .describedAs("cluster.xml");
        parser.accepts("name", "[REQUIRED] the name of the store to swap")
                .withRequiredArg()
                .describedAs("store-name");
        parser.accepts("file", "[REQUIRED] uri of a directory containing the new store files")
                .withRequiredArg()
                .describedAs("uri");
        parser.accepts("timeout", "http timeout for the fetch in ms")
                .withRequiredArg()
                .describedAs("timeout ms")
                .ofType(Integer.class);
        parser.accepts("rollback", "Rollback store to older version");
        parser.accepts("push-version", "[REQUIRED] Version of push to fetch / rollback-to")
                .withRequiredArg()
                .ofType(Long.class);

        OptionSet options = parser.parse(args);
        if(options.has("help")) {
            parser.printHelpOn(System.out);
            System.exit(0);
        }

        Set<String> missing = CmdUtils.missing(options, "cluster", "name", "file", "push-version");
        if(missing.size() > 0) {
            if(!(missing.equals(ImmutableSet.of("file")) && (options.has("rollback")))) {
                System.err.println("Missing required arguments: " + Joiner.on(", ").join(missing));
                parser.printHelpOn(System.err);
                System.exit(1);
            }
        }

        String clusterXml = (String) options.valueOf("cluster");
        String storeName = (String) options.valueOf("name");
        String filePath = (String) options.valueOf("file");
        int timeoutMs = CmdUtils.valueOf(options,
                "timeout",
                (int) (3 * Time.SECONDS_PER_HOUR * Time.MS_PER_SECOND));
        boolean rollbackStore = options.has("rollback");
        Long pushVersion = (Long) options.valueOf("push-version");

        String clusterStr = FileUtils.readFileToString(new File(clusterXml));
        Cluster cluster = new ClusterMapper().readCluster(new StringReader(clusterStr));
        ExecutorService executor = Executors.newFixedThreadPool(cluster.getNumberOfNodes());
        AdminClient adminClient = new AdminClient(cluster, new AdminClientConfig(), new ClientConfig());
        AdminStoreSwapper swapper = new AdminStoreSwapper(cluster, executor, adminClient, timeoutMs);

        try {
            long start = System.currentTimeMillis();
            if(rollbackStore) {
                swapper.invokeRollback(storeName, pushVersion.longValue());
            } else {
                swapper.swapStoreData(storeName, filePath, pushVersion.longValue());
            }
            long end = System.currentTimeMillis();
            logger.info("Succeeded on all nodes in " + ((end - start) / Time.MS_PER_SECOND)
                    + " seconds.");
        } finally {
            if(adminClient != null)
                adminClient.close();
            executor.shutdownNow();
            executor.awaitTermination(1, TimeUnit.SECONDS);
        }
        System.exit(0);
    }

    public static class Response {
        private final boolean successful;
        private final String response;
        private final Exception exception;

        private Response(boolean successful, String response, Exception exception) {
            this.successful = successful;
            this.response = response;
            this.exception = exception;
        }
        /**
         * Constructor for failed fetches
         * @param exception
         */
        public Response(Exception exception) {
            this(false, null, exception);
        }

        /**
         * Constructor for successful fetches
         * @param response
         */
        public Response(String response) {
            this(true, response, null);
        }

        public boolean isSuccessful() {
            return successful;
        }

        public String getResponse() {
            return response;
        }

        public Exception getException() {
            return exception;
        }
    }
}