package voldemort.store.readonly.swapper;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
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
import voldemort.store.quota.QuotaExceededException;
import voldemort.store.readonly.ReadOnlyUtils;
import voldemort.utils.CmdUtils;
import voldemort.utils.Time;
import voldemort.utils.logging.PrefixedLogger;
import voldemort.xml.ClusterMapper;

import java.io.File;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class AdminStoreSwapper {

    private final Logger logger;

    private AdminClient adminClient;
    private long timeoutMs;
    private boolean rollbackFailedSwap = false;
    private final List<FailedFetchStrategy> failedFetchStrategyList;

    protected final Cluster cluster;
    protected final ExecutorService executor;

    /**
     *
     * @param cluster The cluster metadata
     * @param executor Executor to use for running parallel fetch / swaps
     * @param adminClient The admin client to use for querying
     * @param timeoutMs Time out in ms
     * @param rollbackFailedSwap Boolean to indicate we want to rollback the
     * @param failedFetchStrategyList list of {@link FailedFetchStrategy} to execute in case of failure
     * @param clusterName String added as a prefix to all logs. If null, there is no prefix on the logs.
     */
    public AdminStoreSwapper(Cluster cluster,
                             ExecutorService executor,
                             AdminClient adminClient,
                             long timeoutMs,
                             boolean rollbackFailedSwap,
                             List<FailedFetchStrategy> failedFetchStrategyList,
                             String clusterName) {
        this.cluster = cluster;
        this.executor = executor;
        this.adminClient = adminClient;
        this.timeoutMs = timeoutMs;
        this.rollbackFailedSwap = rollbackFailedSwap;
        this.failedFetchStrategyList = failedFetchStrategyList;
        if (clusterName == null) {
            this.logger = Logger.getLogger(AdminStoreSwapper.class.getName());
        } else {
            this.logger = PrefixedLogger.getLogger(AdminStoreSwapper.class.getName(), clusterName);
        }
    }


    /**
     *
     * @param cluster The cluster metadata
     * @param executor Executor to use for running parallel fetch / swaps
     * @param adminClient The admin client to use for querying
     * @param timeoutMs Time out in ms
     * @param deleteFailedFetch Boolean to indicate we want to delete data on
     *                          successful nodes after a fetch fails somewhere.
     * @param rollbackFailedSwap Boolean to indicate we want to rollback the
     *                           data on failed swaps.
     */
    public AdminStoreSwapper(Cluster cluster,
                             ExecutorService executor,
                             AdminClient adminClient,
                             long timeoutMs,
                             boolean deleteFailedFetch,
                             boolean rollbackFailedSwap) {
        this(cluster, executor, adminClient, timeoutMs, rollbackFailedSwap, new ArrayList<FailedFetchStrategy>(), null);
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
        this(cluster, executor, adminClient, timeoutMs, false, new ArrayList<FailedFetchStrategy>(), null);
    }

    public void swapStoreData(String storeName, String basePath, long pushVersion) {
        Map<Node, Response> fetchResponseMap = invokeFetch(storeName, basePath, pushVersion);
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
                logger.info("Attempting rollback for " + node.briefToString() + ", storeName = "
                            + storeName);
                adminClient.readonlyOps.rollbackStore(node.getId(), storeName, pushVersion);
                logger.info("Rollback succeeded for " + node.briefToString());
            } catch(Exception e) {
                exception = e;
                logger.error("Exception thrown during rollback operation on " + node.briefToString()
                             + ": ", e);
            }
        }

        if(exception != null)
            throw new VoldemortException(exception);

    }

    public Map<Node, Response> invokeFetch(final String storeName,
                                    final String basePath,
                                    final long pushVersion) {
        // do fetch
        Map<Integer, Future<String>> fetchDirs = new HashMap<Integer, Future<String>>();
        for(final Node node: cluster.getNodes()) {
            fetchDirs.put(node.getId(), executor.submit(new Callable<String>() {

                public String call() throws Exception {
                    String storeDir = basePath + "/node-" + node.getId();
                    logger.info("Invoking fetch for " + node.briefToString() + " for " + storeDir);
                    String response = adminClient.readonlyOps.fetchStore(node.getId(),
                                                                         storeName,
                                                                         storeDir,
                                                                         pushVersion,
                                                                         timeoutMs);
                    if(response == null)
                        throw new VoldemortException("Fetch request on " + node.briefToString() + " failed");
                    logger.info("Fetch succeeded on " + node.briefToString());
                    return response.trim();
                }
            }));
        }

        Map<Node, Response> fetchResponseMap = Maps.newTreeMap();
        boolean fetchErrors = false;

        /*
         * We wait for all fetches to complete successfully or throw any
         * Exception. We dont handle QuotaException in a special way here. The
         * idea is to protect the disk. It is okay to let the Bnp job run to
         * completion. We still want to delete data of a failed fetch in all
         * nodes that successfully fetched the data. After deleting the
         * failedFetch data, we bubble up the Quota Exception as needed.
         * 
         * The alternate is to cancel all future tasks as soon as we detect a
         * QuotaExceededException. This will save time (fail faster) and protect
         * the disk usage. But does not guarantee a clean state in all nodes wrt
         * to data from failed fetch. Someone manually needs to clean up all the
         * data from failedFetches. Instead we try to cleanup the data as much
         * as we can before we fail the job.
         * 
         * In iteration 2 we can try to improve this to fail faster, by adding
         * either/both:
         * 
         * 1. Client side checks 2. Server side takes care of failing fast as
         * soon as it detect QuotaExceededException in one of the servers. Note
         * that this needs careful decision on how to handle those fetches that
         * already started in other nodes and how & when to clean them up.
         */
        int numQuotaExceededException = 0;
        boolean invalidBootstrapURLExceptions = false;
        for(final Node node: cluster.getNodes()) {
            Future<String> val = fetchDirs.get(node.getId());
            try {
                String response = val.get();
                fetchResponseMap.put(node, new Response(response));
            } catch(ExecutionException e) {
                fetchErrors = true;
                if(e.getCause() instanceof QuotaExceededException) {
                    numQuotaExceededException++;
                    fetchResponseMap.put(node, new Response((QuotaExceededException) e.getCause()));
                } else if(e.getCause() instanceof InvalidBootstrapURLException) {
                    invalidBootstrapURLExceptions = true;
                } else {
                    fetchResponseMap.put(node, new Response(e));
                }
            }
            catch(Exception e) {
                fetchErrors = true;
                fetchResponseMap.put(node, new Response(e));
            }
        }

        // Invalid stores should fail faster
        if(invalidBootstrapURLExceptions) {
            throw new InvalidBootstrapURLException("Exceptions during push. Invalid bootstrap url. Please check your "
                                                   + "cluster bootstrap URL");
        }

        if(fetchErrors) {
            // Log All the errors for the user
            for(Map.Entry<Node, Response> entry: fetchResponseMap.entrySet()) {
                if (!entry.getValue().isSuccessful()) {
                    logger.error("Error on " + entry.getKey().briefToString() + " during push : ",
                            entry.getValue().getException());
                }
            }

            Iterator<FailedFetchStrategy> strategyIterator = failedFetchStrategyList.iterator();
            boolean swapIsPossible = false;
            FailedFetchStrategy strategy = null;
            while (strategyIterator.hasNext() && !swapIsPossible) {
                strategy = strategyIterator.next();
                try {
                    logger.info("About to attempt: " + strategy.toString());
                    swapIsPossible = strategy.dealWithIt(storeName, pushVersion, fetchResponseMap);
                    logger.info("Finished executing: " + strategy.toString() + "; swapIsPossible: " + swapIsPossible);
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
                String errorMessage = "";
                if(numQuotaExceededException > 0) {
                    errorMessage = "Disk Quota exceeded. Swap will be aborted.";
                } else {
                    errorMessage = "Exception during push. Swap will be aborted.";
                }
                throw new VoldemortException(errorMessage);
            }
        }
        return fetchResponseMap;
    }

    public void invokeSwap(final String storeName, final Map<Node, Response> fetchResponseMap) {
        // do swap
        Map<Node, String> previousDirs = Maps.newHashMap();
        HashMap<Node, Exception> exceptions = Maps.newHashMap();

        for(Map.Entry<Node, Response> entry: fetchResponseMap.entrySet()) {
            Response response = entry.getValue();
            if (response.isSuccessful()) {
                Node node = entry.getKey();
                Integer nodeId = node.getId();
                String dir = response.getResponse();
                try {
                    logger.info("Attempting swap for " + node.briefToString() + ", dir = " + dir);
                    previousDirs.put(node, adminClient.readonlyOps.swapStore(nodeId, storeName, dir));
                    logger.info("Swap succeeded for " + node.briefToString());
                } catch(Exception e) {
                    exceptions.put(node, e);
                }
            }
        }

        if(!exceptions.isEmpty()) {

            if(rollbackFailedSwap) {
                // Rollback data on successful nodes
                for(Node node: previousDirs.keySet()) {
                    try {
                        int successfulNodeId = node.getId();
                        logger.info("Rolling back data on successful " + node.briefToString());
                        adminClient.readonlyOps.rollbackStore(successfulNodeId,
                                storeName,
                                ReadOnlyUtils.getVersionId(new File(previousDirs.get(node))));
                        logger.info("Rollback succeeded for " + node.briefToString());
                    } catch(Exception e) {
                        logger.error("Exception thrown during rollback ( after swap ) operation on "
                                             + node.briefToString() + ": ",
                                     e);
                    }
                }
            }

            // Finally log the errors for the user
            for(Node node: exceptions.keySet()) {
                logger.error("Error on " + node.briefToString() + " during swap : ",
                             exceptions.get(node));
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
            swapper.logger.info("Succeeded on all nodes in " + ((end - start) / Time.MS_PER_SECOND)
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

        @Override
        public String toString() {
            if (successful) {
                return "Successful Response: " + response;
            } else {
                return "Unsuccessful Response: " + exception.getMessage();
            }
        }
    }
}