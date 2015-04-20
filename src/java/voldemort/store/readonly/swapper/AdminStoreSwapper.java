package voldemort.store.readonly.swapper;

import java.io.File;
import java.io.StringReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableSet;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.commons.io.FileUtils;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.ClientConfig;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.store.readonly.ReadOnlyUtils;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import voldemort.utils.CmdUtils;
import voldemort.utils.Time;
import voldemort.utils.VoldemortIOUtils;
import voldemort.xml.ClusterMapper;

public class AdminStoreSwapper {

    private static final Logger logger = Logger.getLogger(AdminStoreSwapper.class);

    private AdminClient adminClient;
    private long timeoutMs;
    private boolean deleteFailedFetch = false;
    private boolean rollbackFailedSwap = false;

    protected final Cluster cluster;
    protected final ExecutorService executor;

    public AdminStoreSwapper(Cluster cluster, ExecutorService executor) {
        this.cluster = cluster;
        this.executor = executor;
    }

    /**
     * 
     * @param cluster The cluster metadata
     * @param executor Executor to use for running parallel fetch / swaps
     * @param adminClient The admin client to use for querying
     * @param timeoutMs Time out in ms
     * @param deleteFailedFetch Boolean to indicate we want to delete data on
     *        successful nodes after a fetch fails somewhere
     * @param rollbackFailedSwap Boolean to indicate we want to rollback the
     *        data on successful nodes after a swap fails somewhere
     */
    public AdminStoreSwapper(Cluster cluster,
                             ExecutorService executor,
                             AdminClient adminClient,
                             long timeoutMs,
                             boolean deleteFailedFetch,
                             boolean rollbackFailedSwap) {
        this(cluster, executor);
        this.adminClient = adminClient;
        this.timeoutMs = timeoutMs;
        this.deleteFailedFetch = deleteFailedFetch;
        this.rollbackFailedSwap = rollbackFailedSwap;
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
        List<String> fetched = invokeFetch(storeName, basePath, pushVersion);
        invokeSwap(storeName, fetched);
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

    public List<String> invokeFetch(final String storeName,
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

        // wait for all operations to complete successfully
        TreeMap<Integer, String> results = Maps.newTreeMap();
        HashMap<Integer, Exception> exceptions = Maps.newHashMap();

        for(int nodeId = 0; nodeId < cluster.getNumberOfNodes(); nodeId++) {
            Future<String> val = fetchDirs.get(nodeId);
            try {
                results.put(nodeId, val.get());
            } catch(Exception e) {
                exceptions.put(nodeId, new VoldemortException(e));
            }
        }

        if(!exceptions.isEmpty()) {

            if(deleteFailedFetch) {
                // Delete data from successful nodes
                for(int successfulNodeId: results.keySet()) {
                    try {
                        logger.info("Deleting fetched data from node " + successfulNodeId);

                        adminClient.readonlyOps.failedFetchStore(successfulNodeId,
                                                                 storeName,
                                                                 results.get(successfulNodeId));
                    } catch(Exception e) {
                        logger.error("Exception thrown during delete operation on node "
                                     + successfulNodeId + " : ", e);
                    }
                }
            }

            // Finally log the errors for the user
            for(int failedNodeId: exceptions.keySet()) {
                logger.error("Error on node " + failedNodeId + " during push : ",
                             exceptions.get(failedNodeId));
            }

            throw new VoldemortException("Exception during pushes to nodes "
                                         + Joiner.on(",").join(exceptions.keySet()) + " failed");
        }

        return Lists.newArrayList(results.values());
    }

    public void invokeSwap(final String storeName, final List<String> fetchFiles) {
        // do swap
        Map<Integer, String> previousDirs = new HashMap<Integer, String>();
        HashMap<Integer, Exception> exceptions = Maps.newHashMap();

        for(int nodeId = 0; nodeId < cluster.getNumberOfNodes(); nodeId++) {
            try {
                String dir = fetchFiles.get(nodeId);
                logger.info("Attempting swap for node " + nodeId + " dir = " + dir);
                previousDirs.put(nodeId, adminClient.readonlyOps.swapStore(nodeId, storeName, dir));
                logger.info("Swap succeeded for node " + nodeId);
            } catch(Exception e) {
                exceptions.put(nodeId, e);
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

}