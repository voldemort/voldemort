package voldemort.store.readonly.swapper;

import java.io.File;
import java.io.StringReader;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.commons.httpclient.HostConfiguration;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.utils.CmdUtils;
import voldemort.utils.Time;
import voldemort.xml.ClusterMapper;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;

/**
 * A helper class to invoke the FETCH and SWAP operations on a remote store via
 * HTTP.
 * 
 * 
 */
public abstract class StoreSwapper {

    private static final Logger logger = Logger.getLogger(StoreSwapper.class);

    protected final Cluster cluster;
    protected final ExecutorService executor;

    public StoreSwapper(Cluster cluster, ExecutorService executor) {
        super();
        this.cluster = cluster;
        this.executor = executor;
    }

    public void swapStoreData(String storeName, String basePath, long pushVersion) {
        List<String> fetched = invokeFetch(storeName, basePath, pushVersion);
        invokeSwap(storeName, fetched);
    }

    public abstract List<String> invokeFetch(String storeName, String basePath, long pushVersion);

    public abstract void invokeSwap(String storeName, List<String> fetchFiles);

    public abstract void invokeRollback(String storeName, long pushVersion);

    public static void main(String[] args) throws Exception {
        OptionParser parser = new OptionParser();
        parser.accepts("help", "print usage information");
        parser.accepts("cluster", "[REQUIRED] the voldemort cluster.xml file ")
              .withRequiredArg()
              .describedAs("cluster.xml");
        parser.accepts("name", "[REQUIRED] the name of the store to swap")
              .withRequiredArg()
              .describedAs("store-name");
        parser.accepts("servlet-path", "the path for the read-only management servlet")
              .withRequiredArg()
              .describedAs("path");
        parser.accepts("file", "[REQUIRED] uri of a directory containing the new store files")
              .withRequiredArg()
              .describedAs("uri");
        parser.accepts("timeout", "http timeout for the fetch in ms")
              .withRequiredArg()
              .describedAs("timeout ms")
              .ofType(Integer.class);
        parser.accepts("rollback", "Rollback store to older version");
        parser.accepts("admin", "Use admin services. Default = false");
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
        String mgmtPath = CmdUtils.valueOf(options, "servlet-path", "read-only/mgmt");
        String filePath = (String) options.valueOf("file");
        int timeoutMs = CmdUtils.valueOf(options,
                                         "timeout",
                                         (int) (3 * Time.SECONDS_PER_HOUR * Time.MS_PER_SECOND));
        boolean useAdminServices = options.has("admin");
        boolean rollbackStore = options.has("rollback");
        Long pushVersion = (Long) options.valueOf("push-version");

        String clusterStr = FileUtils.readFileToString(new File(clusterXml));
        Cluster cluster = new ClusterMapper().readCluster(new StringReader(clusterStr));
        ExecutorService executor = Executors.newFixedThreadPool(cluster.getNumberOfNodes());
        StoreSwapper swapper = null;
        AdminClient adminClient = null;

        if(useAdminServices) {
            adminClient = new AdminClient(cluster, new AdminClientConfig());
            swapper = new AdminStoreSwapper(cluster, executor, adminClient, timeoutMs);
        } else {
            HttpConnectionManager manager = new MultiThreadedHttpConnectionManager();

            int numConnections = cluster.getNumberOfNodes() + 3;
            manager.getParams().setMaxTotalConnections(numConnections);
            manager.getParams().setMaxConnectionsPerHost(HostConfiguration.ANY_HOST_CONFIGURATION,
                                                         numConnections);
            HttpClient client = new HttpClient(manager);
            client.getParams().setParameter("http.socket.timeout", timeoutMs);

            swapper = new HttpStoreSwapper(cluster, executor, client, mgmtPath);
        }

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
            if(useAdminServices && adminClient != null)
                adminClient.stop();
            executor.shutdownNow();
            executor.awaitTermination(1, TimeUnit.SECONDS);
        }
        System.exit(0);
    }
}
