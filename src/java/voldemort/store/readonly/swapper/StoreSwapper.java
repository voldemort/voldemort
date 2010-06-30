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

import voldemort.cluster.Cluster;
import voldemort.utils.CmdUtils;
import voldemort.utils.Time;
import voldemort.xml.ClusterMapper;

import com.google.common.base.Joiner;

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

    public void swapStoreData(String storeName, String basePath) {
        List<String> fetched = invokeFetch(storeName, basePath);
        invokeSwap(storeName, fetched);
    }

    protected abstract List<String> invokeFetch(final String storeName, final String basePath);

    protected abstract void invokeSwap(String storeName, List<String> fetchFiles);

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
        parser.accepts("admin", "Use admin services. Default = false");

        OptionSet options = parser.parse(args);
        if(options.has("help")) {
            parser.printHelpOn(System.out);
            System.exit(0);
        }

        Set<String> missing = CmdUtils.missing(options, "cluster", "name", "file");
        if(missing.size() > 0) {
            System.err.println("Missing required arguments: " + Joiner.on(", ").join(missing));
            parser.printHelpOn(System.err);
            System.exit(1);
        }

        String clusterXml = (String) options.valueOf("cluster");
        String storeName = (String) options.valueOf("name");
        String mgmtPath = CmdUtils.valueOf(options, "servlet-path", "read-only/mgmt");
        String filePath = (String) options.valueOf("file");
        int timeoutMs = CmdUtils.valueOf(options,
                                         "timeout",
                                         (int) (3 * Time.SECONDS_PER_HOUR * Time.MS_PER_SECOND));
        boolean useAdminServices = options.has("admin");

        String clusterStr = FileUtils.readFileToString(new File(clusterXml));
        Cluster cluster = new ClusterMapper().readCluster(new StringReader(clusterStr));
        ExecutorService executor = Executors.newFixedThreadPool(10);
        StoreSwapper swapper = null;
        if(useAdminServices) {
            swapper = new AdminStoreSwapper(cluster, executor);
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
        long start = System.currentTimeMillis();
        swapper.swapStoreData(storeName, filePath);
        long end = System.currentTimeMillis();
        logger.info("Swap succeeded on all nodes in " + ((end - start) / Time.MS_PER_SECOND)
                    + " seconds.");
        executor.shutdownNow();
        executor.awaitTermination(1, TimeUnit.SECONDS);
        System.exit(0);
    }
}
