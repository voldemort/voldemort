package voldemort.store.readonly;

import java.io.File;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.httpclient.HostConfiguration;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.utils.Time;
import voldemort.utils.Utils;
import voldemort.xml.ClusterMapper;

/**
 * A helper class to invoke the FETCH and SWAP operations on a remote store via
 * HTTP.
 * 
 * @author jay
 * 
 */
public class StoreSwapper {

    private static final Logger logger = Logger.getLogger(StoreSwapper.class);

    private final Cluster cluster;
    private final ExecutorService executor;
    private final HttpClient httpClient;
    private final String readOnlyMgmtPath;

    public StoreSwapper(Cluster cluster,
                        ExecutorService executor,
                        HttpClient httpClient,
                        String readOnlyMgmtPath) {
        super();
        this.cluster = cluster;
        this.executor = executor;
        this.httpClient = httpClient;
        this.readOnlyMgmtPath = readOnlyMgmtPath;
    }

    public void swapStoreData(String storeName, String basePath) {
        List<String> fetched = invokeFetch(basePath);
        invokeSwap(storeName, fetched);
    }

    private List<String> invokeFetch(final String basePath) {
        // do fetch
        Map<Integer, Future<String>> fetchDirs = new HashMap<Integer, Future<String>>();
        for(final Node node: cluster.getNodes()) {
            fetchDirs.put(node.getId(), executor.submit(new Callable<String>() {

                public String call() throws Exception {
                    String url = node.getHttpUrl() + "/" + readOnlyMgmtPath;
                    PostMethod post = new PostMethod(url);
                    post.addParameter("operation", "fetch");
                    String storeDir = basePath + "/node-" + node.getId();
                    post.addParameter("dir", storeDir);
                    logger.info("Invoking fetch for node " + node.getId() + " for " + storeDir);
                    int responseCode = httpClient.executeMethod(post);
                    String response = post.getResponseBodyAsString(30000);
                    if(responseCode != 200)
                        throw new VoldemortException("Swap request on node " + node.getId() + " ("
                                                     + url + ") failed: " + post.getStatusText());
                    logger.info("Fetch succeeded on node " + node.getId());
                    return response.trim();
                }
            }));
        }

        // wait for all operations to complete successfully
        List<String> results = new ArrayList<String>();
        for(int nodeId = 0; nodeId < cluster.getNumberOfNodes(); nodeId++) {
            Future<String> val = fetchDirs.get(nodeId);
            try {
                results.add(val.get());
            } catch(ExecutionException e) {
                throw new VoldemortException(e.getCause());
            } catch(InterruptedException e) {
                throw new VoldemortException(e);
            }
        }

        return results;
    }

    private void invokeSwap(String storeName, List<String> fetchFiles) {
        // do swap
        int successes = 0;
        Exception exception = null;
        for(int nodeId = 0; nodeId < cluster.getNumberOfNodes(); nodeId++) {
            try {
                Node node = cluster.getNodeById(nodeId);
                String url = node.getHttpUrl() + "/" + readOnlyMgmtPath;
                PostMethod post = new PostMethod(url);
                post.addParameter("operation", "swap");
                String dir = fetchFiles.get(nodeId);
                logger.info("Attempting swap for node " + nodeId + " dir = " + dir);
                post.addParameter("dir", dir);
                post.addParameter("store", storeName);
                int responseCode = httpClient.executeMethod(post);
                String response = post.getStatusText();
                if(responseCode == 200) {
                    successes++;
                    logger.info("Swap succeeded for node " + node.getId());
                } else {
                    throw new VoldemortException(response);
                }
            } catch(Exception e) {
                exception = e;
                logger.error("Exception thrown during swap operation on node " + nodeId + ": ", e);
            }
        }

        if(exception != null)
            throw new VoldemortException(exception);
    }

    public static void main(String[] args) throws Exception {
        if(args.length != 4)
            Utils.croak("USAGE: cluster.xml store_name mgmtpath store_file_path");
        String clusterXml = args[0];
        String storeName = args[1];
        String mgmtPath = args[2];
        String filePath = args[3];

        String clusterStr = FileUtils.readFileToString(new File(clusterXml));
        Cluster cluster = new ClusterMapper().readCluster(new StringReader(clusterStr));
        ExecutorService executor = Executors.newFixedThreadPool(10);
        HttpConnectionManager manager = new MultiThreadedHttpConnectionManager();

        int numConnections = cluster.getNumberOfNodes() + 3;
        manager.getParams().setMaxTotalConnections(numConnections);
        manager.getParams().setMaxConnectionsPerHost(HostConfiguration.ANY_HOST_CONFIGURATION,
                                                     numConnections);
        HttpClient client = new HttpClient(manager);
        client.getParams().setParameter("http.socket.timeout", 3 * 60 * 60 * 1000);

        StoreSwapper swapper = new StoreSwapper(cluster, executor, client, mgmtPath);
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
