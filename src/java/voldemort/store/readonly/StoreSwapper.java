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

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
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
    private final String basePath;

    public StoreSwapper(Cluster cluster,
                        ExecutorService executor,
                        HttpClient httpClient,
                        String readOnlyMgmtPath,
                        String basePath) {
        super();
        this.cluster = cluster;
        this.executor = executor;
        this.httpClient = httpClient;
        this.readOnlyMgmtPath = readOnlyMgmtPath;
        this.basePath = basePath;
    }

    public void swapStoreData(String storeName) {
        List<String[]> fetched = invokeFetch();
        invokeSwap(storeName, fetched);
    }

    private List<String[]> invokeFetch() {
        // do fetch
        Map<Integer, Future<String[]>> fetchFiles = new HashMap<Integer, Future<String[]>>();
        for(final Node node: cluster.getNodes()) {
            fetchFiles.put(node.getId(), executor.submit(new Callable<String[]>() {

                public String[] call() throws Exception {
                    String url = node.getHttpUrl() + "/" + readOnlyMgmtPath;
                    PostMethod post = new PostMethod(url);
                    post.addParameter("operation", "fetch");
                    String indexFile = basePath + "/" + node.getId() + ".index";
                    String dataFile = basePath + "/" + node.getId() + ".data";
                    post.addParameter("index", indexFile);
                    post.addParameter("data", dataFile);
                    logger.info("Invoking fetch for node " + node.getId() + " for " + indexFile
                                + " and " + dataFile);
                    int responseCode = httpClient.executeMethod(post);
                    String response = post.getResponseBodyAsString(30000);
                    if(responseCode != 200)
                        throw new VoldemortException("Swap request on node " + node.getId()
                                                     + " failed: " + post.getStatusText());
                    String[] files = response.split("\n");
                    if(files.length != 2)
                        throw new VoldemortException("Expected two files, but found "
                                                     + files.length + " in '" + response + "'.");
                    logger.info("Fetch succeeded on node " + node.getId());
                    return files;
                }
            }));
        }

        // wait for all operations to complete successfully
        List<String[]> results = new ArrayList<String[]>();
        for(int nodeId = 0; nodeId < cluster.getNumberOfNodes(); nodeId++) {
            Future<String[]> val = fetchFiles.get(nodeId);
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

    private void invokeSwap(String storeName, List<String[]> fetchFiles) {
        // do swap
        int successes = 0;
        Exception exception = null;
        for(int nodeId = 0; nodeId < cluster.getNumberOfNodes(); nodeId++) {
            try {
                Node node = cluster.getNodeById(nodeId);
                String url = node.getHttpUrl() + "/" + readOnlyMgmtPath;
                PostMethod post = new PostMethod(url);
                post.addParameter("operation", "swap");
                String indexFile = fetchFiles.get(nodeId)[0];
                String dataFile = fetchFiles.get(nodeId)[1];
                logger.info("Attempting swap for node " + nodeId + " index = " + indexFile
                            + ", data = " + dataFile);
                post.addParameter("index", indexFile);
                post.addParameter("data", dataFile);
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
            Utils.croak("USAGE: cluster.xml store_name mgmtpath file_path");
        String clusterXml = args[0];
        String storeName = args[1];
        String mgmtPath = args[2];
        String filePath = args[3];

        String clusterStr = FileUtils.readFileToString(new File(clusterXml));
        Cluster cluster = new ClusterMapper().readCluster(new StringReader(clusterStr));
        ExecutorService executor = Executors.newFixedThreadPool(10);
        HttpConnectionManager manager = new MultiThreadedHttpConnectionManager();
        HttpClient client = new HttpClient(manager);
        StoreSwapper swapper = new StoreSwapper(cluster, executor, client, mgmtPath, filePath);
        swapper.swapStoreData(storeName);
        logger.info("Swap succeeded on all nodes.");
        executor.shutdownNow();
        executor.awaitTermination(1, TimeUnit.SECONDS);
        System.exit(0);
    }
}
