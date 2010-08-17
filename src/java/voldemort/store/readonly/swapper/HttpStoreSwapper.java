package voldemort.store.readonly.swapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;

public class HttpStoreSwapper extends StoreSwapper {

    private static final Logger logger = Logger.getLogger(HttpStoreSwapper.class);

    private final HttpClient httpClient;
    private final String readOnlyMgmtPath;

    public HttpStoreSwapper(Cluster cluster,
                            ExecutorService executor,
                            HttpClient httpClient,
                            String readOnlyMgmtPath) {
        super(cluster, executor);
        this.httpClient = httpClient;
        this.readOnlyMgmtPath = readOnlyMgmtPath;
    }

    @Override
    protected List<String> invokeFetch(final String storeName,
                                       final String basePath,
                                       final long pushVersion) {
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
                    post.addParameter("store", storeName);
                    if(pushVersion > 0)
                        post.addParameter("pushVersion", Long.toString(pushVersion));
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

    @Override
    protected void invokeSwap(String storeName, List<String> fetchFiles) {
        // do swap
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

    @Override
    protected void invokeRollback(String storeName) {
        Exception exception = null;
        for(Node node: cluster.getNodes()) {
            try {
                logger.info("Attempting rollback for node " + node.getId() + " storeName = "
                            + storeName);
                String url = node.getHttpUrl() + "/" + readOnlyMgmtPath;
                PostMethod post = new PostMethod(url);
                post.addParameter("operation", "rollback");
                post.addParameter("store", storeName);
                int responseCode = httpClient.executeMethod(post);
                String response = post.getStatusText();
                if(responseCode == 200) {
                    logger.info("Rollback succeeded for node " + node.getId());
                } else {
                    throw new VoldemortException(response);
                }
            } catch(Exception e) {
                exception = e;
                logger.error("Exception thrown during rollback operation on node " + node.getId()
                             + ": ", e);
            }
        }

        if(exception != null)
            throw new VoldemortException(exception);
    }
}
