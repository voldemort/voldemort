package voldemort.store.readonly.swapper;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.store.readonly.ReadOnlyUtils;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

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
                        throw new VoldemortException("Fetch request on node " + node.getId() + " ("
                                                     + url + ") failed: " + post.getStatusText());
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

            // Delete data from successful nodes
            for(int successfulNodeId: results.keySet()) {
                try {
                    String url = cluster.getNodeById(successfulNodeId).getHttpUrl() + "/"
                                 + readOnlyMgmtPath;
                    PostMethod post = new PostMethod(url);
                    post.addParameter("operation", "failed-fetch");
                    post.addParameter("dir", results.get(successfulNodeId));
                    post.addParameter("store", storeName);
                    logger.info("Deleting fetched data from node " + successfulNodeId);

                    int responseCode = httpClient.executeMethod(post);
                    String response = post.getStatusText();

                    if(responseCode == 200) {
                        logger.info("Deleted successfully on node " + successfulNodeId);
                    } else {
                        throw new VoldemortException(response);
                    }
                } catch(Exception e) {
                    logger.error("Exception thrown during delete operation on node "
                                 + successfulNodeId + " : ", e);
                }
            }

            // Finally log the errors for the user
            for(int failedNodeId: exceptions.keySet()) {
                logger.error("Error on node " + failedNodeId + " during push : ",
                             exceptions.get(failedNodeId));
            }

            throw new VoldemortException("Exception during pushes to nodes "
                                         + Joiner.on(',').join(exceptions.keySet()) + " failed");
        }

        return Lists.newArrayList(results.values());
    }

    @Override
    protected void invokeSwap(final String storeName, final List<String> fetchFiles) {
        // do swap in parallel
        Map<Integer, String> previousDirs = new HashMap<Integer, String>();
        HashMap<Integer, Exception> exceptions = Maps.newHashMap();

        for(final Node node: cluster.getNodes()) {
            try {
                String url = node.getHttpUrl() + "/" + readOnlyMgmtPath;
                PostMethod post = new PostMethod(url);
                post.addParameter("operation", "swap");
                String dir = fetchFiles.get(node.getId());
                logger.info("Attempting swap for node " + node.getId() + " dir = " + dir);
                post.addParameter("dir", dir);
                post.addParameter("store", storeName);

                int responseCode = httpClient.executeMethod(post);
                String previousDir = post.getResponseBodyAsString(30000);

                if(responseCode != 200)
                    throw new VoldemortException("Swap request on node " + node.getId() + " ("
                                                 + url + ") failed: " + post.getStatusText());
                logger.info("Swap succeeded on node " + node.getId());
                previousDirs.put(node.getId(), previousDir);
            } catch(Exception e) {
                exceptions.put(node.getId(), e);
                logger.error("Exception thrown during swap operation on node " + node.getId()
                             + ": ", e);
            }
        }

        if(!exceptions.isEmpty()) {
            // Rollback data on successful nodes
            for(int successfulNodeId: previousDirs.keySet()) {
                try {
                    String url = cluster.getNodeById(successfulNodeId).getHttpUrl() + "/"
                                 + readOnlyMgmtPath;
                    PostMethod post = new PostMethod(url);
                    post.addParameter("operation", "rollback");
                    post.addParameter("store", storeName);
                    post.addParameter("pushVersion",
                                      Long.toString(ReadOnlyUtils.getVersionId(new File(previousDirs.get(successfulNodeId)))));

                    logger.info("Rolling back data on successful node " + successfulNodeId);

                    int responseCode = httpClient.executeMethod(post);
                    String response = post.getStatusText();

                    if(responseCode == 200) {
                        logger.info("Rollback succeeded for node " + successfulNodeId);
                    } else {
                        throw new VoldemortException(response);
                    }
                } catch(Exception e) {
                    logger.error("Exception thrown during rollback ( after swap ) operation on node "
                                         + successfulNodeId + " : ",
                                 e);
                }
            }

            // Finally log the errors for the user
            for(int failedNodeId: exceptions.keySet()) {
                logger.error("Error on node " + failedNodeId + " during swap : ",
                             exceptions.get(failedNodeId));
            }

            throw new VoldemortException("Exception during swaps on nodes "
                                         + Joiner.on(',').join(exceptions.keySet()) + " failed");
        }

    }

    @Override
    protected void invokeRollback(String storeName, final long pushVersion) {
        Exception exception = null;
        for(Node node: cluster.getNodes()) {
            try {
                logger.info("Attempting rollback for node " + node.getId() + " storeName = "
                            + storeName);
                String url = node.getHttpUrl() + "/" + readOnlyMgmtPath;
                PostMethod post = new PostMethod(url);
                post.addParameter("operation", "rollback");
                post.addParameter("store", storeName);
                post.addParameter("pushVersion", Long.toString(pushVersion));

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
