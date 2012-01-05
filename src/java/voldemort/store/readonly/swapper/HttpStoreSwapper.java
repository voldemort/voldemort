package voldemort.store.readonly.swapper;

import java.io.File;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.message.BasicNameValuePair;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.store.readonly.ReadOnlyUtils;
import voldemort.utils.VoldemortIOUtils;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class HttpStoreSwapper extends StoreSwapper {

    private static final Logger logger = Logger.getLogger(HttpStoreSwapper.class);

    private final HttpClient httpClient;
    private final String readOnlyMgmtPath;
    private boolean deleteFailedFetch = false;
    private boolean rollbackFailedSwap = false;

    public HttpStoreSwapper(Cluster cluster,
                            ExecutorService executor,
                            HttpClient httpClient,
                            String readOnlyMgmtPath,
                            boolean deleteFailedFetch,
                            boolean rollbackFailedSwap) {
        super(cluster, executor);
        this.httpClient = httpClient;
        this.readOnlyMgmtPath = readOnlyMgmtPath;
        this.deleteFailedFetch = deleteFailedFetch;
        this.rollbackFailedSwap = rollbackFailedSwap;
    }

    public HttpStoreSwapper(Cluster cluster,
                            ExecutorService executor,
                            HttpClient httpClient,
                            String readOnlyMgmtPath) {
        super(cluster, executor);
        this.httpClient = httpClient;
        this.readOnlyMgmtPath = readOnlyMgmtPath;
    }

    @Override
    public List<String> invokeFetch(final String storeName,
                                    final String basePath,
                                    final long pushVersion) {
        // do fetch
        Map<Integer, Future<String>> fetchDirs = new HashMap<Integer, Future<String>>();
        for(final Node node: cluster.getNodes()) {
            fetchDirs.put(node.getId(), executor.submit(new Callable<String>() {

                public String call() throws Exception {
                    String url = node.getHttpUrl() + "/" + readOnlyMgmtPath;
                    HttpPost post = new HttpPost(url);

                    List<NameValuePair> params = new ArrayList<NameValuePair>();
                    params.add(new BasicNameValuePair("operation", "fetch"));
                    String storeDir = basePath + "/node-" + node.getId();
                    params.add(new BasicNameValuePair("dir", storeDir));
                    params.add(new BasicNameValuePair("store", storeName));
                    if(pushVersion > 0)
                        params.add(new BasicNameValuePair("pushVersion", Long.toString(pushVersion)));
                    post.setEntity(new UrlEncodedFormEntity(params));

                    logger.info("Invoking fetch for node " + node.getId() + " for " + storeDir);

                    HttpResponse httpResponse = null;
                    try {
                        httpResponse = httpClient.execute(post);
                        int responseCode = httpResponse.getStatusLine().getStatusCode();
                        InputStream is = httpResponse.getEntity().getContent();
                        String response = VoldemortIOUtils.toString(is, 30000);

                        if(responseCode != HttpURLConnection.HTTP_OK)
                            throw new VoldemortException("Fetch request on node "
                                                         + node.getId()
                                                         + " ("
                                                         + url
                                                         + ") failed: "
                                                         + httpResponse.getStatusLine()
                                                                       .getReasonPhrase());
                        logger.info("Fetch succeeded on node " + node.getId());
                        return response.trim();
                    } finally {
                        VoldemortIOUtils.closeQuietly(httpResponse);
                    }
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
                    HttpResponse httpResponse = null;
                    try {
                        String url = cluster.getNodeById(successfulNodeId).getHttpUrl() + "/"
                                     + readOnlyMgmtPath;
                        HttpPost post = new HttpPost(url);

                        List<NameValuePair> params = new ArrayList<NameValuePair>();
                        params.add(new BasicNameValuePair("operation", "failed-fetch"));
                        params.add(new BasicNameValuePair("dir", results.get(successfulNodeId)));
                        params.add(new BasicNameValuePair("store", storeName));
                        post.setEntity(new UrlEncodedFormEntity(params));

                        logger.info("Deleting fetched data from node " + successfulNodeId);

                        httpResponse = httpClient.execute(post);
                        int responseCode = httpResponse.getStatusLine().getStatusCode();
                        String response = httpResponse.getStatusLine().getReasonPhrase();

                        if(responseCode == 200) {
                            logger.info("Deleted successfully on node " + successfulNodeId);
                        } else {
                            throw new VoldemortException(response);
                        }
                    } catch(Exception e) {
                        logger.error("Exception thrown during delete operation on node "
                                     + successfulNodeId + " : ", e);
                    } finally {
                        VoldemortIOUtils.closeQuietly(httpResponse);
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

    @Override
    public void invokeSwap(final String storeName, final List<String> fetchFiles) {
        // do swap in parallel
        Map<Integer, String> previousDirs = new HashMap<Integer, String>();
        HashMap<Integer, Exception> exceptions = Maps.newHashMap();

        for(final Node node: cluster.getNodes()) {
            HttpResponse httpResponse = null;
            try {
                String url = node.getHttpUrl() + "/" + readOnlyMgmtPath;
                HttpPost post = new HttpPost(url);

                List<NameValuePair> params = new ArrayList<NameValuePair>();
                params.add(new BasicNameValuePair("operation", "swap"));
                String dir = fetchFiles.get(node.getId());
                logger.info("Attempting swap for node " + node.getId() + " dir = " + dir);
                params.add(new BasicNameValuePair("dir", dir));
                params.add(new BasicNameValuePair("store", storeName));
                post.setEntity(new UrlEncodedFormEntity(params));

                httpResponse = httpClient.execute(post);
                int responseCode = httpResponse.getStatusLine().getStatusCode();
                String previousDir = VoldemortIOUtils.toString(httpResponse.getEntity()
                                                                           .getContent(), 30000);

                if(responseCode != HttpURLConnection.HTTP_OK)
                    throw new VoldemortException("Swap request on node " + node.getId() + " ("
                                                 + url + ") failed: "
                                                 + httpResponse.getStatusLine().getReasonPhrase());
                logger.info("Swap succeeded on node " + node.getId());
                previousDirs.put(node.getId(), previousDir);
            } catch(Exception e) {
                exceptions.put(node.getId(), e);
                logger.error("Exception thrown during swap operation on node " + node.getId()
                             + ": ", e);
            } finally {
                VoldemortIOUtils.closeQuietly(httpResponse, node.toString());
            }
        }

        if(!exceptions.isEmpty()) {
            if(rollbackFailedSwap) {
                // Rollback data on successful nodes
                for(int successfulNodeId: previousDirs.keySet()) {
                    HttpResponse httpResponse = null;
                    try {
                        String url = cluster.getNodeById(successfulNodeId).getHttpUrl() + "/"
                                     + readOnlyMgmtPath;
                        HttpPost post = new HttpPost(url);

                        List<NameValuePair> params = new ArrayList<NameValuePair>();
                        params.add(new BasicNameValuePair("operation", "rollback"));
                        params.add(new BasicNameValuePair("store", storeName));
                        params.add(new BasicNameValuePair("pushVersion",
                                                          Long.toString(ReadOnlyUtils.getVersionId(new File(previousDirs.get(successfulNodeId))))));
                        post.setEntity(new UrlEncodedFormEntity(params));

                        logger.info("Rolling back data on successful node " + successfulNodeId);

                        httpResponse = httpClient.execute(post);
                        int responseCode = httpResponse.getStatusLine().getStatusCode();
                        String response = httpResponse.getStatusLine().getReasonPhrase();

                        if(responseCode == 200) {
                            logger.info("Rollback succeeded for node " + successfulNodeId);
                        } else {
                            throw new VoldemortException(response);
                        }
                    } catch(Exception e) {
                        logger.error("Exception thrown during rollback ( after swap ) operation on node "
                                             + successfulNodeId + " : ",
                                     e);
                    } finally {
                        VoldemortIOUtils.closeQuietly(httpResponse);
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

    @Override
    public void invokeRollback(String storeName, final long pushVersion) {
        Exception exception = null;
        for(Node node: cluster.getNodes()) {
            HttpResponse httpResponse = null;
            try {
                logger.info("Attempting rollback for node " + node.getId() + " storeName = "
                            + storeName);
                String url = node.getHttpUrl() + "/" + readOnlyMgmtPath;
                HttpPost post = new HttpPost(url);

                List<NameValuePair> params = new ArrayList<NameValuePair>();
                params.add(new BasicNameValuePair("operation", "rollback"));
                params.add(new BasicNameValuePair("store", storeName));
                params.add(new BasicNameValuePair("pushVersion", Long.toString(pushVersion)));
                post.setEntity(new UrlEncodedFormEntity(params));

                httpResponse = httpClient.execute(post);
                int responseCode = httpResponse.getStatusLine().getStatusCode();
                String response = httpResponse.getStatusLine().getReasonPhrase();
                if(responseCode == 200) {
                    logger.info("Rollback succeeded for node " + node.getId());
                } else {
                    throw new VoldemortException(response);
                }
            } catch(Exception e) {
                exception = e;
                logger.error("Exception thrown during rollback operation on node " + node.getId()
                             + ": ", e);
            } finally {
                VoldemortIOUtils.closeQuietly(httpResponse, String.valueOf(node));
            }
        }

        if(exception != null)
            throw new VoldemortException(exception);
    }

}
