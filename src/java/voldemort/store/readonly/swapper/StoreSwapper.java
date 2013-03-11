/*
 * Copyright 2013 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
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

import org.apache.commons.io.FileUtils;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.apache.log4j.Logger;

import voldemort.client.ClientConfig;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.utils.CmdUtils;
import voldemort.utils.Time;
import voldemort.utils.VoldemortIOUtils;
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

        DefaultHttpClient httpClient = null;
        if(useAdminServices) {
            adminClient = new AdminClient(cluster, new AdminClientConfig(), new ClientConfig());
            swapper = new AdminStoreSwapper(cluster, executor, adminClient, timeoutMs);
        } else {
            int numConnections = cluster.getNumberOfNodes() + 3;
            ThreadSafeClientConnManager connectionManager = new ThreadSafeClientConnManager();
            httpClient = new DefaultHttpClient(connectionManager);

            HttpParams clientParams = httpClient.getParams();

            connectionManager.setMaxTotal(numConnections);
            connectionManager.setDefaultMaxPerRoute(numConnections);
            HttpConnectionParams.setSoTimeout(clientParams, timeoutMs);

            swapper = new HttpStoreSwapper(cluster, executor, httpClient, mgmtPath);
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
                adminClient.close();
            executor.shutdownNow();
            executor.awaitTermination(1, TimeUnit.SECONDS);
            VoldemortIOUtils.closeQuietly(httpClient);
        }
        System.exit(0);
    }
}
