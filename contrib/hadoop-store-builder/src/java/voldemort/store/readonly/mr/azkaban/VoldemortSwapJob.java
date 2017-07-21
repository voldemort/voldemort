/*
 * Copyright 2008-2013 LinkedIn, Inc
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

package voldemort.store.readonly.mr.azkaban;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.Path;

import voldemort.VoldemortException;
import voldemort.client.ClientConfig;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.store.readonly.mr.utils.VoldemortUtils;
import voldemort.store.readonly.swapper.AdminStoreSwapper;
import voldemort.store.readonly.swapper.FailedFetchStrategy;
import voldemort.utils.logging.PrefixedLogger;

import azkaban.jobExecutor.AbstractJob;

/*
 * Call voldemort to swap the current store for the specified store
 */
public class VoldemortSwapJob extends AbstractJob {

    // Immutable internal state
    private final Cluster cluster;
    private final String storeName;
    private final int httpTimeoutMs;
    private final int maxBackoffDelayMs;
    private final boolean rollbackFailedSwap;
    private final String hdfsFetcherProtocol;
    private final String hdfsFetcherPort;
    private final int maxNodeFailures;
    private final List<FailedFetchStrategy> failedFetchStrategyList;
    private final String dataDir;
    private final String clusterName;
    private final boolean buildPrimaryReplicasOnly;

    // The following internal state mutates during run()
    private String modifiedDataDir;
    private long pushVersion;

    public VoldemortSwapJob(String id,
                            Cluster cluster,
                            String dataDir,
                            String storeName,
                            int httpTimeoutMs,
                            long pushVersion,
                            int maxBackoffDelayMs,
                            boolean rollbackFailedSwap,
                            String hdfsFetcherProtocol,
                            String hdfsFetcherPort,
                            int maxNodeFailures,
                            List<FailedFetchStrategy> failedFetchStrategyList,
                            String clusterName,
                            String modifiedDataDir,
                            boolean buildPrimaryReplicasOnly) throws IOException {
        super(id, PrefixedLogger.getLogger(AdminStoreSwapper.class.getName(), clusterName));
        this.cluster = cluster;
        this.dataDir = dataDir;
        this.storeName = storeName;
        this.httpTimeoutMs = httpTimeoutMs;
        this.pushVersion = pushVersion;
        this.maxBackoffDelayMs = maxBackoffDelayMs;
        this.rollbackFailedSwap = rollbackFailedSwap;
        this.hdfsFetcherProtocol = hdfsFetcherProtocol;
        this.hdfsFetcherPort = hdfsFetcherPort;
        this.maxNodeFailures = maxNodeFailures;
        this.failedFetchStrategyList = failedFetchStrategyList;
        this.clusterName = clusterName;
        this.modifiedDataDir  = modifiedDataDir;
        this.buildPrimaryReplicasOnly = buildPrimaryReplicasOnly;
    }

    public void run() throws Exception {
        ExecutorService executor = Executors.newCachedThreadPool();

        /*
         * Replace the default protocol and port with the one derived as above
         */
        try {
            modifiedDataDir =
                VoldemortUtils.modifyURL(modifiedDataDir, hdfsFetcherProtocol, Integer.valueOf(hdfsFetcherPort), false);
        } catch (NumberFormatException nfe) {
            info("The dataDir will not be modified, since hdfsFetcherPort is not a valid port number");
        } catch (IllegalArgumentException e) {
            info("The dataDir will not be modified, since it does not contain the expected "
                + "structure of protocol:hostname:port/some_path");
        }

        try {
            new Path(modifiedDataDir);
        } catch (IllegalArgumentException e) {
            throw new VoldemortException("Could not create a valid data path out of the supplied dataDir: " +
                    dataDir, e);
        }

        // It should not be necessary to set the max conn / node so high, but it should not be a big deal either. New
        // connections will be created as needed, not upfront, so there should be no extra cost associated with the
        // higher setting. There shouldn't be many parallel requests happening in this use case, but we're going to
        // leave it as is for now, just to minimize the potential for unforeseen regressions.
        AdminClientConfig adminConfig = new AdminClientConfig().setMaxConnectionsPerNode(cluster.getNumberOfNodes())
                                                               .setMaxBackoffDelayMs(maxBackoffDelayMs)
        // While processing an admin request, HDFSFailedLock could take long time because of multiple HDFS operations,
        // especially when the name node is in a different data center. So extend timeout to 5 minutes.
                                                               .setAdminSocketTimeoutSec(60 * 5);

        ClientConfig clientConfig = new ClientConfig().setBootstrapUrls(cluster.getBootStrapUrls())
                                                      .setConnectionTimeout(httpTimeoutMs,
                                                                            TimeUnit.MILLISECONDS);
        // Create admin client
        AdminClient client = new AdminClient(adminConfig, clientConfig);

        if (pushVersion == -1L) {
            // Need to retrieve max version
            ArrayList<String> stores = new ArrayList<String>();
            stores.add(storeName);
            Map<String, Long> pushVersions = client.readonlyOps.getROMaxVersion(stores, maxNodeFailures);

            if(pushVersions == null || !pushVersions.containsKey(storeName)) {
                throw new RuntimeException("Push version could not be determined for store "
                                           + storeName);
            }
            pushVersion = pushVersions.get(storeName);
            pushVersion++;
        }

        // do the fetch, and if it succeeds, the swap
        info("Initiating fetch of " + storeName + " with dataDir: " + dataDir);
        AdminStoreSwapper swapper = new AdminStoreSwapper(
                executor,
                client,
                httpTimeoutMs,
                rollbackFailedSwap,
                failedFetchStrategyList,
                clusterName,
                buildPrimaryReplicasOnly);
        swapper.fetchAndSwapStoreData(storeName, modifiedDataDir, pushVersion);
        info("Swap complete.");
        executor.shutdownNow();
        executor.awaitTermination(10, TimeUnit.SECONDS);
    }

}
