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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

import azkaban.jobExecutor.AbstractJob;
import voldemort.client.ClientConfig;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.store.readonly.swapper.AdminStoreSwapper;
import voldemort.store.readonly.swapper.FailedFetchStrategy;

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

    // The following internal state mutates during run()
    private String dataDir;
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
                            List<FailedFetchStrategy> failedFetchStrategyList) throws IOException {
        super(id, Logger.getLogger(VoldemortSwapJob.class.getName()));
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
    }

    public void run() throws Exception {
        ExecutorService executor = Executors.newCachedThreadPool();

        // Read the hadoop configuration settings
        JobConf conf = new JobConf();
        Path dataPath = new Path(dataDir);
        dataDir = dataPath.makeQualified(FileSystem.get(conf)).toString();

        /*
         * Replace the default protocol and port with the one derived as above
         */
        String existingProtocol = "";
        String existingPort = "";
        String[] pathComponents = dataDir.split(":");
        if(pathComponents.length >= 3) {
            existingProtocol = pathComponents[0];
            existingPort = pathComponents[2].split("/")[0];
        }
        info("Existing protocol = " + existingProtocol + " and port = " + existingPort);
        if(hdfsFetcherProtocol.length() > 0 && hdfsFetcherPort.length() > 0) {
            dataDir = dataDir.replaceFirst(existingProtocol, hdfsFetcherProtocol);
            dataDir = dataDir.replaceFirst(existingPort, hdfsFetcherPort);
        }

        // Create admin client
        AdminClient client = new AdminClient(cluster,
                                             new AdminClientConfig().setMaxConnectionsPerNode(cluster.getNumberOfNodes())
                                                                    .setAdminConnectionTimeoutSec(httpTimeoutMs / 1000)
                                                                    .setMaxBackoffDelayMs(maxBackoffDelayMs),
                                             new ClientConfig());

        if(pushVersion == -1L) {
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

        // do the swap
        info("Initiating swap of " + storeName + " with dataDir:" + dataDir);
        AdminStoreSwapper swapper = new AdminStoreSwapper(
                cluster,
                executor,
                client,
                httpTimeoutMs,
                rollbackFailedSwap,
                failedFetchStrategyList);
        swapper.swapStoreData(storeName, dataDir, pushVersion);
        info("Swap complete.");
        executor.shutdownNow();
        executor.awaitTermination(10, TimeUnit.SECONDS);
    }

}
