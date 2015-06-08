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

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

import org.apache.log4j.Logger;
import voldemort.client.ClientConfig;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.store.readonly.mr.utils.HadoopUtils;
import voldemort.store.readonly.swapper.AdminStoreSwapper;
import azkaban.jobExecutor.AbstractJob;
import voldemort.store.readonly.swapper.FailedFetchStrategy;
import voldemort.utils.Props;

/*
 * Call voldemort to swap the current store for the specified store
 */
public class VoldemortSwapJob extends AbstractJob {

    private final Props _props;
    private VoldemortSwapConf swapConf;
    private String hdfsFetcherProtocol;
    private String hdfsFetcherPort;

    public VoldemortSwapJob(String id, Props props) throws IOException {
        super(id, Logger.getLogger(VoldemortSwapJob.class.getName()));
        _props = props;

        this.hdfsFetcherProtocol = props.getString("voldemort.fetcher.protocol", "hftp");
        this.hdfsFetcherPort = props.getString("voldemort.fetcher.port", "50070");
        swapConf = new VoldemortSwapConf(_props);
    }

    public VoldemortSwapJob(String id, Props props, VoldemortSwapConf conf) throws IOException {
        super(id, Logger.getLogger(VoldemortSwapJob.class.getName()));
        _props = props;
        this.hdfsFetcherProtocol = props.getString("voldemort.fetcher.protocol", "hftp");
        this.hdfsFetcherPort = props.getString("voldemort.fetcher.port", "50070");
        swapConf = conf;
    }

    public static final class VoldemortSwapConf {

        private Cluster cluster;
        private String dataDir;
        private String storeName;
        private int httpTimeoutMs;
        private long pushVersion;
        private int maxBackoffDelayMs = 60 * 1000;
        private boolean rollbackFailedSwap = false;
        private List<FailedFetchStrategy> failedFetchStrategyList = Lists.newArrayList();

        public VoldemortSwapConf(Props props) throws IOException {
            this(HadoopUtils.readCluster(props.getString("cluster.xml"), new Configuration()),
                 props.getString("data.dir"),
                 props.get("store.name"),
                 1000 * props.getInt("http.timeout.seconds", 24 * 60 * 60), // for
                                                                            // backwards
                 // compatibility
                 // http timeout =
                 // admin client
                 // timeout
                 props.getLong("push.version", -1L));
        }

        public VoldemortSwapConf(Cluster cluster,
                                 String dataDir,
                                 String storeName,
                                 int httpTimeoutMs,
                                 long pushVersion,
                                 int maxBackoffDelayMs,
                                 boolean rollbackFailedSwap,
                                 List<FailedFetchStrategy> failedFetchStrategyList) {
            this(cluster, dataDir, storeName, httpTimeoutMs, pushVersion);
            this.maxBackoffDelayMs = maxBackoffDelayMs;
            this.rollbackFailedSwap = rollbackFailedSwap;
            this.failedFetchStrategyList = failedFetchStrategyList;
        }

        public VoldemortSwapConf(Cluster cluster,
                                 String dataDir,
                                 String storeName,
                                 int httpTimeoutMs,
                                 long pushVersion) {
            this.cluster = cluster;
            this.dataDir = dataDir;
            this.storeName = storeName;
            this.httpTimeoutMs = httpTimeoutMs;
            this.pushVersion = pushVersion;
        }

        public Cluster getCluster() {
            return cluster;
        }

        public String getDataDir() {
            return dataDir;
        }

        public String getStoreName() {
            return storeName;
        }

        public int getHttpTimeoutMs() {
            return httpTimeoutMs;
        }

        public long getPushVersion() {
            return pushVersion;
        }

        public int getMaxBackoffDelayMs() {
            return maxBackoffDelayMs;
        }

        public boolean getRollbackFailedSwap() {
            return rollbackFailedSwap;
        }

        public List<FailedFetchStrategy> getFailedFetchStrategyList() {
            return failedFetchStrategyList;
        }
    }

    public void run() throws Exception {
        String dataDir = swapConf.getDataDir();
        String storeName = swapConf.getStoreName();
        int httpTimeoutMs = swapConf.getHttpTimeoutMs();
        long pushVersion = swapConf.getPushVersion();
        Cluster cluster = swapConf.getCluster();
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
            dataDir = dataDir.replaceFirst(existingProtocol, this.hdfsFetcherProtocol);
            dataDir = dataDir.replaceFirst(existingPort, this.hdfsFetcherPort);
        }

        // Create admin client
        AdminClient client = new AdminClient(cluster,
                                             new AdminClientConfig().setMaxConnectionsPerNode(cluster.getNumberOfNodes())
                                                                    .setAdminConnectionTimeoutSec(httpTimeoutMs / 1000)
                                                                    .setMaxBackoffDelayMs(swapConf.getMaxBackoffDelayMs()),
                                             new ClientConfig());

        if(pushVersion == -1L) {
            // Need to retrieve max version
            ArrayList<String> stores = new ArrayList<String>();
            stores.add(storeName);
            Map<String, Long> pushVersions = client.readonlyOps.getROMaxVersion(stores);

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
                swapConf.getRollbackFailedSwap(),
                swapConf.getFailedFetchStrategyList());
        swapper.swapStoreData(storeName, dataDir, pushVersion);
        info("Swap complete.");
        executor.shutdownNow();
        executor.awaitTermination(10, TimeUnit.SECONDS);
    }

}
