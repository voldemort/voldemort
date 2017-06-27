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

import java.net.URI;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapred.JobConf;

import voldemort.VoldemortException;
import voldemort.client.ClientConfig;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.store.readonly.mr.utils.VoldemortUtils;
import voldemort.store.readonly.swapper.AdminStoreSwapper;
import voldemort.store.readonly.swapper.FailedFetchStrategy;
import voldemort.utils.logging.PrefixedLogger;
import voldemort.utils.Props;

import gobblin.runtime.api.JobExecutionResult;
import gobblin.runtime.embedded.EmbeddedGobblin;
import gobblin.runtime.embedded.EmbeddedGobblinDistcp;

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
    private final Props props;

    // The following internal state mutates during run()
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
                            boolean buildPrimaryReplicasOnly,
                            Props props) throws IOException {
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
        this.buildPrimaryReplicasOnly = buildPrimaryReplicasOnly;
        this.props = props;
    }

    private void runDistcp(Path from, Path to) throws Exception {
        info("     source: " + from);
        info("destination: " + to);
        EmbeddedGobblin embeddedGobblin = new EmbeddedGobblinDistcp(from, to).mrMode();

        // Used for global throttling"
        embeddedGobblin.distributeJar("lib/*");

        for (Map.Entry<String, String> entry : this.props.entrySet()) {
            if (entry.getKey() instanceof String && ((String) entry.getKey()).startsWith("distcpConf.")) {
                String key = ((String) entry.getKey()).substring("distcpConf.".length());
                embeddedGobblin.setConfiguration(key, entry.getValue().toString());
            }
        }
        JobExecutionResult result = embeddedGobblin.run();

        if (!result.isSuccessful()) {
            throw new RuntimeException("Distcp job failed.", result.getErrorCause());
        }
    }

    private FileSystem getRootFS(String target) throws Exception {
        URI rootURI = new URI(target.replaceAll("(?<=:[0-9]{1,5})/.*", ""));
        FileSystem rootFS = FileSystem.get(rootURI, new JobConf());
        return rootFS;
    }

    private void deleteDir(FileSystem fs, String target) throws Exception {
        Path path = new Path(target.replaceAll(".*://.*?(?=/)", ""));

        if (fs.exists(path)) {
            fs.delete(path, true);
            if (fs.exists(path)) {
                warn("Could not delete temp directory " + path + " in dropbox!");
            } else {
                info("Deleted " + path);
            }
        }
    }

    private void deleteDirOnExit(FileSystem fs, String target) throws Exception {
        Path path = new Path(target.replaceAll(".*://.*?(?=/)", ""));

        fs.deleteOnExit(path);  // Delete the directory even if an exception occurs
        info(path + " is scheduled to be deleted on exit.");
    }

    private void addPermissionsToParents(FileSystem fs, String target) throws Exception {
        String pathFromRoot = target.replaceAll(".*://.*?/", "");
        String parent = "";

        for (String seg: pathFromRoot.split("/")) {
            parent = parent + "/" + seg;
            Path parentPath = new Path(parent);
            FsPermission perm = fs.getFileStatus(parentPath).getPermission();
            if (!perm.getOtherAction().implies(FsAction.READ_EXECUTE)) {
                fs.setPermission(parentPath, new FsPermission(perm.getUserAction(), perm.getGroupAction(), perm.getOtherAction().or(FsAction.READ_EXECUTE)));
                info("for path " + parent + ", permissions changed:");
                info("from: " + perm.toString());
                info("  to: " + fs.getFileStatus(parentPath).getPermission().toString());
            }
        }
    }

    private String pickDropbox() {
        List<String> pushClusters = props.getList(VoldemortBuildAndPushJob.PUSH_CLUSTER);

        if (!props.containsKey(VoldemortBuildAndPushJob.PUSH_DROPBOX_CLUSTER)) {
            return "";
        }
        List<String> dropboxClusters = props.getList(VoldemortBuildAndPushJob.PUSH_DROPBOX_CLUSTER);

        if (pushClusters.size() != dropboxClusters.size()) {
            warn("Cluster sizes are different!");
            warn("Will bypass dropbox!");
            return "";
        }

        int index = pushClusters.indexOf(clusterName);
        return index == -1 ? "" : dropboxClusters.get(index);
    }

    public void run() throws Exception {
        ExecutorService executor = Executors.newCachedThreadPool();

        // Read the hadoop configuration settings
        JobConf conf = new JobConf();
        Path dataPath = new Path(dataDir);
        String modifiedDataDir = dataPath.makeQualified(FileSystem.get(conf)).toString();
        String dropboxURL = pickDropbox();

        if (!dropboxURL.isEmpty()) {
            info("Using dropbox: " + dropboxURL);
            info("####################################");
            info("              Distcp");
            info("####################################");
            Path from = new Path(modifiedDataDir);
            String username = props.getString("env.USER");  // TODO: Is it always existing?
            modifiedDataDir = modifiedDataDir.replaceAll(".*://.*?(?=/)", dropboxURL + "/tmp/VoldemortBnP/" + username);
            Path to = new Path(modifiedDataDir);

            FileSystem rootFS = getRootFS(modifiedDataDir);
            deleteDir(rootFS, modifiedDataDir);
            runDistcp(from, to);
            addPermissionsToParents(rootFS, modifiedDataDir);
            deleteDirOnExit(rootFS, modifiedDataDir);
            info("####################################");
            info("          End of Distcp");
            info("####################################");
        }

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
