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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.ClientConfig;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.store.readonly.mr.utils.VoldemortUtils;
import voldemort.store.readonly.swapper.AdminStoreSwapper;
import azkaban.jobExecutor.AbstractJob;
import azkaban.utils.Props;

import com.google.common.collect.Maps;

/**
 * This job rolls back to [ current version - 1 ]. This may not always work
 * since some folks may start using the version feature to put their own version
 * number. But till that doesn't happen, this will back.
 * 
 */
public class VoldemortRollbackJob extends AbstractJob {

    private final Logger log;

    private final Props props;

    private List<String> storeNames;

    private List<String> clusterUrls;

    public VoldemortRollbackJob(String name, Props props) throws IOException {
        super(name, Logger.getLogger(VoldemortRollbackJob.class.getName()));
        this.props = props;
        this.log = Logger.getLogger(name);
        this.storeNames = VoldemortUtils.getCommaSeparatedStringValues(props.getString("store.name"),
                                                                       "store names");
        this.clusterUrls = VoldemortUtils.getCommaSeparatedStringValues(props.getString("push.cluster"),
                                                                        "cluster urls");

    }

    @Override
    public void run() throws Exception {

        // Go over every cluster and rollback one store at a time
        for(String clusterUrl: clusterUrls) {

            AdminClient adminClient = null;
            ExecutorService service = null;
            try {
                service = Executors.newCachedThreadPool();
                adminClient = new AdminClient(clusterUrl,
                                              new AdminClientConfig(),
                                              new ClientConfig());
                Cluster cluster = adminClient.getAdminClientCluster();
                AdminStoreSwapper swapper = new AdminStoreSwapper(
                        cluster,
                        service,
                        adminClient,
                        1000 * props.getInt("timeout.seconds", 24 * 60 * 60),
                        false,
                        true,
                        true);

                // Get the current version for all stores on all nodes
                Map<Integer, Map<String, Long>> previousVersions = Maps.newHashMap();
                for(Node node: cluster.getNodes()) {
                    Map<String, Long> currentVersion = adminClient.readonlyOps.getROCurrentVersion(node.getId(),
                                                                                                   storeNames);

                    log.info("Retrieving current version information on node " + node.getId());
                    Map<String, Long> previousVersion = Maps.newHashMap();
                    for(Entry<String, Long> entry: currentVersion.entrySet()) {
                        previousVersion.put(entry.getKey(), entry.getValue() - 1);
                        if(entry.getValue() == 0) {
                            throw new VoldemortException("Store '" + entry.getKey() + "' on node "
                                                         + node.getId()
                                                         + " does not have version to rollback to");
                        }
                    }
                    previousVersions.put(node.getId(), previousVersion);
                }

                // Swap one store at a time
                for(String storeName: storeNames) {
                    for(Node node: cluster.getNodes()) {
                        log.info("Rolling back data on node " + node.getId() + " and for store "
                                 + storeName + " to version "
                                 + previousVersions.get(node.getId()).get(storeName));
                        swapper.invokeRollback(storeName,
                                               previousVersions.get(node.getId()).get(storeName));
                        log.info("Successfully rolled back data on node " + node.getId()
                                 + " and for store " + storeName);
                    }
                }
            } finally {
                if(service != null) {
                    service.shutdownNow();
                    service.awaitTermination(10, TimeUnit.SECONDS);
                    service = null;
                }
                if(adminClient != null) {
                    adminClient.close();
                    adminClient = null;
                }
            }
        }
    }
}
