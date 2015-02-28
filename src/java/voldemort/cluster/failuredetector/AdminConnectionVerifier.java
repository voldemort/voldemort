/*
 * Copyright 2014 LinkedIn, Inc.
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

package voldemort.cluster.failuredetector;

import voldemort.VoldemortException;
import voldemort.client.ClientConfig;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.store.UnreachableStoreException;
import voldemort.store.metadata.MetadataStore;
import voldemort.utils.Utils;

/**
 * AdminConnectionVerifier is used to verify admin port connectivity.
 */

public class AdminConnectionVerifier implements ConnectionVerifier {

    private Cluster cluster;
    private AdminClient adminClient;

    public AdminConnectionVerifier(Cluster cluster) {
        Utils.notNull(cluster);
        this.cluster = cluster;
        this.adminClient = null;
    }

    public synchronized AdminClient getAdminClient() {
        if (adminClient == null) {
            adminClient = new AdminClient(this.cluster,
                                          new AdminClientConfig(),
                                          new ClientConfig().setSelectors(1));
        }
        return adminClient;
    }

    @Override
    public void verifyConnection(Node node) throws UnreachableStoreException, VoldemortException {
        Integer returnNodeId = Integer.parseInt(getAdminClient().metadataMgmtOps.getRemoteMetadata(node.getId(),
                                                                                                   MetadataStore.NODE_ID_KEY)
                                                                                .getValue());
        if(returnNodeId != node.getId()) {
            throw new VoldemortException("Incorrect node id " + returnNodeId
                                         + " returned from node " + node.getId());
        }
    }

    @Override
    public void flushCachedStores() {}

    public synchronized void setCluster(Cluster cluster) {
        Utils.notNull(cluster);
        this.cluster = cluster;
        if(this.adminClient != null) {
            this.adminClient.close();
            this.adminClient = null;
        }
    }

}
