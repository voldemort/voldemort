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

package voldemort.store.rebalancing;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import voldemort.VoldemortException;
import voldemort.client.DefaultStoreClient;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.server.RequestRoutingType;
import voldemort.server.StoreRepository;
import voldemort.server.VoldemortConfig;
import voldemort.store.DelegatingStore;
import voldemort.store.InvalidMetadataException;
import voldemort.store.Store;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.routed.RoutedStore;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.utils.ByteArray;
import voldemort.utils.RebalanceUtils;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * The RebootstrappingStore catch all InvalidMetadataException and updates the
 * routed store with latest cluster metadata, client rebootstrapping behavior
 * same in {@link DefaultStoreClient} for server side routing<br>
 * 
 */
public class RebootstrappingStore extends DelegatingStore<ByteArray, byte[], byte[]> {

    private final int maxMetadataRefreshAttempts = 3;

    private final MetadataStore metadata;
    private final StoreRepository storeRepository;
    private final VoldemortConfig voldemortConfig;
    private final RoutedStore routedStore;
    private final SocketStoreFactory storeFactory;

    public RebootstrappingStore(MetadataStore metadata,
                                StoreRepository storeRepository,
                                VoldemortConfig voldemortConfig,
                                RoutedStore routedStore,
                                SocketStoreFactory storeFactory) {
        super(routedStore);
        this.metadata = metadata;
        this.storeRepository = storeRepository;
        this.voldemortConfig = voldemortConfig;
        this.routedStore = routedStore;
        this.storeFactory = storeFactory;
    }

    private void reinit() {
        AdminClient adminClient = RebalanceUtils.createTempAdminClient(voldemortConfig,
                                                                       metadata.getCluster(),
                                                                       voldemortConfig.getClientMaxConnectionsPerNode());
        try {
            Versioned<Cluster> latestCluster = RebalanceUtils.getLatestCluster(new ArrayList<Integer>(),
                                                                               adminClient);
            metadata.put(MetadataStore.CLUSTER_KEY, latestCluster.getValue());

            checkAndAddNodeStore();

            routedStore.updateRoutingStrategy(metadata.getRoutingStrategy(getName()));
        } finally {
            adminClient.close();
        }
    }

    /**
     * Check that all nodes in the new cluster have a corrosponding entry in
     * storeRepositiry and innerStores. add a NodeStore if not present, is
     * needed as with rebalancing we can add new nodes on the fly.
     * 
     */
    private void checkAndAddNodeStore() {
        for(Node node: metadata.getCluster().getNodes()) {
            if(!routedStore.getInnerStores().containsKey(node.getId())) {
                if(!storeRepository.hasNodeStore(getName(), node.getId())) {
                    storeRepository.addNodeStore(node.getId(), createNodeStore(node));
                }
                routedStore.getInnerStores().put(node.getId(),
                                                 storeRepository.getNodeStore(getName(),
                                                                              node.getId()));
            }
        }
    }

    private Store<ByteArray, byte[], byte[]> createNodeStore(Node node) {
        return storeFactory.create(getName(),
                                   node.getHost(),
                                   node.getSocketPort(),
                                   voldemortConfig.getRequestFormatType(),
                                   RequestRoutingType.NORMAL);
    }

    @Override
    public boolean delete(final ByteArray key, final Version version) {
        for(int attempts = 0; attempts < this.maxMetadataRefreshAttempts; attempts++) {
            try {
                return super.delete(key, version);
            } catch(InvalidMetadataException e) {
                reinit();
            }
        }
        throw new VoldemortException(this.maxMetadataRefreshAttempts
                                     + " metadata refresh attempts failed for server side routing.");
    }

    @Override
    public List<Version> getVersions(ByteArray key) {
        for(int attempts = 0; attempts < this.maxMetadataRefreshAttempts; attempts++) {
            try {
                return super.getVersions(key);
            } catch(InvalidMetadataException e) {
                reinit();
            }
        }
        throw new VoldemortException(this.maxMetadataRefreshAttempts
                                     + " metadata refresh attempts failed for server side routing.");
    }

    @Override
    public List<Versioned<byte[]>> get(ByteArray key, byte[] transforms) {
        for(int attempts = 0; attempts < this.maxMetadataRefreshAttempts; attempts++) {
            try {
                return super.get(key, transforms);
            } catch(InvalidMetadataException e) {
                reinit();
            }
        }
        throw new VoldemortException(this.maxMetadataRefreshAttempts
                                     + " metadata refresh attempts failed for server side routing.");
    }

    @Override
    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys,
                                                          Map<ByteArray, byte[]> transforms) {
        for(int attempts = 0; attempts < this.maxMetadataRefreshAttempts; attempts++) {
            try {
                return super.getAll(keys, transforms);
            } catch(InvalidMetadataException e) {
                reinit();
            }
        }
        throw new VoldemortException(this.maxMetadataRefreshAttempts
                                     + " metadata refresh attempts failed for server side routing.");
    }

    @Override
    public void put(final ByteArray key, final Versioned<byte[]> versioned, final byte[] transforms)
            throws ObsoleteVersionException {
        for(int attempts = 0; attempts < this.maxMetadataRefreshAttempts; attempts++) {
            try {
                super.put(key, versioned, transforms);
                return;
            } catch(InvalidMetadataException e) {
                reinit();
            }
        }
        throw new VoldemortException(this.maxMetadataRefreshAttempts
                                     + " metadata refresh attempts failed for server side routing.");
    }
}
