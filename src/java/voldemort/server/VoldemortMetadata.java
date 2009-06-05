/*
 * Copyright 2008-2009 LinkedIn, Inc
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

package voldemort.server;

import java.io.File;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.store.StoreDefinition;
import voldemort.store.metadata.MetadataStore;

/**
 * One place to hold it all :) keeps current metadata about voldemort cluster
 * 
 * @author bbansal
 * 
 */
public class VoldemortMetadata implements Serializable {

    public static enum ServerState {
        NORMAL_STATE,
        REBALANCING_STEALER_STATE,
        REBALANCING_DONOR_STATE
    }

    private static final long serialVersionUID = 1;

    private final Node node;
    private final Map<String, RoutingStrategy> routingStrategyByStoreName;

    private Cluster currentCluster;
    private Cluster rollbackCluster;
    private Map<String, StoreDefinition> storeDefMap;
    private VoldemortMetadata.ServerState serverState;
    private List<Integer> currentPartitionStealList;
    private Node currentDonorNode = null;

    public VoldemortMetadata(Cluster cluster, List<StoreDefinition> storeDefs, int nodeId) {
        this.rollbackCluster = this.currentCluster = cluster;
        this.storeDefMap = new HashMap<String, StoreDefinition>();
        for(StoreDefinition storeDef: storeDefs) {
            this.storeDefMap.put(storeDef.getName(), storeDef);
        }
        this.node = cluster.getNodeById(nodeId);
        this.serverState = VoldemortMetadata.ServerState.NORMAL_STATE;
        this.routingStrategyByStoreName = new HashMap<String, RoutingStrategy>();
        reinitRoutingStrategies();
    }

    public VoldemortMetadata(String metadataDir, int nodeId) {
        MetadataStore metadataStore = MetadataStore.readFromDirectory(new File(metadataDir));
        this.currentCluster = metadataStore.getCluster();
        this.rollbackCluster = metadataStore.getRollBackCluster();
        this.storeDefMap = new HashMap<String, StoreDefinition>();
        for(StoreDefinition storeDef: metadataStore.getStores())
            this.storeDefMap.put(storeDef.getName(), storeDef);
        this.serverState = VoldemortMetadata.ServerState.NORMAL_STATE;
        this.node = currentCluster.getNodeById(nodeId);
        this.routingStrategyByStoreName = new HashMap<String, RoutingStrategy>();
        reinitRoutingStrategies();
    }

    public void reinitRoutingStrategies() {
        RoutingStrategyFactory factory = new RoutingStrategyFactory(getCurrentCluster());
        for(StoreDefinition storeDef: this.storeDefMap.values())
            routingStrategyByStoreName.put(storeDef.getName(), factory.getRoutingStrategy(storeDef));
    }

    public Node getIdentityNode() {
        return node;
    }

    public void setCurrentCluster(Cluster cluster) {
        this.currentCluster = cluster;
    }

    public Cluster getCurrentCluster() {
        return currentCluster;
    }

    public Cluster getRollbackCluster() {
        return rollbackCluster;
    }

    public void setRollbackCluster(Cluster rollbackCluster) {
        this.rollbackCluster = rollbackCluster;
    }

    public void setStoreDefMap(Map<String, StoreDefinition> storeDefMap) {
        this.storeDefMap = storeDefMap;
    }

    public void setStoreDefMap(List<StoreDefinition> storeDefList) {
        this.storeDefMap.clear();
        for(StoreDefinition storeDef: storeDefList) {
            this.storeDefMap.put(storeDef.getName(), storeDef);
        }
    }

    public StoreDefinition getStoreDef(String storeName) {
        return storeDefMap.get(storeName);
    }

    public void setStoreDef(StoreDefinition storeDef) {
        storeDefMap.put(storeDef.getName(), storeDef);
    }

    public Map<String, StoreDefinition> getStoreDefs() {
        return storeDefMap;
    }

    public void setStoreDefs(List<StoreDefinition> storeDefList) {
        for(StoreDefinition storeDef: storeDefList) {
            setStoreDef(storeDef);
        }
    }

    public VoldemortMetadata.ServerState getServerState() {
        return serverState;
    }

    public void setServerState(VoldemortMetadata.ServerState serverState) {
        this.serverState = serverState;
    }

    public Node getDonorNode() {
        return currentDonorNode;
    }

    public void setDonorNode(Node donorNode) {
        this.currentDonorNode = donorNode;
    }

    public List<Integer> getCurrentPartitionStealList() {
        return currentPartitionStealList;
    }

    public void setCurrentPartitionStealList(List<Integer> currentPartitionStealList) {
        this.currentPartitionStealList = currentPartitionStealList;
    }

    public RoutingStrategy getRoutingStrategy(String storeName) {
        return routingStrategyByStoreName.get(storeName);
    }

    public void setRoutingStrategy(String storeName, RoutingStrategy routingStrategy) {
        routingStrategyByStoreName.put(storeName, routingStrategy);
    }
}
