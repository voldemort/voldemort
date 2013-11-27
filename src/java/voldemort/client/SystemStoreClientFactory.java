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

package voldemort.client;

import java.util.concurrent.TimeUnit;

import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.store.Store;

/**
 * Helper Factory to create System Stores. These are used to interact with the
 * metadata stores managed by the cluster.
 * 
 * It acts as a centralized way of specifying the config for the System Store.
 * It also limits the #selectors to 1 for conserving network resources.
 * 
 * @param <K> Key serializer for the system store
 * @param <V> Value serializer for the system store
 */
public class SystemStoreClientFactory<K, V> {

    private final SocketStoreClientFactory socketStoreFactory;

    public SystemStoreClientFactory(ClientConfig clientConfig) {
        ClientConfig systemStoreConfig = new ClientConfig();

        systemStoreConfig.setSelectors(1)
                         .setBootstrapUrls(clientConfig.getBootstrapUrls())
                         .setMaxConnectionsPerNode(clientConfig.getSysMaxConnectionsPerNode())
                         .setConnectionTimeout(clientConfig.getSysConnectionTimeout(),
                                               TimeUnit.MILLISECONDS)
                         .setSocketTimeout(clientConfig.getSysSocketTimeout(),
                                           TimeUnit.MILLISECONDS)
                         .setRoutingTimeout(clientConfig.getSysRoutingTimeout(),
                                            TimeUnit.MILLISECONDS)
                         .setEnableJmx(clientConfig.getSysEnableJmx())
                         .setClientZoneId(clientConfig.getClientZoneId());
        this.socketStoreFactory = new SocketStoreClientFactory(systemStoreConfig);
    }

    public SystemStoreClient<K, V> createSystemStore(String storeName,
                                                     String clusterXml,
                                                     FailureDetector fd) {
        Store<K, V, Object> sysStore = this.socketStoreFactory.getSystemStore(storeName,
                                                                              clusterXml,
                                                                              fd);
        return new SystemStoreClient<K, V>(storeName, sysStore);
    }

    public SystemStoreClient<K, V> createSystemStore(String storeName) {
        Store<K, V, Object> sysStore = this.socketStoreFactory.getSystemStore(storeName, null, null);
        return new SystemStoreClient<K, V>(storeName, sysStore);
    }

    public void close() {
        if(socketStoreFactory != null) {
            socketStoreFactory.close();
        }
    }

}
