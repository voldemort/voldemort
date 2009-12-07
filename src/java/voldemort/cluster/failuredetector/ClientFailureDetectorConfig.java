/*
 * Copyright 2009 Mustard Grain, Inc.
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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import voldemort.client.ClientConfig;
import voldemort.client.HttpStoreClientFactory;
import voldemort.client.SocketStoreClientFactory;
import voldemort.cluster.Node;
import voldemort.store.Store;
import voldemort.utils.ByteArray;
import voldemort.utils.SystemTime;
import voldemort.utils.Time;

/**
 * ClientFailureDetectorConfig is used to retrieve configuration data for a
 * client environment. The node->store mapping is not known at the early point
 * in the client lifecycle that it can be provided, so it is performed on
 * demand. This class is abstract due to needing to be implemented differently
 * by the known store client implementations {@link SocketStoreClientFactory}
 * and {@link HttpStoreClientFactory}.
 * 
 * @author Kirk True
 */

public abstract class ClientFailureDetectorConfig implements FailureDetectorConfig {

    protected final ClientConfig clientConfig;

    protected final Collection<Node> nodes;

    protected final Map<Integer, Store<ByteArray, byte[]>> stores;

    protected ClientFailureDetectorConfig(ClientConfig clientConfig, Collection<Node> nodes) {
        this.clientConfig = clientConfig;
        this.nodes = nodes;
        stores = new HashMap<Integer, Store<ByteArray, byte[]>>();
    }

    public String getImplementationClassName() {
        return clientConfig.getFailureDetector();
    }

    public long getNodeBannagePeriod() {
        return clientConfig.getNodeBannagePeriod(TimeUnit.MILLISECONDS);
    }

    public Collection<Node> getNodes() {
        return nodes;
    }

    public Store<ByteArray, byte[]> getStore(Node node) {
        synchronized(stores) {
            Store<ByteArray, byte[]> store = stores.get(node.getId());

            if(store == null) {
                store = getStoreInternal(node);
                stores.put(node.getId(), store);
            }

            return store;
        }
    }

    public Time getTime() {
        return SystemTime.INSTANCE;
    }

    protected abstract Store<ByteArray, byte[]> getStoreInternal(Node node);

}
