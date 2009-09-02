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

package voldemort.cluster.nodeavailabilitydetector;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import voldemort.client.ClientConfig;
import voldemort.cluster.Node;
import voldemort.store.Store;
import voldemort.utils.ByteArray;

import com.google.common.collect.Maps;

public abstract class ClientNodeAvailabilityDetectorConfig implements
        NodeAvailabilityDetectorConfig {

    protected final ClientConfig clientConfig;

    protected final Map<Integer, Store<ByteArray, byte[]>> stores;

    protected ClientNodeAvailabilityDetectorConfig(ClientConfig clientConfig) {
        this.clientConfig = clientConfig;
        stores = Maps.newHashMap();
    }

    public String getImplementationClassName() {
        return clientConfig.getNodeAvailabilityDetector();
    }

    public long getNodeBannagePeriod() {
        return clientConfig.getNodeBannagePeriod(TimeUnit.MILLISECONDS);
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

    protected abstract Store<ByteArray, byte[]> getStoreInternal(Node node);

}
