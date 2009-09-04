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

import java.util.HashMap;
import java.util.Map;

import voldemort.cluster.Node;
import voldemort.server.StoreRepository;
import voldemort.server.VoldemortConfig;
import voldemort.store.Store;
import voldemort.store.metadata.MetadataStore;
import voldemort.utils.ByteArray;

/**
 * ServerFailureDetectorConfig is used to retrieve configuration data for a
 * server environment. The node->store mapping is not known at the early point
 * in the client lifecycle that it can be provided, so it is performed on
 * demand.
 * 
 * @author Kirk True
 */

public class ServerFailureDetectorConfig implements FailureDetectorConfig {

    private final VoldemortConfig voldemortConfig;

    private final StoreRepository storeRepository;

    private final Map<Integer, Store<ByteArray, byte[]>> stores;

    public ServerFailureDetectorConfig(VoldemortConfig voldemortConfig,
                                       StoreRepository storeRepository) {
        this.voldemortConfig = voldemortConfig;
        this.storeRepository = storeRepository;
        stores = new HashMap<Integer, Store<ByteArray, byte[]>>();
    }

    public String getImplementationClassName() {
        return voldemortConfig.getFailureDetector();
    }

    public long getNodeBannagePeriod() {
        return voldemortConfig.getClientNodeBannageMs();
    }

    public Store<ByteArray, byte[]> getStore(Node node) {
        synchronized(stores) {
            Store<ByteArray, byte[]> store = stores.get(node.getId());

            if(store == null) {
                if(node.getId() == voldemortConfig.getNodeId())
                    store = storeRepository.getLocalStore(MetadataStore.METADATA_STORE_NAME);
                else
                    store = storeRepository.getNodeStore(MetadataStore.METADATA_STORE_NAME,
                                                         node.getId());

                stores.put(node.getId(), store);
            }

            return store;
        }
    }

}
