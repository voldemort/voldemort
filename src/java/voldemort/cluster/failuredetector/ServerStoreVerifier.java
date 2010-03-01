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

import voldemort.VoldemortException;
import voldemort.cluster.Node;
import voldemort.server.StoreRepository;
import voldemort.store.Store;
import voldemort.store.UnreachableStoreException;
import voldemort.store.metadata.MetadataStore;
import voldemort.utils.ByteArray;

/**
 * ServerStoreVerifier is used to verify store connectivity for a server
 * environment. The node->store mapping is not known at the early point in the
 * client lifecycle that it can be provided, so it is performed on demand using
 * the {@link StoreRepository}.
 * 
 */

public class ServerStoreVerifier implements StoreVerifier {

    private final StoreRepository storeRepository;

    private final int nodeId;

    private final Map<Integer, Store<ByteArray, byte[]>> stores;

    private final ByteArray key = new ByteArray((byte) 1);

    public ServerStoreVerifier(StoreRepository storeRepository, int nodeId) {
        this.storeRepository = storeRepository;
        this.nodeId = nodeId;
        stores = new HashMap<Integer, Store<ByteArray, byte[]>>();
    }

    public void verifyStore(Node node) throws UnreachableStoreException, VoldemortException {
        synchronized(stores) {
            Store<ByteArray, byte[]> store = stores.get(node.getId());

            if(store == null) {
                if(node.getId() == nodeId)
                    store = storeRepository.getLocalStore(MetadataStore.METADATA_STORE_NAME);
                else
                    store = storeRepository.getNodeStore(MetadataStore.METADATA_STORE_NAME,
                                                         node.getId());

                stores.put(node.getId(), store);
            }

            store.get(key);
        }
    }

}
