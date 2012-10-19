/*
 * Copyright 2009-2010 Mustard Grain, Inc., LinkedIn, Inc.
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
import voldemort.server.RequestRoutingType;
import voldemort.server.StoreRepository;
import voldemort.server.VoldemortConfig;
import voldemort.store.Store;
import voldemort.store.UnreachableStoreException;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.utils.ByteArray;
import voldemort.utils.Utils;

/**
 * ServerStoreVerifier is used to verify store connectivity for a server
 * environment. The node->store mapping is not known at the early point in the
 * client lifecycle that it can be provided, so it is performed on demand using
 * the {@link StoreRepository}.
 */

public class ServerStoreVerifier implements StoreVerifier {

    private final SocketStoreFactory storeFactory;

    private final MetadataStore metadataStore;

    private final VoldemortConfig voldemortConfig;

    private final Map<Integer, Store<ByteArray, byte[], byte[]>> stores;

    public ServerStoreVerifier(SocketStoreFactory storeFactory,
                               MetadataStore metadataStore,
                               VoldemortConfig voldemortConfig) {
        this.storeFactory = Utils.notNull(storeFactory);
        this.metadataStore = Utils.notNull(metadataStore);
        this.voldemortConfig = Utils.notNull(voldemortConfig);
        stores = new HashMap<Integer, Store<ByteArray, byte[], byte[]>>();
    }

    public void verifyStore(Node node) throws UnreachableStoreException, VoldemortException {
        Store<ByteArray, byte[], byte[]> store = null;

        if(node.getId() == voldemortConfig.getNodeId()) {
            store = metadataStore;
        } else {
            synchronized(stores) {
                store = stores.get(node.getId());

                if(store == null) {
                    store = storeFactory.create(MetadataStore.METADATA_STORE_NAME,
                                                node.getHost(),
                                                node.getSocketPort(),
                                                voldemortConfig.getRequestFormatType(),
                                                RequestRoutingType.NORMAL);
                    stores.put(node.getId(), store);
                }
            }
        }

        store.get(KEY, null);
    }

    public void flushCachedStores() {
        this.stores.clear();
    }

}
