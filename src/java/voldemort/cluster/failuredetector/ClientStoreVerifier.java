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

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.HttpStoreClientFactory;
import voldemort.client.SocketStoreClientFactory;
import voldemort.cluster.Node;
import voldemort.store.Store;
import voldemort.store.UnreachableStoreException;
import voldemort.utils.ByteArray;

/**
 * ClientStoreVerifier is used to test stores in a client environment. The
 * node->store mapping is not known at the early point in the client lifecycle
 * that it can be provided, so it is performed on demand. This class is abstract
 * due to needing to be implemented differently by the known store client
 * implementations {@link SocketStoreClientFactory} and
 * {@link HttpStoreClientFactory}.
 */

public abstract class ClientStoreVerifier implements StoreVerifier {

    private final Map<Integer, Store<ByteArray, byte[], byte[]>> stores;
    protected final Logger logger = Logger.getLogger(getClass().getName());

    protected ClientStoreVerifier() {
        stores = new HashMap<Integer, Store<ByteArray, byte[], byte[]>>();
    }

    public void verifyStore(Node node) throws UnreachableStoreException, VoldemortException {
        Store<ByteArray, byte[], byte[]> store = null;

        synchronized(stores) {
            store = stores.get(node.getId());

            if(store == null) {
                if(logger.isDebugEnabled()) {
                    logger.debug("Getting metadata store for node " + node.getId());
                }
                store = getStoreInternal(node);
                if(logger.isDebugEnabled()) {
                    logger.debug("Successfully retrieved metadata store for node " + node.getId());
                }
                stores.put(node.getId(), store);
            }
        }

        if(logger.isDebugEnabled()) {
            logger.debug("Trying to get node id from metadata store for node " + node.getId());
        }
        store.get(KEY, null);
    }

    protected abstract Store<ByteArray, byte[], byte[]> getStoreInternal(Node node);

    public void flushCachedStores() {
        synchronized(stores) {
            this.stores.clear();
        }
    }

}
