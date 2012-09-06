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

import java.util.Map;

import voldemort.VoldemortException;
import voldemort.cluster.Node;
import voldemort.store.Store;
import voldemort.store.UnreachableStoreException;

/**
 * BasicStoreVerifier is used to test Node->Store mappings when the mappings are
 * already present at the time of FailureDetector implementation creation. This
 * is usually (always) in the case of tests rather than running "live."
 * 
 */

public class BasicStoreVerifier<K, V, T> implements StoreVerifier {

    protected final Map<Integer, Store<K, V, T>> stores;

    private final K key;

    public BasicStoreVerifier(Map<Integer, Store<K, V, T>> stores, K key) {
        this.stores = stores;
        this.key = key;
    }

    public void verifyStore(Node node) throws UnreachableStoreException, VoldemortException {
        Store<K, V, T> store = stores.get(node.getId());

        if(store == null)
            throw new VoldemortException("Node " + node.getId()
                                         + " store is null; cannot determine node availability");

        // This is our test.
        store.get(key, null);
    }

    public void flushCachedStores() {
        this.stores.clear();
    }
}
