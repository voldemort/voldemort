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

import voldemort.cluster.Node;
import voldemort.store.Store;
import voldemort.utils.ByteArray;

/**
 * BasicStoreResolver is used to resolve Node->Store mappings when the mappings
 * are already present at the time of FailureDetector implementation creation.
 * This is usually (always) in the case of tests rather than running "live."
 * 
 * @author Kirk True
 */

public class BasicStoreResolver implements StoreResolver {

    private final Map<Integer, Store<ByteArray, byte[]>> stores;

    public BasicStoreResolver(Map<Integer, Store<ByteArray, byte[]>> stores) {
        this.stores = stores;
    }

    public Store<ByteArray, byte[]> getStore(Node node) {
        return stores.get(node.getId());
    }

}
