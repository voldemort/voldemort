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

package voldemort.store;

import java.util.List;

import voldemort.VoldemortException;
import voldemort.server.VoldemortMetadata;
import voldemort.utils.ByteArray;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * An InvalidMetadataCheckingStore store is a store wrapper that delegates to an
 * inner store, and throws {@link InvalidMetadataException} if a client requests
 * a partition which is not or should not be available at this node.
 * 
 * @author bbansal
 * 
 */
public class InvalidMetadataCheckingStore extends DelegatingStore<ByteArray, byte[]> {

    private final int nodeId;
    private final VoldemortMetadata metadata;

    /**
     * Create a store which delegates its operations to its inner store and
     * throws {@link InvalidMetadataException} if the partition for key
     * requested should not lie with this node.
     * 
     * @param node The id of the destination node
     * @param innerStore The store which we delegate write operations to
     * @param routingStrategy the routing stratgey for this cluster
     *        configuration.
     */
    public InvalidMetadataCheckingStore(int node,
                                        Store<ByteArray, byte[]> innerStore,
                                        VoldemortMetadata metadata) {
        super(innerStore);
        this.metadata = metadata;
        this.nodeId = node;
    }

    @Override
    public boolean delete(ByteArray key, Version version) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        StoreUtils.assertValidMetadata(key, metadata.getRoutingStrategy(getName()), nodeId);

        return getInnerStore().delete(key, version);
    }

    @Override
    public void put(ByteArray key, Versioned<byte[]> value) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        StoreUtils.assertValidMetadata(key, metadata.getRoutingStrategy(getName()), nodeId);

        getInnerStore().put(key, value);
    }

    @Override
    public List<Versioned<byte[]>> get(ByteArray key) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        StoreUtils.assertValidMetadata(key, metadata.getRoutingStrategy(getName()), nodeId);

        return getInnerStore().get(key);
    }
}
