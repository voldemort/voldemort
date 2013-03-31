/*
 * Copyright 2008-2013 LinkedIn, Inc
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

package voldemort.coordinator;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.AbstractStoreClientFactory;
import voldemort.client.DefaultStoreClient;
import voldemort.client.StoreClientFactory;
import voldemort.store.CompositeVersionedPutVoldemortRequest;
import voldemort.store.CompositeVoldemortRequest;
import voldemort.store.InvalidMetadataException;
import voldemort.store.StoreTimeoutException;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.google.common.collect.Maps;

/**
 * A special store client to invoke Voldemort operations with the following new
 * features: 1) Per call timeout facility 2) Ability to disable resolution per
 * call
 * 
 * @param <K> Type of the Key
 * @param <V> Type of the Value
 */
public class DynamicTimeoutStoreClient<K, V> extends DefaultStoreClient<K, V> {

    private final Logger logger = Logger.getLogger(DynamicTimeoutStoreClient.class);

    /**
     * 
     * @param storeName Name of the store this client connects to
     * @param storeFactory Reference to the factory used to create this client
     * @param maxMetadataRefreshAttempts Number of retries to retrieve the state
     * @param storesXml The storesXml used during bootstrap
     * @param clusterXml The clusterXml used during bootstrap
     */
    public DynamicTimeoutStoreClient(String storeName,
                                     StoreClientFactory storeFactory,
                                     int maxMetadataRefreshAttempts,
                                     String storesXml,
                                     String clusterXml) {
        this.storeName = storeName;
        this.storeFactory = storeFactory;
        this.metadataRefreshAttempts = maxMetadataRefreshAttempts;
        bootStrap(clusterXml, storesXml);
    }

    // Bootstrap using the given cluster xml and stores xml
    // The super class bootStrap() method is used to handle the
    // InvalidMetadataException
    public void bootStrap(String customClusterXml, String customStoresXml) {
        AbstractStoreClientFactory factory = (AbstractStoreClientFactory) this.storeFactory;
        this.store = factory.getRawStore(storeName, null, customStoresXml, customClusterXml, null);
    }

    public Versioned<V> getWithCustomTimeout(CompositeVoldemortRequest<K, V> requestWrapper) {
        validateTimeout(requestWrapper.getRoutingTimeoutInMs());
        for(int attempts = 0; attempts < this.metadataRefreshAttempts; attempts++) {
            try {
                List<Versioned<V>> items = store.get(requestWrapper);
                return getItemOrThrow(requestWrapper.getKey(), requestWrapper.getValue(), items);
            } catch(InvalidMetadataException e) {
                logger.info("Received invalid metadata exception during get [  " + e.getMessage()
                            + " ] on store '" + storeName + "'. Rebootstrapping");
                bootStrap();
            }
        }
        throw new VoldemortException(this.metadataRefreshAttempts
                                     + " metadata refresh attempts failed.");
    }

    public Version putWithCustomTimeout(CompositeVoldemortRequest<K, V> requestWrapper) {
        validateTimeout(requestWrapper.getRoutingTimeoutInMs());
        Versioned<V> versioned;
        long startTime = System.currentTimeMillis();

        // We use the full timeout for doing the Get. In this, we're being
        // optimistic that the subsequent put might be faster all the steps
        // might finish within the alloted time
        versioned = getWithCustomTimeout(requestWrapper);

        long endTime = System.currentTimeMillis();
        if(versioned == null)
            versioned = Versioned.value(requestWrapper.getRawValue(), new VectorClock());
        else
            versioned.setObject(requestWrapper.getRawValue());

        // This should not happen unless there's a bug in the
        // getWithCustomTimeout
        if((endTime - startTime) > requestWrapper.getRoutingTimeoutInMs()) {
            throw new StoreTimeoutException("PUT request timed out");
        }

        return putVersionedWithCustomTimeout(new CompositeVersionedPutVoldemortRequest<K, V>(requestWrapper.getKey(),
                                                                                             versioned,
                                                                                             (requestWrapper.getRoutingTimeoutInMs() - (endTime - startTime))));
    }

    public Version putVersionedWithCustomTimeout(CompositeVoldemortRequest<K, V> requestWrapper)
            throws ObsoleteVersionException {
        validateTimeout(requestWrapper.getRoutingTimeoutInMs());
        for(int attempts = 0; attempts < this.metadataRefreshAttempts; attempts++) {
            try {
                store.put(requestWrapper);
                return requestWrapper.getValue().getVersion();
            } catch(InvalidMetadataException e) {
                logger.info("Received invalid metadata exception during put [  " + e.getMessage()
                            + " ] on store '" + storeName + "'. Rebootstrapping");
                bootStrap();
            }
        }
        throw new VoldemortException(this.metadataRefreshAttempts
                                     + " metadata refresh attempts failed.");
    }

    public Map<K, Versioned<V>> getAllWithCustomTimeout(CompositeVoldemortRequest<K, V> requestWrapper) {
        validateTimeout(requestWrapper.getRoutingTimeoutInMs());
        Map<K, List<Versioned<V>>> items = null;
        for(int attempts = 0;; attempts++) {
            if(attempts >= this.metadataRefreshAttempts)
                throw new VoldemortException(this.metadataRefreshAttempts
                                             + " metadata refresh attempts failed.");
            try {
                items = store.getAll(requestWrapper);
                break;
            } catch(InvalidMetadataException e) {
                logger.info("Received invalid metadata exception during getAll [  "
                            + e.getMessage() + " ] on store '" + storeName + "'. Rebootstrapping");
                bootStrap();
            }
        }
        Map<K, Versioned<V>> result = Maps.newHashMapWithExpectedSize(items.size());

        for(Entry<K, List<Versioned<V>>> mapEntry: items.entrySet()) {
            Versioned<V> value = getItemOrThrow(mapEntry.getKey(), null, mapEntry.getValue());
            result.put(mapEntry.getKey(), value);
        }
        return result;
    }

    public boolean deleteWithCustomTimeout(CompositeVoldemortRequest<K, V> deleteRequestObject) {
        validateTimeout(deleteRequestObject.getRoutingTimeoutInMs());
        if(deleteRequestObject.getVersion() == null) {

            long startTimeInMs = System.currentTimeMillis();

            // We use the full timeout for doing the Get. In this, we're being
            // optimistic that the subsequent delete might be faster all the
            // steps might finish within the alloted time
            Versioned<V> versioned = getWithCustomTimeout(deleteRequestObject);
            if(versioned == null) {
                return false;
            }

            long endTimeInMs = System.currentTimeMillis();
            long diffInMs = endTimeInMs - startTimeInMs;

            // This should not happen unless there's a bug in the
            // getWithCustomTimeout
            if(diffInMs > deleteRequestObject.getRoutingTimeoutInMs()) {
                throw new StoreTimeoutException("DELETE request timed out");
            }

            // Update the version and the new timeout
            deleteRequestObject.setVersion(versioned.getVersion());
            deleteRequestObject.setRoutingTimeoutInMs(deleteRequestObject.getRoutingTimeoutInMs()
                                                      - diffInMs);

        }

        return store.delete(deleteRequestObject);
    }

    // Make sure that the timeout specified is valid
    private void validateTimeout(long opTimeoutInMs) {
        if(opTimeoutInMs <= 0) {
            throw new IllegalArgumentException("Illegal parameter: Timeout is too low: "
                                               + opTimeoutInMs);
        }
    }

    public String getStoreName() {
        return this.storeName;
    }

}
