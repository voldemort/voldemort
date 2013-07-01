/*
 * Copyright 2013 LinkedIn, Inc
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

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.AbstractStoreClientFactory;
import voldemort.client.DefaultStoreClient;
import voldemort.client.StoreClientFactory;
import voldemort.store.CompositeVersionedPutVoldemortRequest;
import voldemort.store.CompositeVoldemortRequest;
import voldemort.store.InvalidMetadataException;
import voldemort.store.Store;
import voldemort.store.StoreTimeoutException;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * A special store client to invoke Voldemort operations with the following new
 * features: 1) Per call timeout facility 2) Ability to disable resolution per
 * call
 * 
 * TODO: Merge this with DefaultStoreClient eventually.
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

    /**
     * Dummy constructor for Unit test purposes
     * 
     * @param customStore A custom store object to use for performing the
     *        operations
     */
    public DynamicTimeoutStoreClient(Store<K, V, Object> customStore) {
        this.store = customStore;
        this.metadataRefreshAttempts = 1;
    }

    // Bootstrap using the given cluster xml and stores xml
    // The super class bootStrap() method is used to handle the
    // InvalidMetadataException
    public void bootStrap(String customClusterXml, String customStoresXml) {
        AbstractStoreClientFactory factory = (AbstractStoreClientFactory) this.storeFactory;
        this.store = factory.getRawStore(storeName, null, customStoresXml, customClusterXml, null);
    }

    /**
     * Performs a get operation with the specified composite request object
     * 
     * @param requestWrapper A composite request object containing the key (and
     *        / or default value) and timeout.
     * @return The Versioned value corresponding to the key
     */
    public List<Versioned<V>> getWithCustomTimeout(CompositeVoldemortRequest<K, V> requestWrapper) {
        validateTimeout(requestWrapper.getRoutingTimeoutInMs());
        for(int attempts = 0; attempts < this.metadataRefreshAttempts; attempts++) {
            try {
                List<Versioned<V>> items = store.get(requestWrapper);
                return items;
            } catch(InvalidMetadataException e) {
                logger.info("Received invalid metadata exception during get [  " + e.getMessage()
                            + " ] on store '" + storeName + "'. Rebootstrapping");
                bootStrap();
            }
        }
        throw new VoldemortException(this.metadataRefreshAttempts
                                     + " metadata refresh attempts failed.");
    }

    /**
     * Performs a put operation with the specified composite request object
     * 
     * @param requestWrapper A composite request object containing the key and
     *        value
     * @return Version of the value for the successful put
     */
    public Version putWithCustomTimeout(CompositeVoldemortRequest<K, V> requestWrapper) {
        validateTimeout(requestWrapper.getRoutingTimeoutInMs());
        List<Versioned<V>> versionedValues;
        long startTime = System.currentTimeMillis();

        // We use the full timeout for doing the Get. In this, we're being
        // optimistic that the subsequent put might be faster such that all the
        // steps might finish within the alloted time
        requestWrapper.setResolveConflicts(true);
        versionedValues = getWithCustomTimeout(requestWrapper);
        Versioned<V> versioned = getItemOrThrow(requestWrapper.getKey(), null, versionedValues);

        long endTime = System.currentTimeMillis();
        if(versioned == null)
            versioned = Versioned.value(requestWrapper.getRawValue(), new VectorClock());
        else
            versioned.setObject(requestWrapper.getRawValue());

        // This should not happen unless there's a bug in the
        // getWithCustomTimeout
        long timeLeft = requestWrapper.getRoutingTimeoutInMs() - (endTime - startTime);
        if(timeLeft <= 0) {
            throw new StoreTimeoutException("PUT request timed out");
        }

        return putVersionedWithCustomTimeout(new CompositeVersionedPutVoldemortRequest<K, V>(requestWrapper.getKey(),
                                                                                             versioned,
                                                                                             timeLeft));
    }

    /**
     * Performs a Versioned put operation with the specified composite request
     * object
     * 
     * @param requestWrapper Composite request object containing the key and the
     *        versioned object
     * @return Version of the value for the successful put
     * @throws ObsoleteVersionException
     */
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

    /**
     * Performs a get all operation with the specified composite request object
     * 
     * @param requestWrapper Composite request object containing a reference to
     *        the Iterable keys
     * 
     * @return Map of the keys to the corresponding versioned values
     */
    public Map<K, List<Versioned<V>>> getAllWithCustomTimeout(CompositeVoldemortRequest<K, V> requestWrapper) {
        validateTimeout(requestWrapper.getRoutingTimeoutInMs());
        Map<K, List<Versioned<V>>> items = null;
        for(int attempts = 0;; attempts++) {
            if(attempts >= this.metadataRefreshAttempts)
                throw new VoldemortException(this.metadataRefreshAttempts
                                             + " metadata refresh attempts failed.");
            try {
                items = store.getAll(requestWrapper);
                return items;
            } catch(InvalidMetadataException e) {
                logger.info("Received invalid metadata exception during getAll [  "
                            + e.getMessage() + " ] on store '" + storeName + "'. Rebootstrapping");
                bootStrap();
            }
        }
    }

    /**
     * Performs a delete operation with the specified composite request object
     * 
     * @param deleteRequestObject Composite request object containing the key to
     *        delete
     * @return true if delete was successful. False otherwise
     */
    public boolean deleteWithCustomTimeout(CompositeVoldemortRequest<K, V> deleteRequestObject) {
        List<Versioned<V>> versionedValues;

        validateTimeout(deleteRequestObject.getRoutingTimeoutInMs());
        if(deleteRequestObject.getVersion() == null) {

            long startTimeInMs = System.currentTimeMillis();

            // We use the full timeout for doing the Get. In this, we're being
            // optimistic that the subsequent delete might be faster all the
            // steps might finish within the alloted time
            deleteRequestObject.setResolveConflicts(true);
            versionedValues = getWithCustomTimeout(deleteRequestObject);
            Versioned<V> versioned = getItemOrThrow(deleteRequestObject.getKey(),
                                                    null,
                                                    versionedValues);

            if(versioned == null) {
                return false;
            }

            long timeLeft = deleteRequestObject.getRoutingTimeoutInMs()
                            - (System.currentTimeMillis() - startTimeInMs);

            // This should not happen unless there's a bug in the
            // getWithCustomTimeout
            if(timeLeft < 0) {
                throw new StoreTimeoutException("DELETE request timed out");
            }

            // Update the version and the new timeout
            deleteRequestObject.setVersion(versioned.getVersion());
            deleteRequestObject.setRoutingTimeoutInMs(timeLeft);

        }

        return store.delete(deleteRequestObject);
    }

    /**
     * Function to check that the timeout specified is valid
     * 
     * @param opTimeoutInMs The specified timeout in milliseconds
     */
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
