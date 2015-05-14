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

package voldemort.rest.coordinator;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.AbstractStoreClientFactory;
import voldemort.client.DefaultStoreClient;
import voldemort.client.StoreClientFactory;
import voldemort.rest.RestUtils;
import voldemort.store.CompositeVersionedPutVoldemortRequest;
import voldemort.store.CompositeVoldemortRequest;
import voldemort.store.InvalidMetadataException;
import voldemort.store.Store;
import voldemort.store.StoreTimeoutException;
import voldemort.utils.ByteArray;
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
                long startTimeInMs = System.currentTimeMillis();
                String keyHexString = "";
                if(logger.isDebugEnabled()) {
                    ByteArray key = (ByteArray) requestWrapper.getKey();
                    keyHexString = RestUtils.getKeyHexString(key);
                    debugLogStart("GET",
                                  requestWrapper.getRequestOriginTimeInMs(),
                                  startTimeInMs,
                                  keyHexString);
                }
                List<Versioned<V>> items = store.get(requestWrapper);
                if(logger.isDebugEnabled()) {
                    int vcEntrySize = 0;
                    for(Versioned<V> vc: items) {
                        vcEntrySize += ((VectorClock) vc.getVersion()).getVersionMap().size();
                    }
                    debugLogEnd("GET",
                                requestWrapper.getRequestOriginTimeInMs(),
                                startTimeInMs,
                                System.currentTimeMillis(),
                                keyHexString,
                                vcEntrySize);
                }
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
        String keyHexString = "";
        if(logger.isDebugEnabled()) {
            ByteArray key = (ByteArray) requestWrapper.getKey();
            keyHexString = RestUtils.getKeyHexString(key);
            logger.debug("PUT requested for key: " + keyHexString + " , for store: "
                         + this.storeName + " at time(in ms): " + startTime
                         + " . Nested GET and PUT VERSION requests to follow ---");
        }

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
        CompositeVersionedPutVoldemortRequest<K, V> putVersionedRequestObject = new CompositeVersionedPutVoldemortRequest<K, V>(requestWrapper.getKey(),
                                                                                                                                versioned,
                                                                                                                                timeLeft);
        putVersionedRequestObject.setRequestOriginTimeInMs(requestWrapper.getRequestOriginTimeInMs());
        Version result = putVersionedWithCustomTimeout(putVersionedRequestObject);
        long endTimeInMs = System.currentTimeMillis();
        if(logger.isDebugEnabled()) {
            logger.debug("PUT response recieved for key: " + keyHexString + " , for store: "
                         + this.storeName + " at time(in ms): " + endTimeInMs);
        }
        return result;
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
                String keyHexString = "";
                long startTimeInMs = System.currentTimeMillis();
                if(logger.isDebugEnabled()) {
                    ByteArray key = (ByteArray) requestWrapper.getKey();
                    keyHexString = RestUtils.getKeyHexString(key);
                    debugLogStart("PUT_VERSION",
                                  requestWrapper.getRequestOriginTimeInMs(),
                                  startTimeInMs,
                                  keyHexString);
                }
                store.put(requestWrapper);
                if(logger.isDebugEnabled()) {
                    debugLogEnd("PUT_VERSION",
                                requestWrapper.getRequestOriginTimeInMs(),
                                startTimeInMs,
                                System.currentTimeMillis(),
                                keyHexString,
                                0);
                }
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
                String KeysHexString = "";
                long startTimeInMs = System.currentTimeMillis();
                if(logger.isDebugEnabled()) {
                    Iterable<ByteArray> keys = (Iterable<ByteArray>) requestWrapper.getIterableKeys();
                    KeysHexString = getKeysHexString(keys);
                    debugLogStart("GET_ALL",
                                  requestWrapper.getRequestOriginTimeInMs(),
                                  startTimeInMs,
                                  KeysHexString);
                }
                items = store.getAll(requestWrapper);
                if(logger.isDebugEnabled()) {
                    int vcEntrySize = 0;

                    for(List<Versioned<V>> item: items.values()) {
                        for(Versioned<V> vc: item) {
                            vcEntrySize += ((VectorClock) vc.getVersion()).getVersionMap().size();
                        }
                    }

                    debugLogEnd("GET_ALL",
                                requestWrapper.getRequestOriginTimeInMs(),
                                startTimeInMs,
                                System.currentTimeMillis(),
                                KeysHexString,
                                vcEntrySize);
                }
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
        boolean hasVersion = deleteRequestObject.getVersion() == null ? false : true;
        String keyHexString = "";
        if(!hasVersion) {
            long startTimeInMs = System.currentTimeMillis();
            if(logger.isDebugEnabled()) {
                ByteArray key = (ByteArray) deleteRequestObject.getKey();
                keyHexString = RestUtils.getKeyHexString(key);
                logger.debug("DELETE without version requested for key: " + keyHexString
                             + " , for store: " + this.storeName + " at time(in ms): "
                             + startTimeInMs + " . Nested GET and DELETE requests to follow ---");
            }
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
        long deleteVersionStartTimeInNs = System.currentTimeMillis();
        if(logger.isDebugEnabled()) {
            ByteArray key = (ByteArray) deleteRequestObject.getKey();
            keyHexString = RestUtils.getKeyHexString(key);
            debugLogStart("DELETE",
                          deleteRequestObject.getRequestOriginTimeInMs(),
                          deleteVersionStartTimeInNs,
                          keyHexString);
        }
        boolean result = store.delete(deleteRequestObject);
        if(logger.isDebugEnabled()) {
            debugLogEnd("DELETE",
                        deleteRequestObject.getRequestOriginTimeInMs(),
                        deleteVersionStartTimeInNs,
                        System.currentTimeMillis(),
                        keyHexString,
                        0);
        }
        if(!hasVersion && logger.isDebugEnabled()) {
            logger.debug("DELETE without version response recieved for key: " + keyHexString
                         + ", for store: " + this.storeName + " at time(in ms): "
                         + System.currentTimeMillis());
        }
        return result;
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

    /**
     * Traces the duration between origin time in the http Request and time just
     * before being processed by the fat client
     * 
     * @param operationType
     * @param originTimeInMS - origin time in the Http Request
     * @param requestReceivedTimeInMs - System Time in ms
     * @param keyString
     */
    private void debugLogStart(String operationType,
                               Long originTimeInMS,
                               Long requestReceivedTimeInMs,
                               String keyString) {
        long durationInMs = requestReceivedTimeInMs - originTimeInMS;
        logger.debug("Received a new request. Operation Type: " + operationType + " , key(s): "
                     + keyString + " , Store: " + this.storeName + " , Origin time (in ms): "
                     + originTimeInMS + " . Request received at time(in ms): "
                     + requestReceivedTimeInMs
                     + " , Duration from RESTClient to CoordinatorFatClient(in ms): "
                     + durationInMs);

    }

    /**
     * Traces the time taken just by the fat client inside Coordinator to
     * process this request
     * 
     * 
     * @param operationType
     * @param OriginTimeInMs - Original request time in Http Request
     * @param RequestStartTimeInMs - Time recorded just before fat client
     *        started processing
     * @param ResponseReceivedTimeInMs - Time when Response was received from
     *        fat client
     * @param keyString - Hex denotation of the key(s)
     * @param numVectorClockEntries - represents the sum of entries size of all
     *        vector clocks received in response. Size of a single vector clock
     *        represents the number of entries(nodes) in the vector
     */
    private void debugLogEnd(String operationType,
                             Long OriginTimeInMs,
                             Long RequestStartTimeInMs,
                             Long ResponseReceivedTimeInMs,
                             String keyString,
                             int numVectorClockEntries) {
        long durationInMs = ResponseReceivedTimeInMs - RequestStartTimeInMs;
        logger.debug("Received a response from voldemort server for Operation Type: "
                     + operationType
                     + " , For key(s): "
                     + keyString
                     + " , Store: "
                     + this.storeName
                     + " , Origin time of request (in ms): "
                     + OriginTimeInMs
                     + " , Response received at time (in ms): "
                     + ResponseReceivedTimeInMs
                     + " . Request sent at(in ms): "
                     + RequestStartTimeInMs
                     + " , Num vector clock entries: "
                     + numVectorClockEntries
                     + " , Duration from CoordinatorFatClient back to CoordinatorFatClient(in ms): "
                     + durationInMs);
    }

    protected String getKeysHexString(Iterable<ByteArray> keys) {
        return RestUtils.getKeysHexString(keys.iterator());
    }

}
