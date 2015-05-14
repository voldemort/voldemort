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

package voldemort.store;

import voldemort.server.RequestRoutingType;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * A base class for the different types of Voldemort requests containing all the
 * necessary components
 */
public class CompositeVoldemortRequest<K, V> {

    private final K key;
    private final V rawValue;
    private final Iterable<K> getAllIterableKeys;
    private final Versioned<V> value;
    private Version version;
    private boolean resolveConflicts;
    private long timeoutInMs;
    private byte operationType;
    private long requestOriginTimeInMs = -1;
    private RequestRoutingType routingType = RequestRoutingType.NORMAL;

    // Constructor used for REST server by the extending classes
    public CompositeVoldemortRequest(K key,
                                     V rawValue,
                                     Iterable<K> keys,
                                     Versioned<V> value,
                                     Version version,
                                     long timeoutInMs,
                                     boolean resolveConflicts,
                                     byte operationType,
                                     long requestOriginTimeInMs,
                                     RequestRoutingType routingType) {
        this.key = key;
        this.rawValue = rawValue;
        this.getAllIterableKeys = keys;
        this.timeoutInMs = timeoutInMs;
        this.value = value;
        this.version = version;
        this.resolveConflicts = resolveConflicts;
        this.operationType = operationType;
        this.requestOriginTimeInMs = requestOriginTimeInMs;
        this.routingType = routingType;
    }

    // constructor used by co-ordinator
    public CompositeVoldemortRequest(K key,
                                     V rawValue,
                                     Iterable<K> keys,
                                     Versioned<V> value,
                                     Version version,
                                     long timeoutInMs,
                                     boolean resolveConflicts,
                                     byte operationType) {
        this.key = key;
        this.rawValue = rawValue;
        this.getAllIterableKeys = keys;
        this.timeoutInMs = timeoutInMs;
        this.value = value;
        this.version = version;
        this.resolveConflicts = resolveConflicts;
        this.operationType = operationType;
    }

    public K getKey() {
        return key;
    }

    public Versioned<V> getValue() {
        return value;
    }

    public Version getVersion() {
        return version;
    }

    public void setVersion(Version version) {
        this.version = version;
    }

    public long getRoutingTimeoutInMs() {
        return timeoutInMs;
    }

    public void setRoutingTimeoutInMs(long timeoutInMs) {
        this.timeoutInMs = timeoutInMs;
    }

    public boolean resolveConflicts() {
        return resolveConflicts;
    }

    public Iterable<K> getIterableKeys() {
        return getAllIterableKeys;
    }

    public V getRawValue() {
        return rawValue;
    }

    public byte getOperationType() {
        return operationType;
    }

    public boolean isResolveConflicts() {
        return resolveConflicts;
    }

    public void setResolveConflicts(boolean resolveConflicts) {
        this.resolveConflicts = resolveConflicts;
    }

    public long getRequestOriginTimeInMs() {
        return requestOriginTimeInMs;
    }

    public void setRequestOriginTimeInMs(long requestOriginTimeInMs) {
        this.requestOriginTimeInMs = requestOriginTimeInMs;
    }

    public RequestRoutingType getRoutingType() {
        return routingType;
    }

    public void setOperationType(byte opType) {
        this.operationType = opType;
    }
}
