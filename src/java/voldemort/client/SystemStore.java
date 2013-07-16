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

package voldemort.client;

import java.util.List;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.store.Store;
import voldemort.store.system.SystemStoreConstants;
import voldemort.versioning.InconsistentDataException;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * A client interface for interacting with System stores (managed by the
 * cluster). The naming convention is kept consistent with SocketStore (which is
 * also a client interface).
 * 
 * @author csoman
 * 
 * @param <K> Type of Key
 * @param <V> Type of Value
 */
public class SystemStore<K, V> {

    private final Logger logger = Logger.getLogger(SystemStore.class);
    private final String storeName;
    private volatile Store<K, V, Object> sysStore;

    /**
     * Constructor that creates a system store client which can be used to
     * interact with the system stores managed by the cluster
     * 
     * @param storeName Name of the system store
     * @param systemStore The socket store used to interact with the system
     *        stores on the individual server nodes
     */
    public SystemStore(String storeName, Store<K, V, Object> systemStore) {
        String prefix = storeName.substring(0, SystemStoreConstants.NAME_PREFIX.length());
        if(!SystemStoreConstants.NAME_PREFIX.equals(prefix)) {
            throw new VoldemortException("Illegal system store : " + storeName);
        }

        this.storeName = storeName;
        try {
            this.sysStore = systemStore;
        } catch(Exception e) {
            logger.debug("Error while creating a system store client for store : " + this.storeName);
        }
    }

    public Version putSysStore(K key, V value) {
        Version version = null;
        try {
            logger.debug("Invoking Put for key : " + key + " on store name : " + this.storeName);
            Versioned<V> versioned = getSysStore(key);
            if(versioned == null)
                versioned = Versioned.value(value, new VectorClock());
            else
                versioned.setObject(value);
            this.sysStore.put(key, versioned, null);
            version = versioned.getVersion();
        } catch(Exception e) {
            if(logger.isDebugEnabled()) {
                logger.debug("Exception caught during putSysStore: " + e);
            }
        }
        return version;
    }

    public Version putSysStore(K key, Versioned<V> value) {
        Version version = null;
        try {
            logger.debug("Invoking Put for key : " + key + " on store name : " + this.storeName);
            this.sysStore.put(key, value, null);
            version = value.getVersion();
        } catch(Exception e) {
            if(logger.isDebugEnabled()) {
                logger.debug("Exception caught during putSysStore: " + e);
            }
        }
        return version;
    }

    public Versioned<V> getSysStore(K key) {
        logger.debug("Invoking Get for key : " + key + " on store name : " + this.storeName);
        Versioned<V> versioned = null;
        try {
            List<Versioned<V>> items = this.sysStore.get(key, null);

            if(items.size() == 1)
                versioned = items.get(0);
            else if(items.size() > 1)
                throw new InconsistentDataException("Unresolved versions returned from get(" + key
                                                    + ") = " + items, items);
            if(versioned != null)
                logger.debug("Value for key : " + key + " = " + versioned.getValue()
                             + " on store name : " + this.storeName);
            else
                logger.debug("Got null value");
        } catch(Exception e) {
            if(logger.isDebugEnabled()) {
                logger.debug("Exception caught during getSysStore: " + e);
            }
        }
        return versioned;
    }

    public V getValueSysStore(K key) {
        V value = null;
        try {
            logger.debug("Invoking Get for key : " + key + " on store name : " + this.storeName);
            Versioned<V> versioned = getSysStore(key);
            if(versioned != null) {
                logger.debug("Value for key : " + key + " = " + versioned.getValue()
                             + " on store name : " + this.storeName);
                value = versioned.getValue();
            }
        } catch(Exception e) {
            if(logger.isDebugEnabled()) {
                logger.debug("Exception caught during getSysStore: " + e);
            }
        }
        return value;
    }
}
