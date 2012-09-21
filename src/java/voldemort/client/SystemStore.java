/*
 * Copyright 2008-2012 LinkedIn, Inc
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
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.cluster.failuredetector.FailureDetector;
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
    private final SocketStoreClientFactory socketStoreFactory;
    private final String storeName;
    private volatile Store<K, V, Object> sysStore;

    /**
     * Wrapper for the actual SystemStore constructor. Used when we dont have
     * custom Cluster XML, failure detector or a base Voldemort Client config to
     * be used with this system store client.
     * 
     * @param storeName Name of the system store
     * @param bootstrapUrls Bootstrap URLs used to connect to
     * @param clientZoneID Primary zone ID for this system store client
     *        (determines routing strategy)
     */
    public SystemStore(String storeName, String[] bootstrapUrls, int clientZoneID) {
        this(storeName, bootstrapUrls, clientZoneID, null, null, new ClientConfig());
    }

    /**
     * Wrapper for the actual SystemStore constructor. Used when we dont have
     * custom Cluster XML or failure detector to be used with this system store
     * client.
     * 
     * @param storeName Name of the system store
     * @param bootstrapUrls Bootstrap URLs used to connect to
     * @param clientZoneID Primary zone ID for this system store client
     *        (determines routing strategy)
     * @param baseConfig Base Voldemort Client config which specifies properties
     *        for this system store client
     */
    public SystemStore(String storeName,
                       String[] bootstrapUrls,
                       int clientZoneID,
                       ClientConfig baseConfig) {
        this(storeName, bootstrapUrls, clientZoneID, null, null, baseConfig);
    }

    /**
     * SystemStore Constructor wrapper for the actual constructor. Used when we
     * dont want to specify a base Voldemort Client Config.
     * 
     * @param storeName Name of the system store
     * @param bootstrapUrls Bootstrap URLs used to connect to
     * @param clientZoneID Primary zone ID for this system store client
     *        (determines routing strategy)
     * @param clusterXml Custom ClusterXml to be used for this system store
     *        client
     * @param fd Failure Detector to be used with this system store client
     */
    public SystemStore(String storeName,
                       String[] bootstrapUrls,
                       int clientZoneID,
                       String clusterXml,
                       FailureDetector fd) {
        this(storeName, bootstrapUrls, clientZoneID, clusterXml, fd, new ClientConfig());
    }

    /**
     * SystemStore Constructor that creates a system store client which can be
     * used to interact with the system stores managed by the cluster
     * 
     * @param storeName Name of the system store
     * @param bootstrapUrls Bootstrap URLs used to connect to
     * @param clientZoneID Primary zone ID for this system store client
     *        (determines routing strategy)
     * @param clusterXml Custom ClusterXml to be used for this system store
     *        client
     * @param fd Failure Detector to be used with this system store client
     * @param baseConfig Base Voldemort Client config which specifies properties
     *        for this system store client
     */
    public SystemStore(String storeName,
                       String[] bootstrapUrls,
                       int clientZoneID,
                       String clusterXml,
                       FailureDetector fd,
                       ClientConfig baseConfig) {
        String prefix = storeName.substring(0, SystemStoreConstants.NAME_PREFIX.length());
        if(!SystemStoreConstants.NAME_PREFIX.equals(prefix))
            throw new VoldemortException("Illegal system store : " + storeName);

        ClientConfig config = new ClientConfig();
        config.setSelectors(1)
              .setBootstrapUrls(bootstrapUrls)
              .setMaxConnectionsPerNode(baseConfig.getSysMaxConnectionsPerNode())
              .setConnectionTimeout(baseConfig.getSysConnectionTimeout(), TimeUnit.MILLISECONDS)
              .setSocketTimeout(baseConfig.getSysSocketTimeout(), TimeUnit.MILLISECONDS)
              .setRoutingTimeout(baseConfig.getSysRoutingTimeout(), TimeUnit.MILLISECONDS)
              .setEnableJmx(baseConfig.getSysEnableJmx())
              .setEnablePipelineRoutedStore(baseConfig.getSysEnablePipelineRoutedStore())
              .setClientZoneId(clientZoneID);
        this.socketStoreFactory = new SocketStoreClientFactory(config);
        this.storeName = storeName;
        try {
            this.sysStore = this.socketStoreFactory.getSystemStore(this.storeName, clusterXml, fd);
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
