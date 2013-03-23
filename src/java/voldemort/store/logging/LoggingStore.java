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

package voldemort.store.logging;

import java.util.List;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.store.DelegatingStore;
import voldemort.store.Store;
import voldemort.store.StoreCapabilityType;
import voldemort.store.CompositeVoldemortRequest;
import voldemort.utils.SystemTime;
import voldemort.utils.Time;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * A store wrapper that handles debug logging.
 * 
 * 
 */
public class LoggingStore<K, V, T> extends DelegatingStore<K, V, T> {

    private final Logger logger;
    private final Time time;
    private final String instanceName;

    /**
     * Create a logging store that wraps the given store
     * 
     * @param store The store to wrap
     */
    public LoggingStore(Store<K, V, T> store) {
        this(store, new SystemTime());
    }

    /**
     * Create a logging store that wraps the given store
     * 
     * @param store The store to wrap
     * @param time The time implementation to use for computing ellapsed time
     */
    public LoggingStore(Store<K, V, T> store, Time time) {
        this(store, null, time);
    }

    /**
     * Create a logging store that wraps the given store
     * 
     * @param store The store to wrap
     * @param instance The instance name to display in logging messages
     * @param time The time implementation to use for computing ellapsed time
     */
    public LoggingStore(Store<K, V, T> store, String instance, Time time) {
        super(store);
        this.logger = Logger.getLogger(store.getClass());
        this.time = time;
        this.instanceName = instance == null ? ": " : instance + ": ";
    }

    @Override
    public void close() throws VoldemortException {
        if(logger.isDebugEnabled())
            logger.debug("Closing " + getName() + ".");
        super.close();
    }

    @Override
    public boolean delete(K key, Version version) throws VoldemortException {
        long startTimeNs = 0;
        boolean succeeded = false;
        if(logger.isDebugEnabled())
            startTimeNs = time.getNanoseconds();
        try {
            boolean deletedSomething = getInnerStore().delete(key, version);
            succeeded = true;
            return deletedSomething;
        } finally {
            printTimedMessage("DELETE", succeeded, startTimeNs);
        }
    }

    @Override
    public List<Versioned<V>> get(K key, T transform) throws VoldemortException {
        long startTimeNs = 0;
        boolean succeeded = false;
        if(logger.isDebugEnabled())
            startTimeNs = time.getNanoseconds();
        try {
            List<Versioned<V>> l = getInnerStore().get(key, transform);
            succeeded = true;
            return l;
        } finally {
            printTimedMessage("GET", succeeded, startTimeNs);
        }
    }

    @Override
    public void put(K key, Versioned<V> value, T transform) throws VoldemortException {
        long startTimeNs = 0;
        boolean succeeded = false;
        if(logger.isDebugEnabled()) {
            startTimeNs = time.getNanoseconds();
        }
        try {
            getInnerStore().put(key, value, transform);
            succeeded = true;
        } finally {
            printTimedMessage("PUT", succeeded, startTimeNs);
        }
    }

    private void printTimedMessage(String operation, boolean success, long startNs) {
        if(logger.isDebugEnabled()) {
            double elapsedMs = (time.getNanoseconds() - startNs) / (double) Time.NS_PER_MS;
            logger.debug(instanceName + operation + " " + getName() + " "
                         + (success ? "successful" : "unsuccessful") + " in " + elapsedMs + " ms");
        }
    }

    @Override
    public Object getCapability(StoreCapabilityType capability) {
        if(capability == StoreCapabilityType.LOGGER)
            return this.logger;
        else
            return getInnerStore().getCapability(capability);
    }

    @Override
    public List<Versioned<V>> get(CompositeVoldemortRequest<K, V> request) throws VoldemortException {
        long startTimeNs = 0;
        boolean succeeded = false;
        if(logger.isDebugEnabled())
            startTimeNs = time.getNanoseconds();
        try {
            List<Versioned<V>> l = getInnerStore().get(request);
            succeeded = true;
            return l;
        } finally {
            printTimedMessage("GET", succeeded, startTimeNs);
        }
    }

    @Override
    public void put(CompositeVoldemortRequest<K, V> request) throws VoldemortException {
        long startTimeNs = 0;
        boolean succeeded = false;
        if(logger.isDebugEnabled()) {
            startTimeNs = time.getNanoseconds();
        }
        try {
            getInnerStore().put(request);
            succeeded = true;
        } finally {
            printTimedMessage("PUT", succeeded, startTimeNs);
        }
    }

    @Override
    public boolean delete(CompositeVoldemortRequest<K, V> request) throws VoldemortException {
        long startTimeNs = 0;
        boolean succeeded = false;
        if(logger.isDebugEnabled())
            startTimeNs = time.getNanoseconds();
        try {
            boolean deletedSomething = getInnerStore().delete(request);
            succeeded = true;
            return deletedSomething;
        } finally {
            printTimedMessage("DELETE", succeeded, startTimeNs);
        }
    }

}
