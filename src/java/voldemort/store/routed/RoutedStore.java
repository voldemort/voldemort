/*
 * Copyright 2008-2010 LinkedIn, Inc
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

package voldemort.store.routed;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.store.NoSuchCapabilityException;
import voldemort.store.Store;
import voldemort.store.StoreCapabilityType;
import voldemort.store.StoreDefinition;
import voldemort.utils.ByteArray;
import voldemort.utils.Time;
import voldemort.utils.Utils;

/**
 * A Store which multiplexes requests to different internal Stores
 * 
 * 
 */
public abstract class RoutedStore implements Store<ByteArray, byte[], byte[]> {

    protected final String name;
    protected final Map<Integer, Store<ByteArray, byte[], byte[]>> innerStores;
    protected final boolean repairReads;
    protected final ReadRepairer<ByteArray, byte[]> readRepairer;
    protected final long timeoutMs;
    protected final Time time;
    protected final StoreDefinition storeDef;
    protected final FailureDetector failureDetector;
    protected volatile RoutingStrategy routingStrategy;
    protected final Logger logger = Logger.getLogger(getClass());

    protected RoutedStore(String name,
                          Map<Integer, Store<ByteArray, byte[], byte[]>> innerStores,
                          Cluster cluster,
                          StoreDefinition storeDef,
                          boolean repairReads,
                          long timeoutMs,
                          FailureDetector failureDetector,
                          Time time) {
        if(storeDef.getRequiredReads() < 1)
            throw new IllegalArgumentException("Cannot have a storeDef.getRequiredReads() number less than 1.");
        if(storeDef.getRequiredWrites() < 1)
            throw new IllegalArgumentException("Cannot have a storeDef.getRequiredWrites() number less than 1.");
        if(storeDef.getPreferredReads() < storeDef.getRequiredReads())
            throw new IllegalArgumentException("storeDef.getPreferredReads() must be greater or equal to storeDef.getRequiredReads().");
        if(storeDef.getPreferredWrites() < storeDef.getRequiredWrites())
            throw new IllegalArgumentException("storeDef.getPreferredWrites() must be greater or equal to storeDef.getRequiredWrites().");
        if(storeDef.getPreferredReads() > innerStores.size())
            throw new IllegalArgumentException("storeDef.getPreferredReads() is larger than the total number of nodes!");
        if(storeDef.getPreferredWrites() > innerStores.size())
            throw new IllegalArgumentException("storeDef.getPreferredWrites() is larger than the total number of nodes!");

        this.name = name;
        this.innerStores = new ConcurrentHashMap<Integer, Store<ByteArray, byte[], byte[]>>(innerStores);
        this.repairReads = repairReads;
        this.readRepairer = new ReadRepairer<ByteArray, byte[]>();
        this.timeoutMs = timeoutMs;
        this.time = Utils.notNull(time);
        this.storeDef = storeDef;
        this.failureDetector = failureDetector;
        this.routingStrategy = new RoutingStrategyFactory().updateRoutingStrategy(storeDef, cluster);
    }

    public void updateRoutingStrategy(RoutingStrategy routingStrategy) {
        logger.info("Updating routing strategy for RoutedStore:" + getName());
        this.routingStrategy = routingStrategy;
    }

    public String getName() {
        return this.name;
    }

    public void close() {
        VoldemortException exception = null;

        for(Store<?, ?, ?> store: innerStores.values()) {
            try {
                store.close();
            } catch(VoldemortException e) {
                exception = e;
            }
        }

        if(exception != null)
            throw exception;
    }

    public Map<Integer, Store<ByteArray, byte[], byte[]>> getInnerStores() {
        return this.innerStores;
    }

    public Object getCapability(StoreCapabilityType capability) {
        switch(capability) {
            case ROUTING_STRATEGY:
                return this.routingStrategy;
            case READ_REPAIRER:
                return this.readRepairer;
            case VERSION_INCREMENTING:
                return true;
            default:
                throw new NoSuchCapabilityException(capability, getName());
        }
    }

}
