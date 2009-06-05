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

import java.io.Serializable;

import voldemort.client.RoutingTier;
import voldemort.serialization.SerializerDefinition;
import voldemort.utils.Utils;

import com.google.common.base.Objects;

/**
 * The configuration information for a store.
 * 
 * @author jay
 * 
 */
public class StoreDefinition implements Serializable {

    private static final long serialVersionUID = 1;

    private final String name;
    private final String type;
    private final SerializerDefinition keySerializer;
    private final SerializerDefinition valueSerializer;
    private final RoutingTier routingPolicy;
    private final int replicationFactor;
    private final Integer preferredWrites;
    private final int requiredWrites;
    private final Integer preferredReads;
    private final int requiredReads;
    private final Integer retentionPeriodDays;
    private final String routingStrategyType;

    public StoreDefinition(String name,
                           String type,
                           SerializerDefinition keySerializer,
                           SerializerDefinition valueSerializer,
                           RoutingTier routingPolicy,
                           String routingStrategyType,
                           int replicationFactor,
                           Integer preferredReads,
                           int requiredReads,
                           Integer preferredWrites,
                           int requiredWrites,
                           Integer retentionDays) {
        this.name = Utils.notNull(name);
        this.type = Utils.notNull(type);
        this.replicationFactor = replicationFactor;
        this.preferredReads = preferredReads;
        this.requiredReads = requiredReads;
        this.preferredWrites = preferredWrites;
        this.requiredWrites = requiredWrites;
        this.routingPolicy = Utils.notNull(routingPolicy);
        this.keySerializer = Utils.notNull(keySerializer);
        this.valueSerializer = Utils.notNull(valueSerializer);
        this.retentionPeriodDays = retentionDays;
        this.routingStrategyType = routingStrategyType;
        checkParameterLegality();
    }

    private void checkParameterLegality() {
        if(requiredReads < 1)
            throw new IllegalArgumentException("Cannot have a requiredReads number less than 1.");
        else if(requiredReads > replicationFactor)
            throw new IllegalArgumentException("Cannot have more requiredReads then there are replicas.");

        if(requiredWrites < 1)
            throw new IllegalArgumentException("Cannot have a requiredWrites number less than 1.");
        else if(requiredWrites > replicationFactor)
            throw new IllegalArgumentException("Cannot have more requiredWrites then there are replicas.");

        if(preferredWrites != null) {
            if(preferredWrites < requiredWrites)
                throw new IllegalArgumentException("preferredWrites must be greater or equal to requiredWrites.");
            if(preferredWrites > replicationFactor)
                throw new IllegalArgumentException("Cannot have more preferredWrites then there are replicas.");
        }
        if(preferredReads != null) {
            if(preferredReads < requiredReads)
                throw new IllegalArgumentException("preferredReads must be greater or equal to requiredReads.");
            if(preferredReads > replicationFactor)
                throw new IllegalArgumentException("Cannot have more preferredReads then there are replicas.");
        }

        if(retentionPeriodDays != null && retentionPeriodDays < 0)
            throw new IllegalArgumentException("Retention days must be non-negative.");
    }

    public String getName() {
        return name;
    }

    public int getRequiredWrites() {
        return requiredWrites;
    }

    public SerializerDefinition getKeySerializer() {
        return keySerializer;
    }

    public SerializerDefinition getValueSerializer() {
        return valueSerializer;
    }

    public RoutingTier getRoutingPolicy() {
        return this.routingPolicy;
    }

    public int getReplicationFactor() {
        return this.replicationFactor;
    }

    public String getRoutingStrategyType() {
        return routingStrategyType;
    }

    public int getRequiredReads() {
        return this.requiredReads;
    }

    public boolean hasPreferredWrites() {
        return preferredWrites != null;
    }

    public int getPreferredWrites() {
        return preferredWrites == null ? getRequiredWrites() : preferredWrites;
    }

    public int getPreferredReads() {
        return preferredReads == null ? getRequiredReads() : preferredReads;
    }

    public boolean hasPreferredReads() {
        return preferredReads != null;
    }

    public String getType() {
        return type;
    }

    public boolean hasRetentionPeriod() {
        return this.retentionPeriodDays != null;
    }

    public Integer getRetentionDays() {
        return this.retentionPeriodDays;
    }

    @Override
    public boolean equals(Object o) {
        if(this == o)
            return true;
        else if(o == null)
            return false;
        else if(!(o.getClass() == StoreDefinition.class))
            return false;

        StoreDefinition def = (StoreDefinition) o;
        return getName().equals(def.getName()) && getType().equals(def.getType())
               && getReplicationFactor() == def.getReplicationFactor()
               && getRequiredReads() == def.getRequiredReads()
               && Objects.equal(getPreferredReads(), def.getPreferredReads())
               && getRequiredWrites() == def.getRequiredWrites()
               && Objects.equal(getPreferredWrites(), def.getPreferredWrites())
               && getKeySerializer().equals(def.getKeySerializer())
               && getValueSerializer().equals(def.getValueSerializer())
               && getRoutingPolicy() == def.getRoutingPolicy()
               && Objects.equal(getRetentionDays(), def.getRetentionDays());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getName(),
                                getType(),
                                getKeySerializer(),
                                getValueSerializer(),
                                getRoutingPolicy(),
                                getReplicationFactor(),
                                getRequiredReads(),
                                getRequiredWrites(),
                                getPreferredReads(),
                                getPreferredWrites(),
                                getRetentionDays());
    }
}
