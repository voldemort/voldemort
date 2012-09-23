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
import java.util.HashMap;
import java.util.List;

import voldemort.client.RoutingTier;
import voldemort.serialization.SerializerDefinition;
import voldemort.store.slop.strategy.HintedHandoffStrategyType;
import voldemort.store.system.SystemStoreConstants;
import voldemort.utils.Utils;

import com.google.common.base.Objects;

/**
 * The configuration information for a store.
 * 
 * 
 */
public class StoreDefinition implements Serializable {

    private static final long serialVersionUID = 1;

    private final String name;
    private final String type;
    private final String description;
    private final SerializerDefinition keySerializer;
    private final SerializerDefinition valueSerializer;
    private final SerializerDefinition transformsSerializer;
    private final RoutingTier routingPolicy;
    private final int replicationFactor;
    private final Integer preferredWrites;
    private final int requiredWrites;
    private final Integer preferredReads;
    private final int requiredReads;
    private final Integer retentionPeriodDays;
    private final Integer retentionScanThrottleRate;
    private final Integer retentionFrequencyDays;
    private final String routingStrategyType;
    private final String viewOf;
    private final HashMap<Integer, Integer> zoneReplicationFactor;
    private final Integer zoneCountReads;
    private final Integer zoneCountWrites;
    private final String valueTransformation;
    private final String serializerFactory;
    private final HintedHandoffStrategyType hintedHandoffStrategyType;
    private final Integer hintPrefListSize;
    private final List<String> owners;
    private final long memoryFootprintMB;

    public StoreDefinition(String name,
                           String type,
                           String description,
                           SerializerDefinition keySerializer,
                           SerializerDefinition valueSerializer,
                           SerializerDefinition transformsSerializer,
                           RoutingTier routingPolicy,
                           String routingStrategyType,
                           int replicationFactor,
                           Integer preferredReads,
                           int requiredReads,
                           Integer preferredWrites,
                           int requiredWrites,
                           String viewOfStore,
                           String valTrans,
                           HashMap<Integer, Integer> zoneReplicationFactor,
                           Integer zoneCountReads,
                           Integer zoneCountWrites,
                           Integer retentionDays,
                           Integer retentionThrottleRate,
                           Integer retentionFrequencyDays,
                           String factory,
                           HintedHandoffStrategyType hintedHandoffStrategyType,
                           Integer hintPrefListSize,
                           List<String> owners,
                           long memoryFootprintMB) {
        this.name = Utils.notNull(name);
        this.type = type;
        this.description = description;
        this.replicationFactor = replicationFactor;
        this.preferredReads = preferredReads;
        this.requiredReads = requiredReads;
        this.preferredWrites = preferredWrites;
        this.requiredWrites = requiredWrites;
        this.routingPolicy = routingPolicy;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.transformsSerializer = transformsSerializer;
        this.retentionPeriodDays = retentionDays;
        this.retentionScanThrottleRate = retentionThrottleRate;
        this.retentionFrequencyDays = retentionFrequencyDays;
        this.memoryFootprintMB = memoryFootprintMB;
        this.routingStrategyType = routingStrategyType;
        this.viewOf = viewOfStore;
        this.valueTransformation = valTrans;
        this.zoneReplicationFactor = zoneReplicationFactor;
        this.zoneCountReads = zoneCountReads;
        this.zoneCountWrites = zoneCountWrites;
        this.serializerFactory = factory;
        this.hintedHandoffStrategyType = hintedHandoffStrategyType;
        this.hintPrefListSize = hintPrefListSize;
        this.owners = owners;
    }

    protected void checkParameterLegality() {

        // null checks
        Utils.notNull(this.type);
        Utils.notNull(routingPolicy);
        Utils.notNull(keySerializer);
        Utils.notNull(valueSerializer);

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

        if(!SystemStoreConstants.isSystemStore(name) && zoneReplicationFactor != null
           && zoneReplicationFactor.size() != 0) {

            if(zoneCountReads == null || zoneCountReads < 0)
                throw new IllegalArgumentException("Zone Counts reads must be non-negative / non-null");

            if(zoneCountWrites == null || zoneCountWrites < 0)
                throw new IllegalArgumentException("Zone Counts writes must be non-negative");

            int sumZoneReplicationFactor = 0;
            int replicatingZones = 0;
            for(Integer zoneId: zoneReplicationFactor.keySet()) {
                int currentZoneRepFactor = zoneReplicationFactor.get(zoneId);

                sumZoneReplicationFactor += currentZoneRepFactor;
                if(currentZoneRepFactor > 0)
                    replicatingZones++;
            }

            if(replicatingZones <= 0) {
                throw new IllegalArgumentException("Cannot have no zones to replicate to. "
                                                   + "Should have some positive zoneReplicationFactor");
            }

            // Check if sum of individual zones is equal to total replication
            // factor
            if(sumZoneReplicationFactor != replicationFactor) {
                throw new IllegalArgumentException("Sum total of zones ("
                                                   + sumZoneReplicationFactor
                                                   + ") does not match the total replication factor ("
                                                   + replicationFactor + ")");
            }

            // Check if number of zone-count-reads and zone-count-writes are
            // less than zones replicating to
            if(zoneCountReads >= replicatingZones) {
                throw new IllegalArgumentException("Number of zones to block for while reading ("
                                                   + zoneCountReads
                                                   + ") should be less then replicating zones ("
                                                   + replicatingZones + ")");
            }

            if(zoneCountWrites >= replicatingZones) {
                throw new IllegalArgumentException("Number of zones to block for while writing ("
                                                   + zoneCountWrites
                                                   + ") should be less then replicating zones ("
                                                   + replicatingZones + ")");
            }
        }
    }

    public String getDescription() {
        return this.description;
    }

    public String getSerializerFactory() {
        return this.serializerFactory;
    }

    public boolean hasTransformsSerializer() {
        return transformsSerializer != null;
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

    public SerializerDefinition getTransformsSerializer() {
        return transformsSerializer;
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

    public boolean hasRetentionScanThrottleRate() {
        return this.retentionScanThrottleRate != null;
    }

    public Integer getRetentionScanThrottleRate() {
        return this.retentionScanThrottleRate;
    }

    public boolean hasRetentionFrequencyDays() {
        return this.retentionFrequencyDays != null;
    }

    public Integer getRetentionFrequencyDays() {
        return this.retentionFrequencyDays;
    }

    public boolean isView() {
        return this.viewOf != null;
    }

    public String getViewTargetStoreName() {
        return viewOf;
    }

    public boolean hasValueTransformation() {
        return this.valueTransformation != null;
    }

    public String getValueTransformation() {
        return valueTransformation;
    }

    public HashMap<Integer, Integer> getZoneReplicationFactor() {
        return zoneReplicationFactor;
    }

    public Integer getZoneCountReads() {
        return zoneCountReads;
    }

    public boolean hasZoneCountReads() {
        return zoneCountReads != null;
    }

    public Integer getZoneCountWrites() {
        return zoneCountWrites;
    }

    public boolean hasZoneCountWrites() {
        return zoneCountWrites != null;
    }

    public HintedHandoffStrategyType getHintedHandoffStrategyType() {
        return hintedHandoffStrategyType;
    }

    public boolean hasHintedHandoffStrategyType() {
        return hintedHandoffStrategyType != null;
    }

    public Integer getHintPrefListSize() {
        return hintPrefListSize;
    }

    public boolean hasHintPreflistSize() {
        return hintPrefListSize != null;
    }

    public List<String> getOwners() {
        return this.owners;
    }

    public long getMemoryFootprintMB() {
        return this.memoryFootprintMB;
    }

    public boolean hasMemoryFootprint() {
        return memoryFootprintMB != 0;
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
        return getName().equals(def.getName())
               && getType().equals(def.getType())
               && getReplicationFactor() == def.getReplicationFactor()
               && getRequiredReads() == def.getRequiredReads()
               && Objects.equal(getPreferredReads(), def.getPreferredReads())
               && getRequiredWrites() == def.getRequiredWrites()
               && Objects.equal(getPreferredWrites(), def.getPreferredWrites())
               && getKeySerializer().equals(def.getKeySerializer())
               && getValueSerializer().equals(def.getValueSerializer())
               && Objects.equal(getTransformsSerializer() != null ? getTransformsSerializer()
                                                                 : null,
                                def.getTransformsSerializer() != null ? def.getTransformsSerializer()
                                                                     : null)
               && getRoutingPolicy() == def.getRoutingPolicy()
               && Objects.equal(getViewTargetStoreName(), def.getViewTargetStoreName())
               && Objects.equal(getValueTransformation() != null ? getValueTransformation().getClass()
                                                                : null,
                                def.getValueTransformation() != null ? def.getValueTransformation()
                                                                          .getClass() : null)
               && Objects.equal(getZoneReplicationFactor() != null ? getZoneReplicationFactor().getClass()
                                                                  : null,
                                def.getZoneReplicationFactor() != null ? def.getZoneReplicationFactor()
                                                                            .getClass()
                                                                      : null)
               && Objects.equal(getZoneCountReads(), def.getZoneCountReads())
               && Objects.equal(getZoneCountWrites(), def.getZoneCountWrites())
               && Objects.equal(getRetentionDays(), def.getRetentionDays())
               && Objects.equal(getRetentionScanThrottleRate(), def.getRetentionScanThrottleRate())
               && Objects.equal(getSerializerFactory() != null ? getSerializerFactory() : null,
                                def.getSerializerFactory() != null ? def.getSerializerFactory()
                                                                  : null)
               && Objects.equal(getHintedHandoffStrategyType(), def.getHintedHandoffStrategyType())
               && Objects.equal(getHintPrefListSize(), def.getHintPrefListSize())
               && Objects.equal(getMemoryFootprintMB(), def.getMemoryFootprintMB());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getName(),
                                getType(),
                                getDescription(),
                                getKeySerializer(),
                                getValueSerializer(),
                                getTransformsSerializer(),
                                getRoutingPolicy(),
                                getRoutingStrategyType(),
                                getReplicationFactor(),
                                getRequiredReads(),
                                getRequiredWrites(),
                                getPreferredReads(),
                                getPreferredWrites(),
                                getViewTargetStoreName(),
                                getValueTransformation() == null ? null
                                                                : getValueTransformation().getClass(),
                                getZoneReplicationFactor() == null ? null
                                                                  : getZoneReplicationFactor().getClass(),
                                getZoneCountReads(),
                                getZoneCountWrites(),
                                getRetentionDays(),
                                getRetentionScanThrottleRate(),
                                getSerializerFactory(),
                                hasHintedHandoffStrategyType() ? getHintedHandoffStrategyType()
                                                              : null,
                                hasHintPreflistSize() ? getHintPrefListSize() : null,
                                getOwners(),
                                getMemoryFootprintMB());
    }

    @Override
    public String toString() {
        return "StoreDefinition(name = " + getName() + ", type = " + getType() + ", description = "
               + getDescription() + ", key-serializer = " + getKeySerializer()
               + ", value-serializer = " + getValueSerializer() + ", routing = "
               + getRoutingPolicy() + ", routing-strategy = " + getRoutingStrategyType()
               + ", replication = " + getReplicationFactor() + ", required-reads = "
               + getRequiredReads() + ", preferred-reads = " + getPreferredReads()
               + ", required-writes = " + getRequiredWrites() + ", preferred-writes = "
               + getPreferredWrites() + ", view-target = " + getViewTargetStoreName()
               + ", value-transformation = " + getValueTransformation() + ", retention-days = "
               + getRetentionDays() + ", throttle-rate = " + getRetentionScanThrottleRate()
               + ", zone-replication-factor = " + getZoneReplicationFactor()
               + ", zone-count-reads = " + getZoneCountReads() + ", zone-count-writes = "
               + getZoneCountWrites() + ", serializer factory = " + getSerializerFactory() + ")"
               + ", hinted-handoff-strategy = " + getHintedHandoffStrategyType()
               + ", hint-preflist-size = " + getHintPrefListSize() + ", owners = " + getOwners()
               + ", memory-footprint(MB)" + getMemoryFootprintMB() + ")";
    }
}
