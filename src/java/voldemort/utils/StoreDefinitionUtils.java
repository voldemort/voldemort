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

package voldemort.utils;

import static voldemort.serialization.DefaultSerializerFactory.AVRO_GENERIC_TYPE_NAME;
import static voldemort.serialization.DefaultSerializerFactory.AVRO_GENERIC_VERSIONED_TYPE_NAME;
import static voldemort.serialization.DefaultSerializerFactory.AVRO_REFLECTIVE_TYPE_NAME;
import static voldemort.serialization.DefaultSerializerFactory.AVRO_SPECIFIC_TYPE_NAME;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.routing.RoutingStrategyType;
import voldemort.serialization.SerializerDefinition;
import voldemort.serialization.avro.versioned.SchemaEvolutionValidator;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreDefinitionBuilder;
import voldemort.store.readonly.ReadOnlyStorageConfiguration;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class StoreDefinitionUtils {

    private static Logger logger = Logger.getLogger(StoreDefinitionUtils.class);

    /**
     * Given a list of store definitions, filters the list depending on the
     * boolean
     *
     * @param storeDefs Complete list of store definitions
     * @param isReadOnly Boolean indicating whether filter on read-only or not?
     * @return List of filtered store definition
     */
    public static List<StoreDefinition> filterStores(List<StoreDefinition> storeDefs,
                                                     final boolean isReadOnly) {
        List<StoreDefinition> filteredStores = Lists.newArrayList();
        for(StoreDefinition storeDef: storeDefs) {
            if(storeDef.getType().equals(ReadOnlyStorageConfiguration.TYPE_NAME) == isReadOnly) {
                filteredStores.add(storeDef);
            }
        }
        return filteredStores;
    }

    /**
     * Given a list of store definitions return a list of store names
     *
     * @param storeDefList The list of store definitions
     * @return Returns a list of store names
     */
    public static List<String> getStoreNames(List<StoreDefinition> storeDefList) {
        List<String> storeList = new ArrayList<String>();
        for(StoreDefinition def: storeDefList) {
            storeList.add(def.getName());
        }
        return storeList;
    }

    /**
     * Given a list of store definitions return a set of store names
     *
     * @param storeDefList The list of store definitions
     * @return Returns a set of store names
     */
    public static Set<String> getStoreNamesSet(List<StoreDefinition> storeDefList) {
        HashSet<String> storeSet = new HashSet<String>();
        for(StoreDefinition def: storeDefList) {
            storeSet.add(def.getName());
        }
        return storeSet;
    }

    /**
     * Given a store name and a list of store definitions, returns the
     * appropriate store definition ( if it exists )
     *
     * @param storeDefs List of store definitions
     * @param storeName The store name whose store definition is required
     * @return The store definition
     */
    public static StoreDefinition getStoreDefinitionWithName(List<StoreDefinition> storeDefs,
                                                             String storeName) {
        StoreDefinition def = null;
        for(StoreDefinition storeDef: storeDefs) {
            if(storeDef.getName().compareTo(storeName) == 0) {
                def = storeDef;
                break;
            }
        }

        if(def == null) {
            throw new VoldemortException("Could not find store " + storeName);
        }
        return def;
    }

    /**
     * Given a list of store definitions, find out and return a map of similar
     * store definitions + count of them
     *
     * @param storeDefs All store definitions
     * @return Map of a unique store definition + counts
     */
    public static HashMap<StoreDefinition, Integer> getUniqueStoreDefinitionsWithCounts(List<StoreDefinition> storeDefs) {

        HashMap<StoreDefinition, Integer> uniqueStoreDefs = Maps.newHashMap();
        for(StoreDefinition storeDef: storeDefs) {
            if(uniqueStoreDefs.isEmpty()) {
                uniqueStoreDefs.put(storeDef, 1);
            } else {
                StoreDefinition sameStore = null;

                // Go over all the other stores to find if this is unique
                for(StoreDefinition uniqueStoreDef: uniqueStoreDefs.keySet()) {
                    if(uniqueStoreDef.getReplicationFactor() == storeDef.getReplicationFactor()
                       && uniqueStoreDef.getRoutingStrategyType()
                                        .compareTo(storeDef.getRoutingStrategyType()) == 0) {

                        // Further check for the zone routing case
                        if(uniqueStoreDef.getRoutingStrategyType()
                                         .compareTo(RoutingStrategyType.ZONE_STRATEGY) == 0) {
                            boolean zonesSame = true;
                            for(int zoneId: uniqueStoreDef.getZoneReplicationFactor().keySet()) {
                                if(storeDef.getZoneReplicationFactor().get(zoneId) == null
                                   || storeDef.getZoneReplicationFactor().get(zoneId) != uniqueStoreDef.getZoneReplicationFactor()
                                                                                                       .get(zoneId)) {
                                    zonesSame = false;
                                    break;
                                }
                            }
                            if(zonesSame) {
                                sameStore = uniqueStoreDef;
                            }
                        } else {
                            sameStore = uniqueStoreDef;
                        }

                        if(sameStore != null) {
                            // Bump up the count
                            int currentCount = uniqueStoreDefs.get(sameStore);
                            uniqueStoreDefs.put(sameStore, currentCount + 1);
                            break;
                        }
                    }
                }

                if(sameStore == null) {
                    // New store
                    uniqueStoreDefs.put(storeDef, 1);
                }
            }
        }

        return uniqueStoreDefs;
    }

    /**
     * Determine whether or not a given serializedr is "AVRO" based
     *
     * @param serializerName
     * @return
     */
    public static boolean isAvroSchema(String serializerName) {
        if(serializerName.equals(AVRO_GENERIC_VERSIONED_TYPE_NAME)
           || serializerName.equals(AVRO_GENERIC_TYPE_NAME)
           || serializerName.equals(AVRO_REFLECTIVE_TYPE_NAME)
           || serializerName.equals(AVRO_SPECIFIC_TYPE_NAME)) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * If provided with an AVRO schema, validates it and checks if there are
     * backwards compatible.
     *
     * TODO should probably place some similar checks for other serializer types
     * as well?
     *
     * @param serializerDef
     */
    private static void validateIfAvroSchema(SerializerDefinition serializerDef) {
        if(serializerDef.getName().equals(AVRO_GENERIC_VERSIONED_TYPE_NAME)
           || serializerDef.getName().equals(AVRO_GENERIC_TYPE_NAME)) {
            SchemaEvolutionValidator.validateAllAvroSchemas(serializerDef);
            // check backwards compatibility if needed
            if(serializerDef.getName().equals(AVRO_GENERIC_VERSIONED_TYPE_NAME)) {
                SchemaEvolutionValidator.checkSchemaCompatibility(serializerDef);
            }
        }
    }

    /**
     * Validate store schema -- backward compatibility if it is AVRO generic
     * versioned -- sanity checks for avro in general
     *
     * @param storeDefinition the store definition to check on
     */
    public static void validateSchemaAsNeeded(StoreDefinition storeDefinition) {
        logger.info("Validating schema for store: " + storeDefinition.getName());
        SerializerDefinition keySerDef = storeDefinition.getKeySerializer();
        // validate the key schemas
        try {
            validateIfAvroSchema(keySerDef);
        } catch(Exception e) {
            logger.error("Validating key schema failed for store:  " + storeDefinition.getName());
            throw new VoldemortException("Error validating key schema for store:  "
                                         + storeDefinition.getName() + " " + e.getMessage(), e);
        }

        // validate the value schemas
        SerializerDefinition valueSerDef = storeDefinition.getValueSerializer();
        try {
            validateIfAvroSchema(valueSerDef);
        } catch(Exception e) {
            logger.error("Validating value schema failed for store:  " + storeDefinition.getName());
            throw new VoldemortException("Error validating value schema for store:  "
                                         + storeDefinition.getName() + " " + e.getMessage(), e);
        }
    }

    /**
     * Validate store schema for things like backwards compatibility,
     * parseability
     *
     * @param storeDefinitions the list of store definition to check on
     */
    public static void validateSchemasAsNeeded(Collection<StoreDefinition> storeDefinitions) {
        for(StoreDefinition storeDefinition: storeDefinitions) {
            validateSchemaAsNeeded(storeDefinition);
        }
    }

    /**
     * Ensure that new store definitions that are specified for an update do not include breaking changes to the store.
     * @param oldStoreDefs
     * @param newStoreDefs
     */
    public static void validateNewStoreDefsAreNonBreaking(List<StoreDefinition> oldStoreDefs, List<StoreDefinition> newStoreDefs){
        Map<String, StoreDefinition> oldStoreMap = new HashMap<String, StoreDefinition>();
        Map<String, StoreDefinition> newStoreMap = new HashMap<String, StoreDefinition>();
        for (StoreDefinition storeDef : oldStoreDefs){
           oldStoreMap.put(storeDef.getName(), storeDef);
        }
        for (StoreDefinition storeDef : newStoreDefs){
           newStoreMap.put(storeDef.getName(), storeDef);
        }
        for (String storeName : oldStoreMap.keySet()){
            if (newStoreMap.containsKey(storeName)){
               validateNewStoreDefIsNonBreaking(oldStoreMap.get(storeName), newStoreMap.get(storeName));
            }
        }
    }

    /**
     * Ensure that new store definitions that are specified for an update do not include breaking changes to the store.
     *
     * Non-breaking changes include changes to
     *   description
     *   preferredWrites
     *   requiredWrites
     *   preferredReads
     *   requiredReads
     *   retentionPeriodDays
     *   retentionScanThrottleRate
     *   retentionFrequencyDays
     *   viewOf
     *   zoneCountReads
     *   zoneCountWrites
     *   owners
     *   memoryFootprintMB
     *
     * non breaking changes include the serializer definition, as long as the type (name field) is unchanged for
     *   keySerializer
     *   valueSerializer
     *   transformSerializer
     *
     * @param oldStoreDef
     * @param newStoreDef
     */
    public static void validateNewStoreDefIsNonBreaking(StoreDefinition oldStoreDef, StoreDefinition newStoreDef){
        if (!oldStoreDef.getName().equals(newStoreDef.getName())){
            throw new VoldemortException("Cannot compare stores of different names: " + oldStoreDef.getName() + " and " + newStoreDef.getName());
        }
        String store = oldStoreDef.getName();
        verifySamePropertyForUpdate(oldStoreDef.getReplicationFactor(), newStoreDef.getReplicationFactor(), "ReplicationFactor", store);
        verifySamePropertyForUpdate(oldStoreDef.getType(), newStoreDef.getType(), "Type", store);

        verifySameSerializerType(oldStoreDef.getKeySerializer(), newStoreDef.getKeySerializer(), "KeySerializer", store);
        verifySameSerializerType(oldStoreDef.getValueSerializer(), newStoreDef.getValueSerializer(), "ValueSerializer", store);
        verifySameSerializerType(oldStoreDef.getTransformsSerializer(), newStoreDef.getTransformsSerializer(), "TransformSerializer", store);

        verifySamePropertyForUpdate(oldStoreDef.getRoutingPolicy(), newStoreDef.getRoutingPolicy(), "RoutingPolicy", store);
        verifySamePropertyForUpdate(oldStoreDef.getRoutingStrategyType(), newStoreDef.getRoutingStrategyType(), "RoutingStrategyType", store);
        verifySamePropertyForUpdate(oldStoreDef.getZoneReplicationFactor(), newStoreDef.getZoneReplicationFactor(), "ZoneReplicationFactor", store);
        verifySamePropertyForUpdate(oldStoreDef.getValueTransformation(), newStoreDef.getValueTransformation(), "ValueTransformation", store);
        verifySamePropertyForUpdate(oldStoreDef.getSerializerFactory(), newStoreDef.getSerializerFactory(), "SerializerFactory", store);
        verifySamePropertyForUpdate(oldStoreDef.getHintedHandoffStrategyType(), newStoreDef.getHintedHandoffStrategyType(), "HintedHandoffStrategyType", store);
        verifySamePropertyForUpdate(oldStoreDef.getHintPrefListSize(), newStoreDef.getHintPrefListSize(), "HintPrefListSize", store);
    }

    private static void verifySameSerializerType(SerializerDefinition oldSerializer, SerializerDefinition newSerializer, String property, String store){
      boolean same;
      if (oldSerializer == null && newSerializer == null){
        same = true;
      } else if (oldSerializer == null || newSerializer == null){
        same = false;
      } else {
        same = oldSerializer.getName().equals(newSerializer.getName());
      }
      if (!same){
        throw new VoldemortException("Cannot change " + property + " Type from " + oldSerializer.getName() + " to " + newSerializer.getName() + " for store " + store);
      }
    }

    private static void verifySamePropertyForUpdate(Object oldObj, Object newObj, String property, String store){
        boolean same;
        if (oldObj == null && newObj == null){
           same = true;
        } else if (oldObj == null || newObj == null){ /* not both null, so only one is null */
           same = false;
        } else {
            same = oldObj.equals(newObj);
        }
        if (! same){
            throw new VoldemortException("Cannot change " + property + " of store " + store + " from " + oldObj + " to " + newObj);
        }
    }

    public static StoreDefinitionBuilder getBuilderForStoreDef(StoreDefinition storeDef) {
        return new StoreDefinitionBuilder().setName(storeDef.getName())
                                           .setType(storeDef.getType())
                                           .setDescription(storeDef.getDescription())
                                           .setOwners(storeDef.getOwners())
                                           .setKeySerializer(storeDef.getKeySerializer())
                                           .setValueSerializer(storeDef.getValueSerializer())
                                           .setRoutingPolicy(storeDef.getRoutingPolicy())
                                           .setRoutingStrategyType(storeDef.getRoutingStrategyType())
                                           .setReplicationFactor(storeDef.getReplicationFactor())
                                           .setPreferredReads(storeDef.getPreferredReads())
                                           .setRequiredReads(storeDef.getRequiredReads())
                                           .setPreferredWrites(storeDef.getPreferredWrites())
                                           .setRequiredWrites(storeDef.getRequiredWrites())
                                           .setRetentionPeriodDays(storeDef.getRetentionDays())
                                           .setRetentionScanThrottleRate(storeDef.getRetentionScanThrottleRate())
                                           .setRetentionFrequencyDays(storeDef.getRetentionFrequencyDays())
                                           .setZoneReplicationFactor(storeDef.getZoneReplicationFactor())
                                           .setZoneCountReads(storeDef.getZoneCountReads())
                                           .setZoneCountWrites(storeDef.getZoneCountWrites())
                                           .setHintedHandoffStrategy(storeDef.getHintedHandoffStrategyType())
                                           .setHintPrefListSize(storeDef.getHintPrefListSize())
                                           .setMemoryFootprintMB(storeDef.getMemoryFootprintMB());
    }

}
