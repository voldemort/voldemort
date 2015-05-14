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

package voldemort.performance;


import java.util.ArrayList;
import java.util.HashMap;

import voldemort.ServerTestUtils;
import voldemort.client.RoutingTier;
import voldemort.cluster.Cluster;
import voldemort.routing.BaseStoreRoutingPlan;
import voldemort.routing.RoutingStrategyType;
import voldemort.routing.StoreRoutingPlan;
import voldemort.serialization.SerializerDefinition;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreDefinitionBuilder;
import voldemort.store.bdb.BdbStorageConfiguration;
import voldemort.tools.PartitionBalance;

public class StoreRoutingPlanPerf {

    Cluster cluster;
    StoreDefinition storeDefinition;

    // Construct zone-appropriate 3/2/2 store def.
    public StoreDefinition getStoreDef(int numZones) {
        HashMap<Integer, Integer> zoneRep = new HashMap<Integer, Integer>();
        for(int zoneId = 0; zoneId < numZones; zoneId++) {
            zoneRep.put(zoneId, 3);
        }
        int repFactor = numZones * 3;
        StoreDefinition storeDef = new StoreDefinitionBuilder().setName("ZZ322")
                                                                  .setType(BdbStorageConfiguration.TYPE_NAME)
                                                                  .setRoutingPolicy(RoutingTier.CLIENT)
                                                                  .setRoutingStrategyType(RoutingStrategyType.ZONE_STRATEGY)
                                                                  .setKeySerializer(new SerializerDefinition("string"))
                                                                  .setValueSerializer(new SerializerDefinition("string"))
                                                                  .setReplicationFactor(repFactor)
                                                                  .setZoneReplicationFactor(zoneRep)
                                                                  .setRequiredReads(2)
                                                                  .setRequiredWrites(2)
                                                                  .setZoneCountReads(0)
                                                                  .setZoneCountWrites(0)
                                                                  .build();
        return storeDef;
    }

    public Cluster getCluster(int numZones, int numNodes, int numPartitions) {
        if (numPartitions % numNodes != 0) {
            System.err.println("numPartitions does not evenly divide by numNodes: " + numPartitions + " / " + numNodes);
            System.exit(1);
        }
        if (numNodes % numZones != 0) {
            System.err.println("numNodes does not evenly divide by numZones: " + numNodes + " / " + numZones);
            System.exit(1);
        }

        int[] nodeToZoneMapping = new int[numNodes];
        for(int i = 0; i < numNodes; ++i) {
            nodeToZoneMapping[i] = i % numZones;
        }

        int[][] partitionMapping = new int[numNodes][];
        for(int i = 0; i < numPartitions; ++i) {
            int nodeId = i % numNodes;
            partitionMapping[nodeId] = new int[numPartitions/numNodes];
        }
        for(int i = 0; i < numPartitions; ++i) {
            int nodeId = i % numNodes;
            int partitionOffset = i / numNodes;
            partitionMapping[nodeId][partitionOffset] = i;
        }

        int[] dummyPorts = new int[numNodes * 3];
        for(int i = 0; i < numNodes; i++) {
            dummyPorts[i * 3] = 1000;
            dummyPorts[i * 3 + 1] = 2000;
            dummyPorts[i * 3 + 2] = 3000;
        }

        return ServerTestUtils.getLocalZonedCluster(numNodes,
                                                    numZones,
                                                    nodeToZoneMapping,
                                                    partitionMapping,
                                                    dummyPorts);
    }

    StoreRoutingPlanPerf() {
        cluster = null;
        storeDefinition = null;
    }

    /**
     * Repeatedly constructs StoreRoutingPlan objects.
     * 
     * @param count of constructions to do.
     * @return ns to complete count constructions
     */
    public long perfStoreRoutingPlan(int count) {
        long startNs = System.nanoTime();
        for(int i = 0; i < count; ++i) {
            new StoreRoutingPlan(cluster, storeDefinition);
        }

        return System.nanoTime() - startNs;
    }

    /**
     * Repeatedly constructs BaseStoreRoutingPlan objects.
     * 
     * @param count of constructions to do.
     * @return ns to complete count constructions
     */
    public long perfBaseStoreRoutingPlan(int count) {
        long startNs = System.nanoTime();
        for(int i = 0; i < count; ++i) {
            new BaseStoreRoutingPlan(cluster, storeDefinition);
        }

        return System.nanoTime() - startNs;
    }

    /**
     * Repeatedly constructs PartitionBalanceobjects.
     * 
     * @param count of constructions to do.
     * @return ns to complete count constructions
     */
    public long perfPartitionBalance(int count) {
        ArrayList<StoreDefinition> storeDefs = new ArrayList<StoreDefinition>();
        storeDefs.add(storeDefinition);

        long startNs = System.nanoTime();
        for(int i = 0; i < count; ++i) {
            new PartitionBalance(cluster, storeDefs);
        }

        return System.nanoTime() - startNs;
    }

    public void perfTest(int numZones, int numNodes, int numPartitions) {
        cluster = getCluster(numZones, numNodes, numPartitions);
        // System.out.println(cluster);
        storeDefinition = getStoreDef(numZones);
        // System.out.println(storeDefinition);

        long baseNs = perfBaseStoreRoutingPlan(1000);
        long storeNs = perfStoreRoutingPlan(1000);
        long pbNs = perfPartitionBalance(10);
        
        System.out.println("Zones (" + numZones + ") / Nodes (" + numNodes + ") / Partitions ("
                       + numPartitions + "):");
        System.out.println("BaseRP: " + baseNs + " ns (1000x)");
        System.out.println("StoreRP: " + storeNs + " ns (1000x)");
        System.out.println("PartitionBalance: " + pbNs + " ns (10x)");

    }


    public static void main(String[] args) throws Exception {
        StoreRoutingPlanPerf srpp = new StoreRoutingPlanPerf();

        srpp.perfTest(2, 10, 100);
        System.out.println("Ignore the above result. Doing one throw away perf test to prime java foo.");

        srpp.perfTest(2, 10, 100);
        srpp.perfTest(2, 10, 500);
        srpp.perfTest(2, 10, 2500);
        srpp.perfTest(2, 50, 500);
        srpp.perfTest(2, 50, 2500);
        srpp.perfTest(2, 250, 2500);
        srpp.perfTest(5, 50, 500);
        srpp.perfTest(5, 50, 2500);
        srpp.perfTest(5, 250, 2500);
    }
}
