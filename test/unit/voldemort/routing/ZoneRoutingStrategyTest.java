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

package voldemort.routing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import junit.framework.TestCase;
import voldemort.ClusterTestUtils;
import voldemort.ServerTestUtils;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;

public class ZoneRoutingStrategyTest extends TestCase {

    private ZoneRoutingStrategy getRouter(int... zonesRepFactor) {
        int totalZoneRepFactor = 0;
        for(int i = 0; i < zonesRepFactor.length; i++) {
            totalZoneRepFactor += zonesRepFactor[i];
        }
        return new ZoneRoutingStrategy(getTestCluster(),
                                       getTestZoneReplicationFactor(zonesRepFactor),
                                       totalZoneRepFactor);
    }

    private HashMap<Integer, Integer> getTestZoneReplicationFactor(int... zonesRepFactor) {
        HashMap<Integer, Integer> returnHashMap = new HashMap<Integer, Integer>();
        int zoneId = 0;
        for(int zoneRepFactor: zonesRepFactor) {
            returnHashMap.put(zoneId++, zoneRepFactor);
        }
        return returnHashMap;
    }

    private Cluster getTestCluster() {
        int numberOfNodes = 13;
        int numberOfZones = 4;
        int[] nodeToZoneMapping = new int[] { 0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 3, 3 };
        int[][] partitionMap = new int[][] { { 0, 1, 2 }, { 3, 4, 5 }, { 6, 7 }, { 8, 9, 10 },
                { 11, 12, 13 }, { 14, 15 }, { 16, 17, 18 }, { 19, 20, 21 }, { 22, 23, 24, 25 },
                { 26, 27, 28 }, { 29, 30 }, { 31 }, { 32 } };

        int ports[] = new int [3*numberOfNodes];
        for (int i=0; i<numberOfNodes; i++) {
            ports[i*3] = 8080;
            ports[i*3+1] = 6666;
            ports[i*3+2] = 6667;

        }

        return ServerTestUtils.getLocalZonedCluster(numberOfNodes,
                                                    numberOfZones,
                                                    nodeToZoneMapping,
                                                    partitionMap,
                                                    ports);

    }
    
    private void assertReplicationPartitions(List<Integer> partitions, int... expected) {
        assertEquals("Router produced unexpected number of replication partitions.",
                     expected.length,
                     partitions.size());
        for(int i = 0; i < partitions.size(); i++)
            assertEquals("Replication partitions should match",
                         new Integer(expected[i]),
                         partitions.get(i));
    }

    public void testReplication() {
        assertReplicationPartitions(getRouter(1).getReplicatingPartitionList(0), 0);
        assertReplicationPartitions(getRouter(1, 1).getReplicatingPartitionList(0), 0, 11);
        assertReplicationPartitions(getRouter(1, 1, 1).getReplicatingPartitionList(0), 0, 11, 22);
        assertReplicationPartitions(getRouter(1, 1, 1, 1).getReplicatingPartitionList(0),
                                    0,
                                    11,
                                    22,
                                    31);
        // Should ignore the last zone
        assertReplicationPartitions(getRouter(1, 1, 1, 1, 1).getReplicatingPartitionList(0),
                                    0,
                                    11,
                                    22,
                                    31);
        assertReplicationPartitions(getRouter(2).getReplicatingPartitionList(0), 0, 3);
        assertReplicationPartitions(getRouter(2, 1).getReplicatingPartitionList(0), 0, 3, 11);
        assertReplicationPartitions(getRouter(2, 1, 1).getReplicatingPartitionList(0), 0, 3, 11, 22);
        assertReplicationPartitions(getRouter(2, 1, 1, 1).getReplicatingPartitionList(0),
                                    0,
                                    3,
                                    11,
                                    22,
                                    31);
        assertReplicationPartitions(getRouter(2, 2).getReplicatingPartitionList(0), 0, 3, 11, 14);
        assertReplicationPartitions(getRouter(2, 2, 1).getReplicatingPartitionList(0),
                                    0,
                                    3,
                                    11,
                                    14,
                                    22);
        assertReplicationPartitions(getRouter(2, 2, 1, 2).getReplicatingPartitionList(0),
                                    0,
                                    3,
                                    11,
                                    14,
                                    22,
                                    31,
                                    32);
        // Should not fail
        assertReplicationPartitions(getRouter(2, 2, 1, 3).getReplicatingPartitionList(0),
                                    0,
                                    3,
                                    11,
                                    14,
                                    22,
                                    31,
                                    32);
    }
    
    private ZoneRoutingStrategy getRouterForNonContiguousZones(List<Integer> zoneIds, int... zonesRepFactor) {
        int totalZoneRepFactor = 0;
        for(int i = 0; i < zonesRepFactor.length; i++) {
            totalZoneRepFactor += zonesRepFactor[i];
        }
        return new ZoneRoutingStrategy(getTestClusterWithNonContiguousZones(),
                                       getTestZoneReplicationFactorWithNonContiguousZones(zoneIds, zonesRepFactor),
                                       totalZoneRepFactor);
    }

    private HashMap<Integer, Integer> getTestZoneReplicationFactorWithNonContiguousZones(List<Integer> zoneIds,
                                                                                         int... zonesRepFactor) {
        HashMap<Integer, Integer> returnHashMap = new HashMap<Integer, Integer>();
        int index = 0;
        for(int zoneId: zoneIds) {
            returnHashMap.put(zoneId, zonesRepFactor[index]);
            index++;
        }
        return returnHashMap;
    }

    private Cluster getTestClusterWithNonContiguousZones() {
        return ClusterTestUtils.getZ1Z3Z5Z10ClusterWithNonContiguousNodeIds();
    }

    
    public void testReplicationWithNonContiguousZonesAndNodes() {
        ArrayList<Integer> zoneIds = new ArrayList<Integer>();
        
        zoneIds.add(1);
        assertReplicationPartitions(getRouterForNonContiguousZones(zoneIds, 1)
                                                          .getReplicatingPartitionList(0), 0);
                                                          
        zoneIds.add(3);
        assertReplicationPartitions(getRouterForNonContiguousZones(zoneIds, 1, 1)
                                                          .getReplicatingPartitionList(0), 0, 4);
        
        zoneIds.add(5);
        assertReplicationPartitions(getRouterForNonContiguousZones(zoneIds, 1, 1, 1)
                                                          .getReplicatingPartitionList(0), 0, 4, 22);
        
        zoneIds.add(10);
        assertReplicationPartitions(getRouterForNonContiguousZones(zoneIds, 1, 1, 1, 1)
                                    .getReplicatingPartitionList(0), 0, 4, 22, 29);
       
        zoneIds.clear();
        
        zoneIds.add(1);
        assertReplicationPartitions(getRouterForNonContiguousZones(zoneIds, 2)
                                    .getReplicatingPartitionList(0), 0, 1);
        
        zoneIds.add(3);
        assertReplicationPartitions(getRouterForNonContiguousZones(zoneIds, 2, 1)
                                    .getReplicatingPartitionList(0), 0, 1, 4);
        
        zoneIds.add(5);
        assertReplicationPartitions(getRouterForNonContiguousZones(zoneIds, 2, 1, 1)
                                    .getReplicatingPartitionList(0), 0, 1, 4, 22);
        
        zoneIds.add(10);
        assertReplicationPartitions(getRouterForNonContiguousZones(zoneIds, 2, 1, 1, 1)
                                    .getReplicatingPartitionList(0), 0, 1, 4, 22, 29);
        
        zoneIds.clear();
        
        zoneIds.add(1);
        zoneIds.add(3);
        assertReplicationPartitions(getRouterForNonContiguousZones(zoneIds, 2, 2)
                                    .getReplicatingPartitionList(0), 0, 1, 4, 5);
        
        zoneIds.add(5);
        assertReplicationPartitions(getRouterForNonContiguousZones(zoneIds, 2, 2, 1)
                                    .getReplicatingPartitionList(0), 0, 1, 4, 5, 22); 
        
        zoneIds.add(10);
        assertReplicationPartitions(getRouterForNonContiguousZones(zoneIds, 2, 2, 1, 2)
                                    .getReplicatingPartitionList(0),
                                    0, 1, 4, 5, 22, 29, 31);
        // Should not fail
        assertReplicationPartitions(getRouterForNonContiguousZones(zoneIds, 2, 2, 1, 3)
                                    .getReplicatingPartitionList(0),
                                    0, 1, 4, 5, 22, 29, 31);
    }

}
