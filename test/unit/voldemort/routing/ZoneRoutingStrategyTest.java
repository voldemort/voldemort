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
import voldemort.cluster.Node;

import com.google.common.collect.ImmutableList;

public class ZoneRoutingStrategyTest extends TestCase {

    private final byte[] key = new byte[0];

    private ZoneRoutingStrategy getRouter(int... zonesRepFactor) {
        int totalZoneRepFactor = 0;
        for(int i = 0; i < zonesRepFactor.length; i++) {
            totalZoneRepFactor += zonesRepFactor[i];
        }
        return new ZoneRoutingStrategy(getTestNodes(),
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

    private List<Node> getTestNodes() {
        return ImmutableList.of(node(0, 0, 0, 1, 2),
                                node(1, 0, 3, 4, 5),
                                node(2, 0, 6, 7),
                                node(3, 0, 8, 9, 10),
                                node(4, 1, 11, 12, 13),
                                node(5, 1, 14, 15),
                                node(6, 1, 16, 17, 18),
                                node(7, 1, 19, 20, 21),
                                node(8, 2, 22, 23, 24, 25),
                                node(9, 2, 26, 27, 28),
                                node(10, 2, 29, 30),
                                node(11, 3, 31),
                                node(12, 3, 32));

    }

    private Node node(int id, int zoneId, int... tags) {
        List<Integer> list = new ArrayList<Integer>(tags.length);
        for(int tag: tags)
            list.add(tag);
        return new Node(id, "localhost", 8080, 6666, 6667, zoneId, list);
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

}
