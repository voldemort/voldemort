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

import static org.junit.Assert.assertTrue;

import java.util.Map;

import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.cluster.Cluster;

public class ClusterUtilsTest {

    // TODO: Switch from numberOfZones to a set of zoneIds
    @Test
    public void testGetMapOfContiguousPartitionRunLengths() {
        int numberOfZones = 2;
        int nodesPerZone[][] = new int[][] { { 0, 1, 2 }, { 3, 4, 5 } };
        int partitionMap[][] = new int[][] { { 0, 6, 12, 16, 17 }, { 1, 7, 15 }, { 2, 8, 14 },
                { 3, 9, 13 }, { 4, 10 }, { 5, 11 } };
        Cluster cluster = ServerTestUtils.getLocalZonedCluster(numberOfZones,
                                                               nodesPerZone,
                                                               partitionMap,
                                                               ServerTestUtils.findFreePorts(6 * 3));
        Map<Integer, Integer> iiMap;
        // Zone 0:
        // 0, 1, 2, 6, 7, 8, 12, 14, 15, 16, 17
        // -------oo-------oo--oo-------------- !Wraps around!

        // => {14,7}, {6,3}, {12,1}
        iiMap = ClusterUtils.getMapOfContiguousPartitions(cluster, 0);
        assertTrue(iiMap.containsKey(6));
        assertTrue(iiMap.get(6) == 3);
        assertTrue(iiMap.containsKey(12));
        assertTrue(iiMap.get(12) == 1);
        assertTrue(iiMap.containsKey(14));
        assertTrue(iiMap.get(14) == 7);

        // => {3,1}, {1,1}, {7,1}
        iiMap = ClusterUtils.getMapOfContiguousPartitionRunLengths(cluster, 0);
        assertTrue(iiMap.containsKey(1));
        assertTrue(iiMap.get(1) == 1);
        assertTrue(iiMap.containsKey(3));
        assertTrue(iiMap.get(3) == 1);
        assertTrue(iiMap.containsKey(7));
        assertTrue(iiMap.get(7) == 1);

        // Zone 1:
        // 3, 4, 5, 9, 10, 11, 13
        // -------oo---------oo--

        // => {3,3}, {9,3}, {13,1}
        iiMap = ClusterUtils.getMapOfContiguousPartitions(cluster, 1);
        assertTrue(iiMap.containsKey(3));
        assertTrue(iiMap.get(3) == 3);
        assertTrue(iiMap.containsKey(9));
        assertTrue(iiMap.get(9) == 3);
        assertTrue(iiMap.containsKey(13));
        assertTrue(iiMap.get(13) == 1);

        // => {1,1}, {3,2}
        iiMap = ClusterUtils.getMapOfContiguousPartitionRunLengths(cluster, 1);
        assertTrue(iiMap.containsKey(1));
        assertTrue(iiMap.get(1) == 1);
        assertTrue(iiMap.containsKey(3));
        assertTrue(iiMap.get(3) == 2);
    }
}
