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

package voldemort.tools;

import static org.junit.Assert.assertTrue;

import org.apache.log4j.Logger;
import org.junit.Test;

import voldemort.ClusterTestUtils;
import voldemort.VoldemortException;

/**
 * This test focuses on constructing PartitionBalances. This exercises all of
 * the partition/replicaType code paths.
 * 
 * The positive test cases cover expected configurations:
 * <ul>
 * <li>2 or 3 zones
 * <li>0 or 1 zones without partitions
 * <li>one or more zones with new nodes ('new' meaning no partitions on that
 * node)
 * </ul>
 * 
 * The "negative test cases" cover:
 * <ul>
 * <li>Store definition mis-match with cluster in terms of number of zones.
 * <li>Insufficient nodes in new zone to reach desired replication level.
 * </ul>
 * 
 */
public class PartitionBalanceTest {
    
    private static final Logger logger = Logger.getLogger(PartitionBalanceTest.class.getName());
    
    @Test
    public void testBasicThingsThatShouldWork() {
        PartitionBalance pb = new PartitionBalance(ClusterTestUtils.getZZCluster(),
                                                   ClusterTestUtils.getZZStoreDefsInMemory());
        // Print out results so there is a test case that demonstrates toString
        // method output for 2 zones
        System.out.println(pb);

        pb = new PartitionBalance(ClusterTestUtils.getZZZCluster(),
                                  ClusterTestUtils.getZZZStoreDefsInMemory());
        // Print out results so there is a test case that demonstrates toString
        // method output for 3 zones
        System.out.println(pb);
    }

    @Test
    public void testEmptyZoneThingsThatShouldWork() {
        new PartitionBalance(ClusterTestUtils.getZECluster(),
                             ClusterTestUtils.getZZStoreDefsInMemory());

        new PartitionBalance(ClusterTestUtils.getZEZCluster(),
                             ClusterTestUtils.getZZZStoreDefsInMemory());
    }

    @Test
    public void testNewNodeThingsThatShouldWork() {
        new PartitionBalance(ClusterTestUtils.getZZClusterWithNN(),
                             ClusterTestUtils.getZZStoreDefsInMemory());

        new PartitionBalance(ClusterTestUtils.getZEZClusterWithXNN(),
                             ClusterTestUtils.getZZZStoreDefsInMemory());
    }

    @Test
    public void testClusterStoreZoneCountMismatch() {
        boolean veCaught;

        veCaught = false;
        try {
            new PartitionBalance(ClusterTestUtils.getZZCluster(),
                                 ClusterTestUtils.getZZZStoreDefsInMemory());
        } catch(VoldemortException ve) {
            veCaught = true;
        }
        assertTrue(veCaught);

        veCaught = false;
        try {
            new PartitionBalance(ClusterTestUtils.getZZZCluster(),
                                 ClusterTestUtils.getZZStoreDefsInMemory());
        } catch(VoldemortException ve) {
            veCaught = true;
        }
        assertTrue(veCaught);
    }

    @Test
    public void testClusterWithZoneThatCannotFullyReplicate() {
        boolean veCaught = false;
        try {
            new PartitionBalance(ClusterTestUtils.getZZZClusterWithOnlyOneNodeInNewZone(),
                                 ClusterTestUtils.getZZZStoreDefsInMemory());
        } catch(VoldemortException ve) {
            veCaught = true;
        }
        assertTrue(veCaught);
    }
    
    @Test
    public void testBasicThingsWithNonContiguousZones() {
        PartitionBalance pb = new PartitionBalance(ClusterTestUtils.getZ1Z3ClusterWithNonContiguousNodeIds(),
                                                   ClusterTestUtils.getZ1Z3StoreDefsInMemory());
        // Print out results so there is a test case that demonstrates toString
        // method output for 2 non contiguous zones
        logger.info("Partiton balance for 2 zones" + pb);

        pb = new PartitionBalance(ClusterTestUtils.getZ1Z3Z5ClusterWithNonContiguousNodeIds(),
                                  ClusterTestUtils.getZ1Z3Z5StoreDefsInMemory());
        // Print out results so there is a test case that demonstrates toString
        // method output for 3 contiguous zones
        logger.info("Partiton balance for 3 zones" + pb);
    }
    


    @Test
    public void testClusterStoreZoneCountMismatchWithNonContiguousZone() {
        boolean veCaught = false;
        try {
            new PartitionBalance(ClusterTestUtils.getZ1Z3ClusterWithNonContiguousNodeIds(),
                                 ClusterTestUtils.getZ1Z3Z5StoreDefsInMemory());
        } catch(VoldemortException ve) {
            veCaught = true;
        }
        assertTrue(veCaught);
        veCaught = false;
        try {
            new PartitionBalance(ClusterTestUtils.getZ1Z3Z5ClusterWithNonContiguousNodeIds(),
                                 ClusterTestUtils.getZ1Z3StoreDefsInMemory());
        } catch(VoldemortException ve) {
            veCaught = true;
        }
        assertTrue(veCaught);
    }

    @Test
    public void testClusterWithZoneThatCannotFullyReplicateWithNonContiguousZone() {
        boolean veCaught = false;
        try {
            new PartitionBalance(ClusterTestUtils.getZ1Z3Z5ClusterWithOnlyOneNodeInNewZone(),
                                 ClusterTestUtils.getZ1Z3Z5StoreDefsInMemory());
        } catch(VoldemortException ve) {
            veCaught = true;
        }
        assertTrue(veCaught);
    }
    

    @Test
    public void testNewNodeThingsThatShouldWorkWithNonContiguousZone() {
        new PartitionBalance(ClusterTestUtils.getZ1Z3ClusterWithNonContiguousNodeIdsWithNN(),
                             ClusterTestUtils.getZ1Z3StoreDefsInMemory());
      
    }

    /**
     * Confirm that zone Ids need not be contiguous with contiguous node ids.
     */
    @Test
    public void testNonContiguousZoneIdsWithContiguousNodeIDs() {
        new PartitionBalance(ClusterTestUtils.getZ0Z2ClusterWithContiguousNodeIDs(),
                             ClusterTestUtils.getZ0Z2322StoreDefs("InMemoryStorageConfiguration.TYPE_NAME"));
    }

    /**
     * Confirm that zone Ids and node ids need not be contiguous. 
     */

    @Test
    public void testNonContiguousZoneIdsWithNonContiguousNodeIDs() {
        new PartitionBalance(ClusterTestUtils.getZ1Z3ClusterWithNonContiguousNodeIds(),
                             ClusterTestUtils.getZ1Z3322StoreDefs("InMemoryStorageConfiguration.TYPE_NAME"));

    }

}
