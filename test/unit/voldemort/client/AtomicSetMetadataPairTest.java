/*
 * Copyright 2012-2013 LinkedIn, Inc
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

package voldemort.client;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import voldemort.ClusterTestUtils;
import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.VoldemortAdminTool;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.server.VoldemortServer;
import voldemort.store.StoreDefinition;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.collect.Lists;

/*
 * This class tests that the metadata pair <cluster.xml, stores.xml> is update
 * atomically. It starts with an olderCluster, updates the metdata on each node
 * and then makes sure that the new metadata operation was successful. It does
 * so by matching the partition list before and after the metadata update. It
 * also tests for stores.xml update.
 * 
 * There are three configuration that it checks : (i) Cluster with non
 * contiguous zone/node ids (ii) Cluster with contiguous zone/node ids and (iii)
 * Cluster with a non zoned topology
 */

@RunWith(Parameterized.class)
public class AtomicSetMetadataPairTest {

    private static final String CLUSTER_KEY = "cluster.xml";
    private static final String STORES_KEY = "stores.xml";

    private static String oldStoresXmlfile = "test/common/voldemort/config/three-stores-with-zones.xml";
    private static String newStoresXmlfile = "test/common/voldemort/config/three-stores-with-zones-modified.xml";

    String[] bootStrapUrls = null;
    public static String socketUrl = "";
    private VoldemortServer[] servers;

    private Cluster oldCluster;
    private Cluster newCluster;
    private List<Integer> oldPartitionIds;
    private List<Integer> newPartitionIds;

    public AtomicSetMetadataPairTest(Cluster oldCluster,
                                     Cluster newCluster,
                                     List<Integer> oldPartitionIds,
                                     List<Integer> newPartitionIds) {
        this.oldCluster = oldCluster;
        this.newCluster = newCluster;
        this.oldPartitionIds = oldPartitionIds;
        this.newPartitionIds = newPartitionIds;

    }

    @Parameterized.Parameters
    public static Collection<Object[]> configs() {

        int originalPartitionMap[][] = { { 0, 1 }, { 2, 3 }, { 4, 5 }, { 6, 7 }, { 8, 9 },
                { 10, 11 } };
        int swappedPartitionMap[][] = { { 0, 1 }, { 2, 3 }, { 4, 5 }, { 6, 7 }, { 8, 10 },
                { 9, 11 } };

        return Arrays.asList(new Object[][] {
                {
                        ClusterTestUtils.getZ1Z3Z5ClusterWithNonContiguousNodeIds(),
                        ClusterTestUtils.getZ1Z3Z5ClusterWithNonContiguousNodeIdsWithSwappedPartitions(),
                        Lists.newArrayList(2, 11, 7), Lists.newArrayList(2, 11, 23) },
                { ClusterTestUtils.getZZZCluster(),
                        ClusterTestUtils.getZZZClusterWithSwappedPartitions(),
                        Lists.newArrayList(5, 14), Lists.newArrayList(14) },
                { ServerTestUtils.getLocalCluster(6, originalPartitionMap),
                        ServerTestUtils.getLocalCluster(6, swappedPartitionMap),
                        Lists.newArrayList(10, 11), Lists.newArrayList(9, 11) } });
    }

    @Before
    public void setUp() throws Exception {

        servers = new VoldemortServer[oldCluster.getNodes().size()];
        oldCluster = ServerTestUtils.startVoldemortCluster(servers,
                                                           null,
                                                           null,
                                                           oldStoresXmlfile,
                                                           new Properties(),
                                                           oldCluster);

        socketUrl = servers[0].getIdentityNode().getSocketUrl().toString();
        bootStrapUrls = new String[1];
        bootStrapUrls[0] = socketUrl;

    }

    @After
    public void tearDown() throws Exception {
        for(VoldemortServer server: servers) {
            ServerTestUtils.stopVoldemortServer(server);
        }
    }

    /**
     * Bug fix: The old approach tried to test store metadata update by
     * replacing an existing stores.xml with a completely different stores.xml.
     * This has been fixed such that, the new stores.xml is the same as the
     * original, except for required replication factor = 2.
     * 
     */
    @Test
    public void testClusterAndStoresAreSetAtomically() {
        try {

            AdminClient adminClient = new AdminClient(bootStrapUrls[0],
                                                      new AdminClientConfig(),
                                                      new ClientConfig());

            StoreDefinitionsMapper storeDefsMapper = new StoreDefinitionsMapper();
            List<StoreDefinition> storeDefs = storeDefsMapper.readStoreList(new File(newStoresXmlfile));
            ClusterMapper clusterMapper = new ClusterMapper();

            for(Node node: oldCluster.getNodes()) {
                VoldemortAdminTool.executeSetMetadataPair(node.getId(),
                                                          adminClient,
                                                          CLUSTER_KEY,
                                                          clusterMapper.writeCluster(newCluster),
                                                          STORES_KEY,
                                                          storeDefsMapper.writeStoreList(storeDefs));
            }

            String dirPath = TestUtils.createTempDir().getAbsolutePath();

            for(Node node: newCluster.getNodes()) {

                VoldemortAdminTool.executeGetMetadata(node.getId(),
                                                      adminClient,
                                                      CLUSTER_KEY,
                                                      dirPath);

                // Make sure cluster metadata was updated
                Cluster newClusterFromMetadataRepo = clusterMapper.readCluster(new File(dirPath,
                                                                                        CLUSTER_KEY
                                                                                                + "_"
                                                                                                + node.getId()));
                // All nodes should have this old list
                assertTrue(oldCluster.getNodeById(5).getPartitionIds().equals(oldPartitionIds));

                // As per the new metadata node 5 should have this list
                assertTrue(newClusterFromMetadataRepo.getNodeById(5)
                                                     .getPartitionIds()
                                                     .equals(newPartitionIds));

                // Make sure store metadata was updated
                VoldemortAdminTool.executeGetMetadata(node.getId(),
                                                      adminClient,
                                                      STORES_KEY,
                                                      dirPath);
                List<StoreDefinition> newStoreDefsFromMetadatRepo = storeDefsMapper.readStoreList(new File(dirPath,
                                                                                                           STORES_KEY
                                                                                                                   + "_"
                                                                                                                   + node.getId()));

                // Check that the required replication factor has been updated
                assertTrue(newStoreDefsFromMetadatRepo.get(1).getRequiredReads() == 2);
                assertTrue(newStoreDefsFromMetadatRepo.get(1).getRequiredWrites() == 2);

            }
        } catch(Exception e) {
            fail("Error in validating end to end client rebootstrap : " + e);
        }
    }
}
