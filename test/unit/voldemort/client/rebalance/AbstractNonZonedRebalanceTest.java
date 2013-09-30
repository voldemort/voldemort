/*
 * Copyright 2008-2013 LinkedIn, Inc
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

package voldemort.client.rebalance;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import voldemort.ClusterTestUtils;
import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.client.ClientConfig;
import voldemort.client.DefaultStoreClient;
import voldemort.client.RoutingTier;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.BaseStoreRoutingPlan;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.routing.RoutingStrategyType;
import voldemort.serialization.SerializerDefinition;
import voldemort.serialization.json.JsonReader;
import voldemort.server.VoldemortServer;
import voldemort.store.InvalidMetadataException;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreDefinitionBuilder;
import voldemort.store.UnreachableStoreException;
import voldemort.store.bdb.BdbStorageConfiguration;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.metadata.MetadataStore.VoldemortState;
import voldemort.store.readonly.JsonStoreBuilder;
import voldemort.store.readonly.ReadOnlyStorageConfiguration;
import voldemort.store.readonly.ReadOnlyStorageEngineTestInstance;
import voldemort.store.readonly.ReadOnlyStorageFormat;
import voldemort.store.readonly.swapper.AdminStoreSwapper;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.UpdateClusterUtils;
import voldemort.versioning.ClockEntry;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.collect.Lists;

public abstract class AbstractNonZonedRebalanceTest extends AbstractRebalanceTest {

    private static final Logger logger = Logger.getLogger(AbstractNonZonedRebalanceTest.class.getName());

    protected static int NUM_RO_CHUNKS_PER_BUCKET = 10;
    protected static String testStoreNameRW = "test";
    protected static String testStoreNameRW2 = "test2";
    protected static String testStoreNameRO = "test-ro";

    protected static String storeDefFileWithoutReplication;
    protected static String storeDefFileWithReplication;
    protected static String roStoreDefFileWithReplication;
    protected static String rwStoreDefFileWithReplication;
    protected static String rwTwoStoreDefFileWithReplication;

    private List<StoreDefinition> storeDefWithoutReplication;
    private List<StoreDefinition> storeDefWithReplication;
    private StoreDefinition roStoreDefWithoutReplication;
    private StoreDefinition rwStoreDefWithoutReplication;
    private StoreDefinition roStoreDefWithReplication;
    private StoreDefinition rwStoreDefWithReplication;
    private StoreDefinition rwStoreDefWithReplication2;

    public AbstractNonZonedRebalanceTest(boolean useNio) {
        super(useNio);
    }

    @Before
    public void setUp() throws IOException {
        // First without replication
        roStoreDefWithoutReplication = new StoreDefinitionBuilder().setName(testStoreNameRO)
                                                                   .setType(ReadOnlyStorageConfiguration.TYPE_NAME)
                                                                   .setKeySerializer(new SerializerDefinition("string"))
                                                                   .setValueSerializer(new SerializerDefinition("string"))
                                                                   .setRoutingPolicy(RoutingTier.CLIENT)
                                                                   .setRoutingStrategyType(RoutingStrategyType.CONSISTENT_STRATEGY)
                                                                   .setReplicationFactor(1)
                                                                   .setPreferredReads(1)
                                                                   .setRequiredReads(1)
                                                                   .setPreferredWrites(1)
                                                                   .setRequiredWrites(1)
                                                                   .build();
        rwStoreDefWithoutReplication = new StoreDefinitionBuilder().setName(testStoreNameRW)
                                                                   .setType(BdbStorageConfiguration.TYPE_NAME)
                                                                   .setKeySerializer(new SerializerDefinition("string"))
                                                                   .setValueSerializer(new SerializerDefinition("string"))
                                                                   .setRoutingPolicy(RoutingTier.CLIENT)
                                                                   .setRoutingStrategyType(RoutingStrategyType.CONSISTENT_STRATEGY)
                                                                   .setReplicationFactor(1)
                                                                   .setPreferredReads(1)
                                                                   .setRequiredReads(1)
                                                                   .setPreferredWrites(1)
                                                                   .setRequiredWrites(1)
                                                                   .build();

        storeDefWithoutReplication = Lists.newArrayList(roStoreDefWithoutReplication,
                                                        rwStoreDefWithoutReplication);
        String storeDefWithoutReplicationString = new StoreDefinitionsMapper().writeStoreList(storeDefWithoutReplication);
        File file = File.createTempFile("two-stores-", ".xml");
        FileUtils.writeStringToFile(file, storeDefWithoutReplicationString);
        storeDefFileWithoutReplication = file.getAbsolutePath();

        // Now with replication

        roStoreDefWithReplication = new StoreDefinitionBuilder().setName(testStoreNameRO)
                                                                .setType(ReadOnlyStorageConfiguration.TYPE_NAME)
                                                                .setKeySerializer(new SerializerDefinition("string"))
                                                                .setValueSerializer(new SerializerDefinition("string"))
                                                                .setRoutingPolicy(RoutingTier.CLIENT)
                                                                .setRoutingStrategyType(RoutingStrategyType.CONSISTENT_STRATEGY)
                                                                .setReplicationFactor(2)
                                                                .setPreferredReads(1)
                                                                .setRequiredReads(1)
                                                                .setPreferredWrites(1)
                                                                .setRequiredWrites(1)
                                                                .build();
        file = File.createTempFile("ro-stores-", ".xml");
        FileUtils.writeStringToFile(file,
                                    new StoreDefinitionsMapper().writeStoreList(Lists.newArrayList(roStoreDefWithReplication)));
        roStoreDefFileWithReplication = file.getAbsolutePath();

        rwStoreDefWithReplication = new StoreDefinitionBuilder().setName(testStoreNameRW)
                                                                .setType(BdbStorageConfiguration.TYPE_NAME)
                                                                .setKeySerializer(new SerializerDefinition("string"))
                                                                .setValueSerializer(new SerializerDefinition("string"))
                                                                .setRoutingPolicy(RoutingTier.CLIENT)
                                                                .setRoutingStrategyType(RoutingStrategyType.CONSISTENT_STRATEGY)
                                                                .setReplicationFactor(2)
                                                                .setPreferredReads(1)
                                                                .setRequiredReads(1)
                                                                .setPreferredWrites(1)
                                                                .setRequiredWrites(1)
                                                                .build();
        rwStoreDefWithReplication2 = new StoreDefinitionBuilder().setName(testStoreNameRW2)
                                                                 .setType(BdbStorageConfiguration.TYPE_NAME)
                                                                 .setKeySerializer(new SerializerDefinition("string"))
                                                                 .setValueSerializer(new SerializerDefinition("string"))
                                                                 .setRoutingPolicy(RoutingTier.CLIENT)
                                                                 .setRoutingStrategyType(RoutingStrategyType.CONSISTENT_STRATEGY)
                                                                 .setReplicationFactor(2)
                                                                 .setPreferredReads(1)
                                                                 .setRequiredReads(1)
                                                                 .setPreferredWrites(1)
                                                                 .setRequiredWrites(1)
                                                                 .build();

        file = File.createTempFile("rw-stores-", ".xml");
        FileUtils.writeStringToFile(file,
                                    new StoreDefinitionsMapper().writeStoreList(Lists.newArrayList(rwStoreDefWithReplication)));
        rwStoreDefFileWithReplication = file.getAbsolutePath();

        file = File.createTempFile("rw-two-stores-", ".xml");
        FileUtils.writeStringToFile(file,
                                    new StoreDefinitionsMapper().writeStoreList(Lists.newArrayList(rwStoreDefWithReplication,
                                                                                                   rwStoreDefWithReplication2)));
        rwTwoStoreDefFileWithReplication = file.getAbsolutePath();

        storeDefWithReplication = Lists.newArrayList(roStoreDefWithReplication,
                                                     rwStoreDefWithReplication);
        String storeDefWithReplicationString = new StoreDefinitionsMapper().writeStoreList(storeDefWithReplication);
        file = File.createTempFile("two-stores-", ".xml");
        FileUtils.writeStringToFile(file, storeDefWithReplicationString);
        storeDefFileWithReplication = file.getAbsolutePath();
    }

    @After
    public void tearDown() {
        testEntries.clear();
        testEntries = null;
        socketStoreFactory.close();
        socketStoreFactory = null;
        ClusterTestUtils.reset();
    }

    @Test(timeout = 600000)
    public void testRORWRebalance() throws Exception {
        logger.info("Starting testRORWRebalance");
        try {
            Cluster currentCluster = ServerTestUtils.getLocalCluster(2, new int[][] {
                    { 0, 1, 2, 3, 4, 5, 6, 7, 8 }, {} });

            Cluster finalCluster = UpdateClusterUtils.createUpdatedCluster(currentCluster,
                                                                       1,
                                                                       Lists.newArrayList(2, 3));

            // start servers 0 , 1 only
            List<Integer> serverList = Arrays.asList(0, 1);
            Map<String, String> configProps = new HashMap<String, String>();
            configProps.put("admin.max.threads", "50");
            currentCluster = startServers(currentCluster,
                                          storeDefFileWithoutReplication,
                                          serverList,
                                          configProps);

            String bootstrapUrl = getBootstrapUrl(currentCluster, 0);
            
            final ClusterTestUtils.RebalanceKit rebalanceKit = ClusterTestUtils.getRebalanceKit(bootstrapUrl,
                                                                                                finalCluster);

            try {

                // Populate the two stores
                populateData(currentCluster,
                             roStoreDefWithoutReplication,
                             rebalanceKit.controller.getAdminClient(),
                             true);

                populateData(currentCluster,
                             rwStoreDefWithoutReplication,
                             rebalanceKit.controller.getAdminClient(),
                             false);

                rebalanceAndCheck(rebalanceKit.plan, rebalanceKit.controller, Arrays.asList(1));

                checkConsistentMetadata(finalCluster, serverList);
            } finally {
                // stop servers
                stopServer(serverList);
            }
        } catch(AssertionError ae) {
            logger.error("Assertion broken in testRORWRebalance ", ae);
            throw ae;
        }
    }

    @Test(timeout = 600000)
    public void testRORWRebalanceWithReplication() throws Exception {
        logger.info("Starting testRORWRebalanceWithReplication");
        try {
            Cluster currentCluster = ServerTestUtils.getLocalCluster(2, new int[][] {
                    { 0, 1, 2, 3, 4, 5, 6 }, { 7, 8 } });

            Cluster finalCluster = UpdateClusterUtils.createUpdatedCluster(currentCluster,
                                                                       1,
                                                                       Lists.newArrayList(2, 3));

            // start servers 0 , 1 only
            List<Integer> serverList = Arrays.asList(0, 1);
            Map<String, String> configProps = new HashMap<String, String>();
            configProps.put("admin.max.threads", "5");

            currentCluster = startServers(currentCluster,
                                          storeDefFileWithReplication,
                                          serverList,
                                          configProps);

            String bootstrapUrl = getBootstrapUrl(currentCluster, 0);
            final ClusterTestUtils.RebalanceKit rebalanceKit = ClusterTestUtils.getRebalanceKit(bootstrapUrl,
                                                                                                finalCluster);

            try {
                // Populate the two stores
                populateData(currentCluster,
                             roStoreDefWithReplication,
                             rebalanceKit.controller.getAdminClient(),
                             true);

                populateData(currentCluster,
                             rwStoreDefWithReplication,
                             rebalanceKit.controller.getAdminClient(),
                             false);

                rebalanceAndCheck(rebalanceKit.plan, rebalanceKit.controller, Arrays.asList(0, 1));

                checkConsistentMetadata(finalCluster, serverList);
            } finally {
                // stop servers
                stopServer(serverList);
            }
        } catch(AssertionError ae) {
            logger.error("Assertion broken in testRORWRebalanceWithReplication ", ae);
            throw ae;
        }
    }

    @Test(timeout = 600000)
    public void testRORebalanceWithReplication() throws Exception {
        logger.info("Starting testRORebalanceWithReplication");
        try {
            Cluster currentCluster = ServerTestUtils.getLocalCluster(2, new int[][] {
                    { 0, 1, 2, 3, 4, 5, 6 }, { 7, 8 } });

            Cluster finalCluster = UpdateClusterUtils.createUpdatedCluster(currentCluster,
                                                                       1,
                                                                       Lists.newArrayList(2, 3));

            // start servers 0 , 1 only
            List<Integer> serverList = Arrays.asList(0, 1);

            // If this test fails, consider increasing the number of admin
            // threads.
            // In particular, if this test fails by RejectedExecutionHandler in
            // SocketServer.java fires with an error message like the following:
            // "[18:46:32,994
            // voldemort.server.socket.SocketServer[admin-server]]
            // ERROR Too many open connections, 20 of 20 threads in use, denying
            // connection from /127.0.0.1:43756 [Thread-552]". Note, this issues
            // seems to only affect ThreadPoolBasedNonblockingStoreImpl tests
            // rather
            // than Nio-based tests.
            Map<String, String> configProps = new HashMap<String, String>();
            configProps.put("admin.max.threads", "5");
            currentCluster = startServers(currentCluster,
                                          roStoreDefFileWithReplication,
                                          serverList,
                                          configProps);

            String bootstrapUrl = getBootstrapUrl(currentCluster, 0);
            final ClusterTestUtils.RebalanceKit rebalanceKit = ClusterTestUtils.getRebalanceKit(bootstrapUrl,
                                                                                                finalCluster);

            try {
                populateData(currentCluster,
                             roStoreDefWithReplication,
                             rebalanceKit.controller.getAdminClient(),
                             true);

                rebalanceAndCheck(rebalanceKit.plan, rebalanceKit.controller, Arrays.asList(0, 1));
                checkConsistentMetadata(finalCluster, serverList);
            } finally {
                // stop servers
                stopServer(serverList);
            }
        } catch(AssertionError ae) {
            logger.error("Assertion broken in testRORebalanceWithReplication ", ae);
            throw ae;
        }
    }

    @Test(timeout = 600000)
    public void testRWRebalanceWithReplication() throws Exception {
        logger.info("Starting testRWRebalanceWithReplication");
        try {
            Cluster currentCluster = ServerTestUtils.getLocalCluster(2, new int[][] {
                    { 0, 1, 2, 3, 4, 5, 6 }, { 7, 8 } });
            Cluster finalCluster = UpdateClusterUtils.createUpdatedCluster(currentCluster,
                                                                       1,
                                                                       Lists.newArrayList(2, 3));
            // start servers 0 , 1 only
            List<Integer> serverList = Arrays.asList(0, 1);
            currentCluster = startServers(currentCluster,
                                          rwStoreDefFileWithReplication,
                                          serverList,
                                          null);

            String bootstrapUrl = getBootstrapUrl(currentCluster, 0);
            final ClusterTestUtils.RebalanceKit rebalanceKit = ClusterTestUtils.getRebalanceKit(bootstrapUrl,
                                                                                                finalCluster);

            try {
                populateData(currentCluster,
                             rwStoreDefWithReplication,
                             rebalanceKit.controller.getAdminClient(),
                             false);

                rebalanceAndCheck(rebalanceKit.plan, rebalanceKit.controller, Arrays.asList(0, 1));

                checkConsistentMetadata(finalCluster, serverList);
            } finally {
                // stop servers
                stopServer(serverList);
            }
        } catch(AssertionError ae) {
            logger.error("Assertion broken in testRWRebalanceWithReplication ", ae);
            throw ae;
        }
    }

    @Test(timeout = 600000)
    public void testRebalanceCleanPrimary() throws Exception {
        logger.info("Starting testRebalanceCleanPrimary");
        try {
            Cluster currentCluster = ServerTestUtils.getLocalCluster(3, new int[][] { { 0 },
                    { 1, 3 }, { 2 } });

            Cluster finalCluster = UpdateClusterUtils.createUpdatedCluster(currentCluster,
                                                                       2,
                                                                       Lists.newArrayList(3));

            // start servers 0 , 1, 2
            Map<String, String> configProps = new HashMap<String, String>();
            configProps.put("enable.repair", "true");
            List<Integer> serverList = Arrays.asList(0, 1, 2);
            currentCluster = startServers(currentCluster,
                                          rwStoreDefFileWithReplication,
                                          serverList,
                                          configProps);

            String bootstrapUrl = getBootstrapUrl(currentCluster, 0);
            final ClusterTestUtils.RebalanceKit rebalanceKit = ClusterTestUtils.getRebalanceKit(bootstrapUrl,
                                                                                                finalCluster);

            try {
                AdminClient adminClient = rebalanceKit.controller.getAdminClient();
                populateData(currentCluster, rwStoreDefWithReplication, adminClient, false);

                // Figure out the positive keys to check
                List<ByteArray> positiveTestKeyList = sampleKeysFromPartition(adminClient,
                                                                              1,
                                                                              rwStoreDefWithReplication.getName(),
                                                                              Arrays.asList(1),
                                                                              20);

                rebalanceAndCheck(rebalanceKit.plan,
                                  rebalanceKit.controller,
                                  Arrays.asList(0, 1, 2));
                checkConsistentMetadata(finalCluster, serverList);

                // Do the cleanup operation
                for(int i = 0; i < 3; i++) {
                    adminClient.storeMntOps.repairJob(i);
                }
                // wait for the repairs to complete
                for(int i = 0; i < 3; i++) {
                    ServerTestUtils.waitForAsyncOperationOnServer(serverMap.get(i), "Repair", 5000);
                }

                // do the positive tests
                checkForKeyExistence(adminClient,
                                     1,
                                     rwStoreDefWithReplication.getName(),
                                     positiveTestKeyList);

                logger.info("[Primary] Successful clean after Rebalancing");
            } finally {
                // stop servers
                stopServer(serverList);
            }
        } catch(AssertionError ae) {
            logger.error("Assertion broken in testRebalanceCleanPrimary ", ae);
            throw ae;
        }
    }

    @Test(timeout = 600000)
    public void testRebalanceCleanSecondary() throws Exception {
        logger.info("Starting testRebalanceCleanSecondary");
        try {
            Cluster currentCluster = ServerTestUtils.getLocalCluster(3, new int[][] { { 0, 3 },
                    { 1 }, { 2 } });

            Cluster finalCluster = UpdateClusterUtils.createUpdatedCluster(currentCluster,
                                                                       2,
                                                                       Lists.newArrayList(3));

            // start servers 0 , 1, 2
            Map<String, String> configProps = new HashMap<String, String>();
            configProps.put("enable.repair", "true");
            List<Integer> serverList = Arrays.asList(0, 1, 2);
            currentCluster = startServers(currentCluster,
                                          rwStoreDefFileWithReplication,
                                          serverList,
                                          configProps);

            String bootstrapUrl = getBootstrapUrl(currentCluster, 0);
            final ClusterTestUtils.RebalanceKit rebalanceKit = ClusterTestUtils.getRebalanceKit(bootstrapUrl,
                                                                                                finalCluster);

            try {
                AdminClient adminClient = rebalanceKit.controller.getAdminClient();
                populateData(currentCluster, rwStoreDefWithReplication, adminClient, false);

                // Figure out the positive and negative keys to check
                List<ByteArray> positiveTestKeyList = sampleKeysFromPartition(adminClient,
                                                                              0,
                                                                              rwStoreDefWithReplication.getName(),
                                                                              Arrays.asList(3),
                                                                              20);

                rebalanceAndCheck(rebalanceKit.plan,
                                  rebalanceKit.controller,
                                  Arrays.asList(0, 1, 2));
                checkConsistentMetadata(finalCluster, serverList);

                // Do the cleanup operation
                for(int i = 0; i < 3; i++) {
                    adminClient.storeMntOps.repairJob(i);
                }
                // wait for the repairs to complete
                for(int i = 0; i < 3; i++) {
                    ServerTestUtils.waitForAsyncOperationOnServer(serverMap.get(i), "Repair", 5000);
                }

                // do the positive tests
                checkForKeyExistence(adminClient,
                                     0,
                                     rwStoreDefWithReplication.getName(),
                                     positiveTestKeyList);

                logger.info("[Secondary] Successful clean after Rebalancing");
            } finally {
                // stop servers
                stopServer(serverList);
            }
        } catch(AssertionError ae) {
            logger.error("Assertion broken in testRWRebalanceCleanSecondary ", ae);
            throw ae;
        }
    }

    @Test(timeout = 600000)
    public void testRWRebalanceFourNodes() throws Exception {
        logger.info("Starting testRWRebalanceFourNodes");
        try {
            Cluster currentCluster = ServerTestUtils.getLocalCluster(4, new int[][] {
                    { 0, 1, 4, 7, 9 }, { 2, 3, 5, 6, 8 }, {}, {} });

            ArrayList<Node> nodes = Lists.newArrayList(currentCluster.getNodes());
            int totalPortNum = nodes.size() * 3;
            int[] ports = new int[totalPortNum];
            for(int i = 0; i < nodes.size(); i++) {
                ports[i * 3] = nodes.get(i).getHttpPort();
                ports[i * 3 + 1] = nodes.get(i).getSocketPort();
                ports[i * 3 + 2] = nodes.get(i).getAdminPort();
            }

            Cluster finalCluster = ServerTestUtils.getLocalCluster(4, ports, new int[][] {
                    { 0, 4, 7 }, { 2, 8 }, { 1, 6 }, { 3, 5, 9 } });

            // start servers
            List<Integer> serverList = Arrays.asList(0, 1, 2, 3);
            currentCluster = startServers(currentCluster,
                                          rwTwoStoreDefFileWithReplication,
                                          serverList,
                                          null);

            String bootstrapUrl = getBootstrapUrl(currentCluster, 0);
            int maxParallel = 5;
            final ClusterTestUtils.RebalanceKit rebalanceKit = ClusterTestUtils.getRebalanceKit(bootstrapUrl,
                                                                                                maxParallel,
                                                                                                finalCluster);

            try {
                populateData(currentCluster,
                             rwStoreDefWithReplication,
                             rebalanceKit.controller.getAdminClient(),
                             false);

                populateData(currentCluster,
                             rwStoreDefWithReplication2,
                             rebalanceKit.controller.getAdminClient(),
                             false);

                rebalanceAndCheck(rebalanceKit.plan, rebalanceKit.controller, serverList);

                checkConsistentMetadata(finalCluster, serverList);
            } catch(Exception e) {
                fail(e.getMessage());
            } finally {
                // stop servers
                stopServer(serverList);
            }
        } catch(AssertionError ae) {
            logger.error("Assertion broken in testRWRebalanceFourNodes ", ae);
            throw ae;
        }
    }

    @Test(timeout = 600000)
    public void testRWRebalanceSerial() throws Exception {
        logger.info("Starting testRWRebalanceSerial");
        try {
            Cluster currentCluster = ServerTestUtils.getLocalCluster(4, new int[][] {
                    { 0, 1, 4, 7, 9 }, { 2, 3, 5, 6, 8 }, {}, {} });

            ArrayList<Node> nodes = Lists.newArrayList(currentCluster.getNodes());
            int totalPortNum = nodes.size() * 3;
            int[] ports = new int[totalPortNum];
            for(int i = 0; i < nodes.size(); i++) {
                ports[i * 3] = nodes.get(i).getHttpPort();
                ports[i * 3 + 1] = nodes.get(i).getSocketPort();
                ports[i * 3 + 2] = nodes.get(i).getAdminPort();
            }

            Cluster finalCluster = ServerTestUtils.getLocalCluster(4, ports, new int[][] {
                    { 0, 4, 7 }, { 2, 8 }, { 1, 6 }, { 3, 5, 9 } });

            // start servers
            Map<String, String> serverProps = new HashMap<String, String>();
            serverProps.put("max.parallel.stores.rebalancing", String.valueOf(1));
            List<Integer> serverList = Arrays.asList(0, 1, 2, 3);
            currentCluster = startServers(currentCluster,
                                          rwTwoStoreDefFileWithReplication,
                                          serverList,
                                          serverProps);

            String bootstrapUrl = getBootstrapUrl(currentCluster, 0);
            int maxParallel = 5;
            final ClusterTestUtils.RebalanceKit rebalanceKit = ClusterTestUtils.getRebalanceKit(bootstrapUrl,
                                                                                                maxParallel,
                                                                                                finalCluster);

            try {
                populateData(currentCluster,
                             rwStoreDefWithReplication,
                             rebalanceKit.controller.getAdminClient(),
                             false);

                populateData(currentCluster,
                             rwStoreDefWithReplication2,
                             rebalanceKit.controller.getAdminClient(),
                             false);

                rebalanceAndCheck(rebalanceKit.plan, rebalanceKit.controller, serverList);

                checkConsistentMetadata(finalCluster, serverList);
            } catch(Exception e) {
                fail(e.getMessage());
            } finally {
                // stop servers
                stopServer(serverList);
            }
        } catch(AssertionError ae) {
            logger.error("Assertion broken in testRWRebalanceSerial ", ae);
            throw ae;
        }
    }

    @Test(timeout = 600000)
    public void testProxyGetDuringRebalancing() throws Exception {
        logger.info("Starting testProxyGetDuringRebalancing");
        try {
            final Cluster currentCluster = ServerTestUtils.getLocalCluster(2, new int[][] {
                    { 0, 1, 2, 3, 4, 5, 6 }, { 7, 8 } });

            final Cluster finalCluster = UpdateClusterUtils.createUpdatedCluster(currentCluster,
                                                                             1,
                                                                             Lists.newArrayList(2,
                                                                                                3));
            // start servers 0 , 1 only
            final List<Integer> serverList = Arrays.asList(0, 1);
            Map<String, String> configProps = new HashMap<String, String>();
            configProps.put("admin.max.threads", "5");
            final Cluster updatedCurrentCluster = startServers(currentCluster,
                                                               storeDefFileWithReplication,
                                                               serverList,
                                                               configProps);

            ExecutorService executors = Executors.newFixedThreadPool(2);
            final AtomicBoolean rebalancingComplete = new AtomicBoolean(false);
            final List<Exception> exceptions = Collections.synchronizedList(new ArrayList<Exception>());

            String bootstrapUrl = getBootstrapUrl(currentCluster, 0);
            int maxParallel = 2;
            final ClusterTestUtils.RebalanceKit rebalanceKit = ClusterTestUtils.getRebalanceKit(bootstrapUrl,
                                                                                                maxParallel,
                                                                                                finalCluster);

            // Populate the two stores
            populateData(updatedCurrentCluster,
                         roStoreDefWithReplication,
                         rebalanceKit.controller.getAdminClient(),
                         true);

            populateData(updatedCurrentCluster,
                         rwStoreDefWithReplication,
                         rebalanceKit.controller.getAdminClient(),
                         false);

            final SocketStoreClientFactory factory = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(getBootstrapUrl(updatedCurrentCluster,
                                                                                                                                      0))
                                                                                                    .setEnableLazy(false)
                                                                                                    .setSocketTimeout(120,
                                                                                                                      TimeUnit.SECONDS));

            final StoreClient<String, String> storeClientRW = new DefaultStoreClient<String, String>(testStoreNameRW,
                                                                                                     null,
                                                                                                     factory,
                                                                                                     3);

            final StoreClient<String, String> storeClientRO = new DefaultStoreClient<String, String>(testStoreNameRO,
                                                                                                     null,
                                                                                                     factory,
                                                                                                     3);

            final CountDownLatch latch = new CountDownLatch(2);
            // start get operation.
            executors.execute(new Runnable() {

                @Override
                public void run() {
                    try {
                        List<String> keys = new ArrayList<String>(testEntries.keySet());

                        while(!rebalancingComplete.get()) {
                            // should always able to get values.
                            int index = (int) (Math.random() * keys.size());

                            // should get a valid value
                            try {
                                Versioned<String> value = storeClientRW.get(keys.get(index));
                                assertNotSame("StoreClient get() should not return null.",
                                              null,
                                              value);
                                assertEquals("Value returned should be good",
                                             new Versioned<String>(testEntries.get(keys.get(index))),
                                             value);

                                value = storeClientRO.get(keys.get(index));
                                assertNotSame("StoreClient get() should not return null.",
                                              null,
                                              value);
                                assertEquals("Value returned should be good",
                                             new Versioned<String>(testEntries.get(keys.get(index))),
                                             value);

                            } catch(Exception e) {
                                logger.error("Exception in online thread", e);
                                exceptions.add(e);
                            } finally {
                                latch.countDown();
                            }
                        }
                    } catch(Exception e) {
                        logger.error("Exception in proxy get thread", e);
                        exceptions.add(e);
                    } finally {
                        factory.close();
                    }
                }

            });

            executors.execute(new Runnable() {

                @Override
                public void run() {
                    try {

                        Thread.sleep(500);

                        rebalanceAndCheck(rebalanceKit.plan,
                                          rebalanceKit.controller,
                                          Arrays.asList(0, 1));

                        Thread.sleep(500);
                        rebalancingComplete.set(true);
                        checkConsistentMetadata(finalCluster, serverList);
                    } catch(Exception e) {
                        exceptions.add(e);
                        logger.error("Exception in rebalancing thread", e);
                    } finally {
                        // stop servers
                        try {
                            stopServer(serverList);
                        } catch(Exception e) {
                            throw new RuntimeException(e);
                        }
                        latch.countDown();
                    }
                }
            });

            latch.await();
            executors.shutdown();
            executors.awaitTermination(300, TimeUnit.SECONDS);

            // check No Exception
            if(exceptions.size() > 0) {
                for(Exception e: exceptions) {
                    e.printStackTrace();
                }
                fail("Should not see any exceptions.");
            }
        } catch(AssertionError ae) {
            logger.error("Assertion broken in testProxyGetDuringRebalancing ", ae);
            throw ae;
        }
    }

    @Test(timeout = 600000)
    public void testProxyPutDuringRebalancing() throws Exception {
        logger.info("Starting testProxyPutDuringRebalancing");
        try {
            Cluster currentCluster = ServerTestUtils.getLocalCluster(3, new int[][] { { 0 },
                    { 1, 3 }, { 2 } });

            Cluster finalCluster = UpdateClusterUtils.createUpdatedCluster(currentCluster,
                                                                       2,
                                                                       Lists.newArrayList(3));

            // start servers 0,1,2 only
            final List<Integer> serverList = Arrays.asList(0, 1, 2);
            Map<String, String> configProps = new HashMap<String, String>();
            configProps.put("admin.max.threads", "5");
            final Cluster updatedCurrentCluster = startServers(currentCluster,
                                                               rwStoreDefFileWithReplication,
                                                               serverList,
                                                               configProps);

            ExecutorService executors = Executors.newFixedThreadPool(2);
            final AtomicBoolean rebalancingComplete = new AtomicBoolean(false);
            final List<Exception> exceptions = Collections.synchronizedList(new ArrayList<Exception>());

            // Its is imperative that we test in a single shot since multiple
            // batches would mean the proxy bridges being torn down and
            // established multiple times and we cannot test against the source
            // cluster topology then.
            String bootstrapUrl = getBootstrapUrl(currentCluster, 0);
            int maxParallel = 2;
            final ClusterTestUtils.RebalanceKit rebalanceKit = ClusterTestUtils.getRebalanceKit(bootstrapUrl,
                                                                                                maxParallel,
                                                                                                finalCluster);

            populateData(updatedCurrentCluster,
                         rwStoreDefWithReplication,
                         rebalanceKit.controller.getAdminClient(),
                         false);

            final AdminClient adminClient = rebalanceKit.controller.getAdminClient();
            // the plan would cause these partitions to move
            // Partition : Donor -> Stealer
            // p2 (SEC) : s1 -> s0
            // p3 (PRI) : s1 -> s2
            final List<ByteArray> movingKeysList = sampleKeysFromPartition(adminClient,
                                                                           1,
                                                                           rwStoreDefWithReplication.getName(),
                                                                           Arrays.asList(2, 3),
                                                                           20);
            assertTrue("Empty list of moving keys...", movingKeysList.size() > 0);
            final AtomicBoolean rebalancingStarted = new AtomicBoolean(false);
            final AtomicBoolean proxyWritesDone = new AtomicBoolean(false);
            final HashMap<String, String> baselineTuples = new HashMap<String, String>(testEntries);
            final HashMap<String, VectorClock> baselineVersions = new HashMap<String, VectorClock>();

            for(String key: baselineTuples.keySet()) {
                baselineVersions.put(key, new VectorClock());
            }

            final CountDownLatch latch = new CountDownLatch(2);
            // start get operation.
            executors.execute(new Runnable() {

                @Override
                public void run() {
                    SocketStoreClientFactory factory = null;
                    try {
                        // wait for the rebalancing to begin.
                        List<VoldemortServer> serverList = Lists.newArrayList(serverMap.get(0),
                                                                              serverMap.get(2));
                        while(!rebalancingComplete.get()) {
                            Iterator<VoldemortServer> serverIterator = serverList.iterator();
                            while(serverIterator.hasNext()) {
                                VoldemortServer server = serverIterator.next();
                                if(ByteUtils.getString(server.getMetadataStore()
                                                             .get(MetadataStore.SERVER_STATE_KEY,
                                                                  null)
                                                             .get(0)
                                                             .getValue(),
                                                       "UTF-8")
                                            .compareTo(VoldemortState.REBALANCING_MASTER_SERVER.toString()) == 0) {
                                    logger.info("Server " + server.getIdentityNode().getId()
                                                + " transitioned into REBALANCING MODE");
                                    serverIterator.remove();
                                }
                            }
                            if(serverList.size() == 0) {
                                rebalancingStarted.set(true);
                                break;
                            }
                        }

                        if(!rebalancingComplete.get()) {
                            factory = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(getBootstrapUrl(updatedCurrentCluster,
                                                                                                                       0))
                                                                                     .setEnableLazy(false)
                                                                                     .setSocketTimeout(120,
                                                                                                       TimeUnit.SECONDS));

                            final StoreClient<String, String> storeClientRW = new DefaultStoreClient<String, String>(testStoreNameRW,
                                                                                                                     null,
                                                                                                                     factory,
                                                                                                                     3);
                            // Now perform some writes and determine the end
                            // state
                            // of the changed keys. Initially, all data now with
                            // zero vector clock
                            for(ByteArray movingKey: movingKeysList) {
                                try {
                                    if(rebalancingComplete.get()) {
                                        break;
                                    }
                                    String keyStr = ByteUtils.getString(movingKey.get(), "UTF-8");
                                    String valStr = "proxy_write";
                                    storeClientRW.put(keyStr, valStr);
                                    baselineTuples.put(keyStr, valStr);
                                    // all these keys will have [2:1] vector
                                    // clock
                                    // is node 2 is the pseudo master in both
                                    // moves
                                    baselineVersions.get(keyStr)
                                                    .incrementVersion(2, System.currentTimeMillis());
                                    proxyWritesDone.set(true);
                                } catch(InvalidMetadataException e) {
                                    // let this go
                                    logger.error("Encountered an invalid metadata exception.. ", e);
                                }
                            }
                        }
                    } catch(Exception e) {
                        logger.error("Exception in proxy put thread", e);
                        exceptions.add(e);
                    } finally {
                        if(factory != null)
                            factory.close();
                        latch.countDown();
                    }
                }

            });

            executors.execute(new Runnable() {

                @Override
                public void run() {
                    try {
                        rebalanceKit.rebalance();
                    } catch(Exception e) {
                        logger.error("Error in rebalancing... ", e);
                        exceptions.add(e);
                    } finally {
                        rebalancingComplete.set(true);
                        latch.countDown();
                    }
                }
            });

            latch.await();
            executors.shutdown();
            executors.awaitTermination(300, TimeUnit.SECONDS);

            assertEquals("Client did not see all server transition into rebalancing state",
                         rebalancingStarted.get(),
                         true);
            assertEquals("Not enough time to begin proxy writing", proxyWritesDone.get(), true);
            checkEntriesPostRebalance(updatedCurrentCluster,
                                      finalCluster,
                                      Lists.newArrayList(rwStoreDefWithReplication),
                                      Arrays.asList(0, 1, 2),
                                      baselineTuples,
                                      baselineVersions);
            checkConsistentMetadata(finalCluster, serverList);
            // check No Exception
            if(exceptions.size() > 0) {

                for(Exception e: exceptions) {
                    e.printStackTrace();
                }
                fail("Should not see any exceptions.");
            }
            // check that the proxy writes were made to the original donor, node
            // 1
            List<ClockEntry> clockEntries = new ArrayList<ClockEntry>(serverList.size());
            for(Integer nodeid: serverList)
                clockEntries.add(new ClockEntry(nodeid.shortValue(), System.currentTimeMillis()));
            VectorClock clusterXmlClock = new VectorClock(clockEntries, System.currentTimeMillis());
            for(Integer nodeid: serverList)
                adminClient.metadataMgmtOps.updateRemoteCluster(nodeid,
                                                                currentCluster,
                                                                clusterXmlClock);

            adminClient.setAdminClientCluster(currentCluster);
            checkForTupleEquivalence(adminClient,
                                     1,
                                     testStoreNameRW,
                                     movingKeysList,
                                     baselineTuples,
                                     baselineVersions);

            // stop servers
            try {
                stopServer(serverList);
            } catch(Exception e) {
                throw new RuntimeException(e);
            }
        } catch(AssertionError ae) {
            logger.error("Assertion broken in testProxyPutDuringRebalancing ", ae);
            throw ae;
        }
    }

    @Test(timeout = 600000)
    public void testServerSideRouting() throws Exception {
        logger.info("Starting testServerSideRouting");
        try {
            final Cluster currentCluster = ServerTestUtils.getLocalCluster(2, new int[][] {
                    { 0, 1, 2, 3, 4, 5, 6 }, { 7, 8 } });

            final Cluster finalCluster = UpdateClusterUtils.createUpdatedCluster(currentCluster,
                                                                             1,
                                                                             Lists.newArrayList(2,
                                                                                                3));

            final List<Integer> serverList = Arrays.asList(0, 1);
            Map<String, String> configProps = new HashMap<String, String>();
            configProps.put("admin.max.threads", "50");
            final Cluster updatedCurrentCluster = startServers(currentCluster,
                                                               storeDefFileWithReplication,
                                                               serverList,
                                                               configProps);

            ExecutorService executors = Executors.newFixedThreadPool(2);
            final AtomicBoolean rebalancingToken = new AtomicBoolean(false);
            final List<Exception> exceptions = Collections.synchronizedList(new ArrayList<Exception>());

            String bootstrapUrl = getBootstrapUrl(currentCluster, 0);
            int maxParallel = 2;
            final ClusterTestUtils.RebalanceKit rebalanceKit = ClusterTestUtils.getRebalanceKit(bootstrapUrl,
                                                                                                maxParallel,
                                                                                                finalCluster);

            // Populate the two stores
            populateData(updatedCurrentCluster,
                         roStoreDefWithReplication,
                         rebalanceKit.controller.getAdminClient(),
                         true);

            populateData(updatedCurrentCluster,
                         rwStoreDefWithReplication,
                         rebalanceKit.controller.getAdminClient(),
                         false);

            Node node = updatedCurrentCluster.getNodeById(1);
            final Store<ByteArray, byte[], byte[]> serverSideRoutingStoreRW = getSocketStore(testStoreNameRW,
                                                                                             node.getHost(),
                                                                                             node.getSocketPort(),
                                                                                             true);
            final Store<ByteArray, byte[], byte[]> serverSideRoutingStoreRO = getSocketStore(testStoreNameRO,
                                                                                             node.getHost(),
                                                                                             node.getSocketPort(),
                                                                                             true);

            final CountDownLatch latch = new CountDownLatch(1);

            // start get operation.
            executors.execute(new Runnable() {

                @Override
                public void run() {
                    try {
                        List<String> keys = new ArrayList<String>(testEntries.keySet());

                        while(!rebalancingToken.get()) {
                            // should always able to get values.
                            int index = (int) (Math.random() * keys.size());

                            // should get a valid value
                            try {
                                List<Versioned<byte[]>> values = serverSideRoutingStoreRW.get(new ByteArray(ByteUtils.getBytes(keys.get(index),
                                                                                                                               "UTF-8")),
                                                                                              null);

                                assertEquals("serverSideRoutingStore should return value.",
                                             1,
                                             values.size());
                                assertEquals("Value returned should be good",
                                             new Versioned<String>(testEntries.get(keys.get(index))),
                                             new Versioned<String>(ByteUtils.getString(values.get(0)
                                                                                             .getValue(),
                                                                                       "UTF-8"),
                                                                   values.get(0).getVersion()));
                                values = serverSideRoutingStoreRO.get(new ByteArray(ByteUtils.getBytes(keys.get(index),
                                                                                                       "UTF-8")),
                                                                      null);

                                assertEquals("serverSideRoutingStore should return value.",
                                             1,
                                             values.size());
                                assertEquals("Value returned should be good",
                                             new Versioned<String>(testEntries.get(keys.get(index))),
                                             new Versioned<String>(ByteUtils.getString(values.get(0)
                                                                                             .getValue(),
                                                                                       "UTF-8"),
                                                                   values.get(0).getVersion()));

                            } catch(UnreachableStoreException e) {
                                // ignore
                            } catch(Exception e) {
                                exceptions.add(e);
                            }
                        }

                        latch.countDown();
                    } catch(Exception e) {
                        exceptions.add(e);
                    }
                }

            });

            executors.execute(new Runnable() {

                @Override
                public void run() {
                    try {
                        Thread.sleep(500);
                        rebalanceAndCheck(rebalanceKit.plan,
                                          rebalanceKit.controller,
                                          Arrays.asList(0, 1));

                        Thread.sleep(500);
                        rebalancingToken.set(true);
                        checkConsistentMetadata(finalCluster, serverList);
                    } catch(Exception e) {
                        exceptions.add(e);
                    } finally {
                        // stop servers as soon as the client thread has exited
                        // its
                        // loop.
                        try {
                            latch.await(300, TimeUnit.SECONDS);
                            stopServer(serverList);
                        } catch(Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            });

            executors.shutdown();
            executors.awaitTermination(300, TimeUnit.SECONDS);

            // check No Exception
            if(exceptions.size() > 0) {
                for(Exception e: exceptions) {
                    e.printStackTrace();
                }
                fail("Should not see any exceptions !!");
            }
        } catch(AssertionError ae) {
            logger.error("Assertion broken in testServerSideRouting ", ae);
            throw ae;
        }
    }

    protected void populateData(Cluster cluster,
                                StoreDefinition storeDef,
                                AdminClient adminClient,
                                boolean isReadOnly) throws Exception {

        // Populate Read write stores
        if(!isReadOnly) {
            // Create SocketStores for each Node first
            Map<Integer, Store<ByteArray, byte[], byte[]>> storeMap = new HashMap<Integer, Store<ByteArray, byte[], byte[]>>();
            for(Node node: cluster.getNodes()) {
                storeMap.put(node.getId(),
                             getSocketStore(storeDef.getName(),
                                            node.getHost(),
                                            node.getSocketPort()));

            }

            BaseStoreRoutingPlan storeInstance = new BaseStoreRoutingPlan(cluster, storeDef);
            for(Entry<String, String> entry: testEntries.entrySet()) {
                ByteArray keyBytes = new ByteArray(ByteUtils.getBytes(entry.getKey(), "UTF-8"));
                List<Integer> preferenceNodes = storeInstance.getReplicationNodeList(keyBytes.get());

                // Go over every node
                for(int nodeId: preferenceNodes) {
                    try {
                        storeMap.get(nodeId)
                                .put(keyBytes,
                                     new Versioned<byte[]>(ByteUtils.getBytes(entry.getValue(),
                                                                              "UTF-8")),
                                     null);
                    } catch(ObsoleteVersionException e) {
                        logger.info("Why are we seeing this at all here ?? ");
                        e.printStackTrace();
                    }
                }
            }

            // close all socket stores
            for(Store<ByteArray, byte[], byte[]> store: storeMap.values()) {
                store.close();
            }

        } else {
            // Populate Read only stores

            File baseDir = TestUtils.createTempDir();
            JsonReader reader = ReadOnlyStorageEngineTestInstance.makeTestDataReader(testEntries,
                                                                                     baseDir);

            RoutingStrategy router = new RoutingStrategyFactory().updateRoutingStrategy(storeDef,
                                                                                        cluster);

            File outputDir = TestUtils.createTempDir(baseDir);
            JsonStoreBuilder storeBuilder = new JsonStoreBuilder(reader,
                                                                 cluster,
                                                                 storeDef,
                                                                 router,
                                                                 outputDir,
                                                                 null,
                                                                 testEntries.size() / 5,
                                                                 1,
                                                                 NUM_RO_CHUNKS_PER_BUCKET,
                                                                 10000,
                                                                 false);
            storeBuilder.build(ReadOnlyStorageFormat.READONLY_V2);

            AdminStoreSwapper swapper = new AdminStoreSwapper(cluster,
                                                              Executors.newFixedThreadPool(cluster.getNumberOfNodes()),
                                                              adminClient,
                                                              100000);
            swapper.swapStoreData(testStoreNameRO, outputDir.getAbsolutePath(), 1L);
        }
    }
}