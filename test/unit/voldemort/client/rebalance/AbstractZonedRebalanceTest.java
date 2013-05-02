/*
 * Copyright 2008-2012 LinkedIn, Inc
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

import voldemort.ServerTestUtils;
import voldemort.client.ClientConfig;
import voldemort.client.DefaultStoreClient;
import voldemort.client.RoutingTier;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.routing.RoutingStrategyType;
import voldemort.routing.StoreRoutingPlan;
import voldemort.serialization.SerializerDefinition;
import voldemort.server.VoldemortServer;
import voldemort.store.InvalidMetadataException;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreDefinitionBuilder;
import voldemort.store.bdb.BdbStorageConfiguration;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.metadata.MetadataStore.VoldemortState;
import voldemort.store.slop.strategy.HintedHandoffStrategyType;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.RebalanceUtils;
import voldemort.versioning.ClockEntry;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.collect.Lists;

/**
 * Rebalancing tests with zoned configurations with cross zone moves (since
 * {@link AbstractNonZonedRebalanceTest} should cover intra zone moves already
 * 
 */
public abstract class AbstractZonedRebalanceTest extends AbstractRebalanceTest {

    private static final Logger logger = Logger.getLogger(AbstractZonedRebalanceTest.class.getName());

    protected static String testStoreNameRW = "test";
    protected static String testStoreNameRW2 = "test2";

    protected static String storeDefFileWithoutReplication;
    protected static String storeDefFileWithReplication;
    protected static String rwStoreDefFileWithReplication;
    protected static String rwTwoStoreDefFileWithReplication;

    private List<StoreDefinition> storeDefWithoutReplication;
    private List<StoreDefinition> storeDefWithReplication;
    private StoreDefinition rwStoreDefWithoutReplication;
    private StoreDefinition rwStoreDefWithReplication;
    private StoreDefinition rwStoreDefWithReplication2;

    public AbstractZonedRebalanceTest(boolean useNio, boolean useDonorBased) {
        super(useNio, useDonorBased);
    }

    @Before
    public void setUp() throws IOException {
        // First without replication
        HashMap<Integer, Integer> zrfRWStoreWithoutReplication = new HashMap<Integer, Integer>();
        zrfRWStoreWithoutReplication.put(0, 1);
        zrfRWStoreWithoutReplication.put(1, 1);
        rwStoreDefWithoutReplication = new StoreDefinitionBuilder().setName(testStoreNameRW)
                                                                   .setType(BdbStorageConfiguration.TYPE_NAME)
                                                                   .setKeySerializer(new SerializerDefinition("string"))
                                                                   .setValueSerializer(new SerializerDefinition("string"))
                                                                   .setRoutingPolicy(RoutingTier.CLIENT)
                                                                   .setRoutingStrategyType(RoutingStrategyType.ZONE_STRATEGY)
                                                                   .setReplicationFactor(2)
                                                                   .setPreferredReads(1)
                                                                   .setRequiredReads(1)
                                                                   .setPreferredWrites(1)
                                                                   .setRequiredWrites(1)
                                                                   .setZoneCountReads(0)
                                                                   .setZoneCountWrites(0)
                                                                   .setZoneReplicationFactor(zrfRWStoreWithoutReplication)
                                                                   .setHintedHandoffStrategy(HintedHandoffStrategyType.PROXIMITY_STRATEGY)
                                                                   .build();

        storeDefWithoutReplication = Lists.newArrayList(rwStoreDefWithoutReplication);
        String storeDefWithoutReplicationString = new StoreDefinitionsMapper().writeStoreList(storeDefWithoutReplication);
        File file = File.createTempFile("two-stores-", ".xml");
        FileUtils.writeStringToFile(file, storeDefWithoutReplicationString);
        storeDefFileWithoutReplication = file.getAbsolutePath();

        // Now with replication
        HashMap<Integer, Integer> zrfRWStoreWithReplication = new HashMap<Integer, Integer>();
        zrfRWStoreWithReplication.put(0, 2);
        zrfRWStoreWithReplication.put(1, 2);
        rwStoreDefWithReplication = new StoreDefinitionBuilder().setName(testStoreNameRW)
                                                                .setType(BdbStorageConfiguration.TYPE_NAME)
                                                                .setKeySerializer(new SerializerDefinition("string"))
                                                                .setValueSerializer(new SerializerDefinition("string"))
                                                                .setRoutingPolicy(RoutingTier.CLIENT)
                                                                .setRoutingStrategyType(RoutingStrategyType.ZONE_STRATEGY)
                                                                .setReplicationFactor(4)
                                                                .setPreferredReads(1)
                                                                .setRequiredReads(1)
                                                                .setPreferredWrites(1)
                                                                .setRequiredWrites(1)
                                                                .setZoneCountReads(0)
                                                                .setZoneCountWrites(0)
                                                                .setZoneReplicationFactor(zrfRWStoreWithReplication)
                                                                .setHintedHandoffStrategy(HintedHandoffStrategyType.PROXIMITY_STRATEGY)
                                                                .build();
        rwStoreDefWithReplication2 = new StoreDefinitionBuilder().setName(testStoreNameRW2)
                                                                 .setType(BdbStorageConfiguration.TYPE_NAME)
                                                                 .setKeySerializer(new SerializerDefinition("string"))
                                                                 .setValueSerializer(new SerializerDefinition("string"))
                                                                 .setRoutingPolicy(RoutingTier.CLIENT)
                                                                 .setRoutingStrategyType(RoutingStrategyType.ZONE_STRATEGY)
                                                                 .setReplicationFactor(4)
                                                                 .setPreferredReads(1)
                                                                 .setRequiredReads(1)
                                                                 .setPreferredWrites(1)
                                                                 .setRequiredWrites(1)
                                                                 .setZoneCountReads(0)
                                                                 .setZoneCountWrites(0)
                                                                 .setZoneReplicationFactor(zrfRWStoreWithReplication)
                                                                 .setHintedHandoffStrategy(HintedHandoffStrategyType.PROXIMITY_STRATEGY)
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

        storeDefWithReplication = Lists.newArrayList(rwStoreDefWithReplication);
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
    }

    @Test(timeout = 600000)
    public void testRWRebalance() throws Exception {
        logger.info("Starting testRWRebalance");
        try {

            Cluster currentCluster = ServerTestUtils.getLocalZonedCluster(4, 2, new int[] { 0, 0,
                    1, 1 }, new int[][] { { 0, 2, 4, 6 }, {}, { 1, 3, 5, 7 }, {} });
            Cluster targetCluster = RebalanceUtils.createUpdatedCluster(currentCluster,
                                                                        3,
                                                                        Lists.newArrayList(2, 6));
            targetCluster = RebalanceUtils.createUpdatedCluster(targetCluster,
                                                                1,
                                                                Lists.newArrayList(3, 7));

            // start all the servers
            List<Integer> serverList = Arrays.asList(0, 1, 2, 3);
            Map<String, String> configProps = new HashMap<String, String>();
            configProps.put("admin.max.threads", "5");
            currentCluster = startServers(currentCluster,
                                          storeDefFileWithoutReplication,
                                          serverList,
                                          configProps);
            // Update the cluster information based on the node information
            targetCluster = updateCluster(targetCluster);

            RebalanceClientConfig config = new RebalanceClientConfig();
            config.setDeleteAfterRebalancingEnabled(true);
            config.setStealerBasedRebalancing(!useDonorBased);
            RebalanceController rebalanceClient = new RebalanceController(getBootstrapUrl(currentCluster,
                                                                                          0),
                                                                          config);
            try {
                populateData(currentCluster, rwStoreDefWithoutReplication);

                rebalanceAndCheck(currentCluster,
                                  targetCluster,
                                  storeDefWithoutReplication,
                                  rebalanceClient,
                                  Arrays.asList(1, 2));

                checkConsistentMetadata(targetCluster, serverList);
            } finally {
                // stop servers
                stopServer(serverList);
            }
        } catch(AssertionError ae) {
            logger.error("Assertion broken in testRWRebalance ", ae);
            throw ae;
        }
    }

    public void testRWRebalanceWithReplication(boolean serial) throws Exception {
        logger.info("Starting testRWRebalanceWithReplication");

        Cluster currentCluster = ServerTestUtils.getLocalZonedCluster(4,
                                                                      2,
                                                                      new int[] { 0, 0, 1, 1 },
                                                                      new int[][] { { 0, 2, 4 },
                                                                              { 6 }, { 1, 3, 5 },
                                                                              { 7 } });
        Cluster targetCluster = RebalanceUtils.createUpdatedCluster(currentCluster,
                                                                    3,
                                                                    Lists.newArrayList(2));
        targetCluster = RebalanceUtils.createUpdatedCluster(targetCluster, 1, Lists.newArrayList(3));

        // start servers
        List<Integer> serverList = Arrays.asList(0, 1, 2, 3);
        Map<String, String> configProps = new HashMap<String, String>();
        configProps.put("admin.max.threads", "5");
        if(serial)
            configProps.put("max.parallel.stores.rebalancing", String.valueOf(1));
        currentCluster = startServers(currentCluster,
                                      storeDefFileWithReplication,
                                      serverList,
                                      configProps);
        // Update the cluster information based on the node information
        targetCluster = updateCluster(targetCluster);

        RebalanceClientConfig config = new RebalanceClientConfig();
        config.setDeleteAfterRebalancingEnabled(true);
        config.setStealerBasedRebalancing(!useDonorBased);
        config.setPrimaryPartitionBatchSize(100);
        config.setMaxParallelRebalancing(5);
        RebalanceController rebalanceClient = new RebalanceController(getBootstrapUrl(currentCluster,
                                                                                      0),
                                                                      config);
        try {

            populateData(currentCluster, rwStoreDefWithReplication);

            rebalanceAndCheck(currentCluster,
                              targetCluster,
                              storeDefWithReplication,
                              rebalanceClient,
                              Arrays.asList(0, 1, 2, 3));
            checkConsistentMetadata(targetCluster, serverList);
        } finally {
            // stop servers
            stopServer(serverList);
        }
    }

    @Test(timeout = 600000)
    public void testRWRebalanceWithReplication() throws Exception {
        try {
            testRWRebalanceWithReplication(false);
        } catch(AssertionError ae) {
            logger.error("Assertion broken in testRWRebalanceWithReplication ", ae);
            throw ae;
        }
    }

    @Test(timeout = 600000)
    public void testRWRebalanceWithReplicationSerial() throws Exception {
        try {
            testRWRebalanceWithReplication(true);
        } catch(AssertionError ae) {
            logger.error("Assertion broken in testRWRebalanceWithReplicationSerial ", ae);
            throw ae;
        }
    }

    @Test(timeout = 600000)
    public void testRebalanceCleanPrimarySecondary() throws Exception {
        logger.info("Starting testRebalanceCleanPrimary");
        try {
            Cluster currentCluster = ServerTestUtils.getLocalZonedCluster(6, 2, new int[] { 0, 0,
                    0, 1, 1, 1 }, new int[][] { { 0 }, { 1, 6 }, { 2 }, { 3 }, { 4, 7 }, { 5 } });
            Cluster targetCluster = RebalanceUtils.createUpdatedCluster(currentCluster,
                                                                        2,
                                                                        Lists.newArrayList(7));
            targetCluster = RebalanceUtils.createUpdatedCluster(targetCluster,
                                                                5,
                                                                Lists.newArrayList(6));

            /**
             * original server partition ownership
             * 
             * [s0 : p0,p3,p4,p5,p6,p7] [s1 : p1-p7] [s2 : p1,p2] [s3 :
             * p0,p1,p2,p3,p6,p7] [s4 : p1-p7] [s5 : p4,p5]
             * 
             * target server partition ownership
             * 
             * [s0 : p0,p2,p3,p4,p5,p6,p7] [s1 : p0,p1] [s2 : p1-p7] [s3 :
             * p0.p1,p2,p3,p5,p6,p7] [s4 : p0,p1,p2,p3,p4,p7] [s5 : p4,p5,p6]
             */

            // start servers
            List<Integer> serverList = Arrays.asList(0, 1, 2, 3, 4, 5);
            Map<String, String> configProps = new HashMap<String, String>();
            configProps.put("enable.repair", "true");
            currentCluster = startServers(currentCluster,
                                          rwStoreDefFileWithReplication,
                                          serverList,
                                          configProps);
            // Update the cluster information based on the node information
            targetCluster = updateCluster(targetCluster);

            RebalanceClientConfig config = new RebalanceClientConfig();
            config.setDeleteAfterRebalancingEnabled(false);
            config.setStealerBasedRebalancing(!useDonorBased);
            RebalanceController rebalanceClient = new RebalanceController(getBootstrapUrl(currentCluster,
                                                                                          0),
                                                                          config);
            try {
                populateData(currentCluster, rwStoreDefWithReplication);

                AdminClient admin = rebalanceClient.getAdminClient();

                List<ByteArray> p6KeySamples = sampleKeysFromPartition(admin,
                                                                       1,
                                                                       rwStoreDefWithReplication.getName(),
                                                                       Arrays.asList(6),
                                                                       20);
                List<ByteArray> p1KeySamples = sampleKeysFromPartition(admin,
                                                                       1,
                                                                       rwStoreDefWithReplication.getName(),
                                                                       Arrays.asList(1),
                                                                       20);
                List<ByteArray> p3KeySamples = sampleKeysFromPartition(admin,
                                                                       0,
                                                                       rwStoreDefWithReplication.getName(),
                                                                       Arrays.asList(3),
                                                                       20);
                List<ByteArray> p2KeySamples = sampleKeysFromPartition(admin,
                                                                       1,
                                                                       rwStoreDefWithReplication.getName(),
                                                                       Arrays.asList(2),
                                                                       20);
                List<ByteArray> p7KeySamples = sampleKeysFromPartition(admin,
                                                                       4,
                                                                       rwStoreDefWithReplication.getName(),
                                                                       Arrays.asList(7),
                                                                       20);

                rebalanceAndCheck(currentCluster,
                                  targetCluster,
                                  Lists.newArrayList(rwStoreDefWithReplication),
                                  rebalanceClient,
                                  Arrays.asList(0, 1, 2, 3));
                checkConsistentMetadata(targetCluster, serverList);

                // Do the cleanup operation
                for(int i = 0; i < 6; i++) {
                    admin.storeMntOps.repairJob(i);
                }
                // wait for the repairs to complete
                for(int i = 0; i < 6; i++) {
                    ServerTestUtils.waitForAsyncOperationOnServer(serverMap.get(i), "Repair", 5000);
                }

                // confirm a primary movement in zone 0 : P6 : s1 -> S2. The
                // zone 0
                // primary changes when p6 moves cross zone
                // check for existence of p6 in server 2,
                checkForKeyExistence(admin, 2, rwStoreDefWithReplication.getName(), p6KeySamples);
                // also check for p6 absence in server 1.
                checkForKeyNonExistence(admin, 1, rwStoreDefWithReplication.getName(), p6KeySamples);

                // confirm a secondary movement in zone 0.. p2 : s1 -> s0
                // check for its existence in server 0
                checkForKeyExistence(admin, 0, rwStoreDefWithReplication.getName(), p2KeySamples);
                // check for its absernce in server 1
                checkForKeyNonExistence(admin, 1, rwStoreDefWithReplication.getName(), p2KeySamples);

                // also check that p1 is stable in server 1 [primary stability]
                checkForKeyExistence(admin, 1, rwStoreDefWithReplication.getName(), p1KeySamples);
                // check that p3 is stable in server 0 [Secondary stability]
                checkForKeyExistence(admin, 0, rwStoreDefWithReplication.getName(), p3KeySamples);

                // finally, test for server 4 which now became the secondary for
                // p7
                // from being a primary before
                checkForKeyExistence(admin, 4, rwStoreDefWithReplication.getName(), p7KeySamples);
            } finally {
                // stop servers
                stopServer(serverList);
            }
        } catch(AssertionError ae) {
            logger.error("Assertion broken in testRebalanceCleanPrimarySecondary ", ae);
            throw ae;
        }
    }

    @Test(timeout = 600000)
    public void testProxyGetDuringRebalancing() throws Exception {
        logger.info("Starting testProxyGetDuringRebalancing");
        try {
            Cluster currentCluster = ServerTestUtils.getLocalZonedCluster(4, 2, new int[] { 0, 0,
                    1, 1 }, new int[][] { { 0, 2, 4 }, { 6 }, { 1, 3, 5 }, { 7 } });
            Cluster targetCluster = RebalanceUtils.createUpdatedCluster(currentCluster,
                                                                        3,
                                                                        Lists.newArrayList(2));
            targetCluster = RebalanceUtils.createUpdatedCluster(targetCluster,
                                                                1,
                                                                Lists.newArrayList(3));

            final List<Integer> serverList = Arrays.asList(0, 1, 2, 3);
            Map<String, String> configProps = new HashMap<String, String>();
            configProps.put("admin.max.threads", "5");
            final Cluster updatedCurrentCluster = startServers(currentCluster,
                                                               storeDefFileWithReplication,
                                                               serverList,
                                                               configProps);
            // Update the cluster information based on the node information
            final Cluster updatedTargetCluster = updateCluster(targetCluster);

            ExecutorService executors = Executors.newFixedThreadPool(2);
            final AtomicBoolean rebalancingComplete = new AtomicBoolean(false);
            final List<Exception> exceptions = Collections.synchronizedList(new ArrayList<Exception>());

            RebalanceClientConfig rebalanceClientConfig = new RebalanceClientConfig();
            rebalanceClientConfig.setMaxParallelRebalancing(2);
            // Again, forced to use steal based since RO does not support donor
            // based yet.
            rebalanceClientConfig.setStealerBasedRebalancing(true);

            final RebalanceController rebalanceClient = new RebalanceController(getBootstrapUrl(updatedCurrentCluster,
                                                                                                0),
                                                                                rebalanceClientConfig);
            try {

                populateData(currentCluster, rwStoreDefWithReplication);

                final SocketStoreClientFactory factory = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(getBootstrapUrl(currentCluster,
                                                                                                                                          0))
                                                                                                        .setEnableLazy(false)
                                                                                                        .setSocketTimeout(120,
                                                                                                                          TimeUnit.SECONDS));

                final StoreClient<String, String> storeClientRW = new DefaultStoreClient<String, String>(rwStoreDefWithReplication.getName(),
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
                                } catch(Exception e) {
                                    logger.error("Exception in proxy get thread", e);
                                    e.printStackTrace();
                                    exceptions.add(e);
                                }
                            }

                        } catch(Exception e) {
                            logger.error("Exception in proxy get thread", e);
                            exceptions.add(e);
                        } finally {
                            factory.close();
                            latch.countDown();
                        }
                    }

                });

                executors.execute(new Runnable() {

                    @Override
                    public void run() {
                        try {

                            Thread.sleep(500);
                            rebalanceAndCheck(updatedCurrentCluster,
                                              updatedTargetCluster,
                                              storeDefWithReplication,
                                              rebalanceClient,
                                              Arrays.asList(0, 1, 2, 3));
                            Thread.sleep(500);
                            rebalancingComplete.set(true);
                            checkConsistentMetadata(updatedTargetCluster, serverList);

                        } catch(Exception e) {
                            exceptions.add(e);
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
            } finally {
                // stop servers
                stopServer(serverList);
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
            Cluster currentCluster = ServerTestUtils.getLocalZonedCluster(6, 2, new int[] { 0, 0,
                    0, 1, 1, 1 }, new int[][] { { 0 }, { 1, 6 }, { 2 }, { 3 }, { 4, 7 }, { 5 } });
            Cluster targetCluster = RebalanceUtils.createUpdatedCluster(currentCluster,
                                                                        2,
                                                                        Lists.newArrayList(7));
            targetCluster = RebalanceUtils.createUpdatedCluster(targetCluster,
                                                                5,
                                                                Lists.newArrayList(6));

            /**
             * original server partition ownership
             * 
             * [s0 : p0,p3,p4,p5,p6,p7] [s1 : p1-p7] [s2 : p1,p2] [s3 :
             * p0,p1,p2,p3,p6,p7] [s4 : p1-p7] [s5 : p4,p5]
             * 
             * target server partition ownership
             * 
             * [s0 : p0,p2,p3,p4,p5,p6,p7] [s1 : p0,p1] [s2 : p1-p7] [s3 :
             * p0.p1,p2,p3,p5,p6,p7] [s4 : p0,p1,p2,p3,p4,p7] [s5 : p4,p5,p6]
             */
            List<Integer> serverList = Arrays.asList(0, 1, 2, 3, 4, 5);
            Map<String, String> configProps = new HashMap<String, String>();
            configProps.put("admin.max.threads", "5");
            final Cluster updatedCurrentCluster = startServers(currentCluster,
                                                               rwStoreDefFileWithReplication,
                                                               serverList,
                                                               configProps);
            // Update the cluster information based on the node information
            final Cluster updatedTargetCluster = updateCluster(targetCluster);

            ExecutorService executors = Executors.newFixedThreadPool(2);
            final AtomicBoolean rebalancingComplete = new AtomicBoolean(false);
            final List<Exception> exceptions = Collections.synchronizedList(new ArrayList<Exception>());

            RebalanceClientConfig rebalanceClientConfig = new RebalanceClientConfig();
            rebalanceClientConfig.setMaxParallelRebalancing(2);
            rebalanceClientConfig.setStealerBasedRebalancing(!useDonorBased);

            final RebalanceController rebalanceClient = new RebalanceController(getBootstrapUrl(updatedCurrentCluster,
                                                                                                0),
                                                                                rebalanceClientConfig);

            populateData(currentCluster, rwStoreDefWithReplication);
            final AdminClient adminClient = rebalanceClient.getAdminClient();
            // the plan would cause the following cross zone move Partition :
            // Donor -> Stealer p6 (PRI) : 1 -> 5
            final List<ByteArray> movingKeysList = sampleKeysFromPartition(adminClient,
                                                                           1,
                                                                           rwStoreDefWithReplication.getName(),
                                                                           Arrays.asList(6),
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
                        // wait for the rebalancing to begin
                        List<VoldemortServer> serverList = Lists.newArrayList(serverMap.get(4),
                                                                              serverMap.get(2),
                                                                              serverMap.get(3),
                                                                              serverMap.get(5));
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

                        if(rebalancingStarted.get()) {
                            factory = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(getBootstrapUrl(updatedCurrentCluster,
                                                                                                                       0))
                                                                                     .setEnableLazy(false)
                                                                                     .setSocketTimeout(120,
                                                                                                       TimeUnit.SECONDS)
                                                                                     .setClientZoneId(1));

                            final StoreClient<String, String> storeClientRW = new DefaultStoreClient<String, String>(testStoreNameRW,
                                                                                                                     null,
                                                                                                                     factory,
                                                                                                                     3);
                            // Now perform some writes and determine the end
                            // state of the changed keys. Initially, all data
                            // now with zero vector clock
                            for(ByteArray movingKey: movingKeysList) {
                                try {
                                    String keyStr = ByteUtils.getString(movingKey.get(), "UTF-8");
                                    String valStr = "proxy_write";
                                    storeClientRW.put(keyStr, valStr);
                                    baselineTuples.put(keyStr, valStr);
                                    // all these keys will have [5:1] vector
                                    // clock is node 5 is the new pseudo master
                                    baselineVersions.get(keyStr)
                                                    .incrementVersion(5, System.currentTimeMillis());
                                    proxyWritesDone.set(true);
                                    if(rebalancingComplete.get()) {
                                        break;
                                    }
                                } catch(InvalidMetadataException e) {
                                    // let this go
                                    logger.error("Encountered an invalid metadata exception.. ", e);
                                }
                            }
                        }
                    } catch(Exception e) {
                        logger.error("Exception in proxy write thread..", e);
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
                        rebalanceClient.rebalance(updatedTargetCluster);
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
                                      updatedTargetCluster,
                                      Lists.newArrayList(rwStoreDefWithReplication),
                                      Arrays.asList(0, 1, 2, 3, 4, 5),
                                      baselineTuples,
                                      baselineVersions);
            checkConsistentMetadata(updatedTargetCluster, serverList);
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

    protected void populateData(Cluster cluster, StoreDefinition storeDef) throws Exception {

        // Create SocketStores for each Node first
        Map<Integer, Store<ByteArray, byte[], byte[]>> storeMap = new HashMap<Integer, Store<ByteArray, byte[], byte[]>>();
        for(Node node: cluster.getNodes()) {
            storeMap.put(node.getId(),
                         getSocketStore(storeDef.getName(), node.getHost(), node.getSocketPort()));
        }

        RoutingStrategy routing = new RoutingStrategyFactory().updateRoutingStrategy(storeDef,
                                                                                     cluster);
        StoreRoutingPlan storeInstance = new StoreRoutingPlan(cluster, storeDef);
        for(Entry<String, String> entry: testEntries.entrySet()) {
            ByteArray keyBytes = new ByteArray(ByteUtils.getBytes(entry.getKey(), "UTF-8"));
            List<Integer> preferenceNodes = storeInstance.getReplicationNodeList(keyBytes.get());
            // Go over every node
            for(int nodeId: preferenceNodes) {
                try {
                    storeMap.get(nodeId)
                            .put(keyBytes,
                                 new Versioned<byte[]>(ByteUtils.getBytes(entry.getValue(), "UTF-8")),
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
    }
}
