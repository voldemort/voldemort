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
import voldemort.client.ClientConfig;
import voldemort.client.DefaultStoreClient;
import voldemort.client.RoutingTier;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
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

    static Cluster zzCurrent;
    static Cluster zzShuffle;
    static Cluster zzClusterExpansionNN;
    static Cluster zzClusterExpansionPP;
    static String zzStoresXml;
    static List<StoreDefinition> zzStores;

    static Cluster zzzCurrent;
    static Cluster zzzShuffle;
    static Cluster zzzClusterExpansionNNN;
    static Cluster zzzClusterExpansionPPP;
    static Cluster zzeZoneExpansion;
    static Cluster zzzZoneExpansionXXP;
    static String zzzStoresXml;
    static List<StoreDefinition> zzzStores;

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
        setUpRWStuff();
        setupZZandZZZ();
    }

    public static void setupZZandZZZ() throws IOException {
        zzCurrent = ClusterTestUtils.getZZCluster();
        zzShuffle = ClusterTestUtils.getZZClusterWithSwappedPartitions();
        zzClusterExpansionNN = ClusterTestUtils.getZZClusterWithNN();
        zzClusterExpansionPP = ClusterTestUtils.getZZClusterWithPP();

        zzStores = ClusterTestUtils.getZZStoreDefsBDB();
        File zzfile = File.createTempFile("zz-stores-", ".xml");
        FileUtils.writeStringToFile(zzfile, new StoreDefinitionsMapper().writeStoreList(zzStores));
        zzStoresXml = zzfile.getAbsolutePath();

        zzzCurrent = ClusterTestUtils.getZZZCluster();
        zzzShuffle = ClusterTestUtils.getZZZClusterWithSwappedPartitions();
        zzzClusterExpansionNNN = ClusterTestUtils.getZZZClusterWithNNN();
        zzzClusterExpansionPPP = ClusterTestUtils.getZZZClusterWithPPP();
        zzeZoneExpansion = ClusterTestUtils.getZZECluster();
        zzzZoneExpansionXXP = ClusterTestUtils.getZZEClusterXXP();

        zzzStores = ClusterTestUtils.getZZZStoreDefsBDB();
        File zzzfile = File.createTempFile("zzz-stores-", ".xml");
        FileUtils.writeStringToFile(zzzfile, new StoreDefinitionsMapper().writeStoreList(zzzStores));
        zzzStoresXml = zzzfile.getAbsolutePath();
    }

    public void setUpRWStuff() throws IOException {
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

    // TODO: (currentCluster vs interimCluster) Ideally, we could go from
    // cCluster to fCluster for zone expansion. Unfortunately, to start a
    // VoldemortServer, you need a cluster xml that includes that server. For
    // now, we assume interim cluster is deployed (i.e., cluster with empty
    // nodes in new zones). Either, deploying interim cluster with empty nodes
    // must be codified in run book and tested as a pre-condition or servers
    // need to be able to start without a cluster xml that includes them.

    // TODO: The tests based on this method are susceptible to TOCTOU
    // BindException issue since findFreePorts is used to determine the ports
    // for localhost:PORT of each node.
    /**
     * Scripts the execution of a specific type of zoned rebalance test: sets up
     * cluster based on cCluster plus any new nodes/zones in fCluster,
     * rebalances to fCluster, verifies rebalance was correct.
     * 
     * @param testTag For pretty printing
     * @param cCluster current cluster
     * @param fCluster final cluster
     * @param cStoresXml XML file with current stores xml
     * @param fStoresXml Unused parameter. Included for symmetry in method
     *        declaration.
     * @param cStoreDefs store defs for current cluster (from on cStoresXml)
     * @param fStoreDefs store defs for final cluster.
     * @throws Exception
     */
    public void testZonedRebalance(String testTag,
                                   Cluster cCluster,
                                   Cluster fCluster,
                                   String cStoresXml,
                                   String fStoresXml,
                                   List<StoreDefinition> cStoreDefs,
                                   List<StoreDefinition> fStoreDefs) throws Exception {
        logger.info("Starting " + testTag);
        // Hacky work around of TOCTOU bind Exception issues. Each test that
        // invokes this method brings servers up & down on the same ports. The
        // OS seems to need a rest between subsequent tests...
        Thread.sleep(TimeUnit.SECONDS.toMillis(2));
        try {
            Cluster interimCluster = RebalanceUtils.getInterimCluster(cCluster, fCluster);

            // start all the servers
            List<Integer> serverList = new ArrayList<Integer>(interimCluster.getNodeIds());
            Map<String, String> configProps = new HashMap<String, String>();
            configProps.put("admin.max.threads", "5");
            interimCluster = startServers(interimCluster, cStoresXml, serverList, configProps);

            String bootstrapUrl = getBootstrapUrl(interimCluster, 0);
            boolean stealerBased = !useDonorBased;
            ClusterTestUtils.RebalanceKit rebalanceKit = ClusterTestUtils.getRebalanceKit(bootstrapUrl,
                                                                                          stealerBased,
                                                                                          fCluster,
                                                                                          fStoreDefs);

            try {
                for(StoreDefinition storeDef: cStoreDefs) {
                    populateData(cCluster, storeDef);
                }

                rebalanceAndCheck(rebalanceKit.plan, rebalanceKit.controller, serverList);

                checkConsistentMetadata(fCluster, serverList);
            } finally {
                // stop servers
                stopServer(serverList);
            }
        } catch(AssertionError ae) {
            logger.error("Assertion broken in " + testTag + " : ", ae);
            throw ae;
        }
    }

    public void testZonedRebalance(String testTag,
                                   Cluster cCluster,
                                   Cluster fCluster,
                                   String storesXml,
                                   List<StoreDefinition> storeDefs) throws Exception {
        testZonedRebalance(testTag, cCluster, fCluster, storesXml, storesXml, storeDefs, storeDefs);
    }

    @Test(timeout = 600000)
    public void testNoopZZ() throws Exception {
        testZonedRebalance("TestNoopZZ", zzCurrent, zzCurrent, zzStoresXml, zzStores);
    }

    @Test(timeout = 600000)
    public void testShuffleZZ() throws Exception {
        testZonedRebalance("TestShuffleZZ", zzCurrent, zzShuffle, zzStoresXml, zzStores);
    }

    @Test(timeout = 600000)
    public void testClusterExpansion() throws Exception {
        testZonedRebalance("TestClusterExpansionZZ",
                           zzCurrent,
                           zzClusterExpansionPP,
                           zzStoresXml,
                           zzStores);
    }

    @Test(timeout = 600000)
    public void testNoopZZZ() throws Exception {
        testZonedRebalance("TestNoopZZZ", zzzCurrent, zzzCurrent, zzzStoresXml, zzzStores);
    }

    @Test(timeout = 600000)
    public void testShuffleZZZ() throws Exception {
        testZonedRebalance("TestShuffleZZZ", zzzCurrent, zzzShuffle, zzzStoresXml, zzzStores);
    }

    @Test(timeout = 600000)
    public void testClusterExpansionZZZ() throws Exception {
        testZonedRebalance("TestClusterExpansionZZZ",
                           zzzCurrent,
                           zzzClusterExpansionPPP,
                           zzzStoresXml,
                           zzzStores);
    }

    @Test(timeout = 600000)
    public void testZoneExpansionZZ2ZZZ() throws Exception {
        // TODO: see todo for method testZonedRebalance to understand why we
        // cannot invoke the following:
        /*-
        testZonedRebalance("TestZoneExpansionZZ2ZZZ",
                           zzCurrent,
                           zzzZoneExpansionXXP,
                           zzStoresXml,
                           zzzStoresXml,
                           zzStores,
                           zzzStores);
         */
        testZonedRebalance("TestZoneExpansionZZ2ZZZ",
                           zzeZoneExpansion,
                           zzzZoneExpansionXXP,
                           zzzStoresXml,
                           zzzStores);
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

            String bootstrapUrl = getBootstrapUrl(currentCluster, 0);
            boolean stealerBased = !useDonorBased;
            ClusterTestUtils.RebalanceKit rebalanceKit = ClusterTestUtils.getRebalanceKit(bootstrapUrl,
                                                                                          stealerBased,
                                                                                          targetCluster);

            try {
                populateData(currentCluster, rwStoreDefWithoutReplication);

                rebalanceAndCheck(rebalanceKit.plan, rebalanceKit.controller, Arrays.asList(1, 2));

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

        String bootstrapUrl = getBootstrapUrl(currentCluster, 0);
        int maxParallel = 5;
        boolean stealerBased = !useDonorBased;
        ClusterTestUtils.RebalanceKit rebalanceKit = ClusterTestUtils.getRebalanceKit(bootstrapUrl,
                                                                                      maxParallel,
                                                                                      stealerBased,
                                                                                      targetCluster);

        try {

            populateData(currentCluster, rwStoreDefWithReplication);

            rebalanceAndCheck(rebalanceKit.plan, rebalanceKit.controller, Arrays.asList(0, 1, 2, 3));

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

            String bootstrapUrl = getBootstrapUrl(currentCluster, 0);
            boolean stealerBased = !useDonorBased;
            ClusterTestUtils.RebalanceKit rebalanceKit = ClusterTestUtils.getRebalanceKit(bootstrapUrl,
                                                                                          stealerBased,
                                                                                          targetCluster);

            try {
                populateData(currentCluster, rwStoreDefWithReplication);

                AdminClient admin = rebalanceKit.controller.getAdminClient();

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

                rebalanceAndCheck(rebalanceKit.plan,
                                  rebalanceKit.controller,
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

                // confirm a secondary movement in zone 0.. p2 : s1 -> s0
                // check for its existence in server 0
                checkForKeyExistence(admin, 0, rwStoreDefWithReplication.getName(), p2KeySamples);
                // check for its absernce in server 1

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
            Cluster tmpTargetCluster = RebalanceUtils.createUpdatedCluster(currentCluster,
                                                                           3,
                                                                           Lists.newArrayList(2));
            final Cluster targetCluster = RebalanceUtils.createUpdatedCluster(tmpTargetCluster,
                                                                              1,
                                                                              Lists.newArrayList(3));

            final List<Integer> serverList = Arrays.asList(0, 1, 2, 3);
            Map<String, String> configProps = new HashMap<String, String>();
            configProps.put("admin.max.threads", "5");
            final Cluster updatedCurrentCluster = startServers(currentCluster,
                                                               storeDefFileWithReplication,
                                                               serverList,
                                                               configProps);

            ExecutorService executors = Executors.newFixedThreadPool(2);
            final AtomicBoolean rebalancingComplete = new AtomicBoolean(false);
            final List<Exception> exceptions = Collections.synchronizedList(new ArrayList<Exception>());

            String bootstrapUrl = getBootstrapUrl(updatedCurrentCluster, 0);
            int maxParallel = 2;
            // Forced to use steal since RO does not support donor based.
            boolean stealerBased = true;
            final ClusterTestUtils.RebalanceKit rebalanceKit = ClusterTestUtils.getRebalanceKit(bootstrapUrl,
                                                                                                maxParallel,
                                                                                                stealerBased,
                                                                                                targetCluster);

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
                            rebalanceAndCheck(rebalanceKit.plan,
                                              rebalanceKit.controller,
                                              Arrays.asList(0, 1, 2, 3));

                            Thread.sleep(500);
                            rebalancingComplete.set(true);
                            checkConsistentMetadata(targetCluster, serverList);

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
             * Original partition map
             * 
             * [s0 : p0] [s1 : p1, p6] [s2 : p2]
             * 
             * [s3 : p3] [s4 : p4, p7] [s5 : p5]
             * 
             * Target server partition ownership
             * 
             * [s0 : p0] [s1 : p1] [s2 : p2, p7]
             * 
             * [s3 : p3] [s4 : p4] [s5 : p5, p6]
             * 
             * Note that rwStoreDefFileWithReplication is a "2/1/1" store def.
             * 
             * Original server n-ary partition ownership
             * 
             * [s0 : p0, p3-7] [s1 : p0-p7] [s2 : p1-2]
             * 
             * [s3 : p0-3, p6-7] [s4 : p0-p7] [s5 : p4-5]
             * 
             * Target server n-ary partition ownership
             * 
             * [s0 : p0, p2-7] [s1 : p0-1] [s2 : p1-p7]
             * 
             * [s3 : p0-3, p5-7] [s4 : p0-4, p7] [s5 : p4-6]
             */
            List<Integer> serverList = Arrays.asList(0, 1, 2, 3, 4, 5);
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
            // cluster topology then. getRebalanceKit uses batch size of
            // infinite, so this should be fine.
            String bootstrapUrl = getBootstrapUrl(updatedCurrentCluster, 0);
            int maxParallel = 2;
            boolean stealerBased = !useDonorBased;
            final ClusterTestUtils.RebalanceKit rebalanceKit = ClusterTestUtils.getRebalanceKit(bootstrapUrl,
                                                                                                maxParallel,
                                                                                                stealerBased,
                                                                                                targetCluster);

            populateData(currentCluster, rwStoreDefWithReplication);
            final AdminClient adminClient = rebalanceKit.controller.getAdminClient();
            // the plan would cause these partitions to move:
            // Partition : Donor -> stealer
            //
            // p2 (Z-SEC) : s1 -> s0
            // p3-6 (Z-PRI) : s1 -> s2
            // p7 (Z-PRI) : s0 -> s2
            //
            // p5 (Z-SEC): s4 -> s3
            // p6 (Z-PRI): s4 -> s5
            //
            // :. rebalancing will run on servers 0, 2, 3, & 5
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
                        List<VoldemortServer> serverList = Lists.newArrayList(serverMap.get(0),
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
                                      targetCluster,
                                      Lists.newArrayList(rwStoreDefWithReplication),
                                      Arrays.asList(0, 1, 2, 3, 4, 5),
                                      baselineTuples,
                                      baselineVersions);
            checkConsistentMetadata(targetCluster, serverList);
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

    private void populateData(Cluster cluster, StoreDefinition storeDef) throws Exception {

        // Create SocketStores for each Node first
        Map<Integer, Store<ByteArray, byte[], byte[]>> storeMap = new HashMap<Integer, Store<ByteArray, byte[], byte[]>>();
        for(Node node: cluster.getNodes()) {
            storeMap.put(node.getId(),
                         getSocketStore(storeDef.getName(), node.getHost(), node.getSocketPort()));
        }

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
