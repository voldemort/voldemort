/*
 * Copyright 2012 LinkedIn, Inc
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

package voldemort.socketpool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.client.ClientConfig;
import voldemort.client.RoutingTier;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.StoreClientFactory;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategyType;
import voldemort.serialization.SerializerDefinition;
import voldemort.server.VoldemortConfig;
import voldemort.server.VoldemortServer;
import voldemort.store.InsufficientOperationalNodesException;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreDefinitionBuilder;
import voldemort.store.bdb.BdbStorageConfiguration;
import voldemort.store.memory.InMemoryStorageConfiguration;
import voldemort.store.slop.strategy.HintedHandoffStrategyType;
import voldemort.store.slow.SlowStorageConfiguration;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.versioning.ObsoleteVersionException;

/**
 * Does an end-to-end unit test of a Voldemort cluster with in memory storage
 * servers. Applies so much load that timeouts and connection resets are
 * expected.
 * 
 */
public class E2EClientRequestExecutorPoolAndFailureDetectorTest {

    private final boolean useNio = true;
    private static final String STORE_NAME = "test";

    private Random random = new Random();

    private static final int KEY_RANGE = 100;
    private static final int SOCKET_BUFFER_SIZE = 32 * 1024;
    private static final boolean SOCKET_KEEP_ALIVE = false;

    private static final int CONNECTION_TIMEOUT_MS = 20;
    private static final int SOCKET_TIMEOUT_MS = 40;
    private static final int ROUTING_TIMEOUT_MS = 40;

    private SocketStoreFactory socketStoreFactory = null;

    private final int numServers = 4;
    private List<VoldemortServer> servers;
    private Cluster cluster;
    StoreClientFactory storeClientFactory;

    public E2EClientRequestExecutorPoolAndFailureDetectorTest() {}

    public static List<StoreDefinition> getStoreDef() {
        List<StoreDefinition> defs = new ArrayList<StoreDefinition>();
        SerializerDefinition serDef = new SerializerDefinition("string");
        String storageConfiguration = SlowStorageConfiguration.TYPE_NAME;
        defs.add(new StoreDefinitionBuilder().setName(STORE_NAME)
                                             .setType(storageConfiguration)
                                             .setKeySerializer(serDef)
                                             .setValueSerializer(serDef)
                                             .setRoutingPolicy(RoutingTier.SERVER)
                                             .setRoutingStrategyType(RoutingStrategyType.CONSISTENT_STRATEGY)
                                             .setHintedHandoffStrategy(HintedHandoffStrategyType.PROXIMITY_STRATEGY)
                                             .setReplicationFactor(3)
                                             .setPreferredReads(1)
                                             .setRequiredReads(1)
                                             .setPreferredWrites(1)
                                             .setRequiredWrites(1)
                                             .build());
        return defs;
    }

    public void setUp(int opSlowMs, int numSelectors, int connectionsPerNode) throws Exception {
        socketStoreFactory = new ClientRequestExecutorPool(numSelectors,
                                                           connectionsPerNode,
                                                           CONNECTION_TIMEOUT_MS,
                                                           SOCKET_TIMEOUT_MS,
                                                           SOCKET_BUFFER_SIZE,
                                                           SOCKET_KEEP_ALIVE);

        cluster = ServerTestUtils.getLocalCluster(numServers, new int[][] { { 0, 4 }, { 1, 5 },
                { 2, 6 }, { 3, 7 } });
        servers = new ArrayList<VoldemortServer>();
        Properties p = new Properties();
        String storageConfigs = BdbStorageConfiguration.class.getName() + ","
                                + InMemoryStorageConfiguration.class.getName() + ","
                                + SlowStorageConfiguration.class.getName();
        p.setProperty("storage.configs", storageConfigs);
        p.setProperty("testing.slow.queueing.put.ms", Long.toString(opSlowMs));
        p.setProperty("testing.slow.queueing.get.ms", Long.toString(opSlowMs));

        // Susceptible to BindException...
        for(int i = 0; i < numServers; i++) {
            VoldemortConfig voldemortConfig = ServerTestUtils.createServerConfigWithDefs(this.useNio,
                                                                                         i,
                                                                                         TestUtils.createTempDir()
                                                                                                  .getAbsolutePath(),
                                                                                         cluster,
                                                                                         getStoreDef(),
                                                                                         p);
            VoldemortServer voldemortServer = ServerTestUtils.startVoldemortServer(socketStoreFactory,
                                                                                   voldemortConfig);
            servers.add(voldemortServer);
        }

        Node node = cluster.getNodeById(0);
        String bootstrapUrl = "tcp://" + node.getHost() + ":" + node.getSocketPort();
        storeClientFactory = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(bootstrapUrl)
                                                                            .setMaxConnectionsPerNode(connectionsPerNode)
                                                                            .setConnectionTimeout(CONNECTION_TIMEOUT_MS,
                                                                                                  TimeUnit.MILLISECONDS)
                                                                            .setSocketTimeout(SOCKET_TIMEOUT_MS,
                                                                                              TimeUnit.MILLISECONDS)
                                                                            .setRoutingTimeout(ROUTING_TIMEOUT_MS,
                                                                                               TimeUnit.MILLISECONDS)
                                                                            .setFailureDetectorThreshold(99)
                                                                            .setFailureDetectorThresholdInterval(250));
    }

    public void tearDown() throws IOException {
        // Servers
        for(VoldemortServer server: servers) {
            ServerTestUtils.stopVoldemortServer(server);
        }
        servers = null;
        cluster = null;

        // Clients
        storeClientFactory.close();
        storeClientFactory = null;
        socketStoreFactory.close();
        socketStoreFactory = null;
    }

    public abstract class Oper implements Runnable {

        private final StoreClient<String, String> storeClient;
        private final CountDownLatch startSignal;
        private final CountDownLatch doneSignal;
        private final int numOps;

        private int numIONEs;

        Oper(CountDownLatch startSignal, CountDownLatch doneSignal, int numOps) {
            this.startSignal = startSignal;
            this.doneSignal = doneSignal;
            this.numOps = numOps;

            this.numIONEs = 0;

            storeClient = storeClientFactory.getStoreClient(STORE_NAME);
        }

        public String getKey() {
            return new Integer(random.nextInt(KEY_RANGE)).toString();
        }

        public String getValue() {
            return "Value ...............................................................................................................";
        }

        abstract public void doOp();

        @Override
        public void run() {
            startSignal.countDown();
            try {
                try {
                    startSignal.await();
                } catch(InterruptedException e) {
                    e.printStackTrace();
                    return;
                }

                for(int i = 0; i < this.numOps; ++i) {
                    try {
                        doOp();
                    } catch(InsufficientOperationalNodesException ione) {
                        this.numIONEs++;
                        // System.out.println("Caught an IONE");
                        try {
                            Thread.sleep(250);
                        } catch(InterruptedException ie) {
                            // Noop
                        }
                    }
                    if(i > 0 && i % 500 == 0) {
                        System.out.println("oper making progress ... (IONES = " + this.numIONEs
                                           + ", op count = " + i + ")");
                    }
                }

            } finally {
                doneSignal.countDown();
            }
            if(this.numIONEs > 0)
                System.out.println("Number of IONEs: " + this.numIONEs);
        }
    }

    public class Putter extends Oper {

        Putter(CountDownLatch startSignal, CountDownLatch doneSignal, int numOps) {
            super(startSignal, doneSignal, numOps);
        }

        @Override
        public void doOp() {
            String key = getKey();
            String value = getValue();
            try {
                super.storeClient.put(key, value);
            } catch(ObsoleteVersionException e) {
                // System.out.println("ObsoleteVersionException caught on put.");
            }
        }
    }

    public class Getter extends Oper {

        Getter(CountDownLatch startSignal, CountDownLatch doneSignal, int numOps) {
            super(startSignal, doneSignal, numOps);
        }

        @Override
        public void doOp() {
            String key = getKey();
            super.storeClient.get(key);
        }
    }

    public void doStressTest(int numPutters, int numGetters, int numOps) {
        int numOpers = numPutters + numGetters;
        CountDownLatch waitForStart = new CountDownLatch(numOpers);
        CountDownLatch waitForDone = new CountDownLatch(numOpers);

        for(int i = 0; i < numPutters; ++i) {
            new Thread(new Putter(waitForStart, waitForDone, numOps)).start();
        }
        for(int i = 0; i < numGetters; ++i) {
            new Thread(new Getter(waitForStart, waitForDone, numOps)).start();
        }

        try {
            waitForDone.await();
        } catch(InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void runStressTest(int opSlowMs,
                              int numSelectors,
                              int connectionsPerNode,
                              int numPutters,
                              int numGetters,
                              int numOps) {
        System.out.println("STARTING: opSlowMs (" + opSlowMs + "), numSelectors (" + numSelectors
                           + "), connectionsPerNode (" + connectionsPerNode + ") putters ("
                           + numPutters + "), getters (" + numGetters + "), and ops (" + numOps
                           + ").");
        try {
            setUp(opSlowMs, numSelectors, connectionsPerNode);
            doStressTest(numPutters, numGetters, numOps);
            tearDown();
        } catch(Exception ex) {
            ex.printStackTrace();
        } finally {
            System.out.println("ENDING...");
            System.out.println("***********************************************************************************");
        }
    }

    @Test
    public void stressTest() {
        final int OP_SLOW_MS = 2;

        final int NUM_SELECTORS_START = 2;
        final int NUM_SELECTORS_END = 4;
        final int NUM_SELECTORS_STEP = 1;

        final int CONNECTIONS_PER_NODE_START = 5;
        final int CONNECTIONS_PER_NODE_END = 20;
        final int CONNECTIONS_PER_NODE_STEP = 5;

        final int NUM_PUTTERS_START = 25;
        final int NUM_PUTTERS_END = 100;
        final int NUM_PUTTERS_STEP = 25;

        final int NUM_GETTERS_START = 25;
        final int NUM_GETTERS_END = 100;
        final int NUM_GETTERS_STEP = 25;

        final int NUM_OPS = 2 * 1000;

        for(int putters = NUM_PUTTERS_START; putters <= NUM_PUTTERS_END; putters += NUM_PUTTERS_STEP) {
            for(int getters = NUM_GETTERS_START; getters <= NUM_GETTERS_END; getters += NUM_GETTERS_STEP) {
                for(int numSelectors = NUM_SELECTORS_START; numSelectors <= NUM_SELECTORS_END; numSelectors += NUM_SELECTORS_STEP) {
                    for(int connectionsPerNode = CONNECTIONS_PER_NODE_START; connectionsPerNode <= CONNECTIONS_PER_NODE_END; connectionsPerNode += CONNECTIONS_PER_NODE_STEP) {
                        if(putters + getters > 0) {
                            runStressTest(OP_SLOW_MS,
                                          numSelectors,
                                          connectionsPerNode,
                                          putters,
                                          getters,
                                          NUM_OPS);
                        }
                    }
                }
            }
        }
    }
}
