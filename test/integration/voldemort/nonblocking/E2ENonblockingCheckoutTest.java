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

package voldemort.nonblocking;

import static org.junit.Assert.assertFalse;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
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
import voldemort.store.StoreDefinition;
import voldemort.store.StoreDefinitionBuilder;
import voldemort.store.bdb.BdbStorageConfiguration;
import voldemort.store.memory.InMemoryStorageConfiguration;
import voldemort.store.slow.SlowStorageConfiguration;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.versioning.ObsoleteVersionException;

/**
 * Does an end-to-end unit test of a Voldemort cluster with slow storage
 * servers. Confirms that client puts which are issued in an asynchronous manner
 * do not block upon the slow servers.
 * 
 */
public class E2ENonblockingCheckoutTest {

    private static final String STORE_NAME = "test";

    private static final int NUM_CLIENTS = 2;
    private static final int NUM_PUTS = 25;
    // Exempt some puts from performance requirements until warmed up
    private static final int NUM_EXEMPT_PUTS = 2;
    private static final long MAX_PUT_TIME_MS = 50;
    private static final long SLOW_PUT_MS = 250;
    /*
     * Need to space out each set of concurrent client put operations to allow
     * prior puts to be completed in the background by the slow server. The plus
     * one ensures that all background work completes before the next period of
     * put operations (assuming SLOW_PUT_MS >> MAX_PUT_TIME_MS).
     */
    private static final long PUT_PERIODICITY_MS = (NUM_CLIENTS + 1) * SLOW_PUT_MS;

    // Ensure that threads will contend at all servers
    private static final int CONNECTIONS_PER_NODE = 1;

    private static final int CONNECTION_TIMEOUT_MS = 500; // 10 * 1000;
    private static final int SOCKET_TIMEOUT_MS = 2 * 1000; // 100 * 1000;
    private static final int ROUTING_TIMEOUT_MS = 10 * 1000; // 100 * 1000;

    private final SocketStoreFactory socketStoreFactory = new ClientRequestExecutorPool(CONNECTIONS_PER_NODE,
                                                                                        CONNECTION_TIMEOUT_MS,
                                                                                        SOCKET_TIMEOUT_MS,
                                                                                        32 * 1024);
    private final boolean useNio;

    private List<VoldemortServer> servers;
    private Cluster cluster;
    StoreClientFactory storeClientFactory;

    public E2ENonblockingCheckoutTest() {
        this.useNio = true;
    }

    public static List<StoreDefinition> getStoreDef(int nodeId) {
        List<StoreDefinition> defs = new ArrayList<StoreDefinition>();
        SerializerDefinition serDef = new SerializerDefinition("string");
        String storageConfiguration = InMemoryStorageConfiguration.TYPE_NAME;
        if(nodeId == 2) {
            storageConfiguration = SlowStorageConfiguration.TYPE_NAME;
        }
        defs.add(new StoreDefinitionBuilder().setName(STORE_NAME)
                                             .setType(storageConfiguration)
                                             .setKeySerializer(serDef)
                                             .setValueSerializer(serDef)
                                             .setRoutingPolicy(RoutingTier.SERVER)
                                             .setRoutingStrategyType(RoutingStrategyType.CONSISTENT_STRATEGY)
                                             .setReplicationFactor(3)
                                             .setPreferredReads(1)
                                             .setRequiredReads(1)
                                             .setPreferredWrites(1)
                                             .setRequiredWrites(1)
                                             .build());
        return defs;
    }

    @Before
    public void setUp() throws Exception {
        // PatternLayout patternLayout = new
        // PatternLayout("%d{ABSOLUTE} %-5p [%t/%c]: %m%n");

        Logger logger;
        /*-
        // To analyze whether checkout/checkin paths are blocking add
        // log4j.trace statements to KeyedResourcePool checkout/checkin methods.
         */
        logger = Logger.getLogger("voldemort.store.socket.clientrequest.ClientRequestExecutorPool");
        logger.setLevel(Level.TRACE);

        logger = Logger.getLogger("voldemort.utils.pool.KeyedResourcePool");
        logger.setLevel(Level.TRACE);

        logger = Logger.getLogger("voldemort.utils.pool.QueuedKeyedResourcePool");
        logger.setLevel(Level.TRACE);

        logger = Logger.getLogger("voldemort.store.socket.SocketStore");
        logger.setLevel(Level.DEBUG);

        logger = Logger.getLogger("voldemort.store.routed.action.PerformParallelPutRequests");
        logger.setLevel(Level.DEBUG);

        logger = Logger.getLogger("voldemort.store.routed.action.PerformSerialPutRequests");
        logger.setLevel(Level.DEBUG);

        cluster = ServerTestUtils.getLocalCluster(3, new int[][] { { 0, 3 }, { 1, 4 }, { 2, 5 } });
        servers = new ArrayList<VoldemortServer>();
        Properties p = new Properties();
        String storageConfigs = BdbStorageConfiguration.class.getName() + ","
                                + InMemoryStorageConfiguration.class.getName() + ","
                                + SlowStorageConfiguration.class.getName();
        p.setProperty("storage.configs", storageConfigs);
        p.setProperty("slow.queueing.put.ms", Long.toString(SLOW_PUT_MS));

        p.setProperty("client.connection.timeout.ms", Integer.toString(CONNECTION_TIMEOUT_MS));
        p.setProperty("client.routing.timeout.ms", Integer.toString(ROUTING_TIMEOUT_MS));

        for(int i = 0; i < 3; i++) {
            VoldemortConfig voldemortConfig = ServerTestUtils.createServerConfigWithDefs(this.useNio,
                                                                                         i,
                                                                                         TestUtils.createTempDir()
                                                                                                  .getAbsolutePath(),
                                                                                         cluster,
                                                                                         getStoreDef(i),
                                                                                         p);
            VoldemortServer voldemortServer = ServerTestUtils.startVoldemortServer(socketStoreFactory,
                                                                                   voldemortConfig);
            servers.add(voldemortServer);
        }

        Node node = cluster.getNodeById(0);
        String bootstrapUrl = "tcp://" + node.getHost() + ":" + node.getSocketPort();
        storeClientFactory = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(bootstrapUrl)
                                                                            .setMaxConnectionsPerNode(CONNECTIONS_PER_NODE)
                                                                            .setConnectionTimeout(CONNECTION_TIMEOUT_MS,
                                                                                                  TimeUnit.MILLISECONDS));
    }

    @After
    public void tearDown() {
        socketStoreFactory.close();
    }

    public class Putter implements Runnable {

        private StoreClient<String, String> storeClient;
        private final CountDownLatch signal;
        private final int puts;
        private final int offsetOrVal;
        private final boolean useOffset;
        private final long putTimeLimitMs;

        /**
         * 
         * @param signal
         * @param puts Number of puts to do.
         * @param offsetOrVal Offset or value to put. (See useOffset.)
         * @param useOffset If true, then Putter will do puts to key 'putCount +
         *        offsetOrVal'. If false, then Putter will do puts to key
         *        'offsetOrVal'
         * @param putTimeLimitMs Time limit in ms.
         */
        Putter(CountDownLatch signal,
               int puts,
               int offsetOrVal,
               boolean useOffset,
               long putTimeLimitMs) {
            storeClient = storeClientFactory.getStoreClient(STORE_NAME);
            this.signal = signal;
            this.puts = puts;
            this.offsetOrVal = offsetOrVal;
            this.useOffset = useOffset;
            this.putTimeLimitMs = putTimeLimitMs;
        }

        private void sleepUntilNextPeriod() {
            long currentTimeMs = System.currentTimeMillis();
            long complement = currentTimeMs % PUT_PERIODICITY_MS;
            try {
                TimeUnit.MILLISECONDS.sleep(PUT_PERIODICITY_MS - complement);
            } catch(InterruptedException e) {
                e.printStackTrace();
            }
        }

        public String getString(int putCount) {
            if(useOffset) {
                return Integer.toString(putCount + this.offsetOrVal);
            } else {
                return Integer.toString(this.offsetOrVal);
            }
        }

        public void run() {
            for(int i = 0; i < puts; ++i) {
                sleepUntilNextPeriod();
                String I = getString(i);
                System.out.println("");
                String context = new String("PUT of " + I + " (Put #: " + i + ", Thread: "
                                            + Thread.currentThread().getName() + ")");
                System.out.println("START " + context);
                long startTimeMs = System.currentTimeMillis();
                try {
                    storeClient.put(I, I);
                } catch(ObsoleteVersionException e) {
                    System.out.println("ObsoleteVersionException caught on put." + context);
                }
                long endTimeMs = System.currentTimeMillis();
                System.out.println(" DONE " + context + " --- Time (ms): "
                                   + (endTimeMs - startTimeMs));
                if(i >= NUM_EXEMPT_PUTS) {
                    assertFalse("Operation completes without blocking on slow server:"
                                        + (endTimeMs - startTimeMs),
                                (endTimeMs - startTimeMs) > this.putTimeLimitMs);
                    if((endTimeMs - startTimeMs) > this.putTimeLimitMs) {
                        System.err.println("Operation blocked! Therefore, operation is not nonblocking... "
                                           + context
                                           + " (Operation time: "
                                           + (endTimeMs - startTimeMs) + " ms)");
                    }
                }
            }
            sleepUntilNextPeriod();
            System.out.println("Thread done. (Thread: " + Thread.currentThread().getName() + ")");
            signal.countDown();
        }
    }

    /**
     * Multiple clients periodically & simultaneously do puts to the same key.
     * This means the puts all use the same master. To determine whether
     * parallel puts are actually non-blocking, then figure out a key that maps
     * to a fast master so that the slow server is used by the parallel phase of
     * put.
     */
    @Test
    public void testPutToSameKey() {
        CountDownLatch waitForPutters = new CountDownLatch(NUM_CLIENTS);

        System.out.println("PRE THREAD CREATION");
        for(int i = 0; i < NUM_CLIENTS; i++) {
            System.out.println("THREAD CREATION");
            /*
             * Key "12" uses node 1, a fast node as the master. This ensures
             * that the parallel put requests first use node 2, the slow node,
             * and then use node 0, the other fast node. If checking out a
             * connection to the slow node is done asynchronously, then
             * operations should not block on the slow node before returning
             * (with secondary puts outstanding in the background). Confirmed
             * this was doing the right thing (i.e., picking the nodes in the
             * order 1, 2, 0) by adding a temporary logger.debug print to
             * PerformSerialPutRequest and visually inspecting the console
             * output.
             */
            new Thread(new Putter(waitForPutters, NUM_PUTS, 12, false, MAX_PUT_TIME_MS)).start();

        }
        System.out.println("POST THREAD CREATION");
        try {
            waitForPutters.await();
            System.out.println("POST AWAIT");
        } catch(InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Multiple clients periodically & simultaneously do puts to distinct keys.
     * Different keys map to potentially different masters. So, sometimes the
     * serial put within the ParallelPut goes to the slow node. This test
     * generates many distinct interleavings of the concurrent put operations
     * interacting with the servers (slow & fast).
     */
    @Test
    public void testPutToDifferentKeys() {
        CountDownLatch waitForPutters = new CountDownLatch(NUM_CLIENTS);

        System.out.println("PRE THREAD CREATION");
        for(int i = 0; i < NUM_CLIENTS; i++) {
            System.out.println("THREAD CREATION");
            // timeoutMs set to accommodate all threads choosing the slow node
            // as the master.
            long timeoutMs = MAX_PUT_TIME_MS + (NUM_CLIENTS * SLOW_PUT_MS);
            new Thread(new Putter(waitForPutters, NUM_PUTS, NUM_PUTS * i, true, timeoutMs)).start();
        }
        System.out.println("POST THREAD CREATION");
        try {
            waitForPutters.await();
            System.out.println("POST AWAIT");
        } catch(InterruptedException e) {
            e.printStackTrace();
        }
    }
}
