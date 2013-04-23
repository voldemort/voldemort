package voldemort.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.StoreClientFactory;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.server.VoldemortServer;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreUtils;
import voldemort.store.routed.NodeValue;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.versioning.ClockEntry;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.collect.Lists;

public class ClusterForkLiftToolTest {

    final static String STORES_XML = "test/common/voldemort/config/two-stores-replicated.xml";
    final static String PRIMARY_RESOLVING_STORE_NAME = "test";
    final static String GLOBALLY_RESOLVING_STORE_NAME = "best";
    final static String MULTIPLE_VERSIONS_STORE_NAME = "no-res";

    private String srcBootStrapUrl;
    private String dstBootStrapUrl;
    private Cluster srcCluster;
    private Cluster dstCluster;
    private VoldemortServer[] srcServers;
    private VoldemortServer[] dstServers;

    private StoreDefinition primaryResolvingStoreDef;
    private StoreDefinition globallyResolvingStoreDef;
    private StoreDefinition nonResolvingStoreDef;

    private HashMap<String, String> kvPairs;
    private String firstKey;
    private String lastKey;
    private String conflictKey;

    private AdminClient srcAdminClient;

    private StoreClientFactory srcfactory;
    private StoreClientFactory dstfactory;
    private StoreClient<String, String> srcPrimaryResolvingStoreClient;
    private StoreClient<String, String> dstPrimaryResolvingStoreClient;
    private StoreClient<String, String> srcGloballyResolvingStoreClient;
    private StoreClient<String, String> dstGloballyResolvingStoreClient;

    @Before
    public void setUpClusters() {
        // setup four nodes with one store and one partition
        final SocketStoreFactory socketStoreFactory = new ClientRequestExecutorPool(2,
                                                                                    10000,
                                                                                    100000,
                                                                                    32 * 1024);

        try {
            int srcPartitionMap[][] = { { 0 }, { 1 }, { 2 }, { 3 } };
            srcServers = new VoldemortServer[4];
            srcCluster = ServerTestUtils.startVoldemortCluster(4,
                                                               srcServers,
                                                               srcPartitionMap,
                                                               socketStoreFactory,
                                                               true,
                                                               null,
                                                               STORES_XML,
                                                               new Properties());
            Node node = srcCluster.getNodeById(0);
            srcBootStrapUrl = "tcp://" + node.getHost() + ":" + node.getSocketPort();

            int dstPartitionMap[][] = { { 0 }, { 1 }, { 2 } };
            dstServers = new VoldemortServer[3];
            dstCluster = ServerTestUtils.startVoldemortCluster(3,
                                                               dstServers,
                                                               dstPartitionMap,
                                                               socketStoreFactory,
                                                               true,
                                                               null,
                                                               STORES_XML,
                                                               new Properties());
            node = dstCluster.getNodeById(0);
            dstBootStrapUrl = "tcp://" + node.getHost() + ":" + node.getSocketPort();

            kvPairs = ServerTestUtils.createRandomKeyValueString(100);
            int keyCount = 0;
            for(String key: kvPairs.keySet()) {
                if(keyCount == 0)
                    firstKey = key;
                if(keyCount == kvPairs.size() - 1)
                    lastKey = key;
                if(keyCount == kvPairs.size() / 2)
                    conflictKey = key;
                keyCount++;
            }

            srcAdminClient = new AdminClient(srcCluster,
                                             new AdminClientConfig(),
                                             new ClientConfig());

            List<StoreDefinition> storeDefs = new StoreDefinitionsMapper().readStoreList(new File(STORES_XML));

            primaryResolvingStoreDef = StoreUtils.getStoreDef(storeDefs,
                                                              PRIMARY_RESOLVING_STORE_NAME);
            globallyResolvingStoreDef = StoreUtils.getStoreDef(storeDefs,
                                                               GLOBALLY_RESOLVING_STORE_NAME);

            nonResolvingStoreDef = StoreUtils.getStoreDef(storeDefs, MULTIPLE_VERSIONS_STORE_NAME);

            srcfactory = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(srcBootStrapUrl)
                                                                        .setSelectors(1)
                                                                        .setRoutingTimeout(1000,
                                                                                           java.util.concurrent.TimeUnit.MILLISECONDS)
                                                                        .setSocketTimeout(1000,
                                                                                          java.util.concurrent.TimeUnit.MILLISECONDS)
                                                                        .setConnectionTimeout(1000,
                                                                                              java.util.concurrent.TimeUnit.MILLISECONDS)
                                                                        .setMaxConnectionsPerNode(1));
            srcPrimaryResolvingStoreClient = srcfactory.getStoreClient(PRIMARY_RESOLVING_STORE_NAME);
            srcGloballyResolvingStoreClient = srcfactory.getStoreClient(GLOBALLY_RESOLVING_STORE_NAME);

            dstfactory = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(dstBootStrapUrl)
                                                                        .setSelectors(1)
                                                                        .setRoutingTimeout(1000,
                                                                                           java.util.concurrent.TimeUnit.MILLISECONDS)
                                                                        .setSocketTimeout(1000,
                                                                                          java.util.concurrent.TimeUnit.MILLISECONDS)
                                                                        .setConnectionTimeout(1000,
                                                                                              java.util.concurrent.TimeUnit.MILLISECONDS)
                                                                        .setMaxConnectionsPerNode(1));
            dstPrimaryResolvingStoreClient = dstfactory.getStoreClient(PRIMARY_RESOLVING_STORE_NAME);
            dstGloballyResolvingStoreClient = dstfactory.getStoreClient(GLOBALLY_RESOLVING_STORE_NAME);

        } catch(IOException e) {
            e.printStackTrace();
            fail("Unexpected exception");
        }
    }

    @Test
    public void testPrimaryResolvingForkLift() throws Exception {

        StoreInstance srcStoreInstance = new StoreInstance(srcCluster, primaryResolvingStoreDef);

        // populate data on the source cluster..
        for(Map.Entry<String, String> entry: kvPairs.entrySet()) {
            srcPrimaryResolvingStoreClient.put(entry.getKey(), entry.getValue());
        }

        // generate a conflict on the master partition
        int masterNode = srcStoreInstance.getNodeIdForPartitionId(srcStoreInstance.getMasterPartitionId(conflictKey.getBytes("UTF-8")));
        VectorClock losingClock = new VectorClock(Lists.newArrayList(new ClockEntry((short) 0, 5)),
                                                  System.currentTimeMillis());
        VectorClock winningClock = new VectorClock(Lists.newArrayList(new ClockEntry((short) 1, 5)),
                                                   losingClock.getTimestamp() + 1);
        srcAdminClient.storeOps.putNodeKeyValue(PRIMARY_RESOLVING_STORE_NAME,
                                                new NodeValue<ByteArray, byte[]>(masterNode,
                                                                                 new ByteArray(conflictKey.getBytes("UTF-8")),
                                                                                 new Versioned<byte[]>("losing value".getBytes("UTF-8"),
                                                                                                       losingClock)));
        srcAdminClient.storeOps.putNodeKeyValue(PRIMARY_RESOLVING_STORE_NAME,
                                                new NodeValue<ByteArray, byte[]>(masterNode,
                                                                                 new ByteArray(conflictKey.getBytes("UTF-8")),
                                                                                 new Versioned<byte[]>("winning value".getBytes("UTF-8"),
                                                                                                       winningClock)));

        // do a write to destination cluster
        dstPrimaryResolvingStoreClient.put(firstKey, "before forklift");

        // perform the forklifting..
        ClusterForkLiftTool forkLiftTool = new ClusterForkLiftTool(srcBootStrapUrl,
                                                                   dstBootStrapUrl,
                                                                   10000,
                                                                   1,
                                                                   1000,
                                                                   Lists.newArrayList(PRIMARY_RESOLVING_STORE_NAME),
                                                                   null,
                                                                   ClusterForkLiftTool.ForkLiftTaskMode.primary_resolution);
        forkLiftTool.run();

        // do a write to destination cluster
        dstPrimaryResolvingStoreClient.put(lastKey, "after forklift");

        // verify data on the destination is as expected
        for(Map.Entry<String, String> entry: kvPairs.entrySet()) {
            if(entry.getKey().equals(firstKey)) {
                assertEquals("Online write overwritten",
                             dstPrimaryResolvingStoreClient.get(firstKey).getValue(),
                             "before forklift");
            } else if(entry.getKey().equals(lastKey)) {
                assertEquals("Online write overwritten",
                             dstPrimaryResolvingStoreClient.get(lastKey).getValue(),
                             "after forklift");
            } else if(entry.getKey().equals(conflictKey)) {
                assertEquals("Conflict resolution incorrect",
                             dstPrimaryResolvingStoreClient.get(conflictKey).getValue(),
                             "winning value");
            } else {
                assertEquals("fork lift data missing",
                             dstPrimaryResolvingStoreClient.get(entry.getKey()).getValue(),
                             entry.getValue());
            }
        }
    }

    @Test
    public void testGloballyResolvingForkLift() throws Exception {

        StoreInstance srcStoreInstance = new StoreInstance(srcCluster, globallyResolvingStoreDef);

        // populate data on the source cluster..
        for(Map.Entry<String, String> entry: kvPairs.entrySet()) {
            srcGloballyResolvingStoreClient.put(entry.getKey(), entry.getValue());
        }

        // generate a conflict on the primary and a secondary
        List<Integer> nodeList = srcStoreInstance.getReplicationNodeList(srcStoreInstance.getMasterPartitionId(conflictKey.getBytes("UTF-8")));
        VectorClock losingClock = new VectorClock(Lists.newArrayList(new ClockEntry((short) 0, 5)),
                                                  System.currentTimeMillis());
        VectorClock winningClock = new VectorClock(Lists.newArrayList(new ClockEntry((short) 1, 5)),
                                                   losingClock.getTimestamp() + 1);
        srcAdminClient.storeOps.putNodeKeyValue(GLOBALLY_RESOLVING_STORE_NAME,
                                                new NodeValue<ByteArray, byte[]>(nodeList.get(0),
                                                                                 new ByteArray(conflictKey.getBytes("UTF-8")),
                                                                                 new Versioned<byte[]>("losing value".getBytes("UTF-8"),
                                                                                                       losingClock)));
        srcAdminClient.storeOps.putNodeKeyValue(GLOBALLY_RESOLVING_STORE_NAME,
                                                new NodeValue<ByteArray, byte[]>(nodeList.get(1),
                                                                                 new ByteArray(conflictKey.getBytes("UTF-8")),
                                                                                 new Versioned<byte[]>("winning value".getBytes("UTF-8"),
                                                                                                       winningClock)));

        // do a write to destination cluster
        dstGloballyResolvingStoreClient.put(firstKey, "before forklift");

        // perform the forklifting..
        ClusterForkLiftTool forkLiftTool = new ClusterForkLiftTool(srcBootStrapUrl,
                                                                   dstBootStrapUrl,
                                                                   10000,
                                                                   1,
                                                                   1000,
                                                                   Lists.newArrayList(GLOBALLY_RESOLVING_STORE_NAME),
                                                                   null,
                                                                   ClusterForkLiftTool.ForkLiftTaskMode.global_resolution);
        forkLiftTool.run();

        // do a write to destination cluster
        dstGloballyResolvingStoreClient.put(lastKey, "after forklift");

        // verify data on the destination is as expected
        for(Map.Entry<String, String> entry: kvPairs.entrySet()) {
            if(entry.getKey().equals(firstKey)) {
                assertEquals("Online write overwritten",
                             dstGloballyResolvingStoreClient.get(firstKey).getValue(),
                             "before forklift");
            } else if(entry.getKey().equals(lastKey)) {
                assertEquals("Online write overwritten",
                             dstGloballyResolvingStoreClient.get(lastKey).getValue(),
                             "after forklift");
            } else if(entry.getKey().equals(conflictKey)) {
                assertEquals("Conflict resolution incorrect",
                             dstGloballyResolvingStoreClient.get(conflictKey).getValue(),
                             "winning value");
            } else {
                assertEquals("fork lift data missing",
                             dstGloballyResolvingStoreClient.get(entry.getKey()).getValue(),
                             entry.getValue());
            }
        }
    }

    @Test
    public void testNoresolutionForkLift() throws Exception {

        int versions = 0;

        StoreInstance srcStoreInstance = new StoreInstance(srcCluster, nonResolvingStoreDef);

        // generate a conflict on the master partition
        int masterNode = srcStoreInstance.getNodeIdForPartitionId(srcStoreInstance.getMasterPartitionId(conflictKey.getBytes("UTF-8")));
        VectorClock losingClock = new VectorClock(Lists.newArrayList(new ClockEntry((short) 0, 5)),
                                                  System.currentTimeMillis());
        VectorClock winningClock = new VectorClock(Lists.newArrayList(new ClockEntry((short) 1, 5)),
                                                   losingClock.getTimestamp() + 1);
        srcAdminClient.storeOps.putNodeKeyValue(MULTIPLE_VERSIONS_STORE_NAME,
                                                new NodeValue<ByteArray, byte[]>(masterNode,
                                                                                 new ByteArray(conflictKey.getBytes("UTF-8")),
                                                                                 new Versioned<byte[]>("losing value".getBytes("UTF-8"),
                                                                                                       losingClock)));
        srcAdminClient.storeOps.putNodeKeyValue(MULTIPLE_VERSIONS_STORE_NAME,
                                                new NodeValue<ByteArray, byte[]>(masterNode,
                                                                                 new ByteArray(conflictKey.getBytes("UTF-8")),
                                                                                 new Versioned<byte[]>("winning value".getBytes("UTF-8"),
                                                                                                       winningClock)));
        // perform the forklifting..
        ClusterForkLiftTool forkLiftTool = new ClusterForkLiftTool(srcBootStrapUrl,
                                                                   dstBootStrapUrl,
                                                                   10000,
                                                                   1,
                                                                   1000,
                                                                   Lists.newArrayList(MULTIPLE_VERSIONS_STORE_NAME),
                                                                   null,
                                                                   ClusterForkLiftTool.ForkLiftTaskMode.no_resolution);
        forkLiftTool.run();

        AdminClient dstAdminClient = new AdminClient(dstBootStrapUrl,
                                                     new AdminClientConfig(),
                                                     new ClientConfig());

        for(Node node: dstAdminClient.getAdminClientCluster().getNodes()) {

            Iterator<Pair<ByteArray, Versioned<byte[]>>> entryItr = srcAdminClient.bulkFetchOps.fetchEntries(node.getId(),
                                                                                                             MULTIPLE_VERSIONS_STORE_NAME,
                                                                                                             node.getPartitionIds(),
                                                                                                             null,
                                                                                                             true);

            while(entryItr.hasNext()) {
                Pair<ByteArray, Versioned<byte[]>> record = entryItr.next();
                ByteArray key = record.getFirst();
                Versioned<byte[]> versioned = record.getSecond();
                versions++;

            }

        }
        assertEquals("Both conflicting versions present", versions, 2);

    }

    @After
    public void tearDownClusters() {

        srcAdminClient.close();

        srcfactory.close();
        dstfactory.close();

        for(VoldemortServer server: srcServers)
            server.stop();
        for(VoldemortServer server: dstServers)
            server.stop();
    }
}
