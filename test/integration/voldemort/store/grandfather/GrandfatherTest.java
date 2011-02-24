package voldemort.store.grandfather;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.VoldemortException;
import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.StoreClientFactory;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.rebalance.MigratePartitions;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.Zone;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.routing.RoutingStrategyType;
import voldemort.server.VoldemortConfig;
import voldemort.server.VoldemortServer;
import voldemort.store.StoreDefinition;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.slop.strategy.HintedHandoffStrategyType;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.versioning.Versioned;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class GrandfatherTest {

    private final int NUM_NODES = 6;
    private int NUM_ZONES = 2;
    private final int NUM_KEYS = 10000;
    private final int NUMBER_STORES = 3;
    private Cluster consistentRoutingCluster;
    private Cluster consistentRoutingClusterModified;
    private Cluster zoneRoutingCluster;
    private Cluster zoneRoutingClusterModified;
    private HashMap<Integer, Integer> zoneReplicationFactors;
    private StoreClientFactory factory;
    protected Map<Integer, VoldemortServer> serverMap = new HashMap<Integer, VoldemortServer>();
    protected SocketStoreFactory socketStoreFactory;

    private List<Node> getBestNodes(Cluster cluster) {
        List<Node> newNodes = Lists.newArrayList();

        int nodesInSingleZone = cluster.getNumberOfNodes() / 2;

        for(int nodeId = 0; nodeId < nodesInSingleZone; nodeId++) {
            List<Integer> partitions = Lists.newArrayList();
            partitions.addAll(cluster.getNodeById(nodeId).getPartitionIds());
            partitions.addAll(cluster.getNodeById(nodeId + nodesInSingleZone).getPartitionIds());

            Collections.shuffle(partitions);

            List<Integer> partitionsNode1 = partitions.subList(0, partitions.size() / 2);
            List<Integer> partitionsNode2 = partitions.subList(partitions.size() / 2,
                                                               partitions.size());

            newNodes.add(new Node(cluster.getNodeById(nodeId).getId(),
                                  cluster.getNodeById(nodeId).getHost(),
                                  cluster.getNodeById(nodeId).getHttpPort(),
                                  cluster.getNodeById(nodeId).getSocketPort(),
                                  cluster.getNodeById(nodeId).getAdminPort(),
                                  cluster.getNodeById(nodeId).getZoneId(),
                                  partitionsNode1));

            newNodes.add(new Node(cluster.getNodeById(nodeId + nodesInSingleZone).getId(),
                                  cluster.getNodeById(nodeId + nodesInSingleZone).getHost(),
                                  cluster.getNodeById(nodeId + nodesInSingleZone).getHttpPort(),
                                  cluster.getNodeById(nodeId + nodesInSingleZone).getSocketPort(),
                                  cluster.getNodeById(nodeId + nodesInSingleZone).getAdminPort(),
                                  cluster.getNodeById(nodeId + nodesInSingleZone).getZoneId(),
                                  partitionsNode2));

        }

        return newNodes;
    }

    @Test
    public void testMultipleStores() throws IOException, InterruptedException {
        List<Zone> zones = ServerTestUtils.getZones(NUM_ZONES);

        final Cluster firstCluster = ServerTestUtils.getLocalCluster(NUM_NODES, 10, NUM_ZONES);
        final Cluster secondCluster = new Cluster("b", getBestNodes(firstCluster), zones);

        final List<StoreDefinition> storeDefs = Lists.newArrayList();

        for(int storeNo = 1; storeNo <= NUMBER_STORES; storeNo++) {
            StoreDefinition def = null;
            if(NUM_ZONES == 1) {
                def = ServerTestUtils.getStoreDef("test" + storeNo,
                                                  storeNo,
                                                  1,
                                                  1,
                                                  1,
                                                  1,
                                                  RoutingStrategyType.CONSISTENT_STRATEGY);
            } else {
                HashMap<Integer, Integer> zoneRepFactor = Maps.newHashMap();
                for(int zoneId = 0; zoneId < NUM_ZONES; zoneId++) {
                    zoneRepFactor.put(zoneId, storeNo);
                }

                def = ServerTestUtils.getStoreDef("test" + storeNo,
                                                  1,
                                                  1,
                                                  1,
                                                  1,
                                                  0,
                                                  0,
                                                  zoneRepFactor,
                                                  HintedHandoffStrategyType.PROXIMITY_STRATEGY,
                                                  RoutingStrategyType.ZONE_STRATEGY);
            }
            storeDefs.add(def);

        }

        test(firstCluster, secondCluster, storeDefs, storeDefs);
    }

    @Test
    public void testConsistentToConsistent() throws IOException, InterruptedException {
        StoreDefinition storeDefRepFactor = ServerTestUtils.getStoreDef("test1",
                                                                        1,
                                                                        1,
                                                                        1,
                                                                        1,
                                                                        1,
                                                                        RoutingStrategyType.CONSISTENT_STRATEGY);
        test(consistentRoutingCluster,
             consistentRoutingClusterModified,
             Lists.newArrayList(storeDefRepFactor),
             Lists.newArrayList(storeDefRepFactor));

        StoreDefinition storeDefRepFactor2 = ServerTestUtils.getStoreDef("test1",
                                                                         2,
                                                                         2,
                                                                         2,
                                                                         2,
                                                                         2,
                                                                         RoutingStrategyType.CONSISTENT_STRATEGY);
        test(consistentRoutingCluster,
             consistentRoutingClusterModified,
             Lists.newArrayList(storeDefRepFactor2),
             Lists.newArrayList(storeDefRepFactor2));
    }

    @Test
    public void testConsistentToConsistentWithRepChange() throws IOException, InterruptedException {
        StoreDefinition beforeStoreDef = ServerTestUtils.getStoreDef("test1",
                                                                     1,
                                                                     1,
                                                                     1,
                                                                     1,
                                                                     1,
                                                                     RoutingStrategyType.CONSISTENT_STRATEGY);
        StoreDefinition afterStoreDef = ServerTestUtils.getStoreDef("test1",
                                                                    2,
                                                                    2,
                                                                    2,
                                                                    2,
                                                                    2,
                                                                    RoutingStrategyType.CONSISTENT_STRATEGY);
        test(consistentRoutingCluster,
             consistentRoutingClusterModified,
             Lists.newArrayList(beforeStoreDef),
             Lists.newArrayList(afterStoreDef));

        beforeStoreDef = ServerTestUtils.getStoreDef("test1",
                                                     2,
                                                     2,
                                                     2,
                                                     2,
                                                     2,
                                                     RoutingStrategyType.CONSISTENT_STRATEGY);
        afterStoreDef = ServerTestUtils.getStoreDef("test1",
                                                    3,
                                                    3,
                                                    3,
                                                    3,
                                                    3,
                                                    RoutingStrategyType.CONSISTENT_STRATEGY);
        test(consistentRoutingCluster,
             consistentRoutingClusterModified,
             Lists.newArrayList(beforeStoreDef),
             Lists.newArrayList(afterStoreDef));
    }

    @Test
    public void testZoneToZone() throws IOException, InterruptedException {
        for(int zoneIds = 0; zoneIds < NUM_ZONES; zoneIds++) {
            zoneReplicationFactors.put(zoneIds, 1);
        }
        StoreDefinition storeDefRepFactor1 = ServerTestUtils.getStoreDef("test1",
                                                                         2,
                                                                         2,
                                                                         2,
                                                                         2,
                                                                         1,
                                                                         1,
                                                                         zoneReplicationFactors,
                                                                         HintedHandoffStrategyType.PROXIMITY_STRATEGY,
                                                                         RoutingStrategyType.ZONE_STRATEGY);
        test(zoneRoutingCluster,
             zoneRoutingClusterModified,
             Lists.newArrayList(storeDefRepFactor1),
             Lists.newArrayList(storeDefRepFactor1));

        zoneReplicationFactors = Maps.newHashMap();
        for(int zoneIds = 0; zoneIds < NUM_ZONES; zoneIds++) {
            zoneReplicationFactors.put(zoneIds, 2);
        }
        StoreDefinition storeDefRepFactor2 = ServerTestUtils.getStoreDef("test1",
                                                                         4,
                                                                         4,
                                                                         4,
                                                                         4,
                                                                         1,
                                                                         1,
                                                                         zoneReplicationFactors,
                                                                         HintedHandoffStrategyType.PROXIMITY_STRATEGY,
                                                                         RoutingStrategyType.ZONE_STRATEGY);
        test(zoneRoutingCluster,
             zoneRoutingClusterModified,
             Lists.newArrayList(storeDefRepFactor2),
             Lists.newArrayList(storeDefRepFactor2));
    }

    @Test
    public void testZoneToZoneWithRepChange() throws IOException, InterruptedException {
        for(int zoneIds = 0; zoneIds < NUM_ZONES; zoneIds++) {
            zoneReplicationFactors.put(zoneIds, 1);
        }
        StoreDefinition beforeStoreDef = ServerTestUtils.getStoreDef("test1",
                                                                     1,
                                                                     1,
                                                                     1,
                                                                     1,
                                                                     1,
                                                                     1,
                                                                     zoneReplicationFactors,
                                                                     HintedHandoffStrategyType.PROXIMITY_STRATEGY,
                                                                     RoutingStrategyType.ZONE_STRATEGY);

        zoneReplicationFactors = Maps.newHashMap();
        for(int zoneIds = 0; zoneIds < NUM_ZONES; zoneIds++) {
            zoneReplicationFactors.put(zoneIds, 2);
        }
        StoreDefinition afterStoreDef = ServerTestUtils.getStoreDef("test1",
                                                                    1,
                                                                    1,
                                                                    1,
                                                                    1,
                                                                    1,
                                                                    1,
                                                                    zoneReplicationFactors,
                                                                    HintedHandoffStrategyType.PROXIMITY_STRATEGY,
                                                                    RoutingStrategyType.ZONE_STRATEGY);

        test(zoneRoutingCluster,
             zoneRoutingClusterModified,
             Lists.newArrayList(beforeStoreDef),
             Lists.newArrayList(afterStoreDef));
    }

    @Test
    public void testConsistentToZone() throws IOException, InterruptedException {
        for(int zoneIds = 0; zoneIds < NUM_ZONES; zoneIds++) {
            zoneReplicationFactors.put(zoneIds, 1);
        }
        StoreDefinition beforeStoreDef = ServerTestUtils.getStoreDef("test1",
                                                                     1,
                                                                     1,
                                                                     1,
                                                                     1,
                                                                     1,
                                                                     RoutingStrategyType.CONSISTENT_STRATEGY);
        StoreDefinition afterStoreDef = ServerTestUtils.getStoreDef("test1",
                                                                    2,
                                                                    2,
                                                                    2,
                                                                    2,
                                                                    0,
                                                                    0,
                                                                    zoneReplicationFactors,
                                                                    HintedHandoffStrategyType.PROXIMITY_STRATEGY,
                                                                    RoutingStrategyType.ZONE_STRATEGY);

        test(consistentRoutingCluster,
             zoneRoutingCluster,
             Lists.newArrayList(beforeStoreDef),
             Lists.newArrayList(afterStoreDef));

        test(consistentRoutingCluster,
             zoneRoutingClusterModified,
             Lists.newArrayList(beforeStoreDef),
             Lists.newArrayList(afterStoreDef));

        // With rep factor = 2
        zoneReplicationFactors = Maps.newHashMap();
        for(int zoneIds = 0; zoneIds < NUM_ZONES; zoneIds++) {
            zoneReplicationFactors.put(zoneIds, 2);
        }
        beforeStoreDef = ServerTestUtils.getStoreDef("test1",
                                                     2,
                                                     2,
                                                     2,
                                                     2,
                                                     2,
                                                     RoutingStrategyType.CONSISTENT_STRATEGY);
        afterStoreDef = ServerTestUtils.getStoreDef("test1",
                                                    2,
                                                    2,
                                                    2,
                                                    2,
                                                    1,
                                                    1,
                                                    zoneReplicationFactors,
                                                    HintedHandoffStrategyType.PROXIMITY_STRATEGY,
                                                    RoutingStrategyType.ZONE_STRATEGY);

        test(consistentRoutingCluster,
             zoneRoutingCluster,
             Lists.newArrayList(beforeStoreDef),
             Lists.newArrayList(afterStoreDef));

        test(consistentRoutingCluster,
             zoneRoutingClusterModified,
             Lists.newArrayList(beforeStoreDef),
             Lists.newArrayList(afterStoreDef));
    }

    @Test
    public void testConsistentToZoneMultiStore() throws IOException, InterruptedException {
        // 1C + 1C => 2Z + 2Z
        for(int zoneIds = 0; zoneIds < NUM_ZONES; zoneIds++) {
            zoneReplicationFactors.put(zoneIds, 1);
        }
        StoreDefinition beforeStoreDef1 = ServerTestUtils.getStoreDef("test1",
                                                                      1,
                                                                      1,
                                                                      1,
                                                                      1,
                                                                      1,
                                                                      RoutingStrategyType.CONSISTENT_STRATEGY), beforeStoreDef2 = ServerTestUtils.getStoreDef("test2",
                                                                                                                                                              1,
                                                                                                                                                              1,
                                                                                                                                                              1,
                                                                                                                                                              1,
                                                                                                                                                              1,
                                                                                                                                                              RoutingStrategyType.CONSISTENT_STRATEGY);
        StoreDefinition afterStoreDef1 = ServerTestUtils.getStoreDef("test1",
                                                                     2,
                                                                     2,
                                                                     2,
                                                                     2,
                                                                     1,
                                                                     1,
                                                                     zoneReplicationFactors,
                                                                     HintedHandoffStrategyType.PROXIMITY_STRATEGY,
                                                                     RoutingStrategyType.ZONE_STRATEGY), afterStoreDef2 = ServerTestUtils.getStoreDef("test2",
                                                                                                                                                      2,
                                                                                                                                                      2,
                                                                                                                                                      2,
                                                                                                                                                      2,
                                                                                                                                                      1,
                                                                                                                                                      1,
                                                                                                                                                      zoneReplicationFactors,
                                                                                                                                                      HintedHandoffStrategyType.PROXIMITY_STRATEGY,
                                                                                                                                                      RoutingStrategyType.ZONE_STRATEGY);

        test(consistentRoutingCluster,
             zoneRoutingClusterModified,
             Lists.newArrayList(beforeStoreDef1, beforeStoreDef2),
             Lists.newArrayList(afterStoreDef1, afterStoreDef2));

        // 2C + 2C => 4Z + 4Z
        zoneReplicationFactors = Maps.newHashMap();
        for(int zoneIds = 0; zoneIds < NUM_ZONES; zoneIds++) {
            zoneReplicationFactors.put(zoneIds, 2);
        }
        beforeStoreDef1 = ServerTestUtils.getStoreDef("test1",
                                                      2,
                                                      2,
                                                      2,
                                                      2,
                                                      2,
                                                      RoutingStrategyType.CONSISTENT_STRATEGY);
        beforeStoreDef2 = ServerTestUtils.getStoreDef("test2",
                                                      2,
                                                      2,
                                                      2,
                                                      2,
                                                      2,
                                                      RoutingStrategyType.CONSISTENT_STRATEGY);
        afterStoreDef1 = ServerTestUtils.getStoreDef("test1",
                                                     2,
                                                     2,
                                                     2,
                                                     2,
                                                     1,
                                                     1,
                                                     zoneReplicationFactors,
                                                     HintedHandoffStrategyType.PROXIMITY_STRATEGY,
                                                     RoutingStrategyType.ZONE_STRATEGY);
        afterStoreDef2 = ServerTestUtils.getStoreDef("test2",
                                                     2,
                                                     2,
                                                     2,
                                                     2,
                                                     1,
                                                     1,
                                                     zoneReplicationFactors,
                                                     HintedHandoffStrategyType.PROXIMITY_STRATEGY,
                                                     RoutingStrategyType.ZONE_STRATEGY);

        test(consistentRoutingCluster,
             zoneRoutingClusterModified,
             Lists.newArrayList(beforeStoreDef1, beforeStoreDef2),
             Lists.newArrayList(afterStoreDef1, afterStoreDef2));

        // 1C + 2C => 2Z + 4Z
        beforeStoreDef1 = ServerTestUtils.getStoreDef("test1",
                                                      1,
                                                      1,
                                                      1,
                                                      1,
                                                      1,
                                                      RoutingStrategyType.CONSISTENT_STRATEGY);
        beforeStoreDef2 = ServerTestUtils.getStoreDef("test2",
                                                      2,
                                                      2,
                                                      2,
                                                      2,
                                                      2,
                                                      RoutingStrategyType.CONSISTENT_STRATEGY);
        zoneReplicationFactors = Maps.newHashMap();
        for(int zoneIds = 0; zoneIds < NUM_ZONES; zoneIds++) {
            zoneReplicationFactors.put(zoneIds, 1);
        }
        afterStoreDef1 = ServerTestUtils.getStoreDef("test1",
                                                     2,
                                                     2,
                                                     2,
                                                     2,
                                                     1,
                                                     1,
                                                     zoneReplicationFactors,
                                                     HintedHandoffStrategyType.PROXIMITY_STRATEGY,
                                                     RoutingStrategyType.ZONE_STRATEGY);
        zoneReplicationFactors = Maps.newHashMap();
        for(int zoneIds = 0; zoneIds < NUM_ZONES; zoneIds++) {
            zoneReplicationFactors.put(zoneIds, 2);
        }
        afterStoreDef2 = ServerTestUtils.getStoreDef("test2",
                                                     2,
                                                     2,
                                                     2,
                                                     2,
                                                     1,
                                                     1,
                                                     zoneReplicationFactors,
                                                     HintedHandoffStrategyType.PROXIMITY_STRATEGY,
                                                     RoutingStrategyType.ZONE_STRATEGY);

        test(consistentRoutingCluster,
             zoneRoutingClusterModified,
             Lists.newArrayList(beforeStoreDef1, beforeStoreDef2),
             Lists.newArrayList(afterStoreDef1, afterStoreDef2));

        // 2C + 3C => 4Z + 6Z
        beforeStoreDef1 = ServerTestUtils.getStoreDef("test1",
                                                      2,
                                                      2,
                                                      2,
                                                      2,
                                                      2,
                                                      RoutingStrategyType.CONSISTENT_STRATEGY);
        beforeStoreDef2 = ServerTestUtils.getStoreDef("test2",
                                                      3,
                                                      3,
                                                      3,
                                                      3,
                                                      3,
                                                      RoutingStrategyType.CONSISTENT_STRATEGY);
        zoneReplicationFactors = Maps.newHashMap();
        for(int zoneIds = 0; zoneIds < NUM_ZONES; zoneIds++) {
            zoneReplicationFactors.put(zoneIds, 2);
        }
        afterStoreDef1 = ServerTestUtils.getStoreDef("test1",
                                                     2,
                                                     2,
                                                     2,
                                                     2,
                                                     1,
                                                     1,
                                                     zoneReplicationFactors,
                                                     HintedHandoffStrategyType.PROXIMITY_STRATEGY,
                                                     RoutingStrategyType.ZONE_STRATEGY);
        zoneReplicationFactors = Maps.newHashMap();
        for(int zoneIds = 0; zoneIds < NUM_ZONES; zoneIds++) {
            zoneReplicationFactors.put(zoneIds, 3);
        }
        afterStoreDef2 = ServerTestUtils.getStoreDef("test2",
                                                     2,
                                                     2,
                                                     2,
                                                     2,
                                                     1,
                                                     1,
                                                     zoneReplicationFactors,
                                                     HintedHandoffStrategyType.PROXIMITY_STRATEGY,
                                                     RoutingStrategyType.ZONE_STRATEGY);

        test(consistentRoutingCluster,
             zoneRoutingClusterModified,
             Lists.newArrayList(beforeStoreDef1, beforeStoreDef2),
             Lists.newArrayList(afterStoreDef1, afterStoreDef2));

        beforeStoreDef1 = ServerTestUtils.getStoreDef("test1",
                                                      1,
                                                      1,
                                                      1,
                                                      1,
                                                      1,
                                                      RoutingStrategyType.CONSISTENT_STRATEGY);
        beforeStoreDef2 = ServerTestUtils.getStoreDef("test2",
                                                      2,
                                                      2,
                                                      2,
                                                      2,
                                                      2,
                                                      RoutingStrategyType.CONSISTENT_STRATEGY);
        StoreDefinition beforeStoreDef3 = ServerTestUtils.getStoreDef("test3",
                                                                      3,
                                                                      3,
                                                                      3,
                                                                      3,
                                                                      3,
                                                                      RoutingStrategyType.CONSISTENT_STRATEGY);
        zoneReplicationFactors = Maps.newHashMap();
        for(int zoneIds = 0; zoneIds < NUM_ZONES; zoneIds++) {
            zoneReplicationFactors.put(zoneIds, 1);
        }
        afterStoreDef1 = ServerTestUtils.getStoreDef("test1",
                                                     2,
                                                     2,
                                                     2,
                                                     2,
                                                     1,
                                                     1,
                                                     zoneReplicationFactors,
                                                     HintedHandoffStrategyType.PROXIMITY_STRATEGY,
                                                     RoutingStrategyType.ZONE_STRATEGY);
        zoneReplicationFactors = Maps.newHashMap();
        for(int zoneIds = 0; zoneIds < NUM_ZONES; zoneIds++) {
            zoneReplicationFactors.put(zoneIds, 2);
        }
        afterStoreDef2 = ServerTestUtils.getStoreDef("test2",
                                                     2,
                                                     2,
                                                     2,
                                                     2,
                                                     1,
                                                     1,
                                                     zoneReplicationFactors,
                                                     HintedHandoffStrategyType.PROXIMITY_STRATEGY,
                                                     RoutingStrategyType.ZONE_STRATEGY);

        zoneReplicationFactors = Maps.newHashMap();
        for(int zoneIds = 0; zoneIds < NUM_ZONES; zoneIds++) {
            zoneReplicationFactors.put(zoneIds, 3);
        }
        StoreDefinition afterStoreDef3 = ServerTestUtils.getStoreDef("test3",
                                                                     2,
                                                                     2,
                                                                     2,
                                                                     2,
                                                                     1,
                                                                     1,
                                                                     zoneReplicationFactors,
                                                                     HintedHandoffStrategyType.PROXIMITY_STRATEGY,
                                                                     RoutingStrategyType.ZONE_STRATEGY);

        test(consistentRoutingCluster,
             zoneRoutingClusterModified,
             Lists.newArrayList(beforeStoreDef1, beforeStoreDef2, beforeStoreDef3),
             Lists.newArrayList(afterStoreDef1, afterStoreDef2, afterStoreDef3));
    }

    @Before
    public void setUp() {
        socketStoreFactory = new ClientRequestExecutorPool(2, 10000, 100000, 32 * 1024);
        List<Node> nodes = Lists.newArrayList();
        int freePorts[] = ServerTestUtils.findFreePorts(3 * NUM_NODES);
        for(int i = 0; i < NUM_NODES; i++) {
            nodes.add(new Node(i,
                               "localhost",
                               freePorts[3 * i],
                               freePorts[3 * i + 1],
                               freePorts[3 * i + 2],
                               Lists.newArrayList(i, i + NUM_NODES)));
        }
        consistentRoutingCluster = new Cluster("consistent1", nodes);

        List<Node> nodes2 = Lists.newArrayList();
        for(int i = 0; i < NUM_NODES; i++) {
            List<Integer> partitions = Lists.newArrayList();
            if(i == 0)
                partitions.add(i + NUM_NODES);
            else {
                partitions.add(i);
                partitions.add(i + NUM_NODES);
            }

            if(i == NUM_NODES - 1)
                partitions.add(0);
            nodes2.add(new Node(i,
                                "localhost",
                                freePorts[3 * i],
                                freePorts[3 * i + 1],
                                freePorts[3 * i + 2],
                                partitions));
        }
        consistentRoutingClusterModified = new Cluster("consistent2", nodes2);

        List<Zone> zones = ServerTestUtils.getZones(NUM_ZONES);

        List<Node> nodes3 = Lists.newArrayList();
        for(int i = 0; i < NUM_NODES; i++) {

            if(i < NUM_NODES / 2)
                nodes3.add(new Node(i,
                                    "localhost",
                                    freePorts[3 * i],
                                    freePorts[3 * i + 1],
                                    freePorts[3 * i + 2],
                                    0,
                                    Lists.newArrayList(i, i + NUM_NODES)));
            else
                nodes3.add(new Node(i,
                                    "localhost",
                                    freePorts[3 * i],
                                    freePorts[3 * i + 1],
                                    freePorts[3 * i + 2],
                                    1,
                                    Lists.newArrayList(i, i + NUM_NODES)));
        }
        zoneRoutingCluster = new Cluster("zone1", nodes3, zones);

        List<Node> nodes4 = Lists.newArrayList();
        for(int i = 0; i < NUM_NODES; i++) {
            List<Integer> partitions = Lists.newArrayList();
            if(i == 0)
                partitions.add(i + NUM_NODES);
            else {
                partitions.add(i);
                partitions.add(i + NUM_NODES);
            }

            if(i == NUM_NODES - 1)
                partitions.add(0);

            if(i < NUM_NODES / 2)
                nodes4.add(new Node(i,
                                    "localhost",
                                    freePorts[3 * i],
                                    freePorts[3 * i + 1],
                                    freePorts[3 * i + 2],
                                    0,
                                    partitions));
            else
                nodes4.add(new Node(i,
                                    "localhost",
                                    freePorts[3 * i],
                                    freePorts[3 * i + 1],
                                    freePorts[3 * i + 2],
                                    1,
                                    partitions));
        }
        zoneRoutingClusterModified = new Cluster("zone1", nodes4, zones);

        zoneReplicationFactors = Maps.newHashMap();
    }

    @SuppressWarnings("unchecked")
    public void test(final Cluster currentCluster,
                     final Cluster targetCluster,
                     final List<StoreDefinition> currentStoreDefs,
                     final List<StoreDefinition> targetStoreDefs) throws IOException,
            InterruptedException {
        int numStores = currentStoreDefs.size();
        RoutingStrategy beforeStrategy[] = new RoutingStrategy[numStores];
        RoutingStrategy afterStrategy[] = new RoutingStrategy[numStores];
        RoutingStrategyFactory routingFactory = new RoutingStrategyFactory();

        for(int i = 0; i < numStores; i++) {
            beforeStrategy[i] = routingFactory.updateRoutingStrategy(currentStoreDefs.get(i),
                                                                     currentCluster);
            afterStrategy[i] = routingFactory.updateRoutingStrategy(targetStoreDefs.get(i),
                                                                    targetCluster);
        }
        VoldemortServer voldemortServer[] = new VoldemortServer[currentCluster.getNumberOfNodes()];

        for(int nodeId = 0; nodeId < currentCluster.getNumberOfNodes(); nodeId++) {
            voldemortServer[nodeId] = startServer(nodeId, currentStoreDefs, currentCluster);
        }

        Thread.sleep(10 * 1000);

        try {
            ClientConfig config = new ClientConfig().setBootstrapUrls("tcp://localhost:"
                                                                      + currentCluster.getNodeById(0)
                                                                                      .getSocketPort())
                                                    .setFailureDetectorBannagePeriod(1)
                                                    .setMaxBootstrapRetries(10)
                                                    .setConnectionTimeout(3000,
                                                                          TimeUnit.MILLISECONDS)
                                                    .setMaxConnectionsPerNode(10)
                                                    .setSelectors(8);
            factory = new SocketStoreClientFactory(config);
            final StoreClient<String, String> storeClients[] = new StoreClient[numStores];
            for(int storeNo = 1; storeNo <= numStores; storeNo++) {
                storeClients[storeNo - 1] = factory.getStoreClient("test" + storeNo);
            }

            for(int i = 0; i < NUM_KEYS; i++) {
                for(int storeNo = 1; storeNo <= numStores; storeNo++) {
                    storeClients[storeNo - 1].put("key" + i, "value" + i + "_" + 1);
                }
            }

            final AdminClient adminClient = ServerTestUtils.getAdminClient(currentCluster);
            ExecutorService service = Executors.newFixedThreadPool(2);
            Future<?> future = service.submit(new Runnable() {

                public void run() {
                    MigratePartitions migrate = new MigratePartitions(currentCluster,
                                                                      targetCluster,
                                                                      currentStoreDefs,
                                                                      targetStoreDefs,
                                                                      adminClient,
                                                                      createTempVoldemortConfig(),
                                                                      null,
                                                                      1,
                                                                      false);
                    migrate.migrate();
                }

            });

            for(int i = 0; i < NUM_KEYS; i++) {
                for(int storeNo = 1; storeNo <= numStores; storeNo++) {
                    try {
                        storeClients[storeNo - 1].put("key" + i, "value" + i + "_" + 2);
                    } catch(Exception e) {
                        storeClients[storeNo - 1].put("key" + i, "value" + i + "_" + 2);
                    }
                }
            }

            try {
                future.get();
            } catch(ExecutionException e) {
                e.printStackTrace();
            }

            Thread.sleep(120 * 1000);

            factory.close();

            // Change metadata for all servers
            for(int nodeId = 0; nodeId < currentCluster.getNumberOfNodes(); nodeId++) {
                voldemortServer[nodeId].getMetadataStore().put(MetadataStore.CLUSTER_KEY,
                                                               targetCluster);
                voldemortServer[nodeId].getMetadataStore().put(MetadataStore.STORES_KEY,
                                                               targetStoreDefs);
            }

            for(int zoneId = 0; zoneId < targetCluster.getZones().size(); zoneId++) {
                config = new ClientConfig().setBootstrapUrls("tcp://localhost:"
                                                             + currentCluster.getNodeById(0)
                                                                             .getSocketPort())
                                           .setMaxBootstrapRetries(10)
                                           .setMaxConnectionsPerNode(10)
                                           .setSelectors(8)
                                           .setConnectionTimeout(3000, TimeUnit.MILLISECONDS)
                                           .setClientZoneId(zoneId);
                factory = new SocketStoreClientFactory(config);

                for(int storeNo = 1; storeNo <= numStores; storeNo++) {
                    storeClients[storeNo - 1] = factory.getStoreClient("test" + storeNo);
                }
                for(int storeNo = 1; storeNo <= numStores; storeNo++) {
                    for(int i = 0; i < NUM_KEYS; i++) {

                        Versioned<String> value = null;
                        try {
                            value = storeClients[storeNo - 1].get("key" + i, null);
                        } catch(Exception e) {
                            value = storeClients[storeNo - 1].get("key" + i, null);
                        }
                        if(value == null) {
                            Assert.fail("Should not have happened for key"
                                        + i
                                        + " => "
                                        + beforeStrategy[storeNo - 1].getPartitionList(new String("key"
                                                                                                  + i).getBytes())
                                        + " => "
                                        + afterStrategy[storeNo - 1].getPartitionList(new String("key"
                                                                                                 + i).getBytes()));
                        } else {
                            Assert.assertEquals(value.getValue(), "value" + i + "_" + 2);
                        }
                    }

                }
                factory.close();
                factory = null;
            }
        } catch(Exception e) {
            e.printStackTrace();
        } finally {
            if(factory != null)
                factory.close();
            stopServer(Lists.newArrayList(targetCluster.getNodes()));
        }
    }

    protected void stopServer(List<Node> nodesToStop) throws IOException {
        for(Node node: nodesToStop) {
            try {
                ServerTestUtils.stopVoldemortServer(serverMap.get(node.getId()));
            } catch(VoldemortException e) {
                // ignore these at stop time
            }
        }
    }

    public VoldemortConfig createTempVoldemortConfig() {
        File temp = new File(System.getProperty("java.io.tmpdir"),
                             Integer.toString(new Random().nextInt()));
        temp.delete();
        temp.mkdir();
        temp.deleteOnExit();
        VoldemortConfig config = new VoldemortConfig(0, temp.getAbsolutePath());
        new File(config.getMetadataDirectory()).mkdir();
        return config;
    }

    private VoldemortServer startServer(int nodeId, List<StoreDefinition> storeDef, Cluster cluster)
            throws IOException {
        VoldemortConfig config = ServerTestUtils.createServerConfigWithDefs(false,
                                                                            nodeId,
                                                                            TestUtils.createTempDir()
                                                                                     .getAbsolutePath(),
                                                                            cluster,
                                                                            storeDef,
                                                                            new Properties());
        config.setGrandfather(true);
        config.setSlopFrequencyMs(10000);
        config.setBdbFairLatches(true);
        VoldemortServer server = new VoldemortServer(config);
        server.start();
        serverMap.put(nodeId, server);
        return server;
    }
}
