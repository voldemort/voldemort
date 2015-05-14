package voldemort.client.rebalance;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import voldemort.ClusterTestUtils;
import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.ZoneAffinity;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.server.VoldemortServer;
import voldemort.store.StoreDefinition;
import voldemort.utils.RebalanceUtils;
import voldemort.versioning.Versioned;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

public class ZoneShrinkageClientTest {

    private static final int DROP_ZONE_ID = 0;

    private Cluster sourceCluster;
    private Cluster targetCluster;
    private List<StoreDefinition> sourceStoreDefs;
    private List<StoreDefinition> targetStoreDefs;
    private VoldemortServer[] servers;
    private AdminClient adminClient;

    @Before
    public void setup() throws IOException {
        ClusterTestUtils.reset();

        sourceCluster = ClusterTestUtils.getZZZCluster();
        targetCluster = RebalanceUtils.vacateZone(sourceCluster, DROP_ZONE_ID);
        sourceStoreDefs = ClusterTestUtils.getZZZStoreDefsBDB();
        targetStoreDefs = RebalanceUtils.dropZone(sourceStoreDefs, DROP_ZONE_ID);

        File sourceStoreDefsXml = File.createTempFile("zzz-stores-", ".xml");
        FileUtils.writeStringToFile(sourceStoreDefsXml,
                                    new StoreDefinitionsMapper().writeStoreList(sourceStoreDefs));
        servers = new VoldemortServer[sourceCluster.getNumberOfNodes()];

        ServerTestUtils.startVoldemortCluster(servers,
                                              null,
                                              null,
                                              sourceStoreDefsXml.getAbsolutePath(),
                                              new Properties(),
                                              sourceCluster);

        Properties adminProperties = new Properties();
        adminProperties.setProperty("max_connections", "2");
        adminClient = new AdminClient(servers[0].getMetadataStore().getCluster(),
                                      new AdminClientConfig(adminProperties),
                                      new ClientConfig());
    }

    @Test
    public void testZoneAffinityClient() {
        ZoneAffinity zoneAffinity = new ZoneAffinity(true, true, true);
        ClientConfig clientConfig = new ClientConfig().setBootstrapUrls(sourceCluster.getNodeById(0)
                                                                                     .getSocketUrl()
                                                                                     .toString())
                                                      .setClientZoneId(DROP_ZONE_ID)
                                                      .setZoneAffinity(zoneAffinity);
        SocketStoreClientFactory factory = new SocketStoreClientFactory(clientConfig);
        StoreClient<String, String> client = factory.getStoreClient(sourceStoreDefs.get(sourceStoreDefs.size() - 1)
                                                                                   .getName());

        // do some operations against the stores from zone 0.
        for(int i = 0; i < 10; i++) {
            try {
                client.put("key" + i, "val" + i);
                assertEquals("Must read value back", "val" + i, client.get("key" + i).getValue());
            } catch(Exception e) {
                fail("Should be not see any failures");
            }
        }

        // shrink the cluster, by dropping zone 0
        String clusterXmlString = new ClusterMapper().writeCluster(targetCluster);
        String storesXmlString = new StoreDefinitionsMapper().writeStoreList(targetStoreDefs);
        int[] remoteNodeList = new int[sourceCluster.getNumberOfNodes()];
        int ni = 0;
        for(Integer nodeId: sourceCluster.getNodeIds()) {
            remoteNodeList[ni++] = nodeId;
        }
        adminClient.metadataMgmtOps.updateRemoteMetadataPair(new ArrayList<Integer>(sourceCluster.getNodeIds()),
                                                             "cluster.xml",
                                                             new Versioned<String>(clusterXmlString,
                                                                                   TestUtils.getClock(remoteNodeList)),
                                                             "stores.xml",
                                                             new Versioned<String>(storesXmlString,
                                                                                   TestUtils.getClock(remoteNodeList)));
        try {
            Thread.sleep(clientConfig.getAsyncMetadataRefreshInMs() * 2);
        } catch(InterruptedException e) {
            e.printStackTrace();
        }

        // do more operations, the client should fail.
        for(int i = 0; i < 10; i++) {
            long startMs = System.currentTimeMillis();
            try {
                client.put("key" + i, "val" + i);
                assertEquals("Must read value back", "val" + i, client.get("key" + i).getValue());
                fail("Should be not see any successes");
            } catch(Exception e) {
                e.printStackTrace();
                long elapsedMs = System.currentTimeMillis() - startMs;
                assertTrue("Operation took longer than timeout to fail :" + elapsedMs + " ms.",
                           elapsedMs < clientConfig.getRoutingTimeout(TimeUnit.MILLISECONDS));
            }
        }
    }

    @After
    public void teardown() {
        for(VoldemortServer server: servers) {
            server.stop();
        }
        adminClient.close();
        ClusterTestUtils.reset();

    }
}
