package voldemort.scheduled;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.VoldemortTestConstants;
import voldemort.client.ClientConfig;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.routing.BaseStoreRoutingPlan;
import voldemort.serialization.SlopSerializer;
import voldemort.server.VoldemortServer;
import voldemort.server.scheduler.slop.BlockingSlopPusherJob;
import voldemort.server.scheduler.slop.StreamingSlopPusherJob;
import voldemort.store.StoreDefinition;
import voldemort.store.routed.NodeValue;
import voldemort.store.slop.Slop;
import voldemort.store.slop.Slop.Operation;
import voldemort.utils.ByteArray;
import voldemort.utils.StoreDefinitionUtils;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;
import voldemort.xml.StoreDefinitionsMapper;

@RunWith(Parameterized.class)
public class SlopPusherDeadSlopTest {

    private static Logger logger = Logger.getLogger(SlopPusherDeadSlopTest.class);

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { StreamingSlopPusherJob.TYPE_NAME },
                { BlockingSlopPusherJob.TYPE_NAME } });
    }

    private static final Integer SLOP_FREQUENCY_MS = 5000;

    private String slopPusherType;
    private VoldemortServer[] servers;
    private Cluster cluster;
    private AdminClient adminClient;

    public SlopPusherDeadSlopTest(String slopPusherType) {
        this.slopPusherType = slopPusherType;
    }

    @Before
    public void setUp() throws Exception {

        try {
            Properties serverProperties = new Properties();
            serverProperties.setProperty("pusher.type", slopPusherType);
            serverProperties.setProperty("slop.frequency.ms", SLOP_FREQUENCY_MS.toString());
            serverProperties.setProperty("auto.purge.dead.slops", "true");
            serverProperties.setProperty("enable.server.routing", "true");

            servers = new VoldemortServer[2];
            int partitionMap[][] = { { 0, 1 }, { 2, 3 } };
            cluster = ServerTestUtils.startVoldemortCluster(servers,
                                                            partitionMap,
                                                            serverProperties,
                                                            "test/common/voldemort/config/single-store.xml");

            Properties adminProperties = new Properties();
            adminProperties.setProperty("max_connections", "2");
            adminClient = new AdminClient(servers[0].getMetadataStore().getCluster(),
                                          new AdminClientConfig(adminProperties),
                                          new ClientConfig());
        } catch(Exception e) {
            logger.error("Error in setup", e);
            throw e;
        }
    }

    @Test
    public void testAutoPurge() {

        try {
            // generate slops for a non existent node 2.
            List<Versioned<Slop>> deadNodeSlops = ServerTestUtils.createRandomSlops(2,
                                                                                    40,
                                                                                    false,
                                                                                    "test");

            // generate slops for a non existent store "deleted_store"
            List<Versioned<Slop>> deadStoreSlops = ServerTestUtils.createRandomSlops(0,
                                                                                     40,
                                                                                     false,
                                                                                     "deleted_store");

            // generate some valid slops and make sure they go into the
            // destination store
            List<Versioned<Slop>> validStoreSlops = ServerTestUtils.createRandomSlops(1,
                                                                                      40,
                                                                                      false,
                                                                                      "test");

            List<Versioned<Slop>> slops = new ArrayList<Versioned<Slop>>();
            slops.addAll(deadStoreSlops);
            slops.addAll(deadNodeSlops);
            slops.addAll(validStoreSlops);
            SlopSerializer slopSerializer = new SlopSerializer();

            // Populate the store with the slops
            for(Versioned<Slop> slop: slops) {
                VectorClock clock = TestUtils.getClock(1);
                NodeValue<ByteArray, byte[]> nodeValue = new NodeValue<ByteArray, byte[]>(0,
                                                                                          slop.getValue()
                                                                                              .makeKey(),
                                                                                          new Versioned<byte[]>(slopSerializer.toBytes(slop.getValue()),
                                                                                                                clock));
                adminClient.storeOps.putNodeKeyValue("slop", nodeValue);
            }

            // wait for twice the slop interval (in case a slop push was
            // underway as we populated)
            Thread.sleep(SLOP_FREQUENCY_MS * 2);

            // Confirm the dead slops are all gone now..
            for(List<Versioned<Slop>> deadSlops: Arrays.asList(deadStoreSlops, deadNodeSlops)) {
                for(Versioned<Slop> slop: deadSlops) {
                    List<Versioned<byte[]>> slopEntry = adminClient.storeOps.getNodeKey("slop",
                                                                                        0,
                                                                                        slop.getValue()
                                                                                            .makeKey());
                    assertEquals("Slop should be purged", 0, slopEntry.size());
                }
            }

            StoreDefinitionsMapper mapper = new StoreDefinitionsMapper();
            List<StoreDefinition> storeDefs = mapper.readStoreList(new StringReader(VoldemortTestConstants.getSingleStoreDefinitionsXml()));
            BaseStoreRoutingPlan rPlan = new BaseStoreRoutingPlan(adminClient.getAdminClientCluster(),
                                                                  StoreDefinitionUtils.getStoreDefinitionWithName(storeDefs,
                                                                                                                  "test"));

            // Confirm the valid ones made it
            for(Versioned<Slop> slop: validStoreSlops) {
                ByteArray key = slop.getValue().getKey();

                if(rPlan.getReplicationNodeList(key.get()).contains(1)) {
                    List<Versioned<byte[]>> slopEntry = adminClient.storeOps.getNodeKey("test",
                                                                                        1,
                                                                                        key);
                    if(slop.getValue().getOperation() == Operation.DELETE) {
                        assertTrue("Delete Slop should have not reached destination",
                                   slopEntry.size() == 0);
                    } else {
                        assertTrue("Put Slop should have reached destination", slopEntry.size() > 0);
                    }
                }

            }

        } catch(Exception e) {
            logger.error("Test failed with", e);
            fail("unexpected exception");
        }
    }

    @After
    public void tearDown() {
        for(VoldemortServer server: servers) {
            server.stop();
        }
        adminClient.close();
    }
}
