package voldemort.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import org.junit.Before;
import org.junit.Test;

import voldemort.utils.SystemTime;
import voldemort.utils.Time;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

public class DefaultSocketStoreClientTest {

    protected StoreClient<String, String> client;
    protected int nodeId;
    protected Time time;

    @Before
    public void setUp() throws Exception {
        String socketUrl = "tcp://localhost:6667";
        String storeName = "test";
        ClientConfig clientConfig = new ClientConfig().setMaxThreads(4)
                                                      .setMaxTotalConnections(4)
                                                      .setMaxConnectionsPerNode(4)
                                                      .setBootstrapUrls(socketUrl);
        SocketStoreClientFactory socketFactory = new SocketStoreClientFactory(clientConfig);
        this.client = socketFactory.getStoreClient(storeName);
        this.nodeId = 0;
        this.time = SystemTime.INSTANCE;
    }

    @Test
    public void test() {
        client.put("k", Versioned.value("v"));
        Versioned<String> v = client.get("k");
        assertEquals("GET should return the version set by PUT.", "v", v.getValue());
        VectorClock expected = new VectorClock();
        expected.incrementVersion(nodeId, time.getMilliseconds());
        assertEquals("The version should be incremented after a put.", expected, v.getVersion());
        try {
            client.put("k", Versioned.value("v"));
            fail("Put of obsolete version should throw exception.");
        } catch(ObsoleteVersionException e) {
            // this is good
        }
        // PUT of a concurrent version should succeed
        client.put("k",
                   new Versioned<String>("v2",
                                         new VectorClock().incremented(nodeId + 1,
                                                                       time.getMilliseconds())));
        assertEquals("GET should return the new value set by PUT.", "v2", client.getValue("k"));
        assertEquals("GET should return the new version set by PUT.",
                     expected.incremented(nodeId + 1, time.getMilliseconds()),
                     client.get("k").getVersion());
        client.delete("k");
        assertNull("After a successful delete(k), get(k) should return null.", client.get("k"));
    }

}
