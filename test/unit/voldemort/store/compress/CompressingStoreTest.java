package voldemort.store.compress;

import java.util.Arrays;
import java.util.Collection;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import voldemort.ServerTestUtils;
import voldemort.VoldemortTestConstants;
import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.server.AbstractSocketService;
import voldemort.store.AbstractByteArrayStoreTest;
import voldemort.store.Store;
import voldemort.store.memory.InMemoryStorageEngine;
import voldemort.utils.ByteArray;

@RunWith(Parameterized.class)
public class CompressingStoreTest extends AbstractByteArrayStoreTest {

    private CompressingStore store;

    private final boolean useNio;

    public CompressingStoreTest(boolean useNio) {
        this.useNio = useNio;
    }

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { true }, { false } });
    }

    @Override
    @Before
    public void setUp() throws Exception {
        this.store = new CompressingStore(new InMemoryStorageEngine<ByteArray, byte[]>("test"),
                                          new GzipCompressionStrategy(),
                                          new GzipCompressionStrategy());
    }

    @Override
    public Store<ByteArray, byte[]> getStore() {
        return store;
    }

    @Test
    public void testPutGetWithSocketService() {
        int freePort = ServerTestUtils.findFreePort();
        String clusterXml = VoldemortTestConstants.getOneNodeClusterXml();
        clusterXml = clusterXml.replace("<socket-port>6666</socket-port>", "<socket-port>"
                                                                           + freePort
                                                                           + "</socket-port>");
        AbstractSocketService socketService = ServerTestUtils.getSocketService(useNio,
                                                                               clusterXml,
                                                                               VoldemortTestConstants.getCompressedStoreDefinitionsXml(),
                                                                               "test",
                                                                               freePort);
        socketService.start();
        SocketStoreClientFactory storeClientFactory = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls("tcp://localhost:"
                                                                                                                       + freePort));
        StoreClient<String, String> storeClient = storeClientFactory.getStoreClient("test");
        storeClient.put("someKey", "someValue");
        assertEquals(storeClient.getValue("someKey"), "someValue");
        socketService.stop();
    }

}
