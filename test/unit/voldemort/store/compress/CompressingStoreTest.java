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
import voldemort.serialization.Compression;
import voldemort.server.AbstractSocketService;
import voldemort.store.AbstractByteArrayStoreTest;
import voldemort.store.Store;
import voldemort.store.memory.InMemoryStorageEngine;
import voldemort.utils.ByteArray;

@RunWith(Parameterized.class)
public class CompressingStoreTest extends AbstractByteArrayStoreTest {

    private CompressingStore store;

    private final boolean useNio;
    private final Compression compression;
    private final CompressionStrategyFactory compressionFactory = new CompressionStrategyFactory();

    public CompressingStoreTest(boolean useNio, String compressionType) {
        this.useNio = useNio;
        this.compression = new Compression(compressionType, null);
    }

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { true, "gzip" }, { false, "gzip" }, { true, "lzf" },
                { false, "lzf" } });
    }

    @Override
    @Before
    public void setUp() throws Exception {
        this.store = new CompressingStore(new InMemoryStorageEngine<ByteArray, byte[]>("test"),
                                          compressionFactory.get(compression),
                                          compressionFactory.get(compression));
    }

    @Override
    public Store<ByteArray, byte[]> getStore() {
        return store;
    }

    @Test
    public void testPutGetWithSocketService() throws Exception {
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

        Thread.sleep(1000);

        SocketStoreClientFactory storeClientFactory = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls("tcp://localhost:"
                                                                                                                       + freePort)
                                                                                                     .setMaxBootstrapRetries(10));
        StoreClient<String, String> storeClient = storeClientFactory.getStoreClient("test");
        storeClient.put("someKey", "someValue");
        assertEquals(storeClient.getValue("someKey"), "someValue");
        socketService.stop();
    }

}
