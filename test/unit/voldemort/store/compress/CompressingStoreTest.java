package voldemort.store.compress;

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

public class CompressingStoreTest extends AbstractByteArrayStoreTest {

    private CompressingStore store;

    @Override
    protected void setUp() throws Exception {
        this.store = new CompressingStore(new InMemoryStorageEngine<ByteArray, byte[]>("test"),
                                          new GzipCompressionStrategy(),
                                          new GzipCompressionStrategy());
    }

    @Override
    public Store<ByteArray, byte[]> getStore() {
        return store;
    }

    public void testPutGetWithSocketService() {
        int freePort = ServerTestUtils.findFreePort();
        String clusterXml = VoldemortTestConstants.getOneNodeClusterXml();
        clusterXml = clusterXml.replace("<socket-port>6666</socket-port>", "<socket-port>"
                                                                           + freePort
                                                                           + "</socket-port>");
        AbstractSocketService socketService = ServerTestUtils.getSocketService(clusterXml,
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
