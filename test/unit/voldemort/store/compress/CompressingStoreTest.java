package voldemort.store.compress;

import junit.framework.Test;
import voldemort.ServerTestUtils;
import voldemort.SocketServiceTestCase;
import voldemort.TestUtils;
import voldemort.VoldemortTestConstants;
import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.server.AbstractSocketService;
import voldemort.store.AbstractByteArrayStoreTest;
import voldemort.store.Store;
import voldemort.store.memory.InMemoryStorageEngine;
import voldemort.utils.ByteArray;

public class CompressingStoreTest extends AbstractByteArrayStoreTest implements
        SocketServiceTestCase {

    private CompressingStore store;

    private boolean useNio;

    public static Test suite() {
        return TestUtils.createSocketServiceTestCaseSuite(CompressingStoreTest.class);
    }

    public void setUseNio(boolean useNio) {
        this.useNio = useNio;
    }

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
