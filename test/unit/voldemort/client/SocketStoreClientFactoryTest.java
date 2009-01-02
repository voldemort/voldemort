package voldemort.client;

import java.net.URISyntaxException;
import java.util.concurrent.Executors;

import voldemort.ServerTestUtils;
import voldemort.serialization.SerializerFactory;
import voldemort.server.socket.SocketServer;

/**
 * @author jay
 *
 */
public class SocketStoreClientFactoryTest extends AbstractStoreClientFactoryTest {

    private SocketServer server;
    
    public SocketStoreClientFactoryTest() {
        super();
    }
    
    @Override
    public void setUp() throws Exception {
        super.setUp();
        server = ServerTestUtils.getSocketServer(getClusterXml(),
                                                 getStoreDefXml(),
                                                 getValidStoreName(),
                                                 getLocalNode().getSocketPort());
    }

    public void tearDown() throws Exception {
        super.tearDown();
        server.shutdown();
    }
    
    @Override
    protected StoreClientFactory getFactory(String...bootstrapUrls) {
        return new SocketStoreClientFactory(Executors.newCachedThreadPool(), 5, 10, 1000, 1000, 10000, bootstrapUrls);
    }

    @Override
    protected StoreClientFactory getFactoryWithSerializer(SerializerFactory factory, String...bootstrapUrls) {
        return new SocketStoreClientFactory(Executors.newCachedThreadPool(), 
                                            5, 
                                            10, 
                                            1000, 
                                            1000,
                                            1000,
                                            factory,
                                            bootstrapUrls);
    }

    @Override
    protected String getValidBootstrapUrl() throws URISyntaxException {
        return getLocalNode().getSocketUrl().toString();
    }

    @Override
    protected String getValidScheme() {
        return SocketStoreClientFactory.URL_SCHEME;
    }

}
