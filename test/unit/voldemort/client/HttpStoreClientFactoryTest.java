package voldemort.client;

import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;

import voldemort.ServerTestUtils;
import voldemort.serialization.SerializerFactory;
import voldemort.store.http.HttpStore;

/**
 * @author jay
 *
 */
public class HttpStoreClientFactoryTest extends AbstractStoreClientFactoryTest {

    private HttpStore httpStore;
    private Server server;
    private Context context;
    private String url;
    
    public void setUp() throws Exception {
        super.setUp();
        int httpPort = 8080;
        url = "http://localhost:" + httpPort;
        context = ServerTestUtils.getJettyServer(getClusterXml(), 
                                                 getStoreDefXml(), 
                                                 getValidStoreName(), 
                                                 httpPort);
        server = context.getServer();
        httpStore = ServerTestUtils.getHttpStore(getValidStoreName(), httpPort);
    }
    
    public void tearDown() throws Exception {
        httpStore.close();
        server.stop();
        context.destroy();
    }
    
    @Override
    protected StoreClientFactory getFactory(String...bootstrapUrls) {
        return new HttpStoreClientFactory(4, bootstrapUrls);
    }
    
    @Override
    protected StoreClientFactory getFactoryWithSerializer(SerializerFactory factory, String...bootstrapUrls) {
        return new HttpStoreClientFactory(3, 1000, 1000, 0, 1000, 1000, 10000, 10, 10, factory, bootstrapUrls);
    }

    @Override
    protected String getValidBootstrapUrl() {
        return url;
    }

    @Override
    protected String getValidScheme() {
        return HttpStoreClientFactory.URL_SCHEME;
    }

}
