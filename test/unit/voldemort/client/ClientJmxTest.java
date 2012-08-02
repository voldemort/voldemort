package voldemort.client;

import java.lang.management.ManagementFactory;
import java.net.URISyntaxException;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.serialization.SerializerFactory;
import voldemort.server.AbstractSocketService;
import voldemort.utils.JmxUtils;

public class ClientJmxTest extends AbstractStoreClientFactoryTest {

    private static String STATS_DOMAIN = "voldemort.store.stats";
    private static String AGGREGATE_STATS_DOMAIN = "voldemort.store.stats.aggregate";
    private static String CLIENT_DOMAIN = "voldemort.client";
    private static String CLUSTER_FAILUREDETECTOR_DOMAIN = "voldemort.cluster.failuredetector";
    private static String CLIENT_REQUEST_DOMAIN = "voldemort.store.socket.clientrequest";

    private AbstractSocketService socketService;
    private MBeanServer mbServer = null;

    private static int factoryJmxId = 0;

    public ClientJmxTest() {
        super();
    }

    private static String getAndIncrementJmxId() {
        int current = factoryJmxId;
        factoryJmxId++;
        return (0 == current ? "" : "." + current);
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        socketService = ServerTestUtils.getSocketService(true,
                                                         getClusterXml(),
                                                         getStoreDefXml(),
                                                         getValidStoreName(),
                                                         getLocalNode().getSocketPort());
        socketService.start();
        mbServer = ManagementFactory.getPlatformMBeanServer();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        mbServer = null;
        super.tearDown();
        socketService.stop();
    }

    @Override
    protected StoreClientFactory getFactory(String... bootstrapUrls) {
        return new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(bootstrapUrls)
                                                              .setEnableLazy(false)
                                                              .setEnableJmx(true));
    }

    protected StoreClientFactory getFactoryWithClientContext(String clientContext,
                                                             String... bootstrapUrls) {
        return new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(bootstrapUrls)
                                                              .setEnableLazy(false)
                                                              .setClientContextName(clientContext)
                                                              .setEnableJmx(true));
    }

    @Test
    public void testTwoClientContextOnJmx() throws Exception {
        String clientContext1 = "clientA";
        String clientContext2 = "clientB";
        String jmxId1 = getAndIncrementJmxId();
        String jmxId2 = getAndIncrementJmxId();

        StoreClient<Object, Object> c1 = getFactoryWithClientContext(clientContext1,
                                                                     getValidBootstrapUrl()).getStoreClient(getValidStoreName());
        StoreClient<Object, Object> c2 = getFactoryWithClientContext(clientContext2,
                                                                     getValidBootstrapUrl()).getStoreClient(getValidStoreName());

        // checking for aggregate stats
        ObjectName c1Name = JmxUtils.createObjectName(AGGREGATE_STATS_DOMAIN, clientContext1
                                                                              + ".aggregate-perf"
                                                                              + jmxId1);
        ObjectName c2Name = JmxUtils.createObjectName(AGGREGATE_STATS_DOMAIN, clientContext2
                                                                              + ".aggregate-perf"
                                                                              + jmxId2);
        checkForMbeanFound(c1Name);
        checkForMbeanFound(c2Name);
        mbServer.unregisterMBean(c1Name);
        mbServer.unregisterMBean(c2Name);

        // checking for per store stats
        String c1type = clientContext1 + ".test" + jmxId1;
        String c2type = clientContext2 + ".test" + jmxId2;
        c1Name = JmxUtils.createObjectName(STATS_DOMAIN,
                                           c1type
                                                   + "."
                                                   + ((ZenStoreClient<Object, Object>) c1).getClientId()
                                                                                               .toString());
        c2Name = JmxUtils.createObjectName(STATS_DOMAIN,
                                           c2type
                                                   + "."
                                                   + ((ZenStoreClient<Object, Object>) c2).getClientId()
                                                                                               .toString());
        checkForMbeanFound(c1Name);
        checkForMbeanFound(c2Name);
        mbServer.unregisterMBean(c1Name);
        mbServer.unregisterMBean(c2Name);
    }

    @Test
    public void testSameContextOnJmx() throws Exception {
        String clientContext = "clientContext";
        String jmxId1 = getAndIncrementJmxId();
        String jmxId2 = getAndIncrementJmxId();

        StoreClient<Object, Object>[] clients = new StoreClient[2];
        for(int i = 0; i < 2; i++) {
            clients[i] = getFactoryWithClientContext(clientContext, getValidBootstrapUrl()).getStoreClient(getValidStoreName());
        }

        // checking for aggregate stats
        ObjectName c1Name = JmxUtils.createObjectName(AGGREGATE_STATS_DOMAIN, clientContext
                                                                              + ".aggregate-perf"
                                                                              + jmxId1);
        ObjectName c2Name = JmxUtils.createObjectName(AGGREGATE_STATS_DOMAIN, clientContext
                                                                              + ".aggregate-perf"
                                                                              + jmxId2);
        checkForMbeanFound(c1Name);
        checkForMbeanFound(c2Name);
        mbServer.unregisterMBean(c1Name);
        mbServer.unregisterMBean(c2Name);

        // checking for per store stats
        String c1type = clientContext + ".test" + jmxId1;
        String c2type = clientContext + ".test" + jmxId2;
        c1Name = JmxUtils.createObjectName(STATS_DOMAIN,
                                           c1type
                                                   + "."
                                                   + ((ZenStoreClient<Object, Object>) clients[0]).getClientId()
                                                                                                       .toString());
        c2Name = JmxUtils.createObjectName(STATS_DOMAIN,
                                           c2type
                                                   + "."
                                                   + ((ZenStoreClient<Object, Object>) clients[1]).getClientId()
                                                                                                       .toString());
        checkForMbeanFound(c1Name);
        checkForMbeanFound(c2Name);
        mbServer.unregisterMBean(c1Name);
        mbServer.unregisterMBean(c2Name);
    }

    @Test
    public void testTwoClientNoContextOnJmx() throws Exception {
        String clientContextCompare = "default";
        String jmxId1 = getAndIncrementJmxId();
        String jmxId2 = getAndIncrementJmxId();

        StoreClient<Object, Object> c1 = getFactory(getValidBootstrapUrl()).getStoreClient(getValidStoreName());
        StoreClient<Object, Object> c2 = getFactory(getValidBootstrapUrl()).getStoreClient(getValidStoreName());

        // checking for aggregate stats
        ObjectName c1Name = JmxUtils.createObjectName(AGGREGATE_STATS_DOMAIN, clientContextCompare
                                                                              + ".aggregate-perf"
                                                                              + jmxId1);
        ObjectName c2Name = JmxUtils.createObjectName(AGGREGATE_STATS_DOMAIN, clientContextCompare
                                                                              + ".aggregate-perf"
                                                                              + jmxId2);
        checkForMbeanFound(c1Name);
        checkForMbeanFound(c2Name);
        mbServer.unregisterMBean(c1Name);
        mbServer.unregisterMBean(c2Name);

        // checking for per store stats
        String c1type = clientContextCompare + ".test" + jmxId1;
        String c2type = clientContextCompare + ".test" + jmxId2;
        c1Name = JmxUtils.createObjectName(STATS_DOMAIN,
                                           c1type
                                                   + "."
                                                   + ((ZenStoreClient<Object, Object>) c1).getClientId()
                                                                                               .toString());
        c2Name = JmxUtils.createObjectName(STATS_DOMAIN,
                                           c2type
                                                   + "."
                                                   + ((ZenStoreClient<Object, Object>) c2).getClientId()
                                                                                               .toString());
        checkForMbeanFound(c1Name);
        checkForMbeanFound(c2Name);
        mbServer.unregisterMBean(c1Name);
        mbServer.unregisterMBean(c2Name);
    }

    @Test
    public void testTwoClientNullContextOnJmx() throws Exception {
        String clientContextCompare = "";
        String jmxId1 = getAndIncrementJmxId();
        String jmxId2 = getAndIncrementJmxId();

        StoreClient<Object, Object> c1 = getFactoryWithClientContext(null, getValidBootstrapUrl()).getStoreClient(getValidStoreName());
        StoreClient<Object, Object> c2 = getFactoryWithClientContext(null, getValidBootstrapUrl()).getStoreClient(getValidStoreName());

        // checking for aggregate stats
        ObjectName c1Name = JmxUtils.createObjectName(AGGREGATE_STATS_DOMAIN, clientContextCompare
                                                                              + ".aggregate-perf"
                                                                              + jmxId1);
        ObjectName c2Name = JmxUtils.createObjectName(AGGREGATE_STATS_DOMAIN, clientContextCompare
                                                                              + ".aggregate-perf"
                                                                              + jmxId2);
        checkForMbeanFound(c1Name);
        checkForMbeanFound(c2Name);
        mbServer.unregisterMBean(c1Name);
        mbServer.unregisterMBean(c2Name);

        // checking for per store stats
        String c1type = clientContextCompare + ".test" + jmxId1;
        String c2type = clientContextCompare + ".test" + jmxId2;
        c1Name = JmxUtils.createObjectName(STATS_DOMAIN,
                                           c1type
                                                   + "."
                                                   + ((ZenStoreClient<Object, Object>) c1).getClientId()
                                                                                               .toString());
        c2Name = JmxUtils.createObjectName(STATS_DOMAIN,
                                           c2type
                                                   + "."
                                                   + ((ZenStoreClient<Object, Object>) c2).getClientId()
                                                                                               .toString());
        checkForMbeanFound(c1Name);
        checkForMbeanFound(c2Name);
        mbServer.unregisterMBean(c1Name);
        mbServer.unregisterMBean(c2Name);
    }

    @Test
    public void testSameContextAndFactory() throws Exception {
        String clientContext = "clientContext";
        String jmxId = getAndIncrementJmxId();
        StoreClientFactory factory = getFactoryWithClientContext(clientContext,
                                                                 getValidBootstrapUrl());

        StoreClient<Object, Object>[] clients = new StoreClient[2];
        for(int i = 0; i < 2; i++) {
            clients[i] = factory.getStoreClient(getValidStoreName());
        }

        ObjectName cName = JmxUtils.createObjectName(AGGREGATE_STATS_DOMAIN, clientContext
                                                                             + ".aggregate-perf"
                                                                             + jmxId);
        checkForMbeanFound(cName);
        mbServer.unregisterMBean(cName);

        // checking for per store stats
        String ctype = clientContext + ".test" + jmxId;
        ObjectName c1Name = JmxUtils.createObjectName(STATS_DOMAIN,
                                                      ctype
                                                              + "."
                                                              + ((ZenStoreClient<Object, Object>) clients[0]).getClientId()
                                                                                                                  .toString());
        ObjectName c2Name = JmxUtils.createObjectName(STATS_DOMAIN,
                                                      ctype
                                                              + "."
                                                              + ((ZenStoreClient<Object, Object>) clients[1]).getClientId()
                                                                                                                  .toString());
        checkForMbeanFound(c1Name);
        checkForMbeanFound(c2Name);
        mbServer.unregisterMBean(c1Name);
    }

    @Test
    public void testDifferentId() throws Exception {
        String clientContext = "clientContext";
        String jmxId = getAndIncrementJmxId();
        StoreClientFactory factory = getFactoryWithClientContext(clientContext,
                                                                 getValidBootstrapUrl());

        StoreClient<Object, Object>[] clients = new StoreClient[2];
        clients[0] = factory.getStoreClient(getValidStoreName());
        clients[1] = factory.getStoreClient(getValidStoreName());

        ObjectName cName = JmxUtils.createObjectName(AGGREGATE_STATS_DOMAIN, clientContext
                                                                             + ".aggregate-perf"
                                                                             + jmxId);
        checkForMbeanFound(cName);
        mbServer.unregisterMBean(cName);

        // checking for per store stats
        String ctype = clientContext + ".test" + jmxId;
        ObjectName c1Name = JmxUtils.createObjectName(STATS_DOMAIN,
                                                      ctype
                                                              + "."
                                                              + ((ZenStoreClient<Object, Object>) clients[0]).getClientId()
                                                                                                                  .toString());
        ObjectName c2Name = JmxUtils.createObjectName(STATS_DOMAIN,
                                                      ctype
                                                              + "."
                                                              + ((ZenStoreClient<Object, Object>) clients[1]).getClientId()
                                                                                                                  .toString());
        checkForMbeanFound(c1Name);
        checkForMbeanFound(c2Name);
        assertTrue(!c1Name.equals(c2Name));
        mbServer.unregisterMBean(c1Name);
        mbServer.unregisterMBean(c2Name);
    }

    private void checkForMbeanFound(ObjectName name) {
        try {
            mbServer.getMBeanInfo(name);
        } catch(InstanceNotFoundException e) {
            fail("MBean not found on the JMX Server: " + name.toString());
        } catch(Exception e) {
            fail("Test failed: " + e.getMessage());
        }
    }

    @Override
    protected StoreClientFactory getFactoryWithSerializer(SerializerFactory factory,
                                                          String... bootstrapUrls) {
        return new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(bootstrapUrls)
                                                              .setEnableLazy(false)
                                                              .setSerializerFactory(factory));
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
