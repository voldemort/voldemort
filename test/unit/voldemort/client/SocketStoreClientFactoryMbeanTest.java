package voldemort.client;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import voldemort.utils.JmxUtils;

/**
 * 
 * Smoke test to see how many Mbeans we create in each monitoring domain
 * 
 */
public class SocketStoreClientFactoryMbeanTest extends SocketStoreClientFactoryTest {

    // there should one of these per store (that has a store client), per
    // factory
    private static String STATS_DOMAIN = "voldemort.store.stats";
    private static String AGGREGATE_STATS_DOMAIN = "voldemort.store.stats.aggregate";
    private static String PIPELINE_ROUTED_STATS_DOMAIN = "voldemort.store.routed";

    // there should one of these per factory
    private static String CLIENT_DOMAIN = "voldemort.client";
    private static String CLUSTER_FAILUREDETECTOR_DOMAIN = "voldemort.cluster.failuredetector";

    // there should be one of these per factory per host in the cluster the
    // factory talks to (plus one aggregate)
    private static String CLIENT_REQUEST_DOMAIN = "voldemort.store.socket.clientrequest";

    private MBeanServer mbServer = null;
    // list of factory objects to be closed at the end.
    private List<StoreClientFactory> factories;

    public SocketStoreClientFactoryMbeanTest(boolean useNio, boolean useLazy) {
        super(useNio, useLazy);
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        mbServer = ManagementFactory.getPlatformMBeanServer();
        factories = new ArrayList<StoreClientFactory>();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        mbServer = null;
        for(StoreClientFactory factory: factories)
            factory.close();

        super.tearDown();
    }

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { true, false } });
    }

    private void checkMbeanIdCount(String domain, String type, int maxMbeans, boolean unregister) {
        ObjectName oName = JmxUtils.createObjectName(domain, type);
        Set<ObjectName> objects = mbServer.queryNames(oName, null);
        assertFalse("Extra mbeans found", objects.size() > maxMbeans);
        assertFalse("Fewer than expected mbeans found", objects.size() < maxMbeans);

        if(unregister) {
            try {
                for(ObjectName objName: objects)
                    mbServer.unregisterMBean(objName);
            } catch(Exception e) {
                fail("Problem unregistering mbeans " + e.getMessage());
            }
        }
    }

    private void bootStrap(List<DefaultStoreClient<Object, Object>> clients, int n) {
        for(int i = 0; i < n; i++) {
            for(DefaultStoreClient<Object, Object> client: clients)
                client.bootStrap();
        }
    }

    @Test
    public void testMultipleDistinctClientsOnSingleFactory() {
        try {
            StoreClientFactory factory = getFactory(getValidBootstrapUrl());
            List<DefaultStoreClient<Object, Object>> clients = new ArrayList<DefaultStoreClient<Object, Object>>();

            clients.add((DefaultStoreClient<Object, Object>) factory.getStoreClient("test"));
            clients.add((DefaultStoreClient<Object, Object>) factory.getStoreClient("best"));
            factories.add(factory);

            // bootstrap a number of times
            bootStrap(clients, 10);

            checkMbeanIdCount(CLIENT_DOMAIN, "ClientThreadPool*", 1, true);
            checkMbeanIdCount(CLIENT_DOMAIN, "ZenStoreClient*", 2, true);
            checkMbeanIdCount(CLUSTER_FAILUREDETECTOR_DOMAIN, "ThresholdFailureDetector*", 1, true);
            checkMbeanIdCount(PIPELINE_ROUTED_STATS_DOMAIN, "*", 2, true);
            checkMbeanIdCount(CLIENT_REQUEST_DOMAIN, "aggregated*", 1, true);
            checkMbeanIdCount(CLIENT_REQUEST_DOMAIN, "stats_localhost*", 1, true);
            checkMbeanIdCount(AGGREGATE_STATS_DOMAIN, "aggregate-perf*", 1, true);
            checkMbeanIdCount(STATS_DOMAIN, "*", 2, true);

        } catch(Exception e) {
            fail("Unexpected error " + e.getMessage());
        }
    }

    @Test
    public void testMultipleIndistinctClientsOnSingleFactory() {
        try {
            StoreClientFactory factory = getFactory(getValidBootstrapUrl());
            List<DefaultStoreClient<Object, Object>> clients = new ArrayList<DefaultStoreClient<Object, Object>>();

            clients.add((DefaultStoreClient<Object, Object>) factory.getStoreClient("test"));
            clients.add((DefaultStoreClient<Object, Object>) factory.getStoreClient("best"));
            clients.add((DefaultStoreClient<Object, Object>) factory.getStoreClient("test"));
            clients.add((DefaultStoreClient<Object, Object>) factory.getStoreClient("best"));
            factories.add(factory);

            // bootstrap a number of times
            bootStrap(clients, 10);

            checkMbeanIdCount(CLIENT_DOMAIN, "ClientThreadPool*", 1, true);
            checkMbeanIdCount(CLIENT_DOMAIN, "ZenStoreClient*", 2, true);
            checkMbeanIdCount(CLUSTER_FAILUREDETECTOR_DOMAIN, "ThresholdFailureDetector*", 1, true);
            checkMbeanIdCount(PIPELINE_ROUTED_STATS_DOMAIN, "*", 2, true);
            checkMbeanIdCount(CLIENT_REQUEST_DOMAIN, "aggregated*", 1, true);
            checkMbeanIdCount(CLIENT_REQUEST_DOMAIN, "stats_localhost*", 1, true);
            checkMbeanIdCount(AGGREGATE_STATS_DOMAIN, "aggregate-perf*", 1, true);
            checkMbeanIdCount(STATS_DOMAIN, "*", 2, true);

        } catch(Exception e) {
            fail("Unexpected error " + e.getMessage());
        }
    }

    @Test
    public void testMultipleDistinctClientsOnMultipleFactories() {
        try {
            StoreClientFactory testfactory = getFactory(getValidBootstrapUrl());
            List<DefaultStoreClient<Object, Object>> clients = new ArrayList<DefaultStoreClient<Object, Object>>();
            clients.add((DefaultStoreClient<Object, Object>) testfactory.getStoreClient("test"));
            StoreClientFactory bestfactory = getFactory(getValidBootstrapUrl());
            clients.add((DefaultStoreClient<Object, Object>) bestfactory.getStoreClient("best"));
            factories.add(testfactory);
            factories.add(bestfactory);

            // bootstrap a number of times
            bootStrap(clients, 10);

            checkMbeanIdCount(CLIENT_DOMAIN, "ClientThreadPool*", 2, true);
            checkMbeanIdCount(CLIENT_DOMAIN, "ZenStoreClient*", 2, true);
            checkMbeanIdCount(CLUSTER_FAILUREDETECTOR_DOMAIN, "ThresholdFailureDetector*", 2, true);
            checkMbeanIdCount(PIPELINE_ROUTED_STATS_DOMAIN, "*", 2, true);
            checkMbeanIdCount(CLIENT_REQUEST_DOMAIN, "aggregated*", 2, true);
            checkMbeanIdCount(CLIENT_REQUEST_DOMAIN, "stats_localhost*", 2, true);
            checkMbeanIdCount(AGGREGATE_STATS_DOMAIN, "aggregate-perf*", 2, true);
            checkMbeanIdCount(STATS_DOMAIN, "*", 2, true);

        } catch(Exception e) {
            fail("Unexpected error " + e.getMessage());
        }
    }

    @Test
    public void testMultipleInDistinctClientsOnMultipleFactories() {
        try {
            StoreClientFactory factory1 = getFactory(getValidBootstrapUrl());
            List<DefaultStoreClient<Object, Object>> clients = new ArrayList<DefaultStoreClient<Object, Object>>();
            clients.add((DefaultStoreClient<Object, Object>) factory1.getStoreClient("test"));
            clients.add((DefaultStoreClient<Object, Object>) factory1.getStoreClient("test"));
            clients.add((DefaultStoreClient<Object, Object>) factory1.getStoreClient("best"));
            clients.add((DefaultStoreClient<Object, Object>) factory1.getStoreClient("best"));
            factories.add(factory1);

            StoreClientFactory factory2 = getFactory(getValidBootstrapUrl());
            clients.add((DefaultStoreClient<Object, Object>) factory2.getStoreClient("test"));
            clients.add((DefaultStoreClient<Object, Object>) factory2.getStoreClient("test"));
            clients.add((DefaultStoreClient<Object, Object>) factory2.getStoreClient("best"));
            clients.add((DefaultStoreClient<Object, Object>) factory2.getStoreClient("best"));
            factories.add(factory2);

            // bootstrap a number of times
            bootStrap(clients, 10);

            checkMbeanIdCount(CLIENT_DOMAIN, "ClientThreadPool*", 2, true);
            checkMbeanIdCount(CLIENT_DOMAIN, "ZenStoreClient*", 2, true);
            checkMbeanIdCount(CLUSTER_FAILUREDETECTOR_DOMAIN, "ThresholdFailureDetector*", 2, true);
            checkMbeanIdCount(PIPELINE_ROUTED_STATS_DOMAIN, "*", 4, true);
            checkMbeanIdCount(CLIENT_REQUEST_DOMAIN, "aggregated*", 2, true);
            checkMbeanIdCount(CLIENT_REQUEST_DOMAIN, "stats_localhost*", 2, true);
            checkMbeanIdCount(AGGREGATE_STATS_DOMAIN, "aggregate-perf*", 2, true);
            checkMbeanIdCount(STATS_DOMAIN, "*", 4, true);

        } catch(Exception e) {
            fail("Unexpected error " + e.getMessage());
        }
    }
}
