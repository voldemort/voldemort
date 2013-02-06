package voldemort.client;

import java.util.Iterator;
import java.util.Set;

import junit.framework.TestCase;

import org.junit.Test;

import voldemort.client.protocol.VoldemortFilter;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.cluster.Cluster;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.NodeUtils;
import voldemort.utils.Pair;
import voldemort.versioning.Versioned;

import com.google.common.collect.Lists;

public abstract class AbstractAdminServiceFilterTest extends TestCase {

    protected static String testStoreName = "test-replication-memory";

    protected abstract AdminClient getAdminClient();

    protected abstract Set<Pair<ByteArray, Versioned<byte[]>>> createEntries();

    protected abstract Store<ByteArray, byte[], byte[]> getStore(int nodeId, String storeName);

    protected abstract Cluster getCluster();

    protected abstract StoreDefinition getStoreDef();

    @Test
    public void testFetchAsStreamWithFilter() {
        // user store should be present
        Store<ByteArray, byte[], byte[]> store = getStore(0, testStoreName);
        assertNotSame("Store '" + testStoreName + "' should not be null", null, store);

        VoldemortFilter filter = new VoldemortFilterImpl();
        int shouldFilterCount = 0;
        RoutingStrategy strategy = new RoutingStrategyFactory().updateRoutingStrategy(getStoreDef(),
                                                                                      getCluster());
        for(Pair<ByteArray, Versioned<byte[]>> pair: createEntries()) {
            if(NodeUtils.getNodeIds(strategy.routeRequest(pair.getFirst().get())).contains(0)) {
                store.put(pair.getFirst(), pair.getSecond(), null);
                if(!filter.accept(pair.getFirst(), pair.getSecond())) {
                    shouldFilterCount++;
                }
            }
        }

        // make fetch stream call with filter
        Iterator<Pair<ByteArray, Versioned<byte[]>>> entryIterator = getAdminClient().bulkFetchOps.fetchEntries(0,
                                                                                                                testStoreName,
                                                                                                                getCluster().getNodeById(0)
                                                                                                                            .getPartitionIds(),
                                                                                                                filter,
                                                                                                                false);

        // assert none of the filtered entries are returned.
        while(entryIterator.hasNext()) {
            Pair<ByteArray, Versioned<byte[]>> entry = entryIterator.next();
            if(!filter.accept(entry.getFirst(), entry.getSecond())) {
                fail();
            }
        }
    }

    @Test
    public void testDeleteStreamWithFilter() {
        // user store should be present
        Store<ByteArray, byte[], byte[]> store = getStore(0, testStoreName);
        assertNotSame("Store '" + testStoreName + "' should not be null", null, store);

        Set<Pair<ByteArray, Versioned<byte[]>>> entrySet = createEntries();

        VoldemortFilter filter = new VoldemortFilterImpl();
        RoutingStrategy strategy = new RoutingStrategyFactory().updateRoutingStrategy(getStoreDef(),
                                                                                      getCluster());
        for(Pair<ByteArray, Versioned<byte[]>> pair: entrySet) {
            if(NodeUtils.getNodeIds(strategy.routeRequest(pair.getFirst().get())).contains(0))
                store.put(pair.getFirst(), pair.getSecond(), null);
        }

        // make delete stream call with filter
        getAdminClient().storeMntOps.deletePartitions(0,
                                                      testStoreName,
                                                      Lists.newArrayList(0, 1),
                                                      filter);

        // assert none of the filtered entries are returned.
        for(Pair<ByteArray, Versioned<byte[]>> entry: entrySet) {
            if(NodeUtils.getNodeIds(strategy.routeRequest(entry.getFirst().get())).contains(0)) {
                if(filter.accept(entry.getFirst(), entry.getSecond())) {
                    assertEquals("All entries should be deleted except the filtered ones.",
                                 0,
                                 store.get(entry.getFirst(), null).size());
                } else {
                    assertNotSame("filtered entry should be still present.",
                                  0,
                                  store.get(entry.getFirst(), null).size());
                    assertEquals("values should match",
                                 new String(entry.getSecond().getValue()),
                                 new String(store.get(entry.getFirst(), null).get(0).getValue()));
                }
            }
        }
    }

    @Test
    public void testUpdateAsStreamWithFilter() {
        VoldemortFilter filter = new VoldemortFilterImpl();
        Set<Pair<ByteArray, Versioned<byte[]>>> entrySet = createEntries();

        // make update stream call with filter
        getAdminClient().streamingOps.updateEntries(0, testStoreName, entrySet.iterator(), filter);

        // assert none of the filtered entries are updated.
        // user store should be present
        Store<ByteArray, byte[], byte[]> store = getStore(0, testStoreName);

        assertNotSame("Store '" + testStoreName + "' should not be null", null, store);

        for(Pair<ByteArray, Versioned<byte[]>> entry: entrySet) {
            if(filter.accept(entry.getFirst(), entry.getSecond())) {
                assertEquals("Store should have this key/value pair",
                             1,
                             store.get(entry.getFirst(), null).size());
                assertEquals("Store should have this key/value pair",
                             entry.getSecond(),
                             store.get(entry.getFirst(), null).get(0));
            } else {
                assertEquals("Store should Not have this key/value pair",
                             0,
                             store.get(entry.getFirst(), null).size());
            }
        }
    }

    public static class VoldemortFilterImpl implements VoldemortFilter {

        public boolean accept(Object key, Versioned<?> value) {
            String keyString = ByteUtils.getString(((ByteArray) key).get(), "UTF-8");
            if(Integer.parseInt(keyString) % 10 == 3) {
                return false;
            }
            return true;
        }
    }
}