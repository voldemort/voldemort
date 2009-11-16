package voldemort.client;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;

import junit.framework.TestCase;
import voldemort.client.protocol.VoldemortFilter;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.store.Store;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.Pair;
import voldemort.versioning.Versioned;

public abstract class AbstractAdminServiceFilterTest extends TestCase {

    private static String testStoreName = "test-replication-memory";

    protected abstract AdminClient getAdminClient();

    protected abstract Set<Pair<ByteArray, Versioned<byte[]>>> createEntries();

    protected abstract Store<ByteArray, byte[]> getStore(int nodeId, String storeName);

    public void testFetchAsStreamWithFilter() {
        // user store should be present
        Store<ByteArray, byte[]> store = getStore(0, testStoreName);
        assertNotSame("Store '" + testStoreName + "' should not be null", null, store);

        VoldemortFilter filter = new VoldemortFilterImpl();
        int shouldFilterCount = 0;
        for(Pair<ByteArray, Versioned<byte[]>> pair: createEntries()) {
            store.put(pair.getFirst(), pair.getSecond());
            if(!filter.accept(pair.getFirst(), pair.getSecond())) {
                shouldFilterCount++;
            }
        }

        assertNotSame("should be filtered key count shoud not be 0.", 0, shouldFilterCount);

        // make fetch stream call with filter
        Iterator<Pair<ByteArray, Versioned<byte[]>>> entryIterator = getAdminClient().fetchPartitionEntries(0,
                                                                                                            testStoreName,
                                                                                                            Arrays.asList(new Integer[] { 0 }),
                                                                                                            filter);

        // assert none of the filtered entries are returned.
        while(entryIterator.hasNext()) {
            Pair<ByteArray, Versioned<byte[]>> entry = entryIterator.next();
            if(!filter.accept(entry.getFirst(), entry.getSecond())) {
                fail();
            }
        }
    }

    public void testDeleteStreamWithFilter() {
        // user store should be present
        Store<ByteArray, byte[]> store = getStore(0, testStoreName);
        assertNotSame("Store '" + testStoreName + "' should not be null", null, store);

        Set<Pair<ByteArray, Versioned<byte[]>>> entrySet = createEntries();

        VoldemortFilter filter = new VoldemortFilterImpl();
        for(Pair<ByteArray, Versioned<byte[]>> pair: entrySet) {
            store.put(pair.getFirst(), pair.getSecond());
        }

        // make delete stream call with filter
        getAdminClient().deletePartitions(0,
                                          testStoreName,
                                          Arrays.asList(new Integer[] { 0, 1, 2, 3 }),
                                          filter);

        // assert none of the filtered entries are returned.
        for(Pair<ByteArray, Versioned<byte[]>> entry: entrySet) {
            if(filter.accept(entry.getFirst(), entry.getSecond())) {
                assertEquals("All entries should be deleted except the filtered ones.",
                             0,
                             store.get(entry.getFirst()).size());
            } else {
                assertNotSame("filtered entry should be still present.",
                              0,
                              store.get(entry.getFirst()).size());
                assertEquals("values should match",
                             new String(entry.getSecond().getValue()),
                             new String(store.get(entry.getFirst()).get(0).getValue()));
            }
        }
    }

    public void testUpdateAsStreamWithFilter() {
        VoldemortFilter filter = new VoldemortFilterImpl();
        Set<Pair<ByteArray, Versioned<byte[]>>> entrySet = createEntries();

        // make update stream call with filter
        getAdminClient().updateEntries(0, testStoreName, entrySet.iterator(), filter);

        // assert none of the filtered entries are updated.
        // user store should be present
        Store<ByteArray, byte[]> store = getStore(0, testStoreName);

        assertNotSame("Store '" + testStoreName + "' should not be null", null, store);

        for(Pair<ByteArray, Versioned<byte[]>> entry: entrySet) {
            if(filter.accept(entry.getFirst(), entry.getSecond())) {
                assertEquals("Store should have this key/value pair",
                             1,
                             store.get(entry.getFirst()).size());
                assertEquals("Store should have this key/value pair",
                             entry.getSecond(),
                             store.get(entry.getFirst()).get(0));
            } else {
                assertEquals("Store should Not have this key/value pair",
                             0,
                             store.get(entry.getFirst()).size());
            }
        }
    }

    public static class VoldemortFilterImpl implements VoldemortFilter {

        public VoldemortFilterImpl() {
            System.out.println("instantiating voldemortFilter");
        }

        public boolean accept(Object key, Versioned<?> value) {
            String keyString = ByteUtils.getString(((ByteArray) key).get(), "UTF-8");
            if(Integer.parseInt(keyString) % 10 == 3) {
                return false;
            }
            return true;
        }
    }
}