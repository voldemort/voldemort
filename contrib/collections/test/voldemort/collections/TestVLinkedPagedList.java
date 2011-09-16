package voldemort.collections;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import junit.framework.TestCase;
import voldemort.client.MockStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.StoreClientFactory;
import voldemort.serialization.Serializer;
import voldemort.serialization.json.JsonTypeSerializer;
import voldemort.utils.Identifiable;

/**
 * Unit tests for voldemort linked and paged list implementation.
 */
public class TestVLinkedPagedList extends TestCase {

    private static StoreClientFactory stackStoreFactory;
    private static StoreClientFactory pageIndexStoreFactory;

    static {
        Serializer<Object> stackKeySerializer = new JsonTypeSerializer("{\"value\": \"string\", \"id\" : \"int32\"}");
        Serializer<Object> stackValueSerializer = new JsonTypeSerializer("{\"id\":\"int32\","
                                                                         + "\"nextId\":\"int32\","
                                                                         + "\"previousId\":\"int32\","
                                                                         + "\"stable\":\"boolean\","
                                                                         + "\"value\": [\"bytes\"]}");

        stackStoreFactory = new MockStoreClientFactory(stackKeySerializer,
                                                       stackValueSerializer,
                                                       null);

        Serializer<Object> pageIndexKeySerializer = new JsonTypeSerializer("\"string\"");
        Serializer<Object> pageIndexValueSerializer = new JsonTypeSerializer("[{\"pageId\" : \"int32\","
                                                                             + "\"lastIndex\" : \"bytes\"}]");
        pageIndexStoreFactory = new MockStoreClientFactory(pageIndexKeySerializer,
                                                           pageIndexValueSerializer,
                                                           null);
    }

    public void testHappyPath() {
        StoreClient<Map<String, Object>, Map<String, Object>> stackClient = stackStoreFactory.getStoreClient("test");
        StoreClient<String, List<Map<String, Object>>> pageIndexClient = pageIndexStoreFactory.getStoreClient("test");

        Serializer<StringOrder> serializer = new StringOrderSerializer();

        VLinkedPagedList<String, StringOrder> stringList = new VLinkedPagedList<String, StringOrder>("stringList",
                                                                                                     stackClient,
                                                                                                     pageIndexClient,
                                                                                                     serializer,
                                                                                                     7,
                                                                                                     5);
        VStack<String, List<byte[]>> stack = new VStack<String, List<byte[]>>("stringList",
                                                                              stackClient);
        initStringList(stringList);

        // verify pageIndex
        int[] pageValues = new int[] { 0, 3, 2, 1 };
        StringOrder[] indexValues = new StringOrder[] { new StringOrder(30), new StringOrder(20),
                new StringOrder(10), new StringOrder(0) };
        int j = 0;
        List<Map<String, Object>> pageIndex = pageIndexClient.getValue("stringList");
        for(Map<String, Object> indexEntryMap: pageIndex) {
            VPageIndexEntry<StringOrder> entry = VPageIndexEntry.valueOf(indexEntryMap, serializer);
            assertEquals(Integer.valueOf(pageValues[j]), entry.getPageId());
            assertEquals(indexValues[j], entry.getLastIndex());
            j++;
        }

        // iterate and verify values
        int c = 40;
        for(StringOrder i: stringList) {
            assertEquals(Integer.valueOf(c), i.getId());
            c -= 2;
        }

        assertEquals(4, stack.size());

        // go forward and back on the same iterator
        MappedListIterator<VLinkedPagedKey, StringOrder> it = stringList.listIterator();
        for(int i = 40; i >= 28; i -= 2) {
            assertEquals(Integer.valueOf(i), it.next().getId());
        }

        for(int i = 28; i <= 40; i += 2) {
            assertEquals(Integer.valueOf(i), it.previous().getId());
        }

        // add values and make sure the stack is sized appropriately
        stringList.add(new StringOrder(50));
        assertEquals(4, stack.size());
        stringList.add(new StringOrder(60));
        assertEquals(5, stack.size());
        stringList.add(new StringOrder(70));
        assertEquals(5, stack.size());

        // test MappedListIterator
        it = stringList.listIterator();
        it.next();
        it.next();
        VLinkedPagedKey key = it.nextId();
        Integer value = stringList.getById(key).getId();
        assertEquals(value, it.next().getId());
        assertEquals(value, stringList.getById(it.lastId()).getId());
        assertEquals(value, stringList.getById(it.previousId()).getId());
    }

    public void testLinearSearch() {
        StoreClient<Map<String, Object>, Map<String, Object>> stackClient = stackStoreFactory.getStoreClient("test");
        StoreClient<String, List<Map<String, Object>>> pageIndexClient = pageIndexStoreFactory.getStoreClient("test");

        Serializer<StringOrder> serializer = new StringOrderSerializer();

        VLinkedPagedList<String, StringOrder> stringList = new VLinkedPagedList<String, StringOrder>("stringList",
                                                                                                     stackClient,
                                                                                                     pageIndexClient,
                                                                                                     serializer,
                                                                                                     7,
                                                                                                     5);
        initStringList(stringList);

        MappedListIterator<VLinkedPagedKey, StringOrder> it = stringList.listIterator(new StringOrder(10),
                                                                                      true);
        assertEquals(Integer.valueOf(10), it.next().getId());

        it = stringList.listIterator(new StringOrder(10), false);
        assertEquals(Integer.valueOf(10), it.previous().getId());

        it = stringList.listIterator(new StringOrder(9), true);
        assertEquals(new Integer(8), it.next().getId());

        it = stringList.listIterator(new StringOrder(9), false);
        assertEquals(new Integer(10), it.previous().getId());
    }

    public void testOutOfBounds() {
        StoreClient<Map<String, Object>, Map<String, Object>> stackClient = stackStoreFactory.getStoreClient("test");
        StoreClient<String, List<Map<String, Object>>> pageIndexClient = pageIndexStoreFactory.getStoreClient("test");

        Serializer<StringOrder> serializer = new StringOrderSerializer();

        VLinkedPagedList<String, StringOrder> stringList = new VLinkedPagedList<String, StringOrder>("stringList",
                                                                                                     stackClient,
                                                                                                     pageIndexClient,
                                                                                                     serializer,
                                                                                                     7,
                                                                                                     5);
        initStringList(stringList);
        MappedListIterator<VLinkedPagedKey, StringOrder> it = stringList.listIterator();
        assertNull(it.previousId());
        try {
            it.previous();
            TestCase.fail("expected NoSuchElementException");
        } catch(NoSuchElementException e) {}

        for(int i = 40; i >= 0; i -= 2) {
            it.next();
        }
        assertNull(it.nextId());
        try {
            it.next();
            TestCase.fail("expected NoSuchElementException");
        } catch(NoSuchElementException e) {}
    }

    public void testEmptyList() {
        StoreClient<Map<String, Object>, Map<String, Object>> stackClient = stackStoreFactory.getStoreClient("test");
        StoreClient<String, List<Map<String, Object>>> pageIndexClient = pageIndexStoreFactory.getStoreClient("test");

        Serializer<StringOrder> serializer = new StringOrderSerializer();

        VLinkedPagedList<String, StringOrder> stringList = new VLinkedPagedList<String, StringOrder>("stringList",
                                                                                                     stackClient,
                                                                                                     pageIndexClient,
                                                                                                     serializer,
                                                                                                     7,
                                                                                                     5);

        Iterator<StringOrder> it = stringList.iterator();
        assertFalse(it.hasNext());
        try {
            it.next();
            TestCase.fail("expected NoSuchElementException");
        } catch(NoSuchElementException e) {}

        MappedListIterator<VLinkedPagedKey, StringOrder> mlit = stringList.listIterator(null, false);
        assertFalse(mlit.hasNext());
        assertFalse(mlit.hasPrevious());
        try {
            mlit.previous();
            TestCase.fail("expected NoSuchElementException");
        } catch(NoSuchElementException e) {}

        try {
            mlit.next();
            TestCase.fail("expected NoSuchElementException");
        } catch(NoSuchElementException e) {}
    }

    /**
     * Initializes a stringList to the following configuration:
     * 
     * Page 0 (head): [40 38 37 36 34 32 30]
     * 
     * Page 3: [28 26 24 22 20]
     * 
     * Page 2: [18 16 14 12 10]
     * 
     * Page 1 (tail): [8 6 4 2 0]
     * 
     * @param stringList
     */
    private void initStringList(VLinkedPagedList<String, StringOrder> stringList) {
        // add even values from 0 to 40
        for(int i = 0; i <= 40; i += 2) {
            stringList.add(new StringOrder(i));
            assertEquals(Integer.valueOf(i), stringList.peek().getId());
        }
    }

    public static class StringOrder implements Comparable<StringOrder>, Identifiable<Integer> {

        private Integer _id;

        public StringOrder(Integer id) {
            _id = id;
        }

        public Integer getId() {
            return _id;
        }

        public int compareTo(StringOrder o) {
            return _id.compareTo(o.getId());
        }

        public boolean equals(Object o) {
            StringOrder so = (StringOrder) o;
            return so._id == this._id;
        }

        public String toString() {
            return _id.toString();
        }
    }

    public static class StringOrderSerializer implements Serializer<StringOrder> {

        public byte[] toBytes(StringOrder object) {
            return new byte[] { object.getId().byteValue() };
        }

        public StringOrder toObject(byte[] bytes) {
            return new StringOrder(Byte.valueOf(bytes[0]).intValue());
        }
    }
}
