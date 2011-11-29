package voldemort.collections;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import junit.framework.TestCase;
import voldemort.client.MockStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.StoreClientFactory;
import voldemort.serialization.Serializer;
import voldemort.serialization.json.JsonTypeSerializer;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Versioned;

/**
 * Unit tests for voldemort stack implementation.
 */
public class TestVStack extends TestCase {

    private static StoreClientFactory storeFactory;

    static {
        Serializer<Object> keySerializer = new JsonTypeSerializer("{\"value\": \"string\", \"id\" : \"int32\"}");
        Serializer<Object> valueSerializer = new JsonTypeSerializer("{\"id\":\"int32\","
                                                                    + "\"nextId\":\"int32\","
                                                                    + "\"previousId\":\"int32\","
                                                                    + "\"stable\":\"boolean\","
                                                                    + "\"value\":\"string\"}");

        storeFactory = new MockStoreClientFactory(keySerializer, valueSerializer, null);
    }

    public void testHappyPath() {
        StoreClient<Map<String, Object>, Map<String, Object>> storeClient = storeFactory.getStoreClient("test");

        VStack<String, String> stack = new VStack<String, String>("stack", storeClient);

        // empty?
        assertTrue(stack.isEmpty());
        assertEquals(0, stack.size());

        // test add
        stack.add("world");
        assertEquals("world", stack.peek());
        assertEquals(1, stack.size());
        assertFalse(stack.isEmpty());

        assertTrue(stack.offer("hello"));
        assertEquals(2, stack.size());
        assertEquals("world", stack.getById(1));

        stack.add("goodbye");
        assertEquals(3, stack.size());
        assertEquals("hello", stack.getById(2));

        // test iterator
        String[] strArray = new String[] { "goodbye", "hello", "world" };
        int i = 0;
        for(String word: stack) {
            assertEquals(strArray[i], word);
            i++;
        }

        // test ListIterator
        i = 0;
        MappedListIterator<Integer, Versioned<String>> it = stack.listIterator();
        while(it.hasNext()) {
            assertEquals(strArray[i], it.next().getValue());
            i++;
        }
        while(it.hasPrevious()) {
            i--;
            assertEquals(strArray[i], it.previous().getValue());
        }

        // previousListIterator
        it = stack.previousListIterator(1);
        i = 3;
        while(it.hasPrevious()) {
            i--;
            assertEquals(strArray[i], it.previous().getValue());
        }

        // nextId, previousId, lastId, getById
        it = stack.listIterator();

        String s1 = it.next().getValue();
        assertEquals(s1, stack.getById(it.previousId()));
        assertEquals(s1, stack.getById(stack.listIterator().nextId()));

        String s2 = it.next().getValue();
        Integer id = it.lastId();
        assertEquals(s2, stack.getById(id));
    }

    public void testErrors() throws Exception {
        StoreClient<Map<String, Object>, Map<String, Object>> storeClient = storeFactory.getStoreClient("test");
        VStack<String, String> stack = new VStack<String, String>("stack2", storeClient);
        stack.add("world");
        stack.add("hello");

        try {
            stack.getById(2);
            TestCase.fail("expected IndexOutOfBoundsException");
        } catch(IndexOutOfBoundsException e) {}

        try {
            stack.add(null);
            TestCase.fail("expected NullPointerException");
        } catch(NullPointerException npe) {}
    }

    public void testSetGetById() throws Exception {
        StoreClient<Map<String, Object>, Map<String, Object>> storeClient = storeFactory.getStoreClient("test");
        VStack<String, String> stack = new VStack<String, String>("stack3", storeClient);
        // 3 2 1
        stack.add("1");
        stack.add("2");
        stack.add("3");
        assertEquals(3, stack.size());

        VListIterator<String> it = stack.listIterator(0);
        assertNull(it.previousId());
        String s = it.next().getValue();
        assertEquals("3", s);
        // 4 2 1
        it.setValue("4");

        assertEquals("4", it.previous().getValue());

        assertEquals("4", it.next().getValue());
        assertEquals("2", it.next().getValue());
        assertEquals("1", it.next().getValue());
        Versioned<String> vs = it.previous();
        assertEquals("1", vs.getValue());

        // 4 1 5
        it.set(new Versioned<String>("5", vs.getVersion()));
        assertEquals("5", it.next().getValue());
        assertEquals(3, stack.size());

        try {
            it.set(new Versioned<String>("6", vs.getVersion()));
            TestCase.fail("expected ObsoleteVersionException");
        } catch(ObsoleteVersionException e) {}

        assertNull(it.nextId());
        Integer id = it.previousId();
        stack.setById(id, "7");
        assertEquals("7", stack.getById(id));
    }

    public void testEmptyStack() throws Exception {
        StoreClient<Map<String, Object>, Map<String, Object>> storeClient = storeFactory.getStoreClient("test");
        VStack<String, String> stack = new VStack<String, String>("stack4", storeClient);

        assertEquals(0, stack.size());

        Iterator<String> it = stack.iterator();
        assertFalse(it.hasNext());

        try {
            it.next();
            TestCase.fail("expected NoSuchElementException");
        } catch(NoSuchElementException e) {}
    }
}
