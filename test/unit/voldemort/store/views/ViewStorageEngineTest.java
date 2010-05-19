package voldemort.store.views;

import java.util.List;
import java.util.Map;

import junit.framework.TestCase;
import voldemort.serialization.Serializer;
import voldemort.serialization.StringSerializer;
import voldemort.store.Store;
import voldemort.store.memory.InMemoryStorageEngine;
import voldemort.store.serialized.SerializingStore;
import voldemort.utils.ByteArray;
import voldemort.versioning.Versioned;

import com.google.common.collect.ImmutableList;

/**
 * Test cases for views
 * 
 * 
 */
public class ViewStorageEngineTest extends TestCase {

    private AddStrViewTrans transform = new AddStrViewTrans("42");
    private InMemoryStorageEngine<ByteArray, byte[], byte[]> targetRaw = new InMemoryStorageEngine<ByteArray, byte[], byte[]>("target");
    private Store<String, String, String> target = SerializingStore.wrap(targetRaw,
                                                                         new StringSerializer(),
                                                                         new StringSerializer(),
                                                                         null);
    private Store<String, String, String> valView = getEngine(transform);

    @Override
    public void setUp() {
        target.put("hello", Versioned.value("world"), null);
    }

    public Store<String, String, String> getEngine(View<?, ?, ?, ?> valTrans) {
        Serializer<String> s = new StringSerializer();
        return SerializingStore.wrap(new ViewStorageEngine("test", targetRaw, s, s, s, s, valTrans),
                                     s,
                                     s,
                                     s);
    }

    public void testGetWithValueTransform() {
        assertEquals("View should add 42", "world42", valView.get("hello", "concat")
                                                             .get(0)
                                                             .getValue());
        assertEquals("Null value should return empty list", 0, valView.get("laksjdf", "concat")
                                                                      .size());
    }

    public void testGetAll() {
        target.put("a", Versioned.value("a"), null);
        target.put("b", Versioned.value("b"), null);
        Map<String, List<Versioned<String>>> found = valView.getAll(ImmutableList.of("a", "b"),
                                                                    null);
        assertTrue(found.containsKey("a"));
        assertTrue(found.containsKey("b"));
        assertEquals("a42", found.get("a").get(0).getValue());
        assertEquals("b42", found.get("b").get(0).getValue());
    }

    public void testPut() {
        valView.put("abc", Versioned.value("cde"), null);
        assertEquals("c", target.get("abc", null).get(0).getValue());
    }

    /* A view that just adds or subtracts the given string */
    private static class AddStrViewTrans implements View<String, String, String, String> {

        private String str;

        public AddStrViewTrans(String str) {
            this.str = str;
        }

        public String storeToView(Store<String, String, String> store, String k, String s, String t) {
            if(s == null)
                return str;
            else if(t.equalsIgnoreCase("concat"))
                return s + str;
            return str;
        }

        public String viewToStore(Store<String, String, String> store, String k, String v, String t) {
            if(v == null)
                return null;
            else
                return v.substring(0, Math.max(0, v.length() - str.length()));
        }

    }
}
