package voldemort.store.views;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;
import voldemort.serialization.Serializer;
import voldemort.serialization.StringSerializer;
import voldemort.serialization.json.JsonTypeSerializer;
import voldemort.store.Store;
import voldemort.store.memory.InMemoryStorageEngine;
import voldemort.store.serialized.SerializingStore;
import voldemort.utils.ByteArray;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * Test cases for views
 * 
 * 
 */
public class ViewStorageEngineTest extends TestCase {

    private AddStrViewTrans transform = new AddStrViewTrans("42");
    private InMemoryStorageEngine<ByteArray, byte[], byte[]> targetRaw1 = new InMemoryStorageEngine<ByteArray, byte[], byte[]>("target1");
    private Store<String, String, String> target1 = SerializingStore.wrap(targetRaw1,
                                                                          new StringSerializer(),
                                                                          new StringSerializer(),
                                                                          new StringSerializer());
    private Store<String, String, String> valView = getEngine1(transform);
    private Serializer<Integer> keySer = new IntegerSerializer();
    private Serializer<List<Integer>> valueSer = new IntegerListSerializer();
    private Serializer<List<Integer>> transSer = new IntegerListSerializer();

    private InMemoryStorageEngine<ByteArray, byte[], byte[]> targetRaw2 = new InMemoryStorageEngine<ByteArray, byte[], byte[]>("target2");

    private Store<Integer, List<Integer>, List<Integer>> target2 = SerializingStore.wrap(targetRaw2,
                                                                                         keySer,
                                                                                         valueSer,
                                                                                         transSer);
    private Store<Integer, List<Integer>, List<Integer>> view = getEngine2(new RangeFilterView());

    @Override
    public void setUp() {
        target1.put("hello", Versioned.value("world"), null);
        Integer[] values1 = { 1, 2, 3, 4, 5, 6, 7, 8 };
        Integer[] values2 = { 100, 200, 300, 400, 500, 600, 700 };
        target2.put(1, Versioned.value(Arrays.asList(values1)), null);
        target2.put(100, Versioned.value(Arrays.asList(values2)), null);
    }

    public Store<String, String, String> getEngine1(View<?, ?, ?, ?> valTrans) {
        Serializer<String> s = new StringSerializer();
        return SerializingStore.wrap(new ViewStorageEngine("test",
                                                           targetRaw1,
                                                           s,
                                                           s,
                                                           s,
                                                           s,
                                                           null,
                                                           valTrans), s, s, s);
    }

    public Store<Integer, List<Integer>, List<Integer>> getEngine2(View<?, ?, ?, ?> view) {
        return SerializingStore.wrap(new ViewStorageEngine("transTest",
                                                           targetRaw2,
                                                           valueSer,
                                                           transSer,
                                                           keySer,
                                                           valueSer,
                                                           null,
                                                           view), keySer, valueSer, transSer);
    }

    public void testGetWithValueTransform() {
        assertEquals("View should add 42", "world42", valView.get("hello", "concat")
                                                             .get(0)
                                                             .getValue());
        assertEquals("Null value should return empty list", 0, valView.get("laksjdf", "concat")
                                                                      .size());
    }

    public void testGetAll() {
        target1.put("a", Versioned.value("a"), null);
        target1.put("b", Versioned.value("b"), null);
        Map<String, List<Versioned<String>>> found = valView.getAll(ImmutableList.of("a", "b"),
                                                                    ImmutableMap.of("a",
                                                                                    "concat",
                                                                                    "b",
                                                                                    "concat"));
        assertTrue(found.containsKey("a"));
        assertTrue(found.containsKey("b"));
        assertEquals("a42", found.get("a").get(0).getValue());
        assertEquals("b42", found.get("b").get(0).getValue());
    }

    public void testPut() {
        valView.put("abc", Versioned.value("cde"), null);
        assertEquals("c", target1.get("abc", null).get(0).getValue());
    }

    public void testGetWithTransforms() {
        Integer[] filter2 = { 5, 8 };
        assertEquals(4, view.get(1, Arrays.asList(filter2)).get(0).getValue().size());

        Integer[] filter1 = { 1, 5 };
        assertEquals(5, view.get(1, Arrays.asList(filter1)).get(0).getValue().size());

    }

    public void testPutWithTransforms() {
        Integer[] values1 = { 9, 90, 10, 15, 25, 106 };
        Integer[] filter1 = { 1, 10 };

        Versioned<List<Integer>> values = Versioned.value(Arrays.asList(values1));
        VectorClock clock = (VectorClock) values.getVersion();
        clock.incrementVersion(0, System.currentTimeMillis());
        view.put(1, Versioned.value(values.getValue(), clock), Arrays.asList(filter1));

        assertEquals(10, view.get(1, Arrays.asList(filter1)).get(0).getValue().size());

        Integer[] filter2 = { 5, 10 };
        assertEquals(6, view.get(1, Arrays.asList(filter2)).get(0).getValue().size());

        Version updatedVersion = view.get(1, Arrays.asList(filter2)).get(0).getVersion();

        Integer[] filter3 = { 1, 50 };

        Integer[] values2 = { 90, 15, 25, 106 };
        clock = (VectorClock) updatedVersion;
        VectorClock clock1 = clock.incremented(0, System.currentTimeMillis());
        view.put(1, Versioned.value(Arrays.asList(values2), clock1), Arrays.asList(filter3));

        assertEquals(12, view.get(1, Arrays.asList(filter3)).get(0).getValue().size());
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
            else if(t != null && t.equalsIgnoreCase("concat"))
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

    public static class IntegerListSerializer implements Serializer<List<Integer>> {

        private JsonTypeSerializer serializer;

        public IntegerListSerializer() {
            this.serializer = new JsonTypeSerializer("[\"int32\"]");
        }

        public byte[] toBytes(List<Integer> object) {
            return serializer.toBytes(object);
        }

        @SuppressWarnings("unchecked")
        public List<Integer> toObject(byte[] bytes) {
            return (List<Integer>) serializer.toObject(bytes);
        }
    }

    private static class IntegerSerializer implements Serializer<Integer> {

        private JsonTypeSerializer serializer;

        public IntegerSerializer() {
            this.serializer = new JsonTypeSerializer("\"int32\"");
        }

        public byte[] toBytes(Integer object) {
            return serializer.toBytes(object);
        }

        public Integer toObject(byte[] bytes) {
            return (Integer) serializer.toObject(bytes);
        }

    }
}
