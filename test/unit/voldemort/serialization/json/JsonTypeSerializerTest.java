/*
 * Copyright 2008-2009 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package voldemort.serialization.json;

import static java.util.Arrays.asList;
import static voldemort.TestUtils.quote;
import static voldemort.serialization.json.JsonTypeDefinition.fromJson;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;
import voldemort.TestUtils;
import voldemort.serialization.SerializationException;
import voldemort.serialization.Serializer;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

/**
 * TODO: cases to consider: null values? extra bytes?
 * 
 * 
 */
@SuppressWarnings("unchecked")
public class JsonTypeSerializerTest extends TestCase {

    /* Get a type serializer for the given json type */
    private JsonTypeSerializer getSerializer(String typeDef) {
        return new JsonTypeSerializer(JsonTypeDefinition.fromJson(typeDef));
    }

    /* Get a type serializer for the given versions and type defs */
    private JsonTypeSerializer getSerializer(Object... versions) {
        Map<Integer, JsonTypeDefinition> defs = Maps.newHashMap();
        int count = 0;
        assertTrue("Must have an equal number of versions and type defs.", versions.length % 2 == 0);
        Integer key = null;
        for(Object o: versions) {
            if(count % 2 == 0) {
                key = (Integer) o;
                defs.put(key, null);
            } else {
                defs.put(key, JsonTypeDefinition.fromJson((String) o));
            }
            count++;
        }
        return new JsonTypeSerializer(defs);
    }

    public void testValidTypeDefs() {
        // simple types
        assertEquals(JsonTypes.INT8, fromJson(quote("int8")).getType());
        assertEquals(JsonTypes.INT16, fromJson(quote("int16")).getType());
        assertEquals(JsonTypes.INT32, fromJson(quote("int32")).getType());
        assertEquals(JsonTypes.INT64, fromJson(quote("int64")).getType());
        assertEquals(JsonTypes.FLOAT32, fromJson(quote("float32")).getType());
        assertEquals(JsonTypes.FLOAT64, fromJson(quote("float64")).getType());
        assertEquals(JsonTypes.STRING, fromJson(quote("string")).getType());
        assertEquals(JsonTypes.BYTES, fromJson(quote("bytes")).getType());

        // arrays
        assertEquals(asList(JsonTypes.INT8), fromJson("[\"int8\"]").getType());
        assertEquals(asList(JsonTypes.STRING), fromJson("[\"string\"]").getType());
        assertEquals(asList(asList(JsonTypes.INT16)), fromJson("[[\"int16\"]]").getType());

        // objects
        assertEquals(Maps.newHashMap(), fromJson("{}").getType());
        assertEquals(ImmutableMap.of("hello", JsonTypes.INT32, "blah", JsonTypes.FLOAT32),
                     fromJson("{\"hello\":\"int32\",\"blah\":\"float32\"}").getType());
    }

    public void testInvalidTypeDefs() {
        assertInvalidTypeDef("1234");
        assertInvalidTypeDef("[1234]");
        assertInvalidTypeDef("abc");
        assertInvalidTypeDef(quote("abc"));
    }

    public void testMapTypeKeyOrder() {
        String typeSig = "{\"name\":\"string\", \"color\":\"string\", \"age\":\"int32\"}";
        Object data = ImmutableMap.of("name", "xxx", "color", "yyy", "age", 28);

        Serializer outOfOrderSerializer = new JsonTypeSerializer(new JsonTypeDefinition(ImmutableMap.of("name",
                                                                                                        JsonTypes.STRING,
                                                                                                        "color",
                                                                                                        JsonTypes.STRING,
                                                                                                        "age",
                                                                                                        JsonTypes.INT32)));
        assertEquals("Key order in Map should not matter while serializing/deserializing",
                     data,
                     outOfOrderSerializer.toObject(getSerializer(typeSig).toBytes(data)));

        outOfOrderSerializer = new JsonTypeSerializer(new JsonTypeDefinition(ImmutableMap.of("color",
                                                                                             JsonTypes.STRING,
                                                                                             "age",
                                                                                             JsonTypes.INT32,
                                                                                             "name",
                                                                                             JsonTypes.STRING)));
        assertEquals("Key order in Map should not matter while serializing/deserializing",
                     data,
                     outOfOrderSerializer.toObject(getSerializer(typeSig).toBytes(data)));

        outOfOrderSerializer = new JsonTypeSerializer(new JsonTypeDefinition(ImmutableMap.of("age",
                                                                                             JsonTypes.INT32,
                                                                                             "name",
                                                                                             JsonTypes.STRING,
                                                                                             "color",
                                                                                             JsonTypes.STRING)));
        assertEquals("Key order in Map should not matter while serializing/deserializing",
                     data,
                     outOfOrderSerializer.toObject(getSerializer(typeSig).toBytes(data)));

        outOfOrderSerializer = new JsonTypeSerializer(new JsonTypeDefinition(ImmutableMap.of("color",
                                                                                             JsonTypes.STRING,
                                                                                             "name",
                                                                                             JsonTypes.STRING,
                                                                                             "age",
                                                                                             JsonTypes.INT32)));
        assertEquals("Key order in Map should not matter while serializing/deserializing",
                     data,
                     outOfOrderSerializer.toObject(getSerializer(typeSig).toBytes(data)));

    }

    public void testToObjectIsInverseOfToBytes() {
        /* test primitive types */
        assertInverse(quote("int8"), (byte) 127);
        assertInverse(quote("int16"), (short) 12700);
        assertInverse(quote("int32"), 12754555);
        assertInverse(quote("int64"), 293847238433L);
        assertInverse(quote("float32"), 12345.1234f);
        assertInverse(quote("float64"), 12345.1234d);
        assertInverse(quote("string"), "asdfasdf d");
        assertInverse(quote("string"), "");
        assertInverse(quote("date"), new Date(1234L));

        // test null values
        assertInverse(quote("string"), null);
        assertInverse(quote("int8"), null);
        assertInverse(quote("int16"), null);
        assertInverse(quote("int32"), null);
        assertInverse(quote("int64"), null);
        assertInverse(quote("float32"), null);
        assertInverse(quote("float64"), null);
        assertInverse(quote("date"), null);
        assertInverse("[\"int32\"]", null);
        assertInverse("[\"int64\"]", null);
        assertInverse("{}", null);

        /* test composition types */
        // []
        assertInverse("[\"string\"]", new ArrayList<String>());

        // ["hello","there"]
        assertInverse("[\"string\"]", asList("hello", "there"));

        // {"name" : "jay", "color":"pale"}
        assertInverse("{\"name\":\"string\", \"color\":\"string\"}", ImmutableMap.of("name",
                                                                                     "jay",
                                                                                     "color",
                                                                                     "pale"));

        // {"name" : "jay", "arms":["right", "left"], "random":{"foo":45}}
        Map<String, Object> m = Maps.newHashMap();
        m.put("name", "jay");
        m.put("arms", asList("right", "left"));
        Map<String, Object> m2 = Maps.newHashMap();
        m2.put("foo", 45);
        m.put("random", m2);
        assertInverse("{\"name\":\"string\", \"arms\":[\"string\"], \"random\":{\"foo\":\"int32\"}}",
                      m);
    }

    public void testBadToObjectInput() {
        assertToObjectFails(quote("int32"), new byte[] { 1, 2, 3 });
        assertToObjectFails(quote("string"), new byte[] {});
        assertToObjectFails(quote("[]"), new byte[] {});
        assertToObjectFails(quote("{}"), new byte[] {});
    }

    public void testVersioning() {
        // Test simple case
        assertEquals(5L, doubleInvert(getSerializer(0, quote("int32"), 1, quote("int64")), 5L));

        // Test schema updating schema to version 1, with existing values in v0
        // schema
        // v0 values should come back according to the v0 schema and v1 values
        // with the v1 schema
        String valueV0 = "hello";
        Date valueV1 = new Date();
        JsonTypeSerializer v0 = getSerializer(0, quote("string"));
        JsonTypeSerializer v1 = getSerializer(0, quote("string"), 1, quote("date"));
        byte[] bytes0 = v0.toBytes(valueV0);
        byte[] bytes1 = v1.toBytes(valueV1);
        assertEquals(valueV0, v1.toObject(bytes0));
        assertEquals(valueV1, v1.toObject(bytes1));
    }

    public void testBadToBytesInput() {
        assertToBytesFails("{\"name\":\"string\"}", Maps.newHashMap());
        assertToBytesFails("[\"string\"]", asList(123));
        assertToBytesFails("[\"string\"]", asList("abc", 123));
        assertToBytesFails(quote("int32"), 1234L);
        assertToBytesFails(quote("string"), new Date());
        assertToBytesFails(quote("int32"), 1234L);

        // test too small value
        assertToBytesFails(quote("int8"), Byte.MIN_VALUE);
        assertToBytesFails(quote("int16"), Short.MIN_VALUE);
        assertToBytesFails(quote("int32"), Integer.MIN_VALUE);
        assertToBytesFails(quote("int64"), Long.MIN_VALUE);
        assertToBytesFails(quote("float32"), Float.MIN_VALUE);
        assertToBytesFails(quote("float64"), Double.MIN_VALUE);
        assertToBytesFails(quote("date"), new Date(Long.MIN_VALUE));

        // list with wrong type
        assertToBytesFails("[\"int32\"]", asList("hello"));

        // map with wrong type
        assertToBytesFails("{\"foo\":\"string\"}", ImmutableMap.of("foo", 43));

        // map missing type
        assertToBytesFails("{\"bar\":\"string\"}", ImmutableMap.of("foo", 43));
    }

    public void testLargeSequences() {
        for(int size: new int[] { Short.MAX_VALUE - 1, 2 * Short.MAX_VALUE }) {
            String s = TestUtils.randomString(TestUtils.LETTERS, size);
            assertInverse("'string'", s);
            List<Byte> vals = new ArrayList<Byte>(size);
            for(int i = 0; i < size; i++)
                vals.add(Byte.valueOf((byte) 9));
            assertInverse("['int8']", vals);
        }
    }

    public void assertInvalidTypeDef(String typeDef) {
        try {
            fromJson(typeDef);
            fail("Invalid type def allowed: " + typeDef);
        } catch(SerializationException e) {
            // This is good
        }
    }

    public void assertToBytesFails(String typeDef, Object o) {
        try {
            getSerializer(typeDef).toBytes(o);
            fail("Invalid serialization allowed.");
        } catch(SerializationException e) {
            // this is good
        }
    }

    public void assertToObjectFails(String typeDef, byte[] bytes) {
        try {
            getSerializer(typeDef).toObject(bytes);
            fail("Invalid re-serialization allowed.");
        } catch(SerializationException e) {
            // this is good
        }
    }

    public Object doubleInvert(JsonTypeSerializer serializer, Object obj) {
        return serializer.toObject(serializer.toBytes(obj));
    }

    public void assertInverse(String typeDef, Object obj) {
        JsonTypeSerializer serializer = getSerializer(typeDef);
        assertEquals(obj, doubleInvert(serializer, obj));
    }

}
