package voldemort.serialization.thrift;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import junit.framework.TestCase;
import voldemort.serialization.DefaultSerializerFactory;
import voldemort.serialization.Serializer;
import voldemort.serialization.SerializerDefinition;

public class ThriftSerializerTest extends TestCase {

    public void testGetSerializer() {
        SerializerDefinition def = new SerializerDefinition("thrift", "java="
                                                                      + MockMessage.class.getName()
                                                                      + ", protocol=binary   ");
        Serializer<?> serializer = new DefaultSerializerFactory().getSerializer(def);
        assertEquals(ThriftSerializer.class, serializer.getClass());
    }

    public void testGetSerializer1() {
        SerializerDefinition def = new SerializerDefinition("thrift", "java="
                                                                      + MockMessage.class.getName()
                                                                      + ",protocol=BiNary");
        Serializer<?> serializer = new DefaultSerializerFactory().getSerializer(def);
        assertEquals(ThriftSerializer.class, serializer.getClass());
    }

    public void testGetSerializer2() {
        try {
            SerializerDefinition def = new SerializerDefinition("thrift",
                                                                "java="
                                                                        + MockMessage.class.getName());
            new DefaultSerializerFactory().getSerializer(def);
        } catch(IllegalArgumentException e) {
            return;
        }
        fail("IllegalArgumentException should have been thrown for missing Thrift protocol");
    }

    public void testGetSerializer3() {
        try {
            SerializerDefinition def = new SerializerDefinition("thrift", "protocol=json");
            new DefaultSerializerFactory().getSerializer(def);
        } catch(IllegalArgumentException e) {
            return;
        }
        fail("IllegalArgumentException should have been thrown for missing Thrift class");
    }

    public void testGetSerializer4() {
        try {
            SerializerDefinition def = new SerializerDefinition("thrift", "");
            new DefaultSerializerFactory().getSerializer(def);
        } catch(IllegalArgumentException e) {
            return;
        }
        fail("IllegalArgumentException should have been thrown for missing Thrift class and protocol");
    }

    public void testGetSerializer5() {
        try {
            SerializerDefinition def = new SerializerDefinition("thrift",
                                                                "java=com.abc.FooBar,protocol=simple-json");
            new DefaultSerializerFactory().getSerializer(def);
        } catch(IllegalArgumentException e) {
            return;
        }
        fail("IllegalArgumentException should have been thrown for non-existing Thrift class");
    }

    public void testGetSerializer6() {
        try {
            SerializerDefinition def = new SerializerDefinition("thrift",
                                                                "java="
                                                                        + MockMessage.class.getName()
                                                                        + ",protocol=bongus");
            new DefaultSerializerFactory().getSerializer(def);
        } catch(IllegalArgumentException e) {
            return;
        }
        fail("IllegalArgumentException should have been thrown for bogus Thrift protocol");
    }

    public void testGetSerializer7() {
        try {
            SerializerDefinition def = new SerializerDefinition("thrift",
                                                                "php=FooBar,protocol=bongus");
            new DefaultSerializerFactory().getSerializer(def);
        } catch(IllegalArgumentException e) {
            return;
        }
        fail("IllegalArgumentException should have been thrown for non-Java Thrift client");
    }

    public void testSerializerRoundtrip() {
        MockMessage message = new MockMessage();
        message.name = "abc";
        Map<String, Integer> strToIntMap = new HashMap<String, Integer>();
        strToIntMap.put("ad1", 1000);
        strToIntMap.put("ad2", 998080);
        message.mappings = new HashMap<Long, Map<String, Integer>>();
        message.mappings.put(10000000L, strToIntMap);
        message.intList = new ArrayList<Short>();
        message.intList.add((short) 1);
        message.intList.add((short) -99);
        message.intList.add((short) 50);
        message.strSet = new HashSet<String>();
        message.strSet.add("hello");
        message.strSet.add("world");

        ThriftSerializer<MockMessage> serializer = new ThriftSerializer<MockMessage>("java=voldemort.serialization.thrift.MockMessage,protocol=binary");
        byte[] b = serializer.toBytes(message);
        MockMessage message2 = serializer.toObject(b);

        assertEquals(message, message2);
    }

    public void testEmptyObjSeserialization() {
        MockMessage message = new MockMessage();
        ThriftSerializer<MockMessage> serializer = new ThriftSerializer<MockMessage>("java=voldemort.serialization.thrift.MockMessage, protocol=binary ");
        byte[] b = serializer.toBytes(message);
        MockMessage message2 = serializer.toObject(b);

        assertEquals(message, message2);
    }
}
