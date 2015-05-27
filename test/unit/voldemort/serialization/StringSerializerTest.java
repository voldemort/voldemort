package voldemort.serialization;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.apache.avro.util.Utf8;
import org.junit.Test;


public class StringSerializerTest {

    String schemaInfo;

    public StringSerializerTest() {

    }

    public Serializer<Object> getStringSerializer(String schemaInfo) {
        SerializerDefinition def;
        if(schemaInfo != null) {
            def = new SerializerDefinition(DefaultSerializerFactory.STRING_SERIALIZER_TYPE_NAME,
                                                            schemaInfo);
        } else {
            def = new SerializerDefinition(DefaultSerializerFactory.STRING_SERIALIZER_TYPE_NAME);
        }

        return (Serializer<Object>) new DefaultSerializerFactory().getSerializer(def);
    }

    @Test
    public void testStringSerializer() {
        String[] schemaInfos = { "utf8", "utf-8", "UTF8", "UTF-8", null };
        for(String schemaInfo: schemaInfos) {
            Serializer<Object> ser = getStringSerializer(schemaInfo);
            assertInverse(ser, "abc");
            assertInverse(ser, null);
            assertSerializationFails(ser, new Utf8("foobar"));
            assertSerializationFails(ser, 'C');
            assertSerializationFails(ser, 123);
            assertSerializationFails(ser, new Integer(123));
            assertSerializationFails(ser, new byte[5]);
        }
    }

    private void assertSerializationFails(Serializer<Object> ser, Object s) {
        try {
            ser.toBytes(s);
            fail("Serialization should have failed");
        } catch(Exception e) {

        }
    }

    private void assertInverse(Serializer<Object> ser, Object s) {
        Object retunValue = ser.toObject(ser.toBytes(s));
        assertEquals("serialized and deserialized value is different", s, retunValue);
    }
}
