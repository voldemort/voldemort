package voldemort.serialization.avro;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.avro.generic.GenericData;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.util.Utf8;

import voldemort.TestRecord;
import voldemort.VoldemortTestConstants;
import voldemort.serialization.Serializer;
import voldemort.serialization.SerializerDefinition;
import voldemort.utils.ByteUtils;

public class AvroResolvingSpecificSerializerTest extends TestCase {

    String SCHEMA1 = VoldemortTestConstants.getTestSpecificRecordSchema1();
    String SCHEMA2 = VoldemortTestConstants.getTestSpecificRecordSchema2();
    String SCHEMA1NS = VoldemortTestConstants.getTestSpecificRecordSchemaWithNamespace1();
    String SCHEMA2NS = VoldemortTestConstants.getTestSpecificRecordSchemaWithNamespace2();
    String SCHEMA = TestRecord.SCHEMA$.toString();

    public void testSchemaHashCode() {
        // Check various things that affect hashCode
        // Base case
        assertEquals(Schema.parse(SCHEMA1).hashCode(), Schema.parse(SCHEMA1).hashCode());
        // Check docs
        Schema s1 = Schema.parse("{\"type\":\"record\",\"name\":\"TestRecord\",\"fields\":[{\"name\":\"f1\",\"type\":\"string\"}]}");
        Schema s2 = Schema.parse("{\"type\":\"record\",\"name\":\"TestRecord\",\"fields\":[{\"name\":\"f1\",\"doc\":\"field docs\",\"type\":\"string\"}]}");
        assertEquals(s1.hashCode(), s2.hashCode());
        // Check field order
        s1 = Schema.parse("{\"type\":\"record\",\"name\":\"TestRecord\",\"fields\":[{\"name\":\"f1\",\"type\":\"string\"},{\"name\":\"f2\",\"type\":\"string\"}]}");
        s2 = Schema.parse("{\"type\":\"record\",\"name\":\"TestRecord\",\"fields\":[{\"name\":\"f2\",\"type\":\"string\"},{\"name\":\"f1\",\"type\":\"string\"}]}");
        assertNotSame(s1.hashCode(), s2.hashCode());
        // Check namespace
        assertNotSame(Schema.parse(SCHEMA1).hashCode(), Schema.parse(SCHEMA1NS).hashCode());
        // Different fields
        assertNotSame(Schema.parse(SCHEMA1).hashCode(), Schema.parse(SCHEMA2).hashCode());
        // Field types
        s1 = Schema.parse("{\"type\":\"record\",\"name\":\"TestRecord\",\"fields\":[{\"name\":\"f1\",\"type\":\"string\"}]}");
        s2 = Schema.parse("{\"type\":\"record\",\"name\":\"TestRecord\",\"fields\":[{\"name\":\"f1\",\"type\":\"int\"}]}");
        assertNotSame(s1.hashCode(), s2.hashCode());
    }

    public void testSingleInvalidSchemaInfo() {
        SerializerDefinition serializerDef = new SerializerDefinition("test", "null");
        try {
            new AvroResolvingSpecificSerializer<SpecificRecord>(serializerDef);
        } catch(SchemaParseException e) {
            return;
        }
        fail("Should have failed due to an invalid schema");
    }

    public void testOneSpecificSchemaInfo() {
        SerializerDefinition serializerDef = new SerializerDefinition("test", SCHEMA);
        Serializer<SpecificRecord> serializer = new AvroResolvingSpecificSerializer<SpecificRecord>(serializerDef);
        TestRecord obj = new TestRecord();
        obj.f1 = new Utf8("foo");
        obj.f2 = new Utf8("bar");
        obj.f3 = 42;
        byte[] bytes1 = serializer.toBytes(obj);
        byte[] bytes2 = serializer.toBytes(obj);
        assertEquals(ByteUtils.compare(bytes1, bytes2), 0);
        assertTrue(obj.equals(serializer.toObject(bytes1)));
        assertTrue(obj.equals(serializer.toObject(bytes2)));
    }

    public void testManySpecificSchemaInfo() {
        Map<Integer, String> schemaInfo = new HashMap<Integer, String>();
        schemaInfo.put(1, SCHEMA1NS);
        schemaInfo.put(2, SCHEMA2NS);
        schemaInfo.put(3, SCHEMA);
        SerializerDefinition serializerDef = new SerializerDefinition("test",
                                                                      schemaInfo,
                                                                      true,
                                                                      null);
        Serializer<TestRecord> serializer = new AvroResolvingSpecificSerializer<TestRecord>(serializerDef);
        TestRecord obj = new TestRecord();
        obj.f1 = new Utf8("foo");
        obj.f2 = new Utf8("bar");
        obj.f3 = 42;
        byte[] bytes1 = serializer.toBytes(obj);
        byte[] bytes2 = serializer.toBytes(obj);
        assertEquals(ByteUtils.compare(bytes1, bytes2), 0);
        assertTrue(obj.equals(serializer.toObject(bytes1)));
        assertTrue(obj.equals(serializer.toObject(bytes2)));
    }

    public void testVersionBytes() {
        SerializerDefinition serializerDef = new SerializerDefinition("test", SCHEMA);
        Serializer<SpecificRecord> serializer = new AvroResolvingSpecificSerializer<SpecificRecord>(serializerDef);
        TestRecord obj = new TestRecord();
        obj.f1 = new Utf8("foo");
        obj.f2 = new Utf8("bar");
        obj.f3 = 42;
        byte[] bytes = serializer.toBytes(obj);
        ByteBuffer bb = ByteBuffer.wrap(bytes, 0, 4);
        assertTrue(((Integer) bb.getInt()).equals(AvroResolvingSerializer.getSchemaVersion(SCHEMA)));
    }

    public void testMissingSchema() {
        SerializerDefinition serializerDef = new SerializerDefinition("test", SCHEMA);
        Serializer<SpecificRecord> serializer = new AvroResolvingSpecificSerializer<SpecificRecord>(serializerDef);
        TestRecord obj = new TestRecord();
        obj.f1 = new Utf8("foo");
        obj.f2 = new Utf8("bar");
        obj.f3 = 42;
        byte[] bytes = serializer.toBytes(obj);
        // Change the version bytes to something bogus
        bytes[0] ^= (byte) 0xFF;
        try {
            serializer.toObject(bytes);

        } catch(Exception e) {
            return;
        }
        fail("Should have failed due to a missing Schema");
    }

    public void testMigrateSpecificDatum() {
        // First serializer
        SerializerDefinition serializerDef1 = new SerializerDefinition("test", SCHEMA1);
        Serializer<GenericData.Record> serializer1 = new AvroResolvingGenericSerializer<GenericData.Record>(serializerDef1);

        // Second serializer
        Map<Integer, String> schemaInfo = new HashMap<Integer, String>();
        schemaInfo.put(1, SCHEMA1NS);
        schemaInfo.put(2, SCHEMA2NS);
        schemaInfo.put(3, SCHEMA);
        SerializerDefinition serializerDef = new SerializerDefinition("test",
                                                                      schemaInfo,
                                                                      true,
                                                                      null);
        Serializer<TestRecord> serializer = new AvroResolvingSpecificSerializer<TestRecord>(serializerDef);
        GenericData.Record datum = new GenericData.Record(Schema.parse(SCHEMA1));
        datum.put("f1", new Utf8("foo"));

        // Write it as the current Schema
        byte[] bytes = serializer1.toBytes(datum);
        // Fix the version bytes so it resolves the correct Schema
        byte[] versionBytes = ByteBuffer.allocate(4)
                                        .putInt(AvroResolvingSerializer.getSchemaVersion(SCHEMA1NS))
                                        .array();
        bytes[0] = versionBytes[0];
        bytes[1] = versionBytes[1];
        bytes[2] = versionBytes[2];
        bytes[3] = versionBytes[3];

        TestRecord datum1 = serializer.toObject(bytes);
        assertTrue(datum1.f1.equals(datum.get("f1")));
        assertTrue(datum1.f2.equals(new Utf8(Schema.parse(SCHEMA)
                                                   .getField("f2")
                                                   .defaultValue()
                                                   .getTextValue())));
        assertEquals(datum1.f3, Schema.parse(SCHEMA).getField("f3").defaultValue().getIntValue());
    }

    public void testMigrateGenericDatum() {
        // First serializer
        SerializerDefinition serializerDef1 = new SerializerDefinition("test", SCHEMA1);
        Serializer<GenericData.Record> serializer1 = new AvroResolvingGenericSerializer<GenericData.Record>(serializerDef1);

        // Second serializer
        Map<Integer, String> schemaInfo2 = new HashMap<Integer, String>();
        schemaInfo2.put(1, SCHEMA1);
        schemaInfo2.put(2, SCHEMA2);
        SerializerDefinition serializerDef2 = new SerializerDefinition("test",
                                                                       schemaInfo2,
                                                                       true,
                                                                       null);
        Serializer<GenericData.Record> serializer2 = new AvroResolvingGenericSerializer<GenericData.Record>(serializerDef2);

        GenericData.Record datum = new GenericData.Record(Schema.parse(SCHEMA1));
        datum.put("f1", new Utf8("foo"));

        // Write it as the current Schema
        byte[] bytes = serializer1.toBytes(datum);

        // Read it back as a different Schema
        GenericData.Record datum1 = serializer2.toObject(bytes);
        assertTrue(datum1.get("f1").equals(datum.get("f1")));
        assertTrue(datum1.get("f2").equals(new Utf8(Schema.parse(SCHEMA2)
                                                          .getField("f2")
                                                          .defaultValue()
                                                          .getTextValue())));
    }
}
