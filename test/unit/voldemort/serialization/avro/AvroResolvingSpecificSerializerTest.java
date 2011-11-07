package voldemort.serialization.avro;

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
        Map<Integer, String> schemaInfo = new HashMap<Integer, String>();
        schemaInfo.put(1, SCHEMA);
        SerializerDefinition serializerDef = new SerializerDefinition("test",
                                                                      schemaInfo,
                                                                      true,
                                                                      null);
        Serializer<SpecificRecord> serializer = new AvroResolvingSpecificSerializer<SpecificRecord>(serializerDef);
        TestRecord obj = new TestRecord();
        obj.f1 = new Utf8("foo");
        obj.f2 = new Utf8("bar");
        obj.f3 = 42;
        byte[] bytes = serializer.toBytes(obj);
        assertEquals(bytes[0], 1);
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
        Map<Integer, String> schemaInfo1 = new HashMap<Integer, String>();
        schemaInfo1.put(1, SCHEMA1NS);
        SerializerDefinition serializerDef1 = new SerializerDefinition("test",
                                                                       schemaInfo1,
                                                                       true,
                                                                       null);
        Serializer<GenericData.Record> serializer1 = new AvroResolvingGenericSerializer<GenericData.Record>(serializerDef1);

        // Second serializer
        Map<Integer, String> schemaInfo2 = new HashMap<Integer, String>();
        schemaInfo2.put(1, SCHEMA1NS);
        schemaInfo2.put(2, SCHEMA);
        SerializerDefinition serializerDef2 = new SerializerDefinition("test",
                                                                       schemaInfo2,
                                                                       true,
                                                                       null);
        Serializer<TestRecord> serializer2 = new AvroResolvingSpecificSerializer<TestRecord>(serializerDef2);

        // Write it as the old Schema
        GenericData.Record datum = new GenericData.Record(Schema.parse(SCHEMA1NS));
        datum.put("f1", new Utf8("foo"));
        byte[] bytes = serializer1.toBytes(datum);
        // Fix the version bytes so it resolves the correct Schema
        bytes[0] = 1;

        // Read is with the new serializer
        Schema schema2 = Schema.parse(SCHEMA2NS);
        TestRecord datum1 = serializer2.toObject(bytes);
        assertTrue(datum1.f1.equals(datum.get("f1")));
        assertTrue(datum1.f2.equals(new Utf8(schema2.getField("f2").defaultValue().getTextValue())));
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
        // Fix the version byte
        bytes[0] = 1;

        // Read it back as a different Schema
        GenericData.Record datum1 = serializer2.toObject(bytes);
        assertTrue(datum1.get("f1").equals(datum.get("f1")));
        assertTrue(datum1.get("f2").equals(new Utf8(Schema.parse(SCHEMA2)
                                                          .getField("f2")
                                                          .defaultValue()
                                                          .getTextValue())));
    }
}
