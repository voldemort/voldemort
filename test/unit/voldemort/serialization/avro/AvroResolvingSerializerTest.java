package voldemort.serialization.avro;

import java.io.StringReader;
import java.util.HashMap;
import java.util.List;
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
import voldemort.store.StoreDefinition;
import voldemort.utils.ByteUtils;
import voldemort.xml.StoreDefinitionsMapper;

public class AvroResolvingSerializerTest extends TestCase {

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

    public void testStoresXML() {
        StoreDefinitionsMapper mapper = new StoreDefinitionsMapper();
        List<StoreDefinition> storeDefs = mapper.readStoreList(new StringReader(VoldemortTestConstants.getAvroStoresXml()));
        StoreDefinition storeDef = storeDefs.get(0);
        SerializerDefinition serializerDef = storeDef.getValueSerializer();

        // Create a serializer with version=1
        Map<Integer, String> schemaInfo = new HashMap<Integer, String>();
        schemaInfo.put(1, serializerDef.getSchemaInfo(1));
        SerializerDefinition newSerializerDef = new SerializerDefinition("test",
                                                                         schemaInfo,
                                                                         true,
                                                                         null);

        Serializer<GenericData.Record> serializer = new AvroResolvingGenericSerializer<GenericData.Record>(newSerializerDef);
        GenericData.Record datum = new GenericData.Record(Schema.parse(newSerializerDef.getCurrentSchemaInfo()));
        datum.put("f1", new Utf8("foo"));
        byte[] bytes = serializer.toBytes(datum);

        // Create a serializer with all versions
        serializer = new AvroResolvingGenericSerializer<GenericData.Record>(serializerDef);
        GenericData.Record datum1 = serializer.toObject(bytes);
        assertTrue(datum1.get("f1").equals(datum.get("f1")));
        assertTrue(datum1.get("f2")
                         .equals(new Utf8(Schema.parse(serializerDef.getCurrentSchemaInfo())
                                                .getField("f2")
                                                .defaultValue()
                                                .getTextValue())));
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
        Byte versionByte = bytes[0];
        assertEquals((Integer) (versionByte.intValue()),
                     (Integer) serializerDef.getCurrentSchemaVersion());
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
        bytes[0] = ((Integer) 0xFF).byteValue();
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
        bytes[0] = ((Integer) 0x01).byteValue();

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
        schemaInfo2.put(0, SCHEMA1);
        schemaInfo2.put(1, SCHEMA2);
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
