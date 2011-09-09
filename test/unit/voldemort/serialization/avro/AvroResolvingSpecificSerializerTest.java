package voldemort.serialization.avro;

import junit.framework.TestCase;

import org.apache.avro.SchemaParseException;
import org.apache.avro.specific.SpecificRecord;

import voldemort.serialization.Serializer;
import voldemort.serialization.SerializerDefinition;

public class AvroResolvingSpecificSerializerTest extends TestCase {

    public String SCHEMA1 = "{\"type\":\"record\",\"name\":\"TestRecord\",\"namespace\":\"voldemort.serialization.avro\","
                            + "\"fields\":[{\"name\": \"foo\", \"type\":\"string\"}]}";

    public void testSingleInvalidSchemaInfo() {
        SerializerDefinition serializerDef = new SerializerDefinition("test", "null");
        try {
            new AvroResolvingSpecificSerializer<SpecificRecord>(serializerDef);
        } catch(SchemaParseException e) {
            return;
        }
        fail("Should have failed due to an invalid schema");
    }

    public void testSingleValidSchemaInfo() {
        SerializerDefinition serializerDef = new SerializerDefinition("test", SCHEMA1);
        Serializer<SpecificRecord> serializer = new AvroResolvingSpecificSerializer<SpecificRecord>(serializerDef);
    }
}
