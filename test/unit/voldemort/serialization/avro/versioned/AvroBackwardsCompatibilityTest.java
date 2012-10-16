package voldemort.serialization.avro.versioned;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.util.Utf8;
import org.junit.Test;

/**
 * A test that the avro serialization remains compatible with older serialized
 * data
 * 
 * 
 */
public class AvroBackwardsCompatibilityTest {

    private static byte[] writeVersion0(Schema s0) {

        GenericData.Record record = new GenericData.Record(s0);
        record.put("original", new Utf8("Abhinay"));
        AvroVersionedGenericSerializer serializer = new AvroVersionedGenericSerializer(s0.toString());
        return serializer.toBytes(record);

    }

    private static Object readVersion0(Map<Integer, String> versions, byte[] versionZeroBytes) {

        AvroVersionedGenericSerializer serializer = new AvroVersionedGenericSerializer(versions);
        return serializer.toObject(versionZeroBytes);

    }

    private static byte[] writeVersion0with1Present(Map<Integer, String> versions, Schema s0) {

        GenericData.Record record = new GenericData.Record(s0);
        record.put("original", new Utf8("Abhinay"));
        AvroVersionedGenericSerializer serializer = new AvroVersionedGenericSerializer(versions);
        return serializer.toBytes(record);

    }

    /*
     * This tests if a client tries to deserialize an object created using an
     * old schema is successful or not
     */
    @Test
    public void testAvroSchemaEvolution() throws IOException {

        String versionZero = "{\"type\": \"record\", \"name\": \"myrec\",\"fields\": [{ \"name\": \"original\", \"type\": \"string\" }]}";

        String versionOne = "{\"type\": \"record\", \"name\": \"myrec\",\"fields\": [{ \"name\": \"original\", \"type\": \"string\" } ,"
                            + "{ \"name\": \"new-field\", \"type\": \"string\", \"default\":\"\" }]}";

        Schema s0 = Schema.parse(versionZero);
        Schema s1 = Schema.parse(versionOne);

        Map<Integer, String> versions = new HashMap<Integer, String>();

        versions.put(0, versionZero);
        versions.put(1, versionOne);

        byte[] versionZeroBytes = writeVersion0(s0);

        GenericData.Record record = (Record) readVersion0(versions, versionZeroBytes);

    }

    /*
     * This tests if a client tries to serialize an object created using an old
     * schema is successful or not
     */
    @Test
    public void testAvroSchemaEvolutionWrite() throws IOException {

        String versionZero = "{\"type\": \"record\", \"name\": \"myrec\",\"fields\": [{ \"name\": \"original\", \"type\": \"string\" }]}";

        String versionOne = "{\"type\": \"record\", \"name\": \"myrec\",\"fields\": [{ \"name\": \"original\", \"type\": \"string\" } ,"
                            + "{ \"name\": \"new-field\", \"type\": \"string\", \"default\":\"\" }]}";

        Schema s0 = Schema.parse(versionZero);
        Schema s1 = Schema.parse(versionOne);

        Map<Integer, String> versions = new HashMap<Integer, String>();

        versions.put(0, versionZero);
        versions.put(1, versionOne);

        byte[] versionZeroBytes = writeVersion0with1Present(versions, s0);

    }
}
