package voldemort.serialization.avro.versioned;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.util.Utf8;

/**
 * A test that the json serialization remains compatible with older serialized
 * data
 * 
 * 
 */
public class AvroBackwardsCompatibilityTest extends TestCase {

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

    public static void testAvroSchemaEvolution() throws IOException {

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
}
