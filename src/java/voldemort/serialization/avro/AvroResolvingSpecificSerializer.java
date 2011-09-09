package voldemort.serialization.avro;

import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;

import voldemort.serialization.Serializer;
import voldemort.serialization.SerializerDefinition;

public class AvroResolvingSpecificSerializer<T extends SpecificRecord> implements Serializer<T> {

    private final Map<Integer, Schema> schemaVersions;

    public AvroResolvingSpecificSerializer(SerializerDefinition serializerDef) {
        Map<Integer, String> allSchemaInfos = serializerDef.getAllSchemaInfoVersions();
        schemaVersions = new HashMap<Integer, Schema>();
        for(String schemaInfo: allSchemaInfos.values()) {
            Schema schema = Schema.parse(schemaInfo);

            String fullName = schema.getFullName();
            String name = schema.getName();
            String namespace = schema.getNamespace();
            schemaVersions.put(schema.toString().hashCode(), schema);
        }
    }

    public byte[] toBytes(T object) {
        return null;
    }

    public T toObject(byte[] bytes) {
        return null;
    }

}
