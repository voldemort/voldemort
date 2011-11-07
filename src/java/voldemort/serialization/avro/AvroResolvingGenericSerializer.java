package voldemort.serialization.avro;

import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

import voldemort.serialization.SerializerDefinition;

public class AvroResolvingGenericSerializer<T> extends AvroResolvingSerializer<T> {

    public AvroResolvingGenericSerializer(SerializerDefinition serializerDef) {
        super(serializerDef);
    }

    @Override
    protected DatumWriter<T> createDatumWriter(Schema schema) {
        return new GenericDatumWriter<T>(schema);
    }

    @Override
    protected DatumReader<T> createDatumReader(Schema writerSchema, Schema readerSchema) {
        return new GenericDatumReader<T>(writerSchema, readerSchema);
    }

    @Override
    protected Map<Byte, Schema> loadSchemas(Map<Integer, String> allSchemaInfos) {
        Map<Byte, Schema> schemaVersions = new HashMap<Byte, Schema>();
        for(Map.Entry<Integer, String> entry: allSchemaInfos.entrySet()) {
            // Make sure we can parse the schema
            Schema schema = Schema.parse(entry.getValue());
            // Check that the version is less than 256
            Integer version = entry.getKey();
            if(version > Byte.MAX_VALUE) {
                throw new IllegalArgumentException("Cannot have schema version higher than "
                                                   + Byte.MAX_VALUE);
            }
            schemaVersions.put(version.byteValue(), schema);
            LOG.info("Loaded schema version (" + version + ")");
        }
        return schemaVersions;
    }

    @Override
    protected Schema getCurrentSchema(SerializerDefinition serializerDef) {
        String schemaInfo = serializerDef.getCurrentSchemaInfo();
        return Schema.parse(schemaInfo);
    }
}
