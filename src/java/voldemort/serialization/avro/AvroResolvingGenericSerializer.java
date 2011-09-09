package voldemort.serialization.avro;

import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

import voldemort.serialization.SerializerDefinition;

public class AvroResolvingGenericSerializer<T extends GenericData.Record> extends
        AvroResolvingSerializer<T> {

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
    protected Map<Integer, Schema> loadSchemas(Map<Integer, String> allSchemaInfos) {
        Map<Integer, Schema> schemaVersions = new HashMap<Integer, Schema>();
        for(String schemaInfo: allSchemaInfos.values()) {
            Schema schema = Schema.parse(schemaInfo);
            schemaVersions.put(getSchemaVersion(schema), schema);
        }
        return schemaVersions;
    }

    @Override
    protected Integer getCurrentSchemaVersion(SerializerDefinition serializerDef) {
        String schemaInfo = serializerDef.getCurrentSchemaInfo();
        return getSchemaVersion(schemaInfo);
    }
}
