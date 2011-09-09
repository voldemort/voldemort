package voldemort.serialization.avro;

import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

import voldemort.serialization.SerializationException;
import voldemort.serialization.SerializerDefinition;

public class AvroResolvingSpecificSerializer<T extends SpecificRecord> extends
        AvroResolvingSerializer<T> {

    public AvroResolvingSpecificSerializer(SerializerDefinition serializerDef) {
        super(serializerDef);
    }

    @Override
    protected DatumWriter<T> createDatumWriter(Schema schema) {
        return new SpecificDatumWriter<T>(schema);
    }

    @Override
    protected DatumReader<T> createDatumReader(Schema writerSchema, Schema readerSchema) {
        return new SpecificDatumReader<T>(writerSchema, readerSchema);
    }

    @Override
    protected Map<Integer, Schema> loadSchemas(Map<Integer, String> allSchemaInfos) {
        Map<Integer, Schema> schemaVersions = new HashMap<Integer, Schema>();
        String fullName = null;
        for(String schemaInfo: allSchemaInfos.values()) {
            Schema schema = Schema.parse(schemaInfo);
            // Make sure each version of the Schema is for the same class name
            if(fullName == null) {
                fullName = schema.getFullName();
            } else {
                if(schema.getFullName().equals(fullName) == false) {
                    throw new IllegalArgumentException("Avro schema must all reference the same class");
                }
            }
            // Make sure the Schema is a Record
            if(schema.getType() != Schema.Type.RECORD) {
                throw new IllegalArgumentException("Avro schema must be a \"record\" type schema");
            }
            schemaVersions.put(getSchemaVersion(schema), schema);
        }
        return schemaVersions;
    }

    @Override
    protected Integer getCurrentSchemaVersion(SerializerDefinition serializerDef) {
        // Make sure we can instantiate the class, and that it extends
        // SpecificRecord
        try {
            String schemaInfo = serializerDef.getCurrentSchemaInfo();
            Schema schema = Schema.parse(schemaInfo);
            String fullName = schema.getFullName();
            Class<T> clazz = (Class<T>) Class.forName(fullName);
            if(!SpecificRecord.class.isAssignableFrom(clazz))
                throw new IllegalArgumentException("Class provided should implement SpecificRecord");
            T inst = clazz.newInstance();
            return getSchemaVersion(inst.getSchema());
        } catch(ClassNotFoundException e) {
            throw new SerializationException(e);
        } catch(IllegalAccessException e) {
            throw new SerializationException(e);
        } catch(InstantiationException e) {
            throw new SerializationException(e);
        }
    }
}
