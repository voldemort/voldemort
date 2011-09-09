package voldemort.serialization.avro;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;

import voldemort.serialization.SerializationException;
import voldemort.serialization.Serializer;
import voldemort.serialization.SerializerDefinition;

public abstract class AvroResolvingSerializer<T> implements Serializer<T> {

    private final Map<Integer, Schema> schemaVersions;
    private final Schema currentSchema;
    private final Integer currentVersion;
    private final DatumWriter<T> writer;
    private final Map<Integer, DatumReader<T>> readers = new HashMap<Integer, DatumReader<T>>();
    private DecoderFactory decoderFactory = new DecoderFactory();

    public AvroResolvingSerializer(SerializerDefinition serializerDef) {
        Map<Integer, String> allSchemaInfos = serializerDef.getAllSchemaInfoVersions();

        // Parse the SerializerDefinition and load up the Schemas into a map
        schemaVersions = loadSchemas(allSchemaInfos);
        currentVersion = getCurrentSchemaVersion(serializerDef);
        currentSchema = schemaVersions.get(currentVersion);
        if(currentSchema == null) {
            throw new IllegalArgumentException("Most recent Schema is not included in the schema-info");
        }

        // Create a DatumReader and DatumWriter for each Schema
        for(Map.Entry<Integer, Schema> entry: schemaVersions.entrySet()) {
            readers.put(entry.getKey(), createDatumReader(entry.getValue(), currentSchema));
        }
        writer = createDatumWriter(currentSchema);
    }

    protected abstract Integer getCurrentSchemaVersion(SerializerDefinition serializerDef);

    protected abstract Map<Integer, Schema> loadSchemas(Map<Integer, String> allSchemaInfos);

    protected abstract DatumWriter<T> createDatumWriter(Schema schema);

    protected abstract DatumReader<T> createDatumReader(Schema writerSchema, Schema readerSchema);

    public byte[] toBytes(T object) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            // Write the version as the first 4 bytes
            byte[] versionBytes = ByteBuffer.allocate(4).putInt(currentVersion).array();
            out.write(versionBytes);
            // Write the serialized Avro object as the remaining bytes
            BinaryEncoder encoder = new BinaryEncoder(out);
            writer.write(object, encoder);
            encoder.flush();
            out.close();
            // Convert to byte[] and return
            return out.toByteArray();
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
    }

    public T toObject(byte[] bytes) {
        try {
            ByteBuffer bb = ByteBuffer.wrap(bytes);
            // First 4 bytes are the version
            Integer version = bb.getInt();
            if(schemaVersions.containsKey(version) == false) {
                throw new SerializationException("Unknown Schema version found in serialized value");
                // TODO: what to do here?
            }
            // Read the remaining bytes, this is the serialized Avro object
            byte[] b = new byte[bb.remaining()];
            bb.get(b);

            // Read the bytes into T object
            DatumReader<T> datumReader = readers.get(version);
            BinaryDecoder decoder = decoderFactory.createBinaryDecoder(b, null);
            return datumReader.read(null, decoder);
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Integer getSchemaVersion(Schema schema) {
        return schema.toString().hashCode();
    }

    public static Integer getSchemaVersion(String schemaAsString) {
        return getSchemaVersion(Schema.parse(schemaAsString));
    }

}
