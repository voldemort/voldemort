package voldemort.serialization.avro;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import voldemort.serialization.SerializationException;
import voldemort.serialization.Serializer;
import voldemort.serialization.SerializerDefinition;

public abstract class AvroResolvingSerializer<T> implements Serializer<T> {

    protected static final Log LOG = LogFactory.getLog(AvroResolvingSerializer.class);
    private final Map<Byte, Schema> avroSchemaVersions;
    private final Schema currentSchema;
    private Byte currentAvroSchemaVersion;
    private final DatumWriter<T> writer;
    private final Map<Byte, DatumReader<T>> readers = new HashMap<Byte, DatumReader<T>>();
    private DecoderFactory decoderFactory = new DecoderFactory();

    public AvroResolvingSerializer(SerializerDefinition serializerDef) {
        Map<Integer, String> allSchemaInfos = serializerDef.getAllSchemaInfoVersions();

        // Parse the SerializerDefinition and load up the Schemas into a map
        avroSchemaVersions = loadSchemas(allSchemaInfos);

        // Make sure the "current" schema is loaded
        currentSchema = getCurrentSchema(serializerDef);
        for(Map.Entry<Byte, Schema> entry: avroSchemaVersions.entrySet()) {
            if(entry.getValue().equals(currentSchema)) {
                currentAvroSchemaVersion = entry.getKey();
                break;
            }
        }
        if(currentAvroSchemaVersion == null) {
            throw new IllegalArgumentException("Most recent Schema is not included in the schema-info");
        }

        // Create a DatumReader for each schema and a DatumWriter for the
        // current schema
        for(Map.Entry<Byte, Schema> entry: avroSchemaVersions.entrySet()) {
            readers.put(entry.getKey(), createDatumReader(entry.getValue(), currentSchema));
        }
        writer = createDatumWriter(currentSchema);
    }

    protected abstract Schema getCurrentSchema(SerializerDefinition serializerDef);

    protected abstract Map<Byte, Schema> loadSchemas(Map<Integer, String> allSchemaInfos);

    protected abstract DatumWriter<T> createDatumWriter(Schema schema);

    protected abstract DatumReader<T> createDatumReader(Schema writerSchema, Schema readerSchema);

    public byte[] toBytes(T object) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            // Write the version as the first byte
            byte[] versionBytes = ByteBuffer.allocate(1).put(currentAvroSchemaVersion).array();
            out.write(versionBytes);
            // Write the serialized Avro object as the remaining bytes
            Encoder encoder = new BinaryEncoder(out);
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
            // First byte is the version
            Byte version = bb.get();
            if(avroSchemaVersions.containsKey(version) == false) {
                throw new SerializationException("Unknown Schema version (" + version
                                                 + ") found in serialized value");
            }
            // Read the remaining bytes, this is the serialized Avro object
            byte[] b = new byte[bb.remaining()];
            bb.get(b);

            // Read the bytes into T object
            DatumReader<T> datumReader = readers.get(version);
            Decoder decoder = decoderFactory.createBinaryDecoder(b, null);
            return datumReader.read(null, decoder);
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
    }
}
