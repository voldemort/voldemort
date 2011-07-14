/*
 * Copyright 2011 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package voldemort.serialization.avro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

import voldemort.serialization.SerializationException;
import voldemort.serialization.SerializationUtils;
import voldemort.serialization.Serializer;

/**
 * <p>
 * Avro serializer that relies on generated Java classes for schemas and
 * protocols.
 * </p>
 * 
 * <p>
 * This API is recommended for most RPC uses and for data applications that
 * always use the same datatypes, i.e., whose schemas are known at compile time.
 * For data applications that accept dynamic datatypes the generic API is
 * recommended.
 * </p>
 * 
 */
public class AvroSpecificSerializer<T extends SpecificRecord> implements Serializer<T> {

    private final Class<T> clazz;

    /**
     * Constructor accepting a Java class name under the convention
     * java=classname.
     * 
     * @param schemaInfo information on the schema for the serializer.
     */
    @SuppressWarnings("unchecked")
    public AvroSpecificSerializer(String schemaInfo) {
        try {
            clazz = (Class<T>) Class.forName(SerializationUtils.getJavaClassFromSchemaInfo(schemaInfo));
            if(!SpecificRecord.class.isAssignableFrom(clazz))
                throw new IllegalArgumentException("Class provided should implement SpecificRecord");
        } catch(ClassNotFoundException e) {
            throw new SerializationException(e);
        }
    }

    public byte[] toBytes(T object) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        Encoder encoder = new BinaryEncoder(output);
        SpecificDatumWriter<T> datumWriter = null;
        try {
            datumWriter = new SpecificDatumWriter<T>(clazz);
            datumWriter.write(object, encoder);
            encoder.flush();
        } catch(IOException e) {
            throw new SerializationException(e);
        } finally {
            SerializationUtils.close(output);
        }
        return output.toByteArray();
    }

    public T toObject(byte[] bytes) {
        Decoder decoder = DecoderFactory.defaultFactory().createBinaryDecoder(bytes, null);
        SpecificDatumReader<T> reader = null;
        try {
            reader = new SpecificDatumReader<T>(clazz);
            return reader.read(null, decoder);
        } catch(IOException e) {
            throw new SerializationException(e);
        }
    }
}
