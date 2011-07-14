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
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;

import voldemort.serialization.SerializationException;
import voldemort.serialization.SerializationUtils;
import voldemort.serialization.Serializer;

/**
 * <p>
 * Avro serializer that uses Java reflection to generate schemas and protocols
 * for existing classes.
 * </p>
 * 
 * <p>
 * This API is not recommended except as a stepping stone for systems that
 * currently uses Java interfaces to define RPC protocols. For new RPC systems,
 * the Avro specific API is preferred. For systems that process dynamic data,
 * the Avro generic API is probably best.
 * </p>
 * 
 * <p>
 * Reflection is supported from either the class, the schema or both.
 * <strong>For now we only support the class case.</strong>
 * </p>
 * 
 */
public class AvroReflectiveSerializer<T> implements Serializer<T> {

    private final Class<T> clazz;

    /**
     * Constructor accepting a Java class name under the convention
     * java=classname.
     * 
     * @param schemaInfo information on the schema for the serializer
     */
    @SuppressWarnings("unchecked")
    public AvroReflectiveSerializer(String schemaInfo) {
        try {
            clazz = (Class<T>) Class.forName(SerializationUtils.getJavaClassFromSchemaInfo(schemaInfo));
        } catch(ClassNotFoundException e) {
            throw new SerializationException(e);
        }
    }

    public byte[] toBytes(T object) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        Encoder encoder = new BinaryEncoder(output);
        ReflectDatumWriter<T> datumWriter = null;
        try {
            datumWriter = new ReflectDatumWriter<T>(clazz);
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
        ReflectDatumReader<T> reader = null;
        try {
            reader = new ReflectDatumReader<T>(clazz);
            return reader.read(null, decoder);
        } catch(IOException e) {
            throw new SerializationException(e);
        }
    }
}
