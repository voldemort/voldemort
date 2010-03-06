/*
 * Copyright 2010 Antoine Toulme
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectData;
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
 * @see http://hadoop.apache.org/avro/docs/current/api/java/org/apache/avro/reflect/package-summary.html
 */
public class AvroReflectiveSerializer implements Serializer<Object> {

    private final Class clazz;

    /**
     * Constructor accepting a Java class name under the convention
     * java=classname.
     * 
     * @param schemaInfo information on the schema for the serializer.
     */
    public AvroReflectiveSerializer(String schemaInfo) {
        try {
            clazz = Class.forName(SerializationUtils.getJavaClassFromSchemaInfo(schemaInfo));
        } catch(ClassNotFoundException e) {
            throw new SerializationException(e);
        }
    }

    public byte[] toBytes(Object object) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        DataFileWriter<Object> writer = null;
        try {
            DatumWriter<Object> datumWriter = new ReflectDatumWriter<Object>(clazz);

            writer = new DataFileWriter<Object>(datumWriter).create(ReflectData.get()
                                                                               .getSchema(clazz),
                                                                    output);
            writer.append(object);
            writer.flush();
            return output.toByteArray();
        } catch(IOException e) {
            throw new SerializationException(e);
        } finally {
            if(writer != null) {
                try {
                    writer.close();
                } catch(IOException e) {}
            }
        }
    }

    public Object toObject(byte[] bytes) {
        ByteArrayInputStream input = new ByteArrayInputStream(bytes);
        DataFileStream<Object> reader = null;
        try {
            DatumReader<Object> datumReader = new ReflectDatumReader(clazz);
            reader = new DataFileStream<Object>(input, datumReader);
            return reader.next();
        } catch(IOException e) {
            throw new SerializationException(e);
        } finally {
            if(reader != null) {
                try {
                    reader.close();
                } catch(IOException e) {}
            }
        }
    }
}
