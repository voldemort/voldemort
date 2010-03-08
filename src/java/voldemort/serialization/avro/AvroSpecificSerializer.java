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
import org.apache.avro.specific.SpecificData;
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
 * @see http://hadoop.apache.org/avro/docs/current/api/java/org/apache/avro/generic/package-summary.html
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
        DataFileWriter<T> writer = null;
        try {
            DatumWriter<T> datumWriter = new SpecificDatumWriter<T>(clazz);

            writer = new DataFileWriter<T>(datumWriter).create(SpecificData.get().getSchema(clazz),
                                                               output);
            writer.append(object);
        } catch(IOException e) {
            throw new SerializationException(e);
        } finally {
            AvroUtils.close(writer);
        }
        return output.toByteArray();
    }

    public T toObject(byte[] bytes) {
        ByteArrayInputStream input = new ByteArrayInputStream(bytes);
        DataFileStream<T> reader = null;
        try {
            DatumReader<T> datumReader = new SpecificDatumReader<T>(clazz);
            reader = new DataFileStream<T>(input, datumReader);
            return reader.next();
        } catch(IOException e) {
            throw new SerializationException(e);
        } finally {
            AvroUtils.close(reader);
        }
    }
}
