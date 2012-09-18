/*
 * Copyright 2008-2009 LinkedIn, Inc
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

package voldemort.serialization;

import java.util.HashMap;
import java.util.Map;

import org.apache.avro.specific.SpecificRecord;
import org.apache.thrift.TBase;

import voldemort.serialization.avro.AvroGenericSerializer;
import voldemort.serialization.avro.AvroReflectiveSerializer;
import voldemort.serialization.avro.AvroSpecificSerializer;
import voldemort.serialization.avro.versioned.AvroVersionedGenericSerializer;
import voldemort.serialization.json.JsonTypeDefinition;
import voldemort.serialization.json.JsonTypeSerializer;
import voldemort.serialization.protobuf.ProtoBufSerializer;
import voldemort.serialization.thrift.ThriftSerializer;

import com.google.protobuf.Message;

/**
 * Factory that maps serialization strings to serializers. Used to get a
 * Serializer from config serializer description.
 * 
 * 
 */
public class DefaultSerializerFactory implements SerializerFactory {

    private static final String JAVA_SERIALIZER_TYPE_NAME = "java-serialization";
    private static final String STRING_SERIALIZER_TYPE_NAME = "string";
    private static final String IDENTITY_SERIALIZER_TYPE_NAME = "identity";
    private static final String JSON_SERIALIZER_TYPE_NAME = "json";
    private static final String PROTO_BUF_TYPE_NAME = "protobuf";
    private static final String THRIFT_TYPE_NAME = "thrift";
    private static final String AVRO_GENERIC_TYPE_NAME = "avro-generic";
    private static final String AVRO_SPECIFIC_TYPE_NAME = "avro-specific";
    private static final String AVRO_REFLECTIVE_TYPE_NAME = "avro-reflective";

    // New serialization types for avro versioning support
    // We cannot change existing serializer classes since
    // this will break existing clients while looking for the version byte

    private static final String AVRO_GENERIC_VERSIONED_TYPE_NAME = "avro-generic-versioned";

    public Serializer<?> getSerializer(SerializerDefinition serializerDef) {
        String name = serializerDef.getName();
        if(name.equals(JAVA_SERIALIZER_TYPE_NAME)) {
            return new ObjectSerializer<Object>();
        } else if(name.equals(STRING_SERIALIZER_TYPE_NAME)) {
            return new StringSerializer(serializerDef.hasSchemaInfo() ? serializerDef.getCurrentSchemaInfo()
                                                                     : "UTF8");
        } else if(name.equals(IDENTITY_SERIALIZER_TYPE_NAME)) {
            return new IdentitySerializer();
        } else if(name.equals(JSON_SERIALIZER_TYPE_NAME)) {
            if(serializerDef.hasVersion()) {
                Map<Integer, JsonTypeDefinition> versions = new HashMap<Integer, JsonTypeDefinition>();
                for(Map.Entry<Integer, String> entry: serializerDef.getAllSchemaInfoVersions()
                                                                   .entrySet())
                    versions.put(entry.getKey(), JsonTypeDefinition.fromJson(entry.getValue()));
                return new JsonTypeSerializer(versions);
            } else {
                return new JsonTypeSerializer(JsonTypeDefinition.fromJson(serializerDef.getCurrentSchemaInfo()));
            }
        } else if(name.equals(PROTO_BUF_TYPE_NAME)) {
            return new ProtoBufSerializer<Message>(serializerDef.getCurrentSchemaInfo());
        } else if(name.equals(THRIFT_TYPE_NAME)) {
            return new ThriftSerializer<TBase<?, ?>>(serializerDef.getCurrentSchemaInfo());
        } else if(name.equals(AVRO_GENERIC_TYPE_NAME)) {
            return new AvroGenericSerializer(serializerDef.getCurrentSchemaInfo());
        } else if(name.equals(AVRO_SPECIFIC_TYPE_NAME)) {
            return new AvroSpecificSerializer<SpecificRecord>(serializerDef.getCurrentSchemaInfo());
        } else if(name.equals(AVRO_REFLECTIVE_TYPE_NAME)) {
            return new AvroReflectiveSerializer<Object>(serializerDef.getCurrentSchemaInfo());
        } else if(name.equals(AVRO_GENERIC_VERSIONED_TYPE_NAME)) {
            if(serializerDef.hasVersion()) {
                Map<Integer, String> versions = new HashMap<Integer, String>();
                for(Map.Entry<Integer, String> entry: serializerDef.getAllSchemaInfoVersions()
                                                                   .entrySet())
                    versions.put(entry.getKey(), entry.getValue());
                return new AvroVersionedGenericSerializer(versions);
            } else {
                return new AvroVersionedGenericSerializer(serializerDef.getCurrentSchemaInfo());
            }

        } else {
            throw new IllegalArgumentException("No known serializer type: "
                                               + serializerDef.getName());
        }
    }
}
