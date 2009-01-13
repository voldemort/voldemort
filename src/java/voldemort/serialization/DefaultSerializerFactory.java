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

import voldemort.serialization.json.JsonTypeDefinition;
import voldemort.serialization.json.JsonTypeSerializer;

/**
 * Factory that maps serialization strings to serializers. Used to get a
 * Serializer from config serializer description.
 * 
 * @author jay
 * 
 */
public class DefaultSerializerFactory implements SerializerFactory {

    private static final String JAVA_SERIALIZER_TYPE_NAME = "java-serialization";
    private static final String STRING_SERIALIZER_TYPE_NAME = "string";
    private static final String IDENTITY_SERIALIZER_TYPE_NAME = "identity";
    private static final String JSON_SERIALIZER_TYPE_NAME = "json";

    public Serializer<?> getSerializer(SerializerDefinition serializerDef) {
        String name = serializerDef.getName();
        if(name.equals(JAVA_SERIALIZER_TYPE_NAME)) {
            return new ObjectSerializer<Object>();
        } else if(name.equals(STRING_SERIALIZER_TYPE_NAME)) {
            return new StringSerializer(serializerDef.getCurrentSchemaInfo());
        } else if(name.equals(IDENTITY_SERIALIZER_TYPE_NAME)) {
            return new IdentitySerializer();
        } else if(name.equals(JSON_SERIALIZER_TYPE_NAME)) {
            Map<Integer, JsonTypeDefinition> versions = new HashMap<Integer, JsonTypeDefinition>();
            for(Map.Entry<Integer, String> entry: serializerDef.getAllSchemaInfoVersions()
                                                               .entrySet())
                versions.put(entry.getKey(), JsonTypeDefinition.fromJson(entry.getValue()));
            return new JsonTypeSerializer(versions);
        } else {
            throw new IllegalArgumentException("No known serializer type: "
                                               + serializerDef.getName());
        }
    }

}
