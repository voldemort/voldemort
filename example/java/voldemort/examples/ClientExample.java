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

package voldemort.examples;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.io.parsing.Parser;
import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.StoreClientFactory;
import voldemort.serialization.DefaultSerializerFactory;
import voldemort.serialization.json.JsonReader;
import voldemort.utils.ByteArray;
import voldemort.versioning.Versioned;

import java.io.StringReader;

public class ClientExample {

    public static void main(String[] args) {
        //stringStoreExample();
        avroStoreExample();
    }

    public static void stringStoreExample() {
        System.out.println("==============String store example=================");

        // In production environment, the StoreClient instantiation should be done using factory pattern
        // through a Framework such as Spring
        String bootstrapUrl = "tcp://localhost:6666";
        StoreClientFactory factory = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(bootstrapUrl));

        StoreClient<String, String> client = factory.getStoreClient("test");

        // put initial value
        System.out.println("Putting an initial value");
        client.put("some_key", "initial value");

        // get the value
        System.out.println("Getting Initial value");
        Versioned<String> versioned = client.get("some_key");

        System.out.println("Initial Versioned Object: " + String.valueOf(versioned));
        System.out.println("           Initial Value: " + String.valueOf(versioned.getValue()));

        // modify the value
        System.out.println("Modifying the value");
        versioned.setObject("new_value");

        // update the value
        System.out.println("Putting the new value");
        client.put("some_key", versioned);

        // get again and print
        System.out.println("Getting the new value");
        versioned = client.get("some_key");
        System.out.println("Putting the value");
        System.out.println("    New Versioned Object: " + String.valueOf(versioned));
        System.out.println("               New Value: " + String.valueOf(versioned.getValue()));

    }

    public static void avroStoreExample() {
        System.out.println("==============Avro store example=================");
 
        // In production environment, the StoreClient instantiation should be done using factory pattern
        // through a Framework such as Spring
        String bootstrapUrl = "tcp://localhost:6666";
        StoreClientFactory factory = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(bootstrapUrl));

        StoreClient<GenericRecord, GenericRecord> client = factory.getStoreClient("avro-example");


        // creating initial k-v pair
        System.out.println("Creating initial Key and Value");
        String keySchemaJson = "{ \"name\": \"key\", \"type\": \"record\", \"fields\": [{ \"name\": \"user_id\", \"type\": \"int\" }] }";
        Schema keySchema = Schema.parse(keySchemaJson);
        GenericRecord key = new GenericData.Record(keySchema);
        key.put("user_id", 123);

        String valueSchemaJson = "{\n" +
                "      \"name\": \"value\",\n" +
                "      \"type\": \"record\",\n" +
                "      \"fields\": [{ \n" +
                "        \"name\": \"user_id\",\n" +
                "        \"type\": \"int\"\n" +
                "      }, {\n" +
                "        \"name\": \"gender\",\n" +
                "        \"type\": \"string\"\n" +
                "      }, {\n" +
                "        \"name\": \"age\",\n" +
                "        \"type\": \"int\",\n" +
                "        \"optional\": true\n" +
                "      }]\n" +
                "    }";
        Schema valueSchema = Schema.parse(valueSchemaJson);
        GenericRecord value = new GenericData.Record(valueSchema);

        value.put("user_id", 123);
        value.put("gender", "male");
        value.put("age", 23);

        // put initial value
        System.out.println("Putting Initial value");
        client.put(key, value);

        // get the value
        System.out.println("Getting the value");
        Versioned<GenericRecord> versioned = client.get(key);

        System.out.println("Initial Versioned Object: " + String.valueOf(versioned));
        System.out.println("           Initial Value: " + String.valueOf(versioned.getValue()));

        // modify the value
        System.out.println("Modifying the value");
        GenericRecord modifiedRecord = versioned.getValue();
        modifiedRecord.put("gender", "female");
        modifiedRecord.put("age", 55);
        versioned.setObject(modifiedRecord);

        // update the value
        System.out.println("Putting the new value");
        client.put(key, versioned);

        // get again and print
        System.out.println("Getting the new value");
        versioned = client.get(key);
        System.out.println("    New Versioned Object: " + String.valueOf(versioned));
        System.out.println("               New Value: " + String.valueOf(versioned.getValue()));
    }
}
