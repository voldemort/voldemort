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

import junit.framework.TestCase;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.util.Utf8;

/**
 * Tests the serialization using the Avro generic approach.
 * 
 * @author antoine
 * 
 */
public class AvroGenericSerializerTest extends TestCase {

    public void testRoundtripAvroWithEnum() throws Exception {
        String jsonSchema = "{\"name\": \"Kind\", \"type\": \"enum\", \"symbols\": [\"FOO\",\"BAR\",\"BAZ\"]}";
        AvroGenericSerializer serializer = new AvroGenericSerializer(jsonSchema);
        byte[] bytes = serializer.toBytes("BAR");
        assertTrue(serializer.toObject(bytes).equals("BAR"));
    }

    public void testRoundtripAvroWithString() throws Exception {
        String jsonSchema = "{\"name\": \"Str\", \"type\": \"string\"}";
        AvroGenericSerializer serializer = new AvroGenericSerializer(jsonSchema);
        byte[] bytes = serializer.toBytes(new Utf8("BAR"));
        assertTrue(serializer.toObject(bytes).equals(new Utf8("BAR")));
    }

    public void testRoundtripAvroWithGenericRecord() throws Exception {
        String jsonSchema = "{\"name\": \"Compact Disk\", \"type\": \"record\", "
                            + "\"fields\": ["
                            + "{\"name\": \"name\", \"type\": \"string\", \"order\": \"ascending\"}"
                            + "]}";

        AvroGenericSerializer serializer = new AvroGenericSerializer(jsonSchema);
        Record record = new Record(Schema.parse(jsonSchema));
        // we need to use a Utf8 instance to map to a String.
        record.put("name", new Utf8("Hello"));
        byte[] bytes = serializer.toBytes(record);
        assertTrue(serializer.toObject(bytes).equals(record));
    }
}
