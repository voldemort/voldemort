/*
 * Copyright 2015 LinkedIn, Inc
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

package voldemort.serialization.avro.versioned;


import static org.junit.Assert.assertArrayEquals;
import junit.framework.TestCase;

import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.util.Utf8;

/**
 * Tests the serialization using the Avro generic approach.
 */
public class AvroVersionedGenericSerializertest extends TestCase {

    public void testSimpleStringSchema() throws Exception {
        String jsonSchema = "\"string\"";
        AvroVersionedGenericSerializer serializer = new AvroVersionedGenericSerializer(jsonSchema);

        String jsonSchema2 = "{\"name\": \"Str\", \"type\": \"string\"}";
        AvroVersionedGenericSerializer serializer2 = new AvroVersionedGenericSerializer(jsonSchema2);

        Utf8 sample = new Utf8("abc");

        byte[] byte1 = serializer.toBytes(sample);
        byte[] byte2 = serializer.toBytes("abc");
        assertArrayEquals(" should serialize to same value", byte1, byte2);

        byte[] byte3 = serializer2.toBytes("abc");
        assertArrayEquals(" should serialize to same value", byte1, byte3);

        // Test deserialization.
        assertEquals("Expected Utf8 Class", Utf8.class, serializer.toObject(byte1).getClass());
        assertEquals("Expected Utf8 Class", Utf8.class, serializer2.toObject(byte1).getClass());

        assertEquals("Value got after serializing and deserailizing is not the same",
                     sample,
                     serializer.toObject(byte1));

        assertEquals("Value got after serializing and deserailizing is not the same",
                     sample,
                     serializer2.toObject(byte3));
    }

    private void assertInverse(AvroVersionedGenericSerializer serializer, Object O) {
        assertEquals("Value after ser/de-ser is not the same",
                     new Utf8(O.toString()),
                     serializer.toObject(serializer.toBytes(O)));
    }

    public void testNull() {
        String jsonSchema = "\"string\"";
        AvroVersionedGenericSerializer serializer = new AvroVersionedGenericSerializer(jsonSchema);

        try {
            assertEquals("Null should be handled correctly",
                     null,
                     serializer.toObject(serializer.toBytes(null)));
            fail("null serialization should have failed");
        } catch(NullPointerException e) {

        }
    }

    public void testStringOtherTypes() {
        String jsonSchema = "\"string\"";
        AvroVersionedGenericSerializer serializer = new AvroVersionedGenericSerializer(jsonSchema);

        assertInverse(serializer, 123);
        assertInverse(serializer, String.class);
        assertInverse(serializer, 'C');
    }

    public void testRecordStringSchema() {
        String stringSchema = "\"string\"";
        AvroVersionedGenericSerializer stringSerializer = new AvroVersionedGenericSerializer(stringSchema);

        // string and a record with string serializes to the same bytes.
        String recordSchema = "{\"type\": \"record\", \"name\": \"myrec\",\"fields\": [{ \"name\": \"original\", \"type\": \"string\" }]}";
        AvroVersionedGenericSerializer recordSerializer = new AvroVersionedGenericSerializer(recordSchema);

        Utf8 sample = new Utf8("abc");

        byte[] byte1 = stringSerializer.toBytes(sample);
        Object obj = recordSerializer.toObject(byte1);

        assertEquals(" should serialize to same value", Record.class, obj.getClass());

    }

}
