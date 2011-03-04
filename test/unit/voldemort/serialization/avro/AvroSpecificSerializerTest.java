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

import junit.framework.TestCase;

import org.apache.avro.ipc.HandshakeRequest;
import org.apache.avro.ipc.MD5;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.util.Utf8;

import voldemort.utils.ByteUtils;

/**
 * Tests the serialization using the Avro specific approach.
 */
public class AvroSpecificSerializerTest extends TestCase {

    public void testFailWithInvalidSchemaInfo() {
        try {
            new AvroSpecificSerializer<SpecificRecord>("ruby=Map");
        } catch(Exception e) {
            return;
        }
        fail("It should have failed with invalid schema info");
    }

    // We use a generated class for the exercise.
    public void testRoundtripAvroWithHandShakeRequest() {

        String className = "java=org.apache.avro.ipc.HandshakeRequest";

        HandshakeRequest req = new HandshakeRequest();
        // set a few values to avoid NPEs
        req.clientHash = new MD5();
        req.clientProtocol = new Utf8("");
        req.serverHash = new MD5();

        AvroSpecificSerializer<HandshakeRequest> serializer = new AvroSpecificSerializer<HandshakeRequest>(className);
        byte[] bytes = serializer.toBytes(req);
        byte[] bytes2 = serializer.toBytes(req);
        assertEquals(ByteUtils.compare(bytes, bytes2), 0);
        assertTrue(serializer.toObject(bytes).equals(req));
        assertTrue(serializer.toObject(bytes2).equals(req));
    }
}
