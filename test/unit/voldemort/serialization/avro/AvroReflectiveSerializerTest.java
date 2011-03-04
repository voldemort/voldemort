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
import voldemort.utils.ByteUtils;

/**
 * Tests the serialization using the Avro reflective approach.
 */
public class AvroReflectiveSerializerTest extends TestCase {

    public static class POJO {

        private int point;
        private double distance;
        private String name;

        public void setPoint(int point) {
            this.point = point;
        }

        public void setDistance(double distance) {
            this.distance = distance;
        }

        public void setName(String name) {
            this.name = name;
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof POJO && name.equals(((POJO) obj).name)
                   && distance == ((POJO) obj).distance && point == ((POJO) obj).point;
        }
    }

    public void testFailWithInvalidSchemaInfo() {
        try {
            new AvroReflectiveSerializer<Object>("ruby=Map");
        } catch(Exception e) {
            return;
        }
        fail("It should have failed with invalid schema info");
    }

    public void testRoundtripPOJO() throws Exception {
        POJO pojo = new POJO();
        pojo.setDistance(1.2);
        pojo.setPoint(1);
        pojo.setName("name");
        AvroReflectiveSerializer<POJO> serializer = new AvroReflectiveSerializer<POJO>("java="
                                                                                       + AvroReflectiveSerializerTest.class.getCanonicalName()
                                                                                       + "$POJO");
        byte[] bytes = serializer.toBytes(pojo);
        byte[] bytes2 = serializer.toBytes(pojo);
        assertEquals(ByteUtils.compare(bytes, bytes2), 0);
        assertTrue("A roundtripping should be possible", serializer.toObject(bytes).equals(pojo));
        assertTrue("A roundtripping should be possible", serializer.toObject(bytes2).equals(pojo));
    }
}
