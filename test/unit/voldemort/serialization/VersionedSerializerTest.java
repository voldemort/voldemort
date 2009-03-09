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

import static voldemort.TestUtils.getClock;
import junit.framework.TestCase;
import voldemort.versioning.Versioned;

public class VersionedSerializerTest extends TestCase {

    private VersionedSerializer<String> serializer;

    @Override
    public void setUp() {
        this.serializer = new VersionedSerializer<String>(new StringSerializer("UTF-8"));
    }

    private void assertSerializes(String message, Versioned<String> obj) {
        assertEquals(obj, this.serializer.toObject(this.serializer.toBytes(obj)));
    }

    public void testSerialization() {
        // assertSerializes("Empty Versioned should equal itself.",
        // new Versioned<String>(null, getClock()));
        assertSerializes("Empty Versioned should equal itself.", new Versioned<String>("",
                                                                                       getClock()));
        assertSerializes("Normal string should equal itself.", new Versioned<String>("hello",
                                                                                     getClock()));
        assertSerializes("Normal string should equal itself.", new Versioned<String>("hello",
                                                                                     getClock(1,
                                                                                              1,
                                                                                              1,
                                                                                              2,
                                                                                              3)));
        assertSerializes("Normal string should equal itself.", new Versioned<String>("hello",
                                                                                     getClock(1)));
        assertSerializes("Normal string should equal itself.", new Versioned<String>("hello", null));
    }
}
