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

import java.util.Date;

import junit.framework.TestCase;
import voldemort.store.slop.Slop;

public class SlopSerializerTest extends TestCase {

    private SlopSerializer serializer;

    public void setUp() {
        this.serializer = new SlopSerializer();
    }

    private void assertSerializes(Slop slop) {
        assertEquals(slop, this.serializer.toObject(this.serializer.toBytes(slop)));
    }

    public void testSerialization() {
        assertSerializes(new Slop("test",
                                  Slop.Operation.DELETE,
                                  "hello".getBytes(),
                                  new byte[] { 1 },
                                  1,
                                  new Date()));
        assertSerializes(new Slop("test", Slop.Operation.GET, "hello there".getBytes(), new byte[] {
                1, 3, 23, 4, 5, 6, 6 }, 1, new Date(0)));
        assertSerializes(new Slop("test",
                                  Slop.Operation.PUT,
                                  "".getBytes(),
                                  new byte[] {},
                                  1,
                                  new Date()));
        assertSerializes(new Slop("test",
                                  Slop.Operation.PUT,
                                  "hello".getBytes(),
                                  null,
                                  1,
                                  new Date()));
    }
}
