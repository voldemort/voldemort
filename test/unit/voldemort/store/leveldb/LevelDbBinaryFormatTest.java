/*
 * Copyright 2008-2012 LinkedIn, Inc
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

package voldemort.store.leveldb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import junit.framework.TestCase;
import voldemort.versioning.ClockEntry;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

public class LevelDbBinaryFormatTest extends TestCase {

    @SuppressWarnings("unchecked")
    public void testSerializeAndDeserialize() {
        // empty list
        assertSerializes(new ArrayList<Versioned<byte[]>>());

        // single value
        assertSerializes(Arrays.asList(Versioned.value("test".getBytes())));

        // complex vector clock
        assertSerializes(Arrays.asList(Versioned.value("test".getBytes(),
                                                       new VectorClock(Arrays.asList(new ClockEntry((short) 12,
                                                                                                    6455),
                                                                                     new ClockEntry((short) 32,
                                                                                                    5645)),
                                                                       923874463))));

        // multiple values
        assertSerializes(Arrays.asList(Versioned.value("test".getBytes()),
                                       Versioned.value("test2".getBytes()),
                                       Versioned.value("test3".getBytes()),
                                       Versioned.value("test4".getBytes())));
    }

    private void assertSerializes(List<Versioned<byte[]>> vals) {
        assertEquals("Value does not serialize and deserialize back to itself.",
                     vals,
                     LevelDbBinaryFormat.fromByteArray(LevelDbBinaryFormat.toByteArray(vals)));
    }

}
